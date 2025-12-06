import asyncio
import base64
import binascii
import io
import threading
import time
import os
from contextlib import contextmanager
from typing import Any, Dict

from flask import Flask, request, jsonify, send_from_directory
from telethon.errors import (
    AuthKeyUnregisteredError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.sync import TelegramClient

app = Flask(__name__)

stop_flag = False
worker = None
status_lock = threading.Lock()
session_lock = threading.Lock()
auth_cache: Dict[str, Dict[str, Any]] = {}
status_state = {
    "active": False,
    "mode": None,
    "started_at": None,
    "last_sent_at": None,
    "sent_count": 0,
    "chat_id": None,
    "interval_seconds": None,
    "content_type": None,
    "user": None,
}
SESSION_STRING_PATH = "session_string.txt"
saved_session_string = None


def ensure_event_loop():
    """Ensure the current thread has an asyncio event loop Telethon can use."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def load_saved_session():
    global saved_session_string
    if saved_session_string:
        return saved_session_string
    if os.path.exists(SESSION_STRING_PATH):
        try:
            with open(SESSION_STRING_PATH, "r", encoding="utf-8") as fh:
                saved_session_string = fh.read().strip() or None
        except OSError:
            saved_session_string = None
    return saved_session_string


def persist_session(session_string: str):
    global saved_session_string
    saved_session_string = session_string
    try:
        with open(SESSION_STRING_PATH, "w", encoding="utf-8") as fh:
            fh.write(session_string)
    except OSError:
        pass


def resolve_target(client: TelegramClient, chat_id: str) -> Any:
    """Resolve chat_id/username/link to a Telethon-friendly target."""
    text = str(chat_id).strip()
    # Channel/supergroup numeric ids need -100 prefix intact.
    if text.startswith("-100"):
        return int(text)
    # Usernames/links work as-is.
    if text.startswith("@") or "t.me/" in text:
        return text
    # Try resolving digits as entity (needs access hash).
    if text.isdigit():
        return client.get_entity(int(text))
    return client.get_entity(text)


def decode_base64_image(image_base64: str) -> bytes:
    """Decode a data URL or plain base64 string into bytes."""
    data = image_base64.strip()
    if data.lower().startswith("data:") and "," in data:
        _, data = data.split(",", 1)
    try:
        return base64.b64decode(data)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Invalid base64 image data") from exc


@contextmanager
def client_connection(session, api_id: int, api_hash: str):
    """Context manager that connects a Telethon client without interactive start()."""
    ensure_event_loop()
    client = TelegramClient(session, api_id, api_hash)
    client.connect()
    try:
        yield client
    finally:
        client.disconnect()


def ensure_authorized(client: TelegramClient):
    """Raise if the provided client is not logged in."""
    if not client.is_user_authorized():
        raise AuthKeyUnregisteredError(request=None)


def send_content(
    client: TelegramClient,
    target: Any,
    content_type: str,
    message: str,
    image_url: str,
    image_bytes: bytes | None = None,
    image_name: str | None = None,
):
    """Send either a text message or an image/document with optional caption."""
    if content_type == "image":
        if image_bytes:
            buf = io.BytesIO(image_bytes)
            buf.name = image_name or "upload"
            return client.send_file(target, buf, caption=message or None)
        return client.send_file(target, image_url, caption=message or None)
    return client.send_message(target, message)


def run_sender(
    api_id,
    api_hash,
    session_string,
    chat_id,
    message,
    interval_seconds,
    content_type,
    image_url,
    image_bytes,
    image_name,
):
    global stop_flag
    ensure_event_loop()
    if not session_string:
        session_string = load_saved_session()
    session = StringSession(session_string) if session_string else "telegram_session"

    with session_lock, client_connection(session, api_id, api_hash) as client:
        try:
            ensure_authorized(client)
        except AuthKeyUnregisteredError:
            with status_lock:
                status_state.update({"active": False, "mode": "unauthorized"})
            return
        try:
            target = resolve_target(client, chat_id)
        except ValueError as exc:
            # Abort the loop if target cannot be resolved.
            stop_flag = True
            print(f"Failed to resolve target: {exc}")
            return

        with status_lock:
            status_state.update(
                {
                    "active": True,
                    "mode": "loop",
                    "started_at": time.time(),
                    "last_sent_at": None,
                    "sent_count": 0,
                    "chat_id": chat_id,
                    "interval_seconds": interval_seconds,
                    "content_type": content_type,
                }
            )

        while not stop_flag:
            send_content(
                client,
                target,
                content_type,
                message,
                image_url,
                image_bytes=image_bytes,
                image_name=image_name,
            )
            with status_lock:
                status_state["last_sent_at"] = time.time()
                status_state["sent_count"] += 1
            time.sleep(interval_seconds)
    with status_lock:
        status_state["active"] = False
        status_state["mode"] = "stopped"


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/start", methods=["POST"])
def start():
    global stop_flag, worker
    data = request.get_json()

    # Prevent overlapping runs from locking the SQLite session.
    if status_state.get("active"):
        return jsonify({"detail": "Auto sender is already running; stop it before starting again."}), 409

    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    session_string = data.get("session_string") or ""
    chat_id = data["chat_id"]
    message = (data.get("message") or "").strip()
    image_url = (data.get("image_url") or "").strip()
    image_base64 = (data.get("image_base64") or "").strip()
    image_name = (data.get("image_name") or "").strip() or "upload"
    image_bytes = None
    content_type = (data.get("content_type") or "text").lower()
    if content_type not in ("text", "image"):
        return jsonify({"detail": "content_type must be 'text' or 'image'"}), 400
    interval_seconds_raw = data.get("interval_seconds") or data.get("interval_minutes")
    if interval_seconds_raw is None:
        return jsonify({"detail": "interval_seconds is required"}), 400
    interval_seconds = float(interval_seconds_raw)

    if content_type == "text" and not message:
        return jsonify({"detail": "message is required for text sends"}), 400
    if content_type == "image":
        if image_base64:
            try:
                image_bytes = decode_base64_image(image_base64)
            except ValueError as exc:
                return jsonify({"detail": str(exc)}), 400
        elif not image_url:
            return jsonify({"detail": "image_url is required when content_type=image"}), 400

    # Validate target before spawning the background sender.
    ensure_event_loop()
    if not session_string:
        session_string = load_saved_session()
    session = StringSession(session_string) if session_string else "telegram_session"
    try:
        with session_lock, client_connection(session, api_id, api_hash) as client:
            ensure_authorized(client)
            resolve_target(client, chat_id)
    except AuthKeyUnregisteredError:
        return jsonify({"detail": "Session is not authorized. Use /auth/send-code then /auth/verify to log in."}), 401
    except ValueError as exc:
        return jsonify({"detail": f"Invalid chat id/username: {exc}"}), 400
    with status_lock:
        status_state["user"] = status_state.get("user") or None

    stop_flag = True
    if worker and worker.is_alive():
        worker.join(timeout=2)

    stop_flag = False
    worker = threading.Thread(
        target=run_sender,
        args=(
            api_id,
            api_hash,
            session_string,
            chat_id,
            message,
            interval_seconds,
            content_type,
            image_url,
            image_bytes,
            image_name,
        ),
        daemon=True,
    )
    worker.start()

    return jsonify({"detail": "Auto sender started"})


@app.route("/send-once", methods=["POST"])
def send_once():
    data = request.get_json()

    if status_state.get("active"):
        return jsonify({"detail": "Stop the auto sender before using send-once."}), 409

    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    session_string = data.get("session_string") or ""
    chat_id = data["chat_id"]
    message = (data.get("message") or "").strip()
    image_url = (data.get("image_url") or "").strip()
    image_base64 = (data.get("image_base64") or "").strip()
    image_name = (data.get("image_name") or "").strip() or "upload"
    image_bytes = None
    content_type = (data.get("content_type") or "text").lower()
    if content_type not in ("text", "image"):
        return jsonify({"detail": "content_type must be 'text' or 'image'"}), 400
    if content_type == "text" and not message:
        return jsonify({"detail": "message is required for text sends"}), 400
    if content_type == "image":
        if image_base64:
            try:
                image_bytes = decode_base64_image(image_base64)
            except ValueError as exc:
                return jsonify({"detail": str(exc)}), 400
        elif not image_url:
            return jsonify({"detail": "image_url is required when content_type=image"}), 400

    ensure_event_loop()
    if not session_string:
        session_string = load_saved_session()
    session = StringSession(session_string) if session_string else "telegram_session"

    with session_lock, client_connection(session, api_id, api_hash) as client:
        try:
            ensure_authorized(client)
        except AuthKeyUnregisteredError:
            return jsonify({"detail": "Session is not authorized. Use /auth/send-code then /auth/verify to log in."}), 401
        try:
            target = resolve_target(client, chat_id)
            send_content(
                client,
                target,
                content_type,
                message,
                image_url,
                image_bytes=image_bytes,
                image_name=image_name,
            )
            now_ts = time.time()
            with status_lock:
                status_state.update(
                    {
                        "active": False,
                        "mode": "once",
                        "started_at": now_ts,
                        "last_sent_at": now_ts,
                        "sent_count": 1,
                        "chat_id": chat_id,
                        "interval_seconds": None,
                        "content_type": content_type,
                        "user": status_state.get("user"),
                    }
                )
        except ValueError as exc:
            return jsonify({"detail": f"Invalid chat id/username: {exc}"}), 400

    return jsonify({"detail": "Message sent once"})


@app.route("/stop", methods=["POST"])
def stop():
    global stop_flag
    stop_flag = True
    with status_lock:
        status_state["active"] = False
        status_state["mode"] = "stopped"
    if worker and worker.is_alive():
        worker.join(timeout=2)
    return jsonify({"detail": "Auto sender stopped"})


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    """Clear cached session info."""
    with status_lock:
        status_state.update(
            {
                "user": None,
                "active": False,
                "mode": "stopped",
                "started_at": None,
                "last_sent_at": None,
                "sent_count": 0,
                "chat_id": None,
                "interval_seconds": None,
                "content_type": None,
            }
        )
    try:
        os.remove(SESSION_STRING_PATH)
    except OSError:
        pass
    return jsonify({"detail": "Signed out"})


@app.route("/auth/send-code", methods=["POST"])
def auth_send_code():
    data = request.get_json()
    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    phone = str(data.get("phone") or "").strip()
    if not phone:
        return jsonify({"detail": "phone is required"}), 400

    try:
        session_obj = StringSession()
        with session_lock, client_connection(session_obj, api_id, api_hash) as client:
            sent = client.send_code_request(phone)
            auth_cache[phone] = {
                "phone_code_hash": sent.phone_code_hash,
                "api_id": api_id,
                "api_hash": api_hash,
                "session_string": client.session.save(),
                "ts": time.time(),
            }
    except FloodWaitError as exc:
        return jsonify({"detail": f"Too many attempts. Wait {exc.seconds} seconds."}), 429
    except Exception as exc:
        return jsonify({"detail": f"Could not send code: {exc}"}), 400

    return jsonify({"detail": "Code sent. Check Telegram and enter it to verify."})


@app.route("/auth/verify", methods=["POST"])
def auth_verify():
    data = request.get_json()
    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    phone = str(data.get("phone") or "").strip()
    code = str(data.get("code") or "").strip()
    password = data.get("password") or ""
    if not phone or not code:
        return jsonify({"detail": "phone and code are required"}), 400

    cached = auth_cache.get(phone)
    if not cached or cached.get("phone_code_hash") is None:
        return jsonify({"detail": "No pending code. Send a code first."}), 400

    session_string = cached.get("session_string") or ""
    session = StringSession(session_string) if session_string else StringSession()
    try:
        with session_lock, client_connection(session, api_id, api_hash) as client:
            try:
                client.sign_in(phone=phone, code=code, phone_code_hash=cached["phone_code_hash"])
            except SessionPasswordNeededError:
                if not password:
                    return jsonify({"detail": "Two-step password required"}), 401
                client.sign_in(password=password)

            session_string = client.session.save()
            me = client.get_me()
            welcome = f"Authorized as {me.first_name}" if me else "Authorized."
            user_info = {
                "id": me.id if me else None,
                "name": " ".join([me.first_name or "", me.last_name or ""]).strip() if me else None,
            }
            if session_string:
                persist_session(session_string)
            with status_lock:
                status_state["user"] = user_info
            # Replace cached hash after successful sign in
            auth_cache.pop(phone, None)
            return jsonify(
                {
                    "detail": welcome + " Session saved below.",
                    "session_string": session_string,
                    "user": user_info,
                }
            )
    except PhoneCodeInvalidError:
        return jsonify({"detail": "Invalid code."}), 401
    except PhoneCodeExpiredError:
        return jsonify({"detail": "Code expired. Send a new one."}), 401
    except FloodWaitError as exc:
        return jsonify({"detail": f"Too many attempts. Wait {exc.seconds} seconds."}), 429
    except Exception as exc:
        return jsonify({"detail": f"Could not verify: {exc}"}), 400


@app.route("/status", methods=["GET"])
def status():
    with status_lock:
        return jsonify(status_state)


if __name__ == "__main__":
    app.run(debug=True)
