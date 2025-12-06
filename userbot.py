import asyncio
import base64
import binascii
import io
import threading
import time
import os
import hashlib
import uuid
import re
from contextlib import contextmanager
from typing import Any, Dict

from flask import Flask, request, jsonify, send_from_directory
from pymongo import MongoClient, errors as mongo_errors
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
    "next_send_at": None,
    "targets": [],
    "run_id": None,
    "messages": [],
    "active_runs": 0,
}
SESSION_STRING_PATH = "session_string.txt"
saved_session_string = None
mongo_client = None
run_registry: Dict[str, Dict[str, Any]] = {}
run_statuses: Dict[str, Dict[str, Any]] = {}


def ensure_event_loop():
    """Ensure the current thread has an asyncio event loop Telethon can use."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def get_mongo():
    global mongo_client
    mongo_url = os.environ.get("MONGO_URL") or os.environ.get("MONGODB_URI")
    db_name = os.environ.get("MONGO_DB", "telegramautosend")
    if not mongo_url:
        return None, None
    if mongo_client:
        return mongo_client[db_name], db_name
    try:
        mongo_client = MongoClient(mongo_url, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")
        return mongo_client[db_name], db_name
    except mongo_errors.PyMongoError:
        mongo_client = None
        return None, None


def list_runs(limit: int = 15):
    """Return recent runs from DB."""
    db, _ = get_mongo()
    if db is None:
        return []
    try:
        cursor = db.runs.find().sort("started_at", -1).limit(limit)
        runs = []
        for doc in cursor:
            runs.append(
                {
                    "id": doc.get("_id"),
                    "active": bool(doc.get("active")),
                    "mode": doc.get("mode"),
                    "started_at": doc.get("started_at"),
                    "last_sent_at": doc.get("last_sent_at"),
                    "sent_count": doc.get("sent_count", 0),
                    "targets": doc.get("targets") or [],
                    "content_type": doc.get("content_type"),
                    "interval_seconds": doc.get("interval_seconds"),
                    "next_send_at": doc.get("next_send_at"),
                    "messages_count": len(doc.get("messages") or []),
                }
            )
        return runs
    except mongo_errors.PyMongoError:
        return []


def save_settings(data: Dict[str, Any]):
    db, _ = get_mongo()
    if db is None:
        return
    sanitized = {k: v for k, v in data.items() if k != "session_string"}
    try:
        db.settings.update_one(
            {"_id": "form"}, {"$set": {"data": sanitized, "updated_at": time.time()}}, upsert=True
        )
    except mongo_errors.PyMongoError:
        pass


def load_settings() -> Dict[str, Any]:
    db, _ = get_mongo()
    if db is None:
        return {}
    try:
        doc = db.settings.find_one({"_id": "form"}) or {}
        data = doc.get("data") or {}
        data.pop("session_string", None)
        return data
    except mongo_errors.PyMongoError:
        return {}


def _get_secret_bytes() -> bytes:
    secret = os.environ.get("SESSION_SECRET", "")
    if not secret:
        return b""
    return hashlib.sha256(secret.encode()).digest()


def encrypt_session_string(session_string: str) -> str:
    key = _get_secret_bytes()
    if not key:
        return session_string
    data = session_string.encode()
    enc = bytes(b ^ key[i % len(key)] for i, b in enumerate(data))
    return base64.urlsafe_b64encode(enc).decode()


def decrypt_session_string(payload: str) -> str:
    key = _get_secret_bytes()
    if not key:
        return payload
    try:
        raw = base64.urlsafe_b64decode(payload.encode())
        dec = bytes(b ^ key[i % len(key)] for i, b in enumerate(raw))
        return dec.decode()
    except Exception:
        return ""


def load_saved_session():
    global saved_session_string
    if saved_session_string:
        return saved_session_string
    # Prefer DB stored session
    db, _ = get_mongo()
    if db is not None:
        try:
            doc = db.sessions.find_one({"_id": "latest"})
            if doc and doc.get("session_enc"):
                decrypted = decrypt_session_string(doc["session_enc"])
                if decrypted:
                    saved_session_string = decrypted
                    if doc.get("user"):
                        with status_lock:
                            status_state["user"] = doc.get("user")
                    return saved_session_string
        except mongo_errors.PyMongoError:
            pass
    if os.path.exists(SESSION_STRING_PATH):
        try:
            with open(SESSION_STRING_PATH, "r", encoding="utf-8") as fh:
                saved_session_string = fh.read().strip() or None
        except OSError:
            saved_session_string = None
    return saved_session_string


def persist_session(session_string: str, user_info: Dict[str, Any] | None = None):
    global saved_session_string
    saved_session_string = session_string
    try:
        with open(SESSION_STRING_PATH, "w", encoding="utf-8") as fh:
            fh.write(session_string)
    except OSError:
        pass
    db, _ = get_mongo()
    if db is not None:
        try:
            db.sessions.update_one(
                {"_id": "latest"},
                {
                    "$set": {
                        "session_enc": encrypt_session_string(session_string),
                        "user": user_info or {},
                        "updated_at": time.time(),
                    }
                },
                upsert=True,
            )
        except mongo_errors.PyMongoError:
            pass


def clear_persisted_session():
    """Remove any cached session info from disk/DB and memory."""
    global saved_session_string
    saved_session_string = None
    for path in (SESSION_STRING_PATH, "telegram_session.session"):
        try:
            os.remove(path)
        except OSError:
            pass
    db, _ = get_mongo()
    if db is not None:
        try:
            db.sessions.delete_one({"_id": "latest"})
        except mongo_errors.PyMongoError:
            pass


def get_current_user() -> Dict[str, Any] | None:
    """Return the user info stored in status_state, if any."""
    with status_lock:
        return status_state.get("user")


def upsert_run(run_id: str, payload: Dict[str, Any]):
    """Persist run progress for a signed-in user."""
    db, _ = get_mongo()
    if db is None:
        return
    try:
        db.runs.update_one({"_id": run_id}, {"$set": payload}, upsert=True)
    except mongo_errors.PyMongoError:
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


def parse_targets(raw: Any) -> list[str]:
    """Split chat identifiers into a clean list (supports commas or new lines)."""
    if raw is None:
        return []
    text = str(raw).replace("\r", "\n")
    parts = re.split(r"[,\n]", text)
    return [p.strip() for p in parts if p.strip()]


def obtain_session_string(payload_session: str | None = None) -> str:
    """Return a session string, preferring the payload then any saved session."""
    session_string = (payload_session or "").strip()
    if session_string:
        return session_string
    return load_saved_session() or ""


def get_user_from_client(client: TelegramClient) -> Dict[str, Any] | None:
    """Fetch user info from Telegram and cache it in status_state."""
    try:
        me = client.get_me()
    except Exception:
        return None
    if not me:
        return None
    user_info = {
        "id": me.id,
        "name": " ".join([me.first_name or "", me.last_name or ""]).strip(),
    }
    with status_lock:
        status_state["user"] = user_info
    existing_session = load_saved_session() or ""
    if existing_session:
        persist_session(existing_session, user_info)
    return user_info


def build_message_list(message: str, messages: list[str] | None) -> list[str]:
    """Return a cleaned list of messages; fallback to single message if list empty."""
    msgs = []
    if messages:
        msgs.extend([m.strip() for m in messages if str(m).strip()])
    if message and message.strip():
        msgs.insert(0, message.strip())
    return msgs or []


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
    targets,
    messages,
    interval_seconds,
    content_type,
    image_url,
    image_bytes,
    image_name,
    run_id: str | None,
    user_info: Dict[str, Any] | None,
    stop_event: threading.Event,
):
    ensure_event_loop()
    if not session_string:
        with status_lock:
            status_state.update({"active": False, "mode": "unauthorized", "next_send_at": None})
        return

    session = StringSession(session_string)

    with session_lock, client_connection(session, api_id, api_hash) as client:
        try:
            ensure_authorized(client)
        except AuthKeyUnregisteredError:
            with status_lock:
                status_state.update({"active": False, "mode": "unauthorized"})
            return
        try:
            resolved_targets = [resolve_target(client, t) for t in targets]
        except ValueError as exc:
            # Abort the loop if target cannot be resolved.
            stop_event.set()
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
                    "chat_id": ", ".join(targets),
                    "targets": targets,
                    "interval_seconds": interval_seconds,
                    "content_type": content_type,
                    "next_send_at": time.time() + interval_seconds,
                    "run_id": run_id,
                    "user": user_info or status_state.get("user"),
                    "messages": messages,
                    "active_runs": len(run_registry),
                }
            )
            if run_id:
                run_statuses[run_id] = dict(status_state)
        if run_id:
            upsert_run(
                run_id,
                {
                    "_id": run_id,
                    "user_id": (user_info or {}).get("id"),
                    "user": user_info or {},
                    "targets": targets,
                    "messages": messages,
                    "image_url": image_url if content_type == "image" else None,
                    "content_type": content_type,
                    "interval_seconds": interval_seconds,
                    "active": True,
                    "mode": "loop",
                    "started_at": status_state["started_at"],
                    "last_sent_at": None,
                    "sent_count": 0,
                    "next_send_at": status_state["next_send_at"],
                },
            )

        while not stop_event.is_set():
            for idx, target in enumerate(resolved_targets):
                for msg in messages:
                    try:
                        send_content(
                            client,
                            target,
                            content_type,
                            msg,
                            image_url,
                            image_bytes=image_bytes,
                            image_name=image_name,
                        )
                        with status_lock:
                            status_state["last_sent_at"] = time.time()
                            status_state["sent_count"] += 1
                            status_state["chat_id"] = targets[idx]
                            status_state["user"] = user_info or status_state.get("user")
                            status_state["active_runs"] = len(run_registry)
                    except Exception as exc:
                        print(f"Failed to send to {targets[idx]}: {exc}")
                        continue
            with status_lock:
                status_state["next_send_at"] = time.time() + interval_seconds
                now_next = status_state["next_send_at"]
                now_last = status_state["last_sent_at"]
                now_sent = status_state["sent_count"]
                if run_id:
                    run_statuses[run_id] = dict(status_state)
            if run_id:
                upsert_run(
                    run_id,
                    {
                        "last_sent_at": now_last,
                        "sent_count": now_sent,
                        "next_send_at": now_next,
                        "active": True,
                        "mode": "loop",
                    },
                )
            time.sleep(interval_seconds)
    with status_lock:
        status_state["active"] = False
        status_state["mode"] = "stopped"
        status_state["next_send_at"] = None
        status_state["run_id"] = None
        status_state["active_runs"] = max(len(run_registry) - 1, 0)
        if run_id:
            run_statuses[run_id] = dict(status_state)
    with status_lock:
        run_registry.pop(run_id, None)
    if run_id:
        upsert_run(
            run_id,
            {
                "active": False,
                "mode": "stopped",
                "stopped_at": time.time(),
                "next_send_at": None,
            },
        )


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/start", methods=["POST"])
def start():
    data = request.get_json()

    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    session_string = obtain_session_string(data.get("session_string"))
    if not session_string:
        return jsonify({"detail": "Session string is required. Login or reuse the saved session first."}), 401
    targets = parse_targets(data.get("chat_id") or data.get("targets"))
    if not targets:
        return jsonify({"detail": "At least one chat id/username is required"}), 400
    message = (data.get("message") or "").strip()
    messages_multi = data.get("messages") or data.get("messages_multi") or []
    if isinstance(messages_multi, str):
        messages_multi = [m for m in messages_multi.replace("\r", "\n").split("\n") if m.strip()]
    messages_list = build_message_list(message, messages_multi)
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

    if content_type == "text" and not messages_list:
        return jsonify({"detail": "message is required for text sends"}), 400
    if content_type == "image":
        if image_base64:
            try:
                image_bytes = decode_base64_image(image_base64)
            except ValueError as exc:
                return jsonify({"detail": str(exc)}), 400
        elif not image_url:
            return jsonify({"detail": "image_url is required when content_type=image"}), 400

    # Validate targets before spawning the background sender and fetch user info.
    ensure_event_loop()
    try:
        session = StringSession(session_string)
    except Exception:
        return jsonify({"detail": "Invalid session string. Login again to continue."}), 400
    user_info = get_current_user()
    try:
        with session_lock, client_connection(session, api_id, api_hash) as client:
            ensure_authorized(client)
            for tgt in targets:
                resolve_target(client, tgt)
            if user_info is None:
                user_info = get_user_from_client(client)
    except AuthKeyUnregisteredError:
        return jsonify({"detail": "Session is not authorized. Use /auth/send-code then /auth/verify to log in."}), 401
    except ValueError as exc:
        return jsonify({"detail": f"Invalid chat id/username: {exc}"}), 400

    run_id = str(uuid.uuid4())
    with status_lock:
        status_state["run_id"] = run_id
    stop_event = threading.Event()
    worker_thread = threading.Thread(
        target=run_sender,
        args=(
            api_id,
            api_hash,
            session_string,
            targets,
            messages_list,
            interval_seconds,
            content_type,
            image_url,
            image_bytes,
            image_name,
            run_id,
            user_info,
            stop_event,
        ),
        daemon=True,
    )
    with status_lock:
        run_registry[run_id] = {"thread": worker_thread, "stop_event": stop_event}
        status_state["active_runs"] = len(run_registry)
    worker_thread.start()

    # Persist settings to DB (non-sensitive fields)
    save_settings(
        {
            "api_id": api_id,
            "api_hash": api_hash,
            "chat_id": ", ".join(targets),
            "message": message,
            "messages": messages_multi,
            "interval_seconds": interval_seconds,
            "content_type": content_type,
            "image_url": image_url,
        }
    )

    return jsonify({"detail": "Auto sender started", "run_id": run_id})


@app.route("/send-once", methods=["POST"])
def send_once():
    data = request.get_json()

    if status_state.get("active"):
        return jsonify({"detail": "Stop the auto sender before using send-once."}), 409

    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    session_string = obtain_session_string(data.get("session_string"))
    if not session_string:
        return jsonify({"detail": "Session string is required. Login or reuse the saved session first."}), 401
    targets = parse_targets(data.get("chat_id") or data.get("targets"))
    if not targets:
        return jsonify({"detail": "At least one chat id/username is required"}), 400
    message = (data.get("message") or "").strip()
    messages_multi = data.get("messages") or data.get("messages_multi") or []
    if isinstance(messages_multi, str):
        messages_multi = [m for m in messages_multi.replace("\r", "\n").split("\n") if m.strip()]
    messages_list = build_message_list(message, messages_multi)
    image_url = (data.get("image_url") or "").strip()
    image_base64 = (data.get("image_base64") or "").strip()
    image_name = (data.get("image_name") or "").strip() or "upload"
    image_bytes = None
    content_type = (data.get("content_type") or "text").lower()
    if content_type not in ("text", "image"):
        return jsonify({"detail": "content_type must be 'text' or 'image'"}), 400
    if content_type == "text" and not messages_list:
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
    try:
        session = StringSession(session_string)
    except Exception:
        return jsonify({"detail": "Invalid session string. Login again to continue."}), 400

    user_info = get_current_user()
    run_id = str(uuid.uuid4())
    with session_lock, client_connection(session, api_id, api_hash) as client:
        try:
            ensure_authorized(client)
        except AuthKeyUnregisteredError:
            return jsonify({"detail": "Session is not authorized. Use /auth/send-code then /auth/verify to log in."}), 401
        try:
            if user_info is None:
                user_info = get_user_from_client(client)
            for tgt in targets:
                target = resolve_target(client, tgt)
                for msg in messages_list:
                    send_content(
                        client,
                        target,
                        content_type,
                        msg,
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
                        "sent_count": len(targets) * len(messages_list),
                        "chat_id": ", ".join(targets),
                        "targets": targets,
                        "interval_seconds": None,
                        "content_type": content_type,
                        "user": user_info or status_state.get("user"),
                        "next_send_at": None,
                        "run_id": run_id,
                        "messages": messages_list,
                        "active_runs": len(run_registry),
                    }
                )
                run_statuses[run_id] = dict(status_state)
            upsert_run(
                run_id,
                {
                    "_id": run_id,
                    "user_id": (user_info or {}).get("id"),
                    "user": user_info or {},
                    "targets": targets,
                    "messages": messages_list,
                    "message": message,
                    "image_url": image_url if content_type == "image" else None,
                    "content_type": content_type,
                    "interval_seconds": None,
                    "active": False,
                    "mode": "once",
                    "started_at": now_ts,
                    "last_sent_at": now_ts,
                    "sent_count": len(targets) * len(messages_list),
                    "next_send_at": None,
                },
            )
        except ValueError as exc:
            return jsonify({"detail": f"Invalid chat id/username: {exc}"}), 400

    return jsonify({"detail": "Message sent once", "run_id": run_id})


@app.route("/stop", methods=["POST"])
def stop():
    stopped_ids = []
    with status_lock:
        registry_items = list(run_registry.items())
    for run_id, meta in registry_items:
        evt = meta.get("stop_event")
        th = meta.get("thread")
        if evt:
            evt.set()
        if th and th.is_alive():
            th.join(timeout=2)
        stopped_ids.append(run_id)
    with status_lock:
        run_registry.clear()
        status_state.update(
            {
                "active": False,
                "mode": "stopped",
                "next_send_at": None,
                "targets": [],
                "run_id": None,
                "active_runs": 0,
            }
        )
    for rid in stopped_ids:
        upsert_run(
            rid,
            {
                "active": False,
                "mode": "stopped",
                "stopped_at": time.time(),
                "next_send_at": None,
            },
        )
    with status_lock:
        for rid in stopped_ids:
            if rid in run_statuses:
                run_statuses[rid].update({"active": False, "mode": "stopped", "next_send_at": None})
    return jsonify({"detail": "All runs stopped", "run_ids": stopped_ids})


@app.route("/runs", methods=["GET"])
def runs():
    """List recent runs."""
    return jsonify({"runs": list_runs()})


@app.route("/runs/<run_id>/stop", methods=["POST"])
def stop_run(run_id: str):
    """Stop a specific run by id."""
    is_current = False
    with status_lock:
        meta = run_registry.get(run_id)
        is_current = meta is not None
    if is_current and meta:
        evt = meta.get("stop_event")
        th = meta.get("thread")
        if evt:
            evt.set()
        if th and th.is_alive():
            th.join(timeout=2)
        with status_lock:
            run_registry.pop(run_id, None)
            status_state["active_runs"] = len(run_registry)
            if status_state.get("run_id") == run_id:
                status_state.update(
                    {
                        "active": False,
                        "mode": "stopped",
                        "next_send_at": None,
                        "targets": [],
                        "run_id": None,
                    }
                )
    upsert_run(
        run_id,
        {
            "active": False,
            "mode": "stopped",
            "stopped_at": time.time(),
            "next_send_at": None,
        },
    )
    with status_lock:
        if run_id in run_statuses:
            run_statuses[run_id].update(
                {
                    "active": False,
                    "mode": "stopped",
                    "next_send_at": None,
                }
            )
    return jsonify({"detail": "Run stopped", "run_id": run_id})


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
                "next_send_at": None,
                "targets": [],
                "run_id": None,
            }
        )
    clear_persisted_session()
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
            persist_session(session_string, user_info)
            with status_lock:
                status_state["user"] = user_info
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


@app.route("/auth/session", methods=["GET"])
def auth_session():
    """Return any stored session string so other devices can reuse login."""
    session_string = load_saved_session() or ""
    with status_lock:
        user = status_state.get("user")
    return jsonify({"session_string": session_string, "user": user})


@app.route("/status", methods=["GET"])
def status():
    with status_lock:
        snapshot = dict(status_state)
        active_runs = len(run_registry)
        snapshot["active_runs"] = active_runs
        if active_runs > 0:
            snapshot["active"] = True
            if active_runs > 1:
                snapshot["mode"] = "multi"
        recent = list(run_statuses.values())
    snapshot["runs"] = recent
    return jsonify(snapshot)


@app.route("/settings", methods=["GET", "POST"])
def settings():
    if request.method == "GET":
        return jsonify(load_settings())
    data = request.get_json() or {}
    save_settings(data)
    return jsonify({"detail": "Settings saved"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
