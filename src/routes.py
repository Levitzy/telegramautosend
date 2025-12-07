import time
import uuid
import threading
from flask import Blueprint, request, jsonify, send_from_directory

from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    AuthKeyUnregisteredError,
)

from . import globals as g
from . import db
from . import utils
from . import bot

bp = Blueprint('routes', __name__)

@bp.route("/")
def index():
    return send_from_directory("../templates", "index.html")

@bp.route("/start", methods=["POST"])
def start():
    data = request.get_json()

    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    session_string = bot.obtain_session_string(data.get("session_string"))
    if not session_string:
        return jsonify({"detail": "Session string is required. Login or reuse the saved session first."}), 401
    targets = utils.parse_targets(data.get("chat_id") or data.get("targets"))
    if not targets:
        return jsonify({"detail": "At least one chat id/username is required"}), 400
    message = (data.get("message") or "").strip()
    messages_multi = data.get("messages") or data.get("messages_multi") or []
    if isinstance(messages_multi, str):
        messages_multi = [m for m in messages_multi.replace("\r", "\n").split("\n") if m.strip()]
    messages_list = utils.build_message_list(message, messages_multi)
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
                image_bytes = utils.decode_base64_image(image_base64)
            except ValueError as exc:
                return jsonify({"detail": str(exc)}), 400
        elif not image_url:
            return jsonify({"detail": "image_url is required when content_type=image"}), 400

    # Validate targets before spawning the background sender and fetch user info.
    utils.ensure_event_loop()
    try:
        session = StringSession(session_string)
    except Exception:
        return jsonify({"detail": "Invalid session string. Login again to continue."}), 400
    user_info = bot.get_current_user()
    try:
        with g.session_lock, bot.client_connection(session, api_id, api_hash) as client:
            bot.ensure_authorized(client)
            for tgt in targets:
                bot.resolve_target(client, tgt)
            if user_info is None:
                user_info = bot.get_user_from_client(client)
    except AuthKeyUnregisteredError:
        return jsonify({"detail": "Session is not authorized. Use /auth/send-code then /auth/verify to log in."}), 401
    except ValueError as exc:
        return jsonify({"detail": f"Invalid chat id/username: {exc}"}), 400

    run_id = str(uuid.uuid4())
    with g.status_lock:
        g.status_state["run_id"] = run_id
        g.run_statuses[run_id] = {
            "active": True,
            "mode": "loop",
            "started_at": time.time(),
            "last_sent_at": None,
            "sent_count": 0,
            "chat_id": ", ".join(targets),
            "targets": targets,
            "interval_seconds": interval_seconds,
            "content_type": content_type,
            "next_send_at": None,
            "run_id": run_id,
            "messages": messages_list,
            "active_runs": len(g.run_registry) + 1,
        }
    stop_event = threading.Event()
    worker_thread = threading.Thread(
        target=bot.run_sender,
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
    with g.status_lock:
        g.run_registry[run_id] = {"thread": worker_thread, "stop_event": stop_event}
        g.status_state["active_runs"] = len(g.run_registry)
    worker_thread.start()

    # Persist settings to DB (non-sensitive fields)
    db.save_settings(
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

@bp.route("/send-once", methods=["POST"])
def send_once():
    data = request.get_json()

    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    session_string = bot.obtain_session_string(data.get("session_string"))
    if not session_string:
        return jsonify({"detail": "Session string is required. Login or reuse the saved session first."}), 401
    targets = utils.parse_targets(data.get("chat_id") or data.get("targets"))
    if not targets:
        return jsonify({"detail": "At least one chat id/username is required"}), 400
    message = (data.get("message") or "").strip()
    messages_multi = data.get("messages") or data.get("messages_multi") or []
    if isinstance(messages_multi, str):
        messages_multi = [m for m in messages_multi.replace("\r", "\n").split("\n") if m.strip()]
    messages_list = utils.build_message_list(message, messages_multi)
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
                image_bytes = utils.decode_base64_image(image_base64)
            except ValueError as exc:
                return jsonify({"detail": str(exc)}), 400
        elif not image_url:
            return jsonify({"detail": "image_url is required when content_type=image"}), 400

    utils.ensure_event_loop()
    try:
        session = StringSession(session_string)
    except Exception:
        return jsonify({"detail": "Invalid session string. Login again to continue."}), 400

    user_info = bot.get_current_user()
    run_id = str(uuid.uuid4())
    with bot.client_connection(session, api_id, api_hash) as client:
        try:
            bot.ensure_authorized(client)
        except AuthKeyUnregisteredError:
            return jsonify({"detail": "Session is not authorized. Use /auth/send-code then /auth/verify to log in."}), 401
        try:
            if user_info is None:
                user_info = bot.get_user_from_client(client)
            for tgt in targets:
                target = bot.resolve_target(client, tgt)
                for msg in messages_list:
                    bot.send_content(
                        client,
                        target,
                        content_type,
                        msg,
                        image_url,
                        image_bytes=image_bytes,
                        image_name=image_name,
                    )
            now_ts = time.time()
            with g.status_lock:
                g.status_state.update(
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
                        "user": user_info or g.status_state.get("user"),
                        "next_send_at": None,
                        "run_id": run_id,
                        "messages": messages_list,
                        "active_runs": len(g.run_registry),
                    }
                )
                g.run_statuses[run_id] = dict(g.status_state)
            db.upsert_run(
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

@bp.route("/stop", methods=["POST"])
def stop():
    stopped_ids = []
    with g.status_lock:
        registry_items = list(g.run_registry.items())
    for run_id, meta in registry_items:
        evt = meta.get("stop_event")
        th = meta.get("thread")
        if evt:
            evt.set()
        if th and th.is_alive():
            th.join(timeout=2)
        stopped_ids.append(run_id)
    with g.status_lock:
        g.run_registry.clear()
        g.status_state.update(
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
        db.upsert_run(
            rid,
            {
                "active": False,
                "mode": "stopped",
                "stopped_at": time.time(),
                "next_send_at": None,
            },
        )
    with g.status_lock:
        for rid in stopped_ids:
            if rid in g.run_statuses:
                g.run_statuses[rid].update({"active": False, "mode": "stopped", "next_send_at": None})
    return jsonify({"detail": "All runs stopped", "run_ids": stopped_ids})

@bp.route("/runs", methods=["GET"])
def runs():
    """List recent runs."""
    user = bot.get_current_user()
    user_id = user.get("id") if user else None
    return jsonify({"runs": db.list_runs(user_id=user_id)})

@bp.route("/runs/<run_id>/stop", methods=["POST"])
def stop_run(run_id: str):
    """Stop a specific run by id."""
    is_current = False
    with g.status_lock:
        meta = g.run_registry.get(run_id)
        is_current = meta is not None
    if is_current and meta:
        evt = meta.get("stop_event")
        th = meta.get("thread")
        if evt:
            evt.set()
        if th and th.is_alive():
            th.join(timeout=2)
        with g.status_lock:
            g.run_registry.pop(run_id, None)
            g.status_state["active_runs"] = len(g.run_registry)
            if g.status_state.get("run_id") == run_id:
                g.status_state.update(
                    {
                        "active": False,
                        "mode": "stopped",
                        "next_send_at": None,
                        "targets": [],
                        "run_id": None,
                    }
                )
    db.upsert_run(
        run_id,
        {
            "active": False,
            "mode": "stopped",
            "stopped_at": time.time(),
            "next_send_at": None,
        },
    )
    with g.status_lock:
        if run_id in g.run_statuses:
            g.run_statuses[run_id].update(
                {
                    "active": False,
                    "mode": "stopped",
                    "next_send_at": None,
                }
            )
    return jsonify({"detail": "Run stopped", "run_id": run_id})

@bp.route("/auth/logout", methods=["POST"])
def auth_logout():
    """Clear cached session info."""
    with g.status_lock:
        g.status_state.update(
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
    db.clear_persisted_session()
    return jsonify({"detail": "Signed out"})

@bp.route("/auth/send-code", methods=["POST"])
def auth_send_code():
    data = request.get_json()
    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    phone = str(data.get("phone") or "").strip()
    if not phone:
        return jsonify({"detail": "phone is required"}), 400

    try:
        session_obj = StringSession()
        with g.session_lock, bot.client_connection(session_obj, api_id, api_hash) as client:
            sent = client.send_code_request(phone)
            g.auth_cache[phone] = {
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

@bp.route("/auth/verify", methods=["POST"])
def auth_verify():
    data = request.get_json()
    api_id = int(data["api_id"])
    api_hash = data["api_hash"]
    phone = str(data.get("phone") or "").strip()
    code = str(data.get("code") or "").strip()
    password = data.get("password") or ""
    if not phone or not code:
        return jsonify({"detail": "phone and code are required"}), 400

    cached = g.auth_cache.get(phone)
    if not cached or cached.get("phone_code_hash") is None:
        return jsonify({"detail": "No pending code. Send a code first."}), 400

    session_string = cached.get("session_string") or ""
    session = StringSession(session_string) if session_string else StringSession()
    try:
        with g.session_lock, bot.client_connection(session, api_id, api_hash) as client:
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
            db.persist_session(session_string, user_info)
            with g.status_lock:
                g.status_state["user"] = user_info
            g.auth_cache.pop(phone, None)
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

@bp.route("/auth/session", methods=["GET"])
def auth_session():
    """Return any stored session string so other devices can reuse login."""
    session_string = db.load_saved_session() or ""
    with g.status_lock:
        user = g.status_state.get("user")
    return jsonify({"session_string": session_string, "user": user})

@bp.route("/status", methods=["GET"])
def status():
    with g.status_lock:
        snapshot = dict(g.status_state)
        active_runs = len(g.run_registry)
        snapshot["active_runs"] = active_runs
        if active_runs > 0:
            snapshot["active"] = True
            if active_runs > 1:
                snapshot["mode"] = "multi"
        recent = list(g.run_statuses.values())
    snapshot["runs"] = recent
    return jsonify(snapshot)

@bp.route("/settings", methods=["GET", "POST"])
def settings():
    if request.method == "GET":
        return jsonify(db.load_settings())
    data = request.get_json() or {}
    db.save_settings(data)
    return jsonify({"detail": "Settings saved"})
