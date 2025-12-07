import io
import threading
import time
from typing import Any, Dict
from contextlib import contextmanager

from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import AuthKeyUnregisteredError

from . import globals as g
from . import utils
from . import db

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

@contextmanager
def client_connection(session, api_id: int, api_hash: str):
    """Context manager that connects a Telethon client without interactive start()."""
    utils.ensure_event_loop()
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

def get_current_user() -> Dict[str, Any] | None:
    """Return the user info stored in status_state, if any."""
    with g.status_lock:
        return g.status_state.get("user")

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
    with g.status_lock:
        g.status_state["user"] = user_info
    existing_session = db.load_saved_session() or ""
    if existing_session:
        db.persist_session(existing_session, user_info)
    return user_info

def obtain_session_string(payload_session: str | None = None) -> str:
    """Return a session string, preferring the payload then any saved session."""
    session_string = (payload_session or "").strip()
    if session_string:
        return session_string
    return db.load_saved_session() or ""

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
    pause_event: threading.Event | None = None,
):
    def wait_until_next_send(next_send_at: float) -> bool:
        """Sleep until the next send, freezing the countdown when paused."""
        while not stop_event.is_set():
            remaining = max(0.0, next_send_at - time.time())
            if remaining <= 0:
                return True

            if pause_event and pause_event.is_set():
                with g.status_lock:
                    g.status_state["mode"] = "paused"
                    g.status_state["next_send_at"] = None
                    if run_id:
                        snap = g.run_statuses.get(run_id, {})
                        snap.update({"mode": "paused", "next_send_at": None})
                        g.run_statuses[run_id] = snap
                if run_id:
                    db.upsert_run(run_id, {"mode": "paused", "active": True, "next_send_at": None})

                while pause_event.is_set() and not stop_event.is_set():
                    if stop_event.wait(timeout=0.5):
                        break

                if stop_event.is_set():
                    return False

                next_send_at = time.time() + remaining
                with g.status_lock:
                    g.status_state["mode"] = "loop"
                    g.status_state["next_send_at"] = next_send_at
                    if run_id:
                        snap = g.run_statuses.get(run_id, {})
                        snap.update({"mode": "loop", "next_send_at": next_send_at})
                        g.run_statuses[run_id] = snap
                if run_id:
                    db.upsert_run(run_id, {"mode": "loop", "active": True, "next_send_at": next_send_at})
                continue

            sleep_for = min(0.5, remaining)
            if stop_event.wait(timeout=sleep_for):
                return False
        return False

    utils.ensure_event_loop()
    if not session_string:
        with g.status_lock:
            g.status_state.update({"active": False, "mode": "unauthorized", "next_send_at": None})
        return

    session = StringSession(session_string)

    with client_connection(session, api_id, api_hash) as client:
        try:
            ensure_authorized(client)
        except AuthKeyUnregisteredError:
            with g.status_lock:
                g.status_state.update({"active": False, "mode": "unauthorized"})
            return
        try:
            resolved_targets = [resolve_target(client, t) for t in targets]
        except ValueError as exc:
            # Abort the loop if target cannot be resolved.
            stop_event.set()
            print(f"Failed to resolve target: {exc}")
            return

        with g.status_lock:
            g.status_state.update(
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
                    "user": user_info or g.status_state.get("user"),
                    "messages": messages,
                    "active_runs": len(g.run_registry),
                }
            )
            if run_id:
                g.run_statuses[run_id] = dict(g.status_state)
        if run_id:
            db.upsert_run(
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
                    "started_at": g.status_state["started_at"],
                    "last_sent_at": None,
                    "sent_count": 0,
                    "next_send_at": g.status_state["next_send_at"],
                },
            )

        while not stop_event.is_set():
            if pause_event and pause_event.is_set():
                with g.status_lock:
                    g.status_state["mode"] = "paused"
                    g.status_state["next_send_at"] = None
                    if run_id:
                        snap = g.run_statuses.get(run_id, {})
                        snap.update({"mode": "paused", "next_send_at": None})
                        g.run_statuses[run_id] = snap
                if run_id:
                    db.upsert_run(run_id, {"mode": "paused", "active": True, "next_send_at": None})
                while pause_event.is_set() and not stop_event.is_set():
                    if stop_event.wait(timeout=0.5):
                        break
                if stop_event.is_set():
                    break
                with g.status_lock:
                    g.status_state["mode"] = "loop"
                    if run_id:
                        snap = g.run_statuses.get(run_id, {})
                        snap["mode"] = "loop"
                        g.run_statuses[run_id] = snap
                if run_id:
                    db.upsert_run(run_id, {"mode": "loop", "active": True})

            if stop_event.is_set():
                break

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
                        with g.status_lock:
                            g.status_state["last_sent_at"] = time.time()
                            g.status_state["sent_count"] += 1
                            g.status_state["chat_id"] = targets[idx]
                            g.status_state["user"] = user_info or g.status_state.get("user")
                            g.status_state["active_runs"] = len(g.run_registry)
                    except Exception as exc:
                        print(f"Failed to send to {targets[idx]}: {exc}")
                        continue
            next_send_at = time.time() + interval_seconds
            with g.status_lock:
                g.status_state["next_send_at"] = next_send_at
                now_next = g.status_state["next_send_at"]
                now_last = g.status_state["last_sent_at"]
                now_sent = g.status_state["sent_count"]
                if run_id:
                    g.run_statuses[run_id] = dict(g.status_state)
            if run_id:
                db.upsert_run(
                    run_id,
                    {
                        "last_sent_at": now_last,
                        "sent_count": now_sent,
                        "next_send_at": now_next,
                        "active": True,
                        "mode": "loop",
                    },
                )
            if not wait_until_next_send(next_send_at):
                break
    with g.status_lock:
        g.status_state["active"] = False
        g.status_state["mode"] = "stopped"
        g.status_state["next_send_at"] = None
        g.status_state["run_id"] = None
        g.status_state["active_runs"] = max(len(g.run_registry) - 1, 0)
        if run_id:
            g.run_statuses[run_id] = dict(g.status_state)
    with g.status_lock:
        g.run_registry.pop(run_id, None)
    if run_id:
        db.upsert_run(
            run_id,
            {
                "active": False,
                "mode": "stopped",
                "stopped_at": time.time(),
                "next_send_at": None,
            },
        )
