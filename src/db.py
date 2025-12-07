import time
import os
from typing import Any, Dict
from pymongo import MongoClient, errors as mongo_errors

from . import globals as g
from . import utils

def get_mongo():
    mongo_url = os.environ.get("MONGO_URL") or os.environ.get("MONGODB_URI")
    db_name = os.environ.get("MONGO_DB", "telegramautosend")
    if not mongo_url:
        return None, None
    if g.mongo_client:
        return g.mongo_client[db_name], db_name
    try:
        g.mongo_client = MongoClient(mongo_url, serverSelectionTimeoutMS=2000)
        g.mongo_client.admin.command("ping")
        return g.mongo_client[db_name], db_name
    except mongo_errors.PyMongoError:
        g.mongo_client = None
        return None, None

def list_runs(limit: int = 15, user_id: int = None):
    """Return recent runs, merging DB and in-memory snapshots."""
    if user_id is None:
        return []

    merged: Dict[str, Dict[str, Any]] = {}
    db, _ = get_mongo()
    if db is not None:
        try:
            cursor = db.runs.find({"user_id": user_id}).sort("started_at", -1).limit(limit)
            for doc in cursor:
                active = bool(doc.get("active"))
                mode = doc.get("mode")
                if active:
                    status = "Running"
                elif mode == "once":
                    status = "Completed"
                else:
                    status = "Stopped"

                merged[str(doc.get("_id"))] = {
                    "id": doc.get("_id"),
                    "active": active,
                    "mode": mode,
                    "status": status,
                    "started_at": doc.get("started_at"),
                    "last_sent_at": doc.get("last_sent_at"),
                    "sent_count": doc.get("sent_count", 0),
                    "targets": doc.get("targets") or [],
                    "content_type": doc.get("content_type"),
                    "interval_seconds": doc.get("interval_seconds"),
                    "next_send_at": doc.get("next_send_at"),
                    "messages_count": len(doc.get("messages") or []),
                }
        except mongo_errors.PyMongoError:
            pass
    with g.status_lock:
        for rid, snap in g.run_statuses.items():
            # Check ownership
            snap_user = snap.get("user") or {}
            if snap_user.get("id") != user_id:
                continue

            active = snap.get("active")
            mode = snap.get("mode")
            if active:
                status = "Running"
            elif mode == "once":
                status = "Completed"
            else:
                status = "Stopped"

            merged.setdefault(
                rid,
                {
                    "id": rid,
                    "active": active,
                    "mode": mode,
                    "status": status,
                    "started_at": snap.get("started_at"),
                    "last_sent_at": snap.get("last_sent_at"),
                    "sent_count": snap.get("sent_count"),
                    "targets": snap.get("targets"),
                    "content_type": snap.get("content_type"),
                    "interval_seconds": snap.get("interval_seconds"),
                    "next_send_at": snap.get("next_send_at"),
                    "messages_count": len(snap.get("messages") or []),
                },
            )
    runs = list(merged.values())
    runs.sort(key=lambda r: r.get("started_at") or 0, reverse=True)
    return runs[:limit]

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

def load_saved_session():
    if g.saved_session_string:
        return g.saved_session_string
    # Prefer DB stored session
    db, _ = get_mongo()
    if db is not None:
        try:
            doc = db.sessions.find_one({"_id": "latest"})
            if doc and doc.get("session_enc"):
                decrypted = utils.decrypt_session_string(doc["session_enc"])
                if decrypted:
                    g.saved_session_string = decrypted
                    if doc.get("user"):
                        with g.status_lock:
                            g.status_state["user"] = doc.get("user")
                    return g.saved_session_string
        except mongo_errors.PyMongoError:
            pass
    if os.path.exists(g.SESSION_STRING_PATH):
        try:
            with open(g.SESSION_STRING_PATH, "r", encoding="utf-8") as fh:
                g.saved_session_string = fh.read().strip() or None
        except OSError:
            g.saved_session_string = None
    return g.saved_session_string

def persist_session(session_string: str, user_info: Dict[str, Any] | None = None):
    g.saved_session_string = session_string
    try:
        with open(g.SESSION_STRING_PATH, "w", encoding="utf-8") as fh:
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
                        "session_enc": utils.encrypt_session_string(session_string),
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
    g.saved_session_string = None
    for path in (g.SESSION_STRING_PATH, "telegram_session.session"):
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

def upsert_run(run_id: str, payload: Dict[str, Any]):
    """Persist run progress for a signed-in user."""
    db, _ = get_mongo()
    if db is None:
        return
    try:
        db.runs.update_one({"_id": run_id}, {"$set": payload}, upsert=True)
    except mongo_errors.PyMongoError:
        pass
