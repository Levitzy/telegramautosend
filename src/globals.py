import threading

worker = None
status_lock = threading.Lock()
session_lock = threading.Lock()
auth_cache = {}

# Initial state
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

run_registry = {}
run_statuses = {}

mongo_client = None
SESSION_STRING_PATH = "session_string.txt"
saved_session_string = None
