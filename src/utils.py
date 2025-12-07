import asyncio
import base64
import binascii
import hashlib
import os
import re
from typing import Any

def ensure_event_loop():
    """Ensure the current thread has an asyncio event loop Telethon can use."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

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

def build_message_list(message: str, messages: list[str] | None) -> list[str]:
    """Return a cleaned list of messages; fallback to single message if list empty."""
    msgs = []
    if messages:
        msgs.extend([m.strip() for m in messages if str(m).strip()])
    if message and message.strip():
        msgs.insert(0, message.strip())
    return msgs or []
