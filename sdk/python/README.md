# adit-python

Zero-dependency Python client for the local `Adit.Daemon` HTTP API.

## Requirements

- Python `3.9+`
- A running daemon, defaulting to `http://127.0.0.1:5037`
- Optional `ADIT_URL` env var if the daemon is not on the default port
- Optional `ADIT_AUTH_TOKEN` env var if the daemon has bearer-token auth enabled

## Install

```bash
pip install ./sdk/python
```

## Setup-First Flow

Treat the daemon's setup contract as the source of truth before you do device or send work:

1. Call `get_setup_guide()` first during setup or recovery.
2. Call `check_setup()` when you need a fresh readiness snapshot.
3. Call `get_agent_context()` when you want setup, capability, doctor, and workflow guidance in one payload.

In the current v1 posture, the supported setup flow is one-time `Link to Windows` pairing first. `enable_notifications()` is a manual retry or opt-in endpoint after that setup, not a second pairing ceremony.

## Usage

```python
import os

from adit import AditClient

client = AditClient(
    base_url="http://127.0.0.1:5037",
    auth_token=os.environ.get("ADIT_AUTH_TOKEN"),
)

setup_guide = client.get_setup_guide()
setup_check = client.check_setup()
agent = client.get_agent_context()

info = client.get_info()
status = client.get_status()
runtime = client.get_runtime()
capabilities = client.get_capabilities()
doctor = client.get_doctor()
devices = client.list_devices()
contacts = client.search_contacts("mom")
notifications = client.list_notifications()
notifications_check = client.check_notifications()
conversations = client.list_conversations()
cached_messages = client.list_cached_messages(limit=10)
folders = client.list_message_folders(name_contains="iPhone")
messages = client.list_messages(name_contains="iPhone", folder="inbox", limit=10)
sent = client.send_message(
    body="hello from adit-python",
    contact_name="mom",
    auto_sync_after_send=True,
)
prepared_reply = client.resolve_message(conversation_id="th_seeded_mom", body="sounds good")
client.reply_to_conversation("th_seeded_mom", "reply from adit-python")
client.enable_notifications()
client.disable_notifications()
client.trigger_sync("manual")
events = client.get_recent_events()
ws_url = client.websocket_url()
```

When `auth_token` is set, HTTP requests send `Authorization: Bearer <token>` automatically and `websocket_url()` appends `?access_token=...`.

## API

- `get_status()`
- `get_info()`
- `get_runtime()`
- `get_capabilities()`
- `get_doctor()`
- `get_setup_guide()`
- `check_setup()`
- `get_agent_context()`
- `list_devices()`
- `list_contacts(device_id=None, name_contains=None, evict_phone_link=False)`
- `search_contacts(query, device_id=None, name_contains=None, limit=None)`
- `list_notifications(device_id=None, name_contains=None, active_only=True, app_identifier=None, limit=None)`
- `list_message_folders(device_id=None, name_contains=None, evict_phone_link=False)`
- `list_messages(device_id=None, name_contains=None, folder=None, limit=None, evict_phone_link=False)`
- `list_cached_messages(device_id=None, name_contains=None, folder=None, conversation_id=None, limit=None)`
- `list_conversations(device_id=None, name_contains=None, limit=None)`
- `get_conversation_messages(conversation_id, device_id=None, name_contains=None, limit=None)`
- `trigger_sync(reason="manual")`
- `get_recent_events(limit=25)`
- `check_notifications()`
- `enable_notifications()`
- `disable_notifications()`
- `websocket_url()`
- `perform_notification_action(notification_uid, action)`
- `resolve_message(recipient=None, body=None, contact_id=None, contact_name=None, preferred_number=None, conversation_id=None, device_id=None, name_contains=None, evict_phone_link=False)`
- `send_message(body, recipient=None, contact_id=None, contact_name=None, preferred_number=None, conversation_id=None, device_id=None, name_contains=None, evict_phone_link=False, auto_sync_after_send=True)`
- `reply_to_conversation(conversation_id, body, device_id=None, name_contains=None, evict_phone_link=False, auto_sync_after_send=True)`

Failed requests raise `AditError`, which includes:

- `status`
- `body`
