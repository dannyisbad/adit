from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request


DEFAULT_BASE_URL = "http://127.0.0.1:5037"


@dataclass(slots=True)
class AditError(Exception):
    message: str
    status: int | None = None
    body: Any = None

    def __str__(self) -> str:
        return self.message


class AditClient:
    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 30.0,
        auth_token: str | None = None,
    ) -> None:
        self.base_url = (base_url or os.environ.get("ADIT_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout
        self.auth_token = _normalize_optional_string(auth_token or os.environ.get("ADIT_AUTH_TOKEN"))

    def get_status(self) -> Any:
        return self._request("GET", "/v1/status")

    def get_info(self) -> Any:
        return self._request("GET", "/v1/info")

    def get_runtime(self) -> Any:
        return self._request("GET", "/v1/runtime")

    def get_capabilities(self) -> Any:
        return self._request("GET", "/v1/capabilities")

    def get_doctor(self) -> Any:
        return self._request("GET", "/v1/doctor")

    def get_setup_guide(self) -> Any:
        return self._request("GET", "/v1/setup/guide")

    def check_setup(self) -> Any:
        return self._request("POST", "/v1/setup/check")

    def get_agent_context(self) -> Any:
        return self._request("GET", "/v1/agent/context")

    def list_devices(self) -> Any:
        return self._request("GET", "/v1/devices")

    def list_contacts(
        self,
        device_id: str | None = None,
        name_contains: str | None = None,
        evict_phone_link: bool = False,
    ) -> Any:
        return self._request(
            "GET",
            "/v1/contacts",
            query={
                "deviceId": device_id,
                "nameContains": name_contains,
                "evictPhoneLink": evict_phone_link,
            },
        )

    def search_contacts(
        self,
        query: str,
        device_id: str | None = None,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> Any:
        if not query:
            raise ValueError("query is required")

        return self._request(
            "GET",
            "/v1/contacts/search",
            query={
                "query": query,
                "deviceId": device_id,
                "nameContains": name_contains,
                "limit": limit,
            },
        )

    def list_notifications(
        self,
        device_id: str | None = None,
        name_contains: str | None = None,
        active_only: bool = True,
        app_identifier: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._request(
            "GET",
            "/v1/notifications",
            query={
                "deviceId": device_id,
                "nameContains": name_contains,
                "activeOnly": active_only,
                "appIdentifier": app_identifier,
                "limit": limit,
            },
        )

    def list_message_folders(
        self,
        device_id: str | None = None,
        name_contains: str | None = None,
        evict_phone_link: bool = False,
    ) -> Any:
        return self._request(
            "GET",
            "/v1/messages/folders",
            query={
                "deviceId": device_id,
                "nameContains": name_contains,
                "evictPhoneLink": evict_phone_link,
            },
        )

    def list_messages(
        self,
        device_id: str | None = None,
        name_contains: str | None = None,
        folder: str | None = None,
        limit: int | None = None,
        evict_phone_link: bool = False,
    ) -> Any:
        return self._request(
            "GET",
            "/v1/messages",
            query={
                "deviceId": device_id,
                "nameContains": name_contains,
                "folder": folder,
                "limit": limit,
                "evictPhoneLink": evict_phone_link,
            },
        )

    def list_cached_messages(
        self,
        device_id: str | None = None,
        name_contains: str | None = None,
        folder: str | None = None,
        conversation_id: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._request(
            "GET",
            "/v1/cache/messages",
            query={
                "deviceId": device_id,
                "nameContains": name_contains,
                "folder": folder,
                "conversationId": conversation_id,
                "limit": limit,
            },
        )

    def list_conversations(
        self,
        device_id: str | None = None,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._request(
            "GET",
            "/v1/conversations",
            query={
                "deviceId": device_id,
                "nameContains": name_contains,
                "limit": limit,
            },
        )

    def get_conversation_messages(
        self,
        conversation_id: str,
        device_id: str | None = None,
        name_contains: str | None = None,
        limit: int | None = None,
    ) -> Any:
        if not conversation_id:
            raise ValueError("conversation_id is required")

        return self._request(
            "GET",
            f"/v1/conversations/{parse.quote(conversation_id, safe='')}",
            query={
                "deviceId": device_id,
                "nameContains": name_contains,
                "limit": limit,
            },
        )

    def trigger_sync(self, reason: str = "manual") -> Any:
        return self._request("POST", "/v1/sync/now", query={"reason": reason})

    def get_recent_events(self, limit: int | None = 25) -> Any:
        return self._request("GET", "/v1/events/recent", query={"limit": limit})

    def check_notifications(self) -> Any:
        return self._request("POST", "/v1/notifications/check")

    def enable_notifications(self) -> Any:
        return self._request("POST", "/v1/notifications/enable")

    def disable_notifications(self) -> Any:
        return self._request("POST", "/v1/notifications/disable")

    def websocket_url(self) -> str:
        parsed = parse.urlsplit(self.base_url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        query = ""
        if self.auth_token:
            query = parse.urlencode({"access_token": self.auth_token})
        return parse.urlunsplit((scheme, parsed.netloc, "/v1/ws", query, ""))

    def perform_notification_action(self, notification_uid: int, action: str) -> Any:
        if notification_uid <= 0:
            raise ValueError("notification_uid must be positive")

        if action not in {"positive", "negative"}:
            raise ValueError("action must be 'positive' or 'negative'")

        return self._request(
            "POST",
            f"/v1/notifications/{notification_uid}/actions/{parse.quote(action, safe='')}",
        )

    def resolve_message(
        self,
        *,
        recipient: str | None = None,
        body: str | None = None,
        contact_id: str | None = None,
        contact_name: str | None = None,
        preferred_number: str | None = None,
        conversation_id: str | None = None,
        device_id: str | None = None,
        name_contains: str | None = None,
        evict_phone_link: bool = False,
    ) -> Any:
        _validate_message_target(recipient, contact_id, contact_name, conversation_id)

        return self._request(
            "POST",
            "/v1/messages/resolve",
            body={
                "recipient": recipient,
                "body": body,
                "deviceId": device_id,
                "nameContains": name_contains,
                "contactId": contact_id,
                "contactName": contact_name,
                "preferredNumber": preferred_number,
                "conversationId": conversation_id,
                "evictPhoneLink": evict_phone_link,
            },
        )

    def send_message(
        self,
        body: str,
        recipient: str | None = None,
        contact_id: str | None = None,
        contact_name: str | None = None,
        preferred_number: str | None = None,
        conversation_id: str | None = None,
        device_id: str | None = None,
        name_contains: str | None = None,
        evict_phone_link: bool = False,
        auto_sync_after_send: bool = True,
    ) -> Any:
        if not body:
            raise ValueError("body is required")

        _validate_message_target(recipient, contact_id, contact_name, conversation_id)

        return self._request(
            "POST",
            "/v1/messages/send",
            body={
                "recipient": recipient,
                "body": body,
                "deviceId": device_id,
                "nameContains": name_contains,
                "contactId": contact_id,
                "contactName": contact_name,
                "preferredNumber": preferred_number,
                "conversationId": conversation_id,
                "autoSyncAfterSend": auto_sync_after_send,
                "evictPhoneLink": evict_phone_link,
            },
        )

    def reply_to_conversation(
        self,
        conversation_id: str,
        body: str,
        *,
        device_id: str | None = None,
        name_contains: str | None = None,
        evict_phone_link: bool = False,
        auto_sync_after_send: bool = True,
    ) -> Any:
        if not conversation_id:
            raise ValueError("conversation_id is required")

        return self.send_message(
            body=body,
            conversation_id=conversation_id,
            device_id=device_id,
            name_contains=name_contains,
            evict_phone_link=evict_phone_link,
            auto_sync_after_send=auto_sync_after_send,
        )

    def _request(self, method: str, path: str, query: dict[str, Any] | None = None, body: Any = None) -> Any:
        url = _build_url(f"{self.base_url}{path}", query)
        data = None
        headers: dict[str, str] = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["content-type"] = "application/json"
        if self.auth_token:
            headers["authorization"] = f"Bearer {self.auth_token}"

        req = request.Request(url, data=data, method=method, headers=headers)

        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                return _read_payload(response.read())
        except error.HTTPError as exc:
            payload = _read_payload(exc.read())
            message = payload.get("error") if isinstance(payload, dict) else None
            raise AditError(
                message or f"Adit request failed with status {exc.code}.",
                status=exc.code,
                body=payload,
            ) from exc
        except error.URLError as exc:
            raise AditError(f"Adit request failed: {exc.reason}") from exc


def create_client(
    base_url: str | None = None,
    timeout: float = 30.0,
    auth_token: str | None = None,
) -> AditClient:
    return AditClient(base_url=base_url, timeout=timeout, auth_token=auth_token)


def _validate_message_target(
    recipient: str | None,
    contact_id: str | None,
    contact_name: str | None,
    conversation_id: str | None,
) -> None:
    if not recipient and not contact_id and not contact_name and not conversation_id:
        raise ValueError("recipient, contact_id, contact_name, or conversation_id is required")


def _build_url(base_url: str, query: dict[str, Any] | None) -> str:
    if not query:
        return base_url

    filtered: dict[str, str] = {}
    for key, value in query.items():
        if value is None or value == "":
            continue

        if isinstance(value, bool):
            filtered[key] = "true" if value else "false"
            continue

        filtered[key] = str(value)

    if not filtered:
        return base_url

    return f"{base_url}?{parse.urlencode(filtered)}"


def _read_payload(raw: bytes) -> Any:
    if not raw:
        return None

    text = raw.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _normalize_optional_string(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None
