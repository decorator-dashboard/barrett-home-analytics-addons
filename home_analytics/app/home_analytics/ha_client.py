from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import ssl
from typing import Any, Callable
from urllib import error as urlerror
from urllib import parse, request

import websocket


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class HAClient:
    base_url: str
    ws_url: str
    token: str
    timeout: int = 30

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: Any | None = None) -> Any:
        url = parse.urljoin(f"{self.base_url}/", path.lstrip("/"))
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        req = request.Request(url, data=body, method=method, headers=self._headers())
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read()
        except urlerror.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Home Assistant HTTP {exc.code}: {detail}") from exc
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))

    def get_config(self) -> dict[str, Any]:
        return self._request("GET", "/api/config")

    def get_states(self) -> list[dict[str, Any]]:
        return self._request("GET", "/api/states")

    def get_state(self, entity_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/states/{entity_id}")

    def call_service(self, domain: str, service: str, data: dict[str, Any] | None = None) -> Any:
        return self._request("POST", f"/api/services/{domain}/{service}", data or {})

    def publish_state(self, entity_id: str, state: Any, attributes: dict[str, Any] | None = None) -> Any:
        payload = {"state": str(state), "attributes": attributes or {}}
        return self._request("POST", f"/api/states/{entity_id}", payload)

    def notify(self, notify_service: str, title: str, message: str) -> None:
        if "." not in notify_service:
            raise ValueError(f"Invalid notify service: {notify_service}")
        domain, service = notify_service.split(".", 1)
        self.call_service(domain, service, {"title": title, "message": message})

    def ws_call(self, message: dict[str, Any]) -> Any:
        ws = self._connect_ws()
        try:
            ws.send(json.dumps(message))
            while True:
                reply = json.loads(ws.recv())
                if reply.get("id") == message["id"]:
                    if reply.get("success", True):
                        return reply.get("result")
                    raise RuntimeError(f"Home Assistant websocket call failed: {reply}")
        finally:
            ws.close()

    def _connect_ws(self) -> websocket.WebSocket:
        ws = websocket.create_connection(
            self.ws_url,
            timeout=self.timeout,
            sslopt={"cert_reqs": ssl.CERT_NONE},
        )
        auth_required = json.loads(ws.recv())
        if auth_required.get("type") != "auth_required":
            raise RuntimeError(f"Unexpected websocket handshake: {auth_required}")
        ws.send(json.dumps({"type": "auth", "access_token": self.token}))
        auth_response = json.loads(ws.recv())
        if auth_response.get("type") != "auth_ok":
            raise RuntimeError(f"Websocket auth failed: {auth_response}")
        return ws

    def get_entity_registry(self) -> list[dict[str, Any]]:
        return self.ws_call({"id": 101, "type": "config/entity_registry/list"}) or []

    def get_device_registry(self) -> list[dict[str, Any]]:
        return self.ws_call({"id": 102, "type": "config/device_registry/list"}) or []

    def subscribe_events(self, callback: Callable[[dict[str, Any]], None]) -> None:
        ws = self._connect_ws()
        try:
            ws.send(json.dumps({"id": 200, "type": "subscribe_events"}))
            ack = json.loads(ws.recv())
            if not ack.get("success"):
                raise RuntimeError(f"Event subscription failed: {ack}")
            while True:
                payload = json.loads(ws.recv())
                if payload.get("type") == "event" and payload.get("event"):
                    callback(payload["event"])
        finally:
            ws.close()

