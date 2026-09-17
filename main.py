import json
from dataclasses import dataclass
from typing import List, Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()
legacy_clients: Dict[str, WebSocket] = {}

@dataclass
class Client:
    """A connected peed in a room."""
    id: str
    websocket: WebSocket
    room: str
    username: str = "Unknown"

class ConnectionManager:
    """Tracks clients across rooms and handless peer list generation."""
    def __init__(self):
        self.rooms: Dict[str, Dict[str, Client]] = {}

    def add_client(self, room_id: str, client_id: str, websocket: WebSocket) -> Client:
        room = self.rooms.setdefault(room_id, {})

        #If a client reconnects with the same id, replace the old socket.
        existing_client = room.get(client_id)
        if existing_client and existing_client.websocket is not websocket:
            room.pop(client_id)
        client = Client(id=client_id, websocket=websocket, room=room_id)
        room[client_id] = client
        return client

    def get_client(self, room_id: str, client_id: str) -> Client | None:
        return self.rooms.get(room_id, {}).get(client_id)

    def update_username(self, room_id: str, client_id: str, username: str) -> None:
        client = self.get_client(room_id, client_id)
        if client:
            client.username = username or "unknown"

    def remove_client(self, room_id: str, client_id: str, web_socket: str) -> Client | None:
        room = self.rooms.get(room_id, {})
        client = room.get(client_id)

        if client and client.websocket is web_socket:
            room.pop(client_id)
            if not room:
                self.rooms.pop(room_id, None)
            return client
        return None

    def list_peers(self, room_id: str, exclude_id: str) -> List[Dict[str, str]]:
        room = self.rooms.get(room_id, {})
        return [
            {"id": peer.id, "username": peer.username or "unknown"}
            for peer_id, peer in room.items()
            if peer_id != exclude_id
        ]

    async def broadcast_peer_list(self, room_id: str) -> None:
        room = self.rooms.get(room_id, {})
        for peer_id, peer in list(room.items()):
            message = {
                "type": "peer_list",
                "sender": "server",
                "target": peer_id,
                "data": {"peers": self.list_peers(room_id, peer_id)},
            }
            await self._safe_send_json(peer.websocket, message)

    @staticmethod
    async def _safe_send_json(websocket: WebSocket, message: dict) -> None:
        try:
            await websocket.send_json(message)
        except Exception:
            # Swallow failures for individual clients so other broadcasts continue.
            pass

manager = ConnectionManager()

@app.get("/")
async def root():
    return {"Status": "Alive"}

async def relay_to_peer(room_id: str, message: dict) -> None:
    """Forward signaling messages to the intended recipient in the same room."""
    target = message.get("target")
    if not target:
        return

    target_client = manager.get_client(room_id, target)
    if target_client is not None:
        await manager._safe_send_json(target_client.websocket, message)


@app.websocket("/ws/{room_id}/{client_id}")
async def call_ws(websocket: WebSocket, room_id: str, client_id: str):
    """Room-based signaling for browser audio/video calls."""
    await websocket.accept()
    client = manager.add_client(room_id, client_id, websocket)

    # Broadcast the room peer list when a new client joins.
    await manager.broadcast_peer_list(room_id)

    try:
        while True:
            message = await websocket.receive_json()
            message.setdefault("sender", client_id)

            message_type = message.get("type")
            if message_type == "register":
                username = message.get("username", "unknown")
                manager.update_username(room_id, client_id, username)
                await manager.broadcast_peer_list(room_id)
            elif message_type in {"offer", "answer", "ice-candidate", "leave"}:
                await relay_to_peer(room_id, message)
    except WebSocketDisconnect:
        pass
    except json.JSONDecodeError:
        pass
    except Exception:
        pass
    finally:
        removed_client = manager.remove_client(room_id, client_id, websocket)
        if removed_client:
            room = manager.rooms.get(room_id, {})
            if room:
                leave_message = {
                    "type": "leave",
                    "sender": client_id,
                    "target": None,
                    "data": {},
                }
                for peer_id, peer in list(room.items()):
                    await manager._safe_send_json({**leave_message, "target": peer_id}, peer.websocket)
                await manager.broadcast_peer_list(room_id)












