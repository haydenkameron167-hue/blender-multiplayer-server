"""Internet WebSocket relay for Blender Online Collaborators.

Deploy behind HTTPS/WSS (Render, Railway, Fly.io, a VPS with a TLS proxy).
In-memory rooms only: everything disappears once the last person leaves.
"""
import asyncio
import json
import logging
import os
import secrets
import string
import uuid

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger('blender-online-relay')
ROOMS = {}
ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
MAX_USERS = 20
MAX_OBJECTS = 400
MAX_ROOM_BYTES = 20 * 1024 * 1024
MAX_PACKET_BYTES = 3 * 1024 * 1024


class Room:
    def __init__(self, code):
        self.code = code
        self.clients = {}  # id -> (websocket, nickname)
        self.objects = {}  # object id -> protocol packet
        self.positions = {}  # client id -> dict

    def peers(self, skip):
        return [dict(id=cid, username=name, **self.positions.get(cid, {}))
                for cid, (_, name) in self.clients.items() if cid != skip]


async def send(ws, data):
    await ws.send(json.dumps(data, separators=(',', ':'), ensure_ascii=False))


async def broadcast(room, data, omit=None):
    text = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    tasks = [ws.send(text) for cid, (ws, _) in room.clients.items() if cid != omit]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def new_code():
    for _ in range(100):
        code = ''.join(secrets.choice(ALPHABET) for _ in range(6))
        if code not in ROOMS:
            return code
    raise RuntimeError('Cannot allocate room code')


def valid_coordinates(value, count):
    return (isinstance(value, list) and len(value) == count
            and all(isinstance(v, (float, int)) and not isinstance(v, bool)
                    and -1000000 <= v <= 1000000 for v in value))


def valid_object(packet):
    if not isinstance(packet, dict):
        return False
    uid = packet.get('id')
    if not isinstance(uid, str) or len(uid) != 32 or any(c not in string.hexdigits for c in uid):
        return False
    if packet.get('kind') not in ('MESH', 'EMPTY'):
        return False
    if not all(valid_coordinates(packet.get(key), 3) for key in ('location', 'rotation', 'scale')):
        return False
    if packet['kind'] == 'MESH':
        v = packet.get('vertices')
        f = packet.get('faces')
        if not isinstance(v, list) or not isinstance(f, list) or len(v) > 4500 or len(f) > 9000:
            return False
        if not all(valid_coordinates(item, 3) for item in v):
            return False
        if not all(isinstance(face, list) and 3 <= len(face) <= 1024
                   and all(isinstance(index, int) and not isinstance(index, bool) and 0 <= index < len(v) for index in face)
                   for face in f):
            return False
    return True


async def handler(ws):
    room = None
    ident = uuid.uuid4().hex
    try:
        # Handshake message must be sent promptly.
        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        if not isinstance(data, dict) or data.get('type') not in ('create', 'join'):
            await send(ws, {'type': 'error', 'message': 'Send create or join first'})
            return
        nickname = str(data.get('username', 'Artist')).strip()[:32] or 'Artist'
        if data['type'] == 'create':
            code = new_code()
            room = Room(code)
            ROOMS[code] = room
        else:
            code = str(data.get('room', '')).strip().upper()
            room = ROOMS.get(code)
            if room is None:
                await send(ws, {'type': 'error', 'message': 'Room not found. Ask the host for a code.'})
                return
            if len(room.clients) >= MAX_USERS:
                await send(ws, {'type': 'error', 'message': 'Room is full (20 people).'})
                return
        await send(ws, {
            'type': 'created' if data['type'] == 'create' else 'joined',
            'id': ident,
            'room': room.code,
            'peers': room.peers(ident),
            'objects': list(room.objects.values()) if data['type'] == 'join' else [],
        })
        room.clients[ident] = (ws, nickname)
        await broadcast(room, {'type': 'peer_joined', 'peer': {'id': ident, 'username': nickname}}, omit=ident)
        LOG.info('%s joined room %s (%d clients)', ident[:8], room.code, len(room.clients))
        async for raw in ws:
            if not isinstance(raw, str) or len(raw) > MAX_PACKET_BYTES:
                await send(ws, {'type': 'error', 'message': 'Message too large'})
                continue
            try:
                data = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(data, dict):
                continue
            typ = data.get('type')
            if typ == 'presence':
                position, rotation = data.get('position'), data.get('rotation')
                if valid_coordinates(position, 3) and valid_coordinates(rotation, 4):
                    room.positions[ident] = {'position': position, 'rotation': rotation}
                    await broadcast(room, {'type': 'presence', 'id': ident,
                                           'username': room.clients[ident][1],
                                           'position': position, 'rotation': rotation}, omit=ident)
            elif typ == 'profile':
                nickname = str(data.get('username', '')).strip()[:32] or 'Artist'
                room.clients[ident] = (ws, nickname)
                await broadcast(room, {'type': 'profile', 'id': ident, 'username': nickname}, omit=ident)
            elif typ == 'object_upsert':
                obj = data.get('object')
                if not valid_object(obj):
                    continue
                if obj['id'] not in room.objects and len(room.objects) >= MAX_OBJECTS:
                    await send(ws, {'type': 'error', 'message': 'Room object limit reached'})
                    continue
                before = room.objects.get(obj['id'])
                room.objects[obj['id']] = obj
                if sum(len(json.dumps(x, separators=(',', ':'))) for x in room.objects.values()) > MAX_ROOM_BYTES:
                    if before is None:
                        del room.objects[obj['id']]
                    else:
                        room.objects[obj['id']] = before
                    await send(ws, {'type': 'error', 'message': 'Room size limit reached'})
                    continue
                await broadcast(room, {'type': 'object_upsert', 'object': obj}, omit=ident)
            elif typ == 'object_remove':
                object_id = data.get('id')
                if isinstance(object_id, str) and object_id in room.objects:
                    del room.objects[object_id]
                    await broadcast(room, {'type': 'object_remove', 'id': object_id}, omit=ident)
    except (ConnectionClosed, asyncio.TimeoutError):
        pass
    except Exception:
        LOG.exception('Error handling a client')
    finally:
        if room is not None and ident in room.clients:
            del room.clients[ident]
            room.positions.pop(ident, None)
            await broadcast(room, {'type': 'peer_left', 'id': ident})
            if not room.clients:
                ROOMS.pop(room.code, None)
            LOG.info('%s left room %s', ident[:8], room.code)
        elif room is not None and not room.clients:
            ROOMS.pop(room.code, None)


async def main():
    port = int(os.environ.get('PORT', '8765'))
    host = os.environ.get('HOST', '0.0.0.0')
    async with serve(handler, host, port, max_size=32 * 1024 * 1024,
                     ping_interval=20, ping_timeout=20) as server:
        LOG.info('Relay listening on %s:%d', host, port)
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
