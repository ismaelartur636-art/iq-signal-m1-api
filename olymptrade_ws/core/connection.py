import asyncio
import json
import logging
from typing import Optional

import websockets

logger = logging.getLogger(__name__)


class Connection:
    def __init__(
        self,
        uri: str,
        access_token: str,
        message_queue: asyncio.Queue,
        connection_lost_handler=None,
    ):
        self.uri = uri
        self.access_token = access_token
        self.message_queue = message_queue
        self.connection_lost_handler = connection_lost_handler
        self.websocket = None
        self.connected = False

    async def connect(self):
        if not self.access_token:
            raise ConnectionError("Token de acesso não informado.")

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Origin": "https://olymptrade.com",
        }

        try:
            self.websocket = await websockets.connect(
                self.uri,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            )

            self.connected = True
            logger.info("Olymp Trade WebSocket conectado.")

            asyncio.create_task(self._receive_loop())

            return True

        except Exception as exc:
            self.connected = False
            logger.error("Erro ao conectar WebSocket Olymp Trade: %s", exc)
            raise ConnectionError(str(exc)) from exc

    async def _receive_loop(self):
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                except Exception:
                    data = message

                await self.message_queue.put(data)

        except Exception as exc:
            logger.error("Conexão Olymp Trade encerrada: %s", exc)

        finally:
            self.connected = False

            if self.connection_lost_handler:
                result = self.connection_lost_handler()

                if asyncio.iscoroutine(result):
                    await result

    async def send(self, data):
        if not self.websocket or not self.connected:
            raise ConnectionError("WebSocket Olymp Trade não conectado.")

        if isinstance(data, (dict, list)):
            data = json.dumps(data)

        await self.websocket.send(data)

    async def disconnect(self):
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass

        self.websocket = None
        self.connected = False

    def is_connected(self):
        return self.connected
