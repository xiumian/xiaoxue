import asyncio
import json
import uuid
from websockets.exceptions import ConnectionClosed


class DeliveryError(Exception):
    def __init__(self, state):
        self.state = state
        super().__init__(state)


class OneBotTransport:
    def __init__(self, ws, timeout=15):
        self.ws, self.timeout = ws, timeout
        self.pending = {}

    def response(self, event):
        future = self.pending.get(str(event.get('echo')))
        if future and not future.done():
            future.set_result(event)

    async def send(self, uid, text):
        echo = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[echo] = future
        try:
            await self.ws.send(json.dumps({'action': 'send_private_msg', 'params': {
                'user_id': uid, 'message': [{'type': 'text', 'data': {'text': text}}]}, 'echo': echo}, ensure_ascii=False))
            result = await asyncio.wait_for(future, self.timeout)
            if result.get('status') != 'ok' or result.get('retcode') != 0:
                raise DeliveryError('failed')
            if not isinstance(result.get('data'), dict) or result['data'].get('message_id') is None:
                raise DeliveryError('unknown')
            return result['data']['message_id']
        except (TimeoutError, OSError, ConnectionClosed):
            raise DeliveryError('unknown') from None
        finally:
            self.pending.pop(echo, None)

    def disconnect(self):
        for future in self.pending.values():
            if not future.done():
                future.set_exception(DeliveryError('unknown'))
