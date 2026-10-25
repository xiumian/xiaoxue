import asyncio
import json
from pathlib import Path
import tempfile
import unittest

from app.persona import load_persona
from app.store import Store
from app.transport import OneBotTransport, DeliveryError
from bot import Bot
from websockets.asyncio.server import serve
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


def event(mid, user=123, bot=456, text='你好'):
    return {'post_type': 'message', 'message_type': 'private', 'user_id': user,
            'self_id': bot, 'message_id': mid,
            'message': [{'type': 'text', 'data': {'text': text}}]}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'test.db'
        self.store = Store(self.path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_persona_is_real_configuration(self):
        prompt = load_persona()
        self.assertIn('许知宁', prompt)
        self.assertIn('21', prompt)
        self.assertIn('明确确认', prompt)

    def test_restart_keeps_delivered_history_and_quota(self):
        key = 'qq:456:private:123:xu_zhining'
        self.assertTrue(self.store.claim(key, '1', '你好'))
        self.assertTrue(self.store.reserve_usage('2026-10-25', key, 1))
        self.store.finish(key, '1', '你好呀', 'delivered')
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(len(self.store.history(key)), 2)
        self.assertFalse(self.store.reserve_usage('2026-10-25', key, 1))
        self.assertFalse(self.store.claim(key, '1', '重复'))
        self.assertEqual(self.store.history('other'), [])
        self.assertTrue(self.store.reserve_usage('2026-10-26', key, 1))

    def test_failed_or_uncertain_delivery_not_history(self):
        for mid, state in [('1', 'failed'), ('2', 'unknown')]:
            self.store.claim('a', mid, '你好')
            self.store.finish('a', mid, '回复', state)
        self.assertEqual(self.store.history('a'), [])

    def test_restart_marks_unfinished_interrupted(self):
        self.store.claim('a', '1', '你好')
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.state('a', '1'), 'interrupted')
        self.assertFalse(self.store.claim('a', '1', '你好'))


class FakeSocket:
    def __init__(self):
        self.sent = []
        self.transport = None
        self.success = True

    async def send(self, raw):
        payload = json.loads(raw)
        self.sent.append(payload)
        if self.transport:
            self.transport.response({'echo': payload['echo'], 'status': 'ok' if self.success else 'failed',
                                     'retcode': 0 if self.success else 100, 'data': {'message_id': 42}})


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'test.db')
        self.bot = Bot({'ALLOWED_QQ_USERS': '123,124', 'DAILY_LIMIT': '100'}, store=self.store)
        self.socket = FakeSocket()
        self.transport = OneBotTransport(self.socket, timeout=0.1)
        self.socket.transport = self.transport
        self.calls = []

        async def complete(messages):
            self.calls.append(messages)
            await asyncio.sleep(0.01)
            return '你好呀'

        self.bot.complete = complete

    async def asyncTearDown(self):
        await self.bot.close()
        self.tmp.cleanup()

    async def test_authorization_and_group_do_not_invoke_model(self):
        self.assertFalse(self.bot.enqueue(event(1, user=999), self.transport))
        group = event(2)
        group['message_type'] = 'group'
        self.assertFalse(self.bot.enqueue(group, self.transport))
        await self.bot.drain()
        self.assertEqual(self.calls, [])

    async def test_queue_dedup_persona_and_user_isolation(self):
        for mid in [1, 2, 3]:
            self.assertTrue(self.bot.enqueue(event(mid), self.transport))
        self.assertFalse(self.bot.enqueue(event(1), self.transport))
        self.assertTrue(self.bot.enqueue(event(4, user=124), self.transport))
        await self.bot.drain()
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(len(self.socket.sent), 4)
        self.assertTrue(all('许知宁' in call[0]['content'] for call in self.calls))
        self.assertEqual(len(self.store.history('qq:456:private:123:xu_zhining')), 6)
        other = [call for call in self.calls if len(call) == 2]
        self.assertEqual(len(other), 2)

    async def test_failed_receipt_not_recorded_as_shared_experience(self):
        self.socket.success = False
        self.bot.enqueue(event(1), self.transport)
        await self.bot.drain()
        key = 'qq:456:private:123:xu_zhining'
        self.assertEqual(self.store.history(key), [])
        self.assertEqual(self.store.state(key, '1'), 'failed')

    async def test_missing_receipt_is_unknown_and_no_retry(self):
        self.socket.transport = None
        with self.assertRaises(DeliveryError) as ctx:
            await self.transport.send(123, '测试')
        self.assertEqual(ctx.exception.state, 'unknown')
        self.assertEqual(len(self.socket.sent), 1)

    async def test_invalid_receipt_not_success(self):
        def invalid(payload):
            future = self.transport.pending.get(str(payload['echo']))
            if future:
                future.set_result({'status': 'ok'})
        self.transport.response = invalid
        with self.assertRaises(DeliveryError):
            await self.transport.send(123, '测试')

    async def test_quota_no_extra_model_call(self):
        self.bot.daily_limit = 1
        self.bot.enqueue(event(1), self.transport)
        self.bot.enqueue(event(2), self.transport)
        await self.bot.drain()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(self.socket.sent), 2)

    async def test_model_failure_not_history(self):
        async def fail(messages):
            raise RuntimeError('test failure')
        self.bot.complete = fail
        self.bot.enqueue(event(1), self.transport)
        await self.bot.drain()
        self.assertEqual(self.store.history('qq:456:private:123:xu_zhining'), [])

    async def test_full_websocket_loop_reads_receipts_while_generating(self):
        self.bot.cfg['ONEBOT_ACCESS_TOKEN'] = 'local-test-token'
        async with serve(self.bot.connection, '127.0.0.1', 0) as server:
            url = 'ws://127.0.0.1:' + str(server.sockets[0].getsockname()[1])
            async with connect(url, additional_headers={'Authorization': 'Bearer wrong'}) as ws:
                with self.assertRaises(ConnectionClosed):
                    await asyncio.wait_for(ws.recv(), 2)
            async with connect(url, additional_headers={'Authorization': 'Bearer local-test-token'}) as ws:
                for mid in [1, 2]:
                    await ws.send(json.dumps(event(mid)))
                for _ in range(2):
                    outgoing = json.loads(await asyncio.wait_for(ws.recv(), 2))
                    self.assertEqual(outgoing['action'], 'send_private_msg')
                    await ws.send(json.dumps({'echo': outgoing['echo'], 'status': 'ok',
                                              'retcode': 0, 'data': {'message_id': 99}}))
                await asyncio.wait_for(self.bot.drain(), 2)
                self.assertEqual(len(self.store.history('qq:456:private:123:xu_zhining')), 4)
                self.assertEqual(len(self.calls[1]), 4)

    async def test_queue_full_is_explicit_and_not_claimed(self):
        for mid in range(30):
            self.assertTrue(self.bot.enqueue(event(mid), self.transport))
        with self.assertRaises(asyncio.QueueFull):
            self.bot.enqueue(event(31), self.transport)
        self.assertIsNone(self.store.state('qq:456:private:123:xu_zhining', '31'))
        await self.bot.drain()
        self.assertEqual(len(self.calls), 30)


if __name__ == '__main__':
    unittest.main()
