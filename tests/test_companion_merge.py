import asyncio
from pathlib import Path
import tempfile
import unittest

from app.humanize import split_reply
from app.store import Store
from app.transport import DeliveryError
from bot import Bot


def event(mid, text, uid=123):
    return {'post_type': 'message', 'message_type': 'private', 'self_id': 456,
            'user_id': uid, 'message_id': mid,
            'message': [{'type': 'text', 'data': {'text': text}}]}


class SplitTests(unittest.TestCase):
    def test_short_and_disabled(self):
        self.assertEqual(split_reply('你好呀。'), ['你好呀。'])
        self.assertEqual(split_reply('你好呀\n忙完啦', enabled=False), ['你好呀\n忙完啦'])
        self.assertEqual(split_reply('  '), [])

    def test_max_parts_and_no_lost_characters(self):
        text = '今天终于忙完了呀。\n先把手里的事情放一放吧。\n我陪你慢慢聊一会儿。\n不用急着想下一件事情。'
        parts = split_reply(text, max_parts=3)
        self.assertEqual(len(parts), 3)
        self.assertEqual(''.join(parts).replace(' ', ''), text.replace('\n', ''))

    def test_protect_links_and_code(self):
        for text in ['这是地址\nhttps://example.com/a?x=1!y=2', '```python\nprint(1)\n```']:
            self.assertEqual(split_reply(text), [text])

    def test_closing_quote_stays_with_sentence(self):
        text = '她说：“今天的事情终于做完了，想好好歇一会儿。”然后又笑着补了一句，明天的事情明天再说吧。'
        parts = split_reply(text)
        self.assertFalse(any(p.startswith('”') for p in parts))
        self.assertEqual(''.join(parts).replace(' ', ''), text)


class CaptureTransport:
    def __init__(self, fail_at=None):
        self.sent = []
        self.fail_at = fail_at

    async def send(self, uid, text):
        self.sent.append((uid, text))
        if len(self.sent) == self.fail_at:
            raise DeliveryError('unknown')
        return len(self.sent)


class MergeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'test.sqlite3'
        self.store = Store(self.path)
        self.bot = Bot({'ALLOWED_QQ_USERS': '123,124', 'MERGE_WINDOW_MS': '30',
                        'MERGE_MAX_WAIT_MS': '100', 'SPLIT_ENABLED': 'true',
                        'SEND_GAP_MS': '0'}, store=self.store)
        self.calls = []

        async def complete(messages):
            self.calls.append(messages)
            return '今天终于忙完啦。\n那就好好歇一会儿。\n我陪你慢慢聊。'

        self.bot.complete = complete
        self.transport = CaptureTransport()

    async def asyncTearDown(self):
        await self.bot.close()
        self.temp.cleanup()

    async def test_burst_merges_and_all_ids_are_deduplicated(self):
        for mid, text in [(1, '我今天'), (2, '终于忙完了'), (3, '好累呀')]:
            self.bot.enqueue(event(mid, text), self.transport)
        await self.bot.drain()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][-1]['content'], '我今天\n终于忙完了\n好累呀')
        self.assertEqual(len(self.store.history('qq:456:private:123:xu_zhining')), 2)
        self.assertFalse(self.bot.enqueue(event(2, '重复消息'), self.transport))
        self.assertEqual(len(self.transport.sent), 3)

    async def test_users_are_separate(self):
        self.bot.enqueue(event(1, '甲的信息'), self.transport)
        self.bot.enqueue(event(2, '乙的信息', uid=124), self.transport)
        await self.bot.drain()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual({m[-1]['content'] for m in self.calls}, {'甲的信息', '乙的信息'})

    async def test_partial_delivery_only_remembers_confirmed_prefix(self):
        self.transport.fail_at = 2
        self.bot.enqueue(event(1, '你好'), self.transport)
        await self.bot.drain()
        self.assertEqual(len(self.transport.sent), 2)
        history = self.store.history('qq:456:private:123:xu_zhining')
        self.assertEqual(history[-1]['content'], '今天终于忙完啦。')
        rows = self.store.delivery_parts('qq:456:private:123:xu_zhining', '1')
        self.assertEqual([row['state'] for row in rows], ['delivered', 'unknown', 'not_sent'])

    async def test_batch_limit_does_not_discard_later_messages(self):
        for mid in range(10):
            self.bot.enqueue(event(mid, str(mid)), self.transport)
        await self.bot.drain()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0][-1]['content'], '\n'.join(map(str, range(8))))
        self.assertEqual(self.calls[1][-1]['content'], '8\n9')

    async def test_switches_restore_single_messages(self):
        self.bot.merge_window = 0
        self.bot.split_enabled = False
        self.bot.enqueue(event(1, '第一条'), self.transport)
        self.bot.enqueue(event(2, '第二条'), self.transport)
        await self.bot.drain()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(len(self.transport.sent), 2)

    async def test_oversize_batch_keeps_both_inputs(self):
        first = '甲' * 3000
        second = '乙' * 2000
        self.bot.enqueue(event(1, first), self.transport)
        self.bot.enqueue(event(2, second), self.transport)
        await self.bot.drain()
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0][-1]['content'], first)
        self.assertEqual(self.calls[1][-1]['content'], second)

    async def test_partial_history_survives_restart(self):
        self.transport.fail_at = 2
        self.bot.enqueue(event(1, '你好'), self.transport)
        await self.bot.drain()
        await self.bot.close()
        self.store = Store(self.path)
        self.bot = Bot({'ALLOWED_QQ_USERS': '123'}, store=self.store)
        self.assertEqual(self.store.history('qq:456:private:123:xu_zhining')[-1]['content'], '今天终于忙完啦。')

    async def test_cancellation_keeps_sent_prefix_and_marks_inflight_unknown(self):
        first_sent = asyncio.Event()
        second_started = asyncio.Event()

        async def send(uid, text):
            self.transport.sent.append((uid,text))
            if len(self.transport.sent) == 1:
                first_sent.set()
                return 1
            second_started.set()
            await asyncio.Future()

        self.transport.send = send
        self.bot.enqueue(event(1,'你好'), self.transport)
        await asyncio.wait_for(first_sent.wait(),2)
        await asyncio.wait_for(second_started.wait(),2)
        task = next(iter(self.bot.workers.values()))
        task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        key='qq:456:private:123:xu_zhining'
        self.assertEqual(self.store.state(key,'1'),'partial')
        self.assertEqual([r['state'] for r in self.store.delivery_parts(key,'1')],['delivered','unknown','not_sent'])

    async def test_restart_during_split_recovers_only_acknowledged_part(self):
        key='qq:456:private:123:xu_zhining'
        self.store.claim(key,'1','你好')
        self.store.prepare_batch(key,['1'],'你好')
        self.store.prepare_parts(key,'1',['已送达','未确定','未发送'])
        self.store.mark_part(key,'1',0,'delivered',99)
        self.store.mark_part(key,'1',1,'sending')
        self.store.finish(key,'1',None,'sending')
        await self.bot.close()
        self.store=Store(self.path)
        self.bot=Bot({'ALLOWED_QQ_USERS':'123'},store=self.store)
        self.assertEqual(self.store.history(key)[-1]['content'],'已送达')
        self.assertEqual(self.store.delivery_parts(key,'1')[1]['state'],'unknown')

    async def test_real_websocket_batch_and_split(self):
        import json
        from websockets.asyncio.server import serve
        from websockets.asyncio.client import connect
        self.bot.cfg['ONEBOT_ACCESS_TOKEN']='local-integration-token'
        async with serve(self.bot.connection,'127.0.0.1',0) as server:
            url='ws://127.0.0.1:'+str(server.sockets[0].getsockname()[1])
            async with connect(url,additional_headers={'Authorization':'Bearer local-integration-token'}) as ws:
                for mid,text in [(1,'今天'),(2,'好累呀')]:
                    await ws.send(json.dumps(event(mid,text)))
                for i in range(3):
                    request=json.loads(await asyncio.wait_for(ws.recv(),2))
                    await ws.send(json.dumps({'echo':request['echo'],'status':'ok','retcode':0,'data':{'message_id':100+i}}))
                await asyncio.wait_for(self.bot.drain(),2)
                self.assertEqual(len(self.calls),1)
                self.assertEqual(self.calls[0][-1]['content'],'今天\n好累呀')
                self.assertEqual([r['state'] for r in self.store.delivery_parts('qq:456:private:123:xu_zhining','1')],['delivered']*3)


if __name__ == '__main__':
    unittest.main()
