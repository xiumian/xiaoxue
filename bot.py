"""Local QQ companion. Run from the project virtual environment."""
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from websockets.asyncio.server import serve

from app.persona import load_persona
from app.store import Store
from app.transport import OneBotTransport, DeliveryError

ROOT = Path(__file__).resolve().parent
TZ = timezone(timedelta(hours=8))


def private_text(e):
    if e.get('post_type') != 'message' or e.get('message_type') != 'private':
        return None
    uid = e.get('user_id')
    if not isinstance(uid, int) or uid <= 0 or uid == e.get('self_id'):
        return None
    parts = e.get('message', [])
    if not isinstance(parts, list):
        return None
    text = ''.join(p.get('data', {}).get('text', '') for p in parts
                   if isinstance(p, dict) and p.get('type') == 'text').strip()
    return (uid, text[:4000]) if text else None


def config():
    cfg = dict(line.split('=', 1) for line in (ROOT / '.env').read_text(encoding='utf-8-sig').splitlines()
               if '=' in line and not line.startswith('#'))
    for key in ('DEEPSEEK_API_KEY', 'DEEPSEEK_BASE_URL', 'ONEBOT_ACCESS_TOKEN', 'ALLOWED_QQ_USERS'):
        if not cfg.get(key, '').strip():
            raise ValueError('Missing configuration: ' + key)
    if cfg['DEEPSEEK_BASE_URL'].rstrip('/') != 'https://api.deepseek.com':
        raise ValueError('Unexpected provider endpoint')
    return cfg


class Bot:
    def __init__(self, cfg, store=None):
        self.cfg = cfg
        self.allowed = {int(uid.strip()) for uid in cfg.get('ALLOWED_QQ_USERS', '').split(',') if uid.strip()}
        if not self.allowed:
            raise ValueError('Allowed QQ users required')
        self.daily_limit = int(cfg.get('DAILY_LIMIT', '100'))
        if not 1 <= self.daily_limit <= 10000:
            raise ValueError('Invalid daily limit')
        self.store = store if store is not None else Store(ROOT / 'data' / 'companion.sqlite3')
        self.persona = load_persona()
        self.client = httpx.AsyncClient(timeout=60)
        self.queues, self.workers = {}, {}
        self.slots = asyncio.Semaphore(3)
        self.active_ws = None

    async def complete(self, messages):
        r = await self.client.post(self.cfg['DEEPSEEK_BASE_URL'].rstrip('/') + '/chat/completions',
            headers={'Authorization': 'Bearer ' + self.cfg['DEEPSEEK_API_KEY']},
            json={'model': self.cfg.get('DEEPSEEK_MODEL', 'deepseek-flash'),
                  'messages': messages, 'max_tokens': 400, 'stream': False})
        r.raise_for_status()
        reply = r.json()['choices'][0]['message']['content']
        if not isinstance(reply, str) or not reply.strip():
            raise ValueError('Empty model reply')
        return reply.strip()[:4000]

    def enqueue(self, e, transport):
        item = private_text(e)
        if item is None or item[0] not in self.allowed:
            return False
        uid, text = item
        account, mid = e.get('self_id'), e.get('message_id')
        if not isinstance(account, int) or mid is None:
            return False
        scope = f'qq:{account}:private:{uid}:xu_zhining'
        if self.store.state(scope, mid) is not None:
            return False
        queue = self.queues.setdefault(scope, asyncio.Queue(maxsize=30))
        if queue.full():
            raise asyncio.QueueFull
        if not self.store.claim(scope, mid, text):
            return False
        queue.put_nowait((uid, str(mid), text, transport))
        if scope not in self.workers or self.workers[scope].done():
            self.workers[scope] = asyncio.create_task(self.worker(scope, queue))
        return True

    async def worker(self, scope, queue):
        while not queue.empty():
            uid, mid, text, transport = queue.get_nowait()
            state = 'interrupted'
            reply = None
            try:
                async with self.slots:
                    self.store.finish(scope, mid, None, 'processing')
                    self.store.cleanup()
                    if self.store.reserve_usage(datetime.now(TZ).date().isoformat(), scope, self.daily_limit):
                        try:
                            reply = await self.complete([{'role': 'system', 'content': self.persona}]
                                                        + self.store.history(scope) + [{'role': 'user', 'content': text}])
                            normal = True
                            logging.info('MODEL_REPLY_OK')
                        except Exception as ex:
                            logging.warning('MODEL_ERROR type=%s', type(ex).__name__)
                            reply, normal = '这会儿连接有点问题，等一下再试试好吗？', False
                    else:
                        reply, normal = '今天的试运行额度已用完，我们明天再聊。', False
                    self.store.finish(scope, mid, reply, 'sending')
                    try:
                        await transport.send(uid, reply)
                        state = 'delivered' if normal else 'notice'
                        logging.info('SEND_RECEIPT_OK')
                    except DeliveryError as ex:
                        state = ex.state
                        logging.warning('SEND_RECEIPT_%s', state.upper())
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                state = 'failed'
                logging.warning('WORKER_ERROR type=%s', type(ex).__name__)
            finally:
                self.store.finish(scope, mid, reply, state)
                queue.task_done()

    async def drain(self):
        await asyncio.gather(*(q.join() for q in self.queues.values()))

    async def connection(self, ws):
        if ws.request.headers.get('Authorization') != 'Bearer ' + self.cfg['ONEBOT_ACCESS_TOKEN']:
            await ws.close(1008, 'Unauthorized')
            return
        if self.active_ws is not None:
            await ws.close(1013, 'Another connection is active')
            return
        self.active_ws = ws
        transport = OneBotTransport(ws)
        overload_task = None

        async def overload_notice(uid):
            try:
                await transport.send(uid, '你发的消息有点多啦，先让我回完前面的，再接着聊。刚才这一条请稍后重发。')
            except DeliveryError:
                logging.warning('OVERLOAD_NOTICE_UNCONFIRMED')

        logging.info('ONEBOT_CONNECTED persona=xu_zhining_v1')
        try:
            async for raw in ws:
                try:
                    e = json.loads(raw)
                    if not isinstance(e, dict):
                        continue
                    if 'echo' in e:
                        transport.response(e)
                    else:
                        self.enqueue(e, transport)
                except asyncio.QueueFull:
                    logging.warning('QUEUE_FULL retry_required')
                    if overload_task is None or overload_task.done():
                        overload_task = asyncio.create_task(overload_notice(e['user_id']))
                except (ValueError, TypeError, AttributeError):
                    logging.warning('INVALID_EVENT')
        finally:
            if overload_task is not None:
                overload_task.cancel()
                await asyncio.gather(overload_task, return_exceptions=True)
            transport.disconnect()
            for task in self.workers.values():
                task.cancel()
            await asyncio.gather(*self.workers.values(), return_exceptions=True)
            for scope, queue in self.queues.items():
                while not queue.empty():
                    _, mid, _, _ = queue.get_nowait()
                    self.store.finish(scope, mid, None, 'interrupted')
                    queue.task_done()
            self.active_ws = None
            logging.info('ONEBOT_DISCONNECTED')

    async def close(self):
        for task in self.workers.values():
            task.cancel()
        await asyncio.gather(*self.workers.values(), return_exceptions=True)
        await self.client.aclose()
        self.store.close()


async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    bot = Bot(config())
    try:
        async with serve(bot.connection, '127.0.0.1', 6199, max_size=1024 * 1024):
            logging.info('READY localhost:6199 persona=xu_zhining_v1; recent history persisted')
            await asyncio.Future()
    finally:
        await bot.close()


if __name__ == '__main__':
    asyncio.run(main())
