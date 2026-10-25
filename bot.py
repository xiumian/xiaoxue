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
from app.humanize import split_reply

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
        self.merge_window = int(cfg.get('MERGE_WINDOW_MS', '0')) / 1000
        self.merge_max_wait = int(cfg.get('MERGE_MAX_WAIT_MS', '3000')) / 1000
        self.split_enabled = cfg.get('SPLIT_ENABLED', 'false').lower() == 'true'
        self.send_gap = int(cfg.get('SEND_GAP_MS', '0')) / 1000
        if not 0 <= self.merge_window <= 2 or not self.merge_window <= self.merge_max_wait <= 5 or not 0 <= self.send_gap <= 2:
            raise ValueError('Invalid companion timing configuration')
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
        carry = None
        try:
            while carry is not None or not queue.empty():
                batch = [carry if carry is not None else queue.get_nowait()]
                carry = None
                prepared = False
                try:
                    size = len(batch[0][2])
                    deadline = asyncio.get_running_loop().time() + self.merge_max_wait
                    while self.merge_window > 0 and len(batch) < 8:
                        remaining = min(self.merge_window, deadline - asyncio.get_running_loop().time())
                        if remaining <= 0:
                            break
                        try:
                            item = await asyncio.wait_for(queue.get(), remaining)
                        except TimeoutError:
                            break
                        if size + 1 + len(item[2]) > 4000:
                            carry = item
                            break
                        batch.append(item)
                        size += 1 + len(item[2])
                    mids = [item[1] for item in batch]
                    text = '\n'.join(item[2] for item in batch)
                    self.store.prepare_batch(scope, mids, text)
                    prepared = True
                    logging.info('BATCH_READY input_count=%s', len(batch))
                    await self.process_batch(scope, batch[0][0], mids[0], text, batch[0][3])
                finally:
                    if not prepared:
                        for _, mid, _, _ in batch:
                            self.store.finish(scope, mid, None, 'interrupted')
                    for _ in batch:
                        queue.task_done()
        finally:
            if carry is not None:
                self.store.finish(scope, carry[1], None, 'interrupted')
                queue.task_done()

    async def process_batch(self, scope, uid, mid, text, transport):
        state, normal = 'interrupted', False
        active_part = None
        try:
            async with self.slots:
                self.store.cleanup()
                if self.store.reserve_usage(datetime.now(TZ).date().isoformat(), scope, self.daily_limit):
                    try:
                        reply = await self.complete([{'role': 'system', 'content': self.persona}]
                                                    + self.store.history(scope) + [{'role': 'user', 'content': text}])
                        normal = True
                        logging.info('MODEL_REPLY_OK')
                    except Exception as ex:
                        logging.warning('MODEL_ERROR type=%s', type(ex).__name__)
                        reply = '这会儿连接有点问题，等一下再试试好吗？'
                else:
                    reply = '今天的试运行额度已用完，我们明天再聊。'
                parts = split_reply(reply, enabled=self.split_enabled and normal)
                self.store.prepare_parts(scope, mid, parts)
                self.store.finish(scope, mid, None, 'sending')
                for number, part in enumerate(parts):
                    if number and self.send_gap:
                        await asyncio.sleep(self.send_gap)
                    active_part = number
                    self.store.mark_part(scope, mid, number, 'sending')
                    try:
                        qq_mid = await transport.send(uid, part)
                        self.store.mark_part(scope, mid, number, 'delivered', qq_mid)
                        active_part = None
                        logging.info('SEND_RECEIPT_OK part=%s total=%s', number + 1, len(parts))
                    except DeliveryError as ex:
                        self.store.mark_part(scope, mid, number, ex.state)
                        active_part = None
                        state = ex.state
                        logging.warning('SEND_RECEIPT_%s part=%s', state.upper(), number + 1)
                        break
                else:
                    state = 'delivered' if normal else 'notice'
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            state = 'failed'
            logging.warning('WORKER_ERROR type=%s', type(ex).__name__)
        finally:
            if active_part is not None:
                self.store.mark_part(scope, mid, active_part, 'unknown')
            prefix = self.store.confirmed_prefix(scope, mid)
            if normal and prefix and state != 'delivered':
                state = 'partial'
            self.store.finish(scope, mid, prefix or None, state)

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

        logging.info('ONEBOT_CONNECTED persona=xu_zhining_v2 merge_ms=%s split=%s', int(self.merge_window * 1000), self.split_enabled)
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
            logging.info('READY localhost:6199 persona=xu_zhining_v2; recent history persisted; merge_ms=%s split=%s', int(bot.merge_window * 1000), bot.split_enabled)
            await asyncio.Future()
    finally:
        await bot.close()


if __name__ == '__main__':
    asyncio.run(main())
