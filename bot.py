"""Local-only QQ private chat prototype. No tools or command execution."""
import asyncio
import json
import logging
import time
from collections import OrderedDict
from pathlib import Path
import httpx
from websockets.asyncio.server import serve

ROOT = Path(__file__).resolve().parent
SYSTEM = '你叫小禾，是温柔自然的AI陪伴伙伴。用简体中文口语简短回复，先理解情绪，不机械说教，不每次追问。不冒充真人，不编造共同经历，不制造依赖。尚无长期记忆，不声称重启后记得。不要主动称陌生用户为主人。'

def private_text(e):
    if e.get('post_type') != 'message' or e.get('message_type') != 'private':
        return None
    uid = e.get('user_id')
    if not uid or uid == e.get('self_id'):
        return None
    parts = e.get('message', [])
    if not isinstance(parts, list):
        return None
    text = ''.join(p.get('data', {}).get('text', '') for p in parts if p.get('type') == 'text').strip()
    return (uid, text[:4000]) if text else None

def config():
    return dict(line.split('=',1) for line in (ROOT/'.env').read_text(encoding='utf-8-sig').splitlines() if '=' in line and not line.startswith('#'))

class Bot:
    def __init__(self, cfg):
        self.cfg = cfg
        self.history = OrderedDict()
        self.seen = OrderedDict()
        self.count = 0
        self.day = time.strftime('%Y-%m-%d')
        self.busy = False
        self.client = httpx.AsyncClient(timeout=60)

    async def complete(self, messages):
        r = await self.client.post(self.cfg['DEEPSEEK_BASE_URL']+'/chat/completions', headers={'Authorization':'Bearer '+self.cfg['DEEPSEEK_API_KEY']}, json={'model':self.cfg.get('DEEPSEEK_MODEL','deepseek-flash'),'messages':messages,'max_tokens':300,'stream':False})
        r.raise_for_status()
        return r.json()['choices'][0]['message']['content']

    async def connection(self, ws):
        if ws.request.headers.get('Authorization') != 'Bearer '+self.cfg['ONEBOT_ACCESS_TOKEN']:
            await ws.close(1008, 'Unauthorized')
            return
        logging.info('ONEBOT_CONNECTED')
        async for raw in ws:
            try:
                e=json.loads(raw)
                if 'echo' in e:
                    logging.info('SEND_RECEIPT status=%s retcode=%s', e.get('status'),e.get('retcode'))
                    continue
                item=private_text(e)
                if item is None:
                    continue
                mid=e.get('message_id')
                if mid is None or mid in self.seen:
                    continue
                self.seen[mid]=True
                if len(self.seen)>1000:
                    self.seen.popitem(last=False)
                if self.busy:
                    continue
                self.busy=True
                try:
                    uid,text=item
                    today=time.strftime('%Y-%m-%d')
                    if today!=self.day:
                        self.day,self.count=today,0
                    if self.count>=100:
                        reply='今天的试运行额度已用完，我们明天再聊。'
                    else:
                        self.count+=1
                        history=self.history.get(uid,[])
                        messages=[{'role':'system','content':SYSTEM}]+history+[{'role':'user','content':text}]
                        try:
                            reply=await self.complete(messages)
                            self.history[uid]=(history+[{'role':'user','content':text},{'role':'assistant','content':reply}])[-12:]
                            self.history.move_to_end(uid)
                            if len(self.history)>100:
                                self.history.popitem(last=False)
                            logging.info('MODEL_REPLY_OK')
                        except Exception as ex:
                            logging.warning('MODEL_ERROR type=%s',type(ex).__name__)
                            reply='这会儿连接有点问题，等一下再试试好吗？'
                    await ws.send(json.dumps({'action':'send_private_msg','params':{'user_id':uid,'message':[{'type':'text','data':{'text':reply}}]},'echo':str(mid)},ensure_ascii=False))
                    logging.info('SEND_REQUESTED')
                finally:
                    self.busy=False
            except Exception as ex:
                logging.warning('EVENT_ERROR type=%s',type(ex).__name__)
        logging.info('ONEBOT_DISCONNECTED')

async def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    bot=Bot(config())
    try:
        async with serve(bot.connection,'127.0.0.1',6199,max_size=1024*1024):
            logging.info('READY localhost:6199; private only; session memory only')
            await asyncio.Future()
    finally:
        await bot.client.aclose()

if __name__=='__main__':
    asyncio.run(main())
