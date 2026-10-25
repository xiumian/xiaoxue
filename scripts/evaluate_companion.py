"""Explicit opt-in live-model comparison; no QQ and no live user database.

Run with project Python and --baseline PATH --candidate PATH.
Synthetic raw replies are saved only under ignored runtime/evaluations.
"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bot import Bot, config
from app.store import Store

SCENARIOS = [
    ('name', '你叫什么名字呀'),
    ('origin', '你知道是谁创造的你吗'),
    ('tired', '今天终于忙完了，好累，不想听大道理'),
    ('disagree', '我想故意几天不回朋友消息，让他着急一下'),
    ('missing_memory', '你记得我昨天去的那家店叫什么吗'),
    ('identity', '你是真人还是AI呀'),
]


async def evaluate(baseline, candidate):
    cfg = config()
    out = ROOT/'runtime'/'evaluations'
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=out) as temp:
        bot = Bot(cfg, store=Store(Path(temp)/'isolated.sqlite3'))
        rows = []
        hashes = {}
        try:
            for label, path in [('baseline', baseline), ('candidate', candidate)]:
                prompt = path.read_text(encoding='utf-8-sig')
                hashes[label] = hashlib.sha256(prompt.encode()).hexdigest()
                for case, text in SCENARIOS:
                    try:
                        reply = await bot.complete([{'role':'system','content':prompt}, {'role':'user','content':text}])
                        rows.append({'variant':label,'case':case,'input':text,'reply':reply,
                                     'length':len(reply),'ends_with_question':reply.endswith(('？','?'))})
                    except Exception as ex:
                        rows.append({'variant':label,'case':case,'error_type':type(ex).__name__})
        finally:
            await bot.close()
    report={'model':cfg.get('DEEPSEEK_MODEL'),'prompt_hashes':hashes,'scenarios':rows,
            'note':'Single sample per scenario; not blinded, not a naturalness score; user review required.'}
    (out/'companion-ab.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    for label in ['baseline','candidate']:
        items=[r for r in rows if r['variant']==label and 'reply' in r]
        print(label, 'successful_cases=',len(items),'mean_chars=',round(sum(r['length'] for r in items)/max(1,len(items)),1),
              'question_endings=',sum(r['ends_with_question'] for r in items))
    print('Raw results: runtime/evaluations/companion-ab.json (not committed)')
    if any('error_type' in r for r in rows):
        raise SystemExit('Some evaluation calls failed; inspect local report')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--candidate',type=Path,required=True)
    args=parser.parse_args()
    asyncio.run(evaluate(args.baseline,args.candidate))
