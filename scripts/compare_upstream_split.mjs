// Explicit offline-only comparison. Imports only the reviewed, pinned Humanizer.
import fs from 'node:fs';
import { createHash } from 'node:crypto';
const reference = new URL('../third_party/ai-companion-kit/humanize.reference.mjs', import.meta.url);
const manifest = JSON.parse(fs.readFileSync(new URL('../third_party/ai-companion-kit/snapshot.json', import.meta.url), 'utf8').replace(/^\uFEFF/, ''));
const expected = manifest.files.find(f => f.path === 'packages/companion/src/humanize.js').sha256.toLowerCase();
if (createHash('sha256').update(fs.readFileSync(reference)).digest('hex') !== expected) throw new Error('Reference hash mismatch');
const { Humanizer } = await import(reference.href);
const samples = [
  {id:'short',text:'你好呀。',same:true},
  {id:'paragraphs',text:'今天终于忙完啦。\n那就好好歇一会儿。\n我陪你慢慢聊。',same:true},
  {id:'tiny',text:'嗯\n好呀\n那就这么说定啦',same:true},
  {id:'cap',text:'第一件事情终于做完啦。\n第二件事情也不用急。\n第三件事情明天再说。\n第四件事情先放着吧。',same:true},
  {id:'emoji',text:'🌿今天辛苦啦\n可以歇一会儿了',same:true},
  {id:'plain_long',text:'今天的事情终于都告一段落接下来就想安安静静坐一会儿不做什么特别的安排也挺好慢慢歇一歇就好',same:true},
  {id:'link',text:'这是地址\nhttps://example.com/a?x=1!y=2',same:false},
  {id:'code',text:'```python\nprint(1)\n```',same:false},
  {id:'quote',text:'她说：“今天的事情终于做完了，想好好歇一会儿。”然后又笑着补了一句，明天的事情明天再说吧。',same:false},
];
const human = new Humanizer({burstMax:3});
console.log(JSON.stringify(samples.map(s=>({...s,upstream:human.split(s.text)}))));
