# AI Companion Kit attribution

Repository: https://github.com/pinganbbOvO/ai-companion-kit
Pinned commit: f9d4d54b5abbf7ccd926bd0bf4fc40f384868578
License: MIT, copyright (c) 2026 AI Companion Kit contributors. Full notice in LICENSE.

`app/humanize.py` adapts `packages/companion/src/humanize.js`'s split method:
newline/sentence segmentation, merging tiny neighbors, and limiting burst count.
Changes: maximum 3 parts by default; preserve closing quotes/ellipses; avoid splitting
links, fenced code and lists; split-off means one original message; no fake typing,
random long delays or time-of-day slowdown. This is an adaptation, not an unmodified port.

`humanize.reference.mjs` is an unmodified upstream snapshot used ONLY by an explicit
offline comparison script. The running Python bot does not import or execute it.
Its hash is recorded in snapshot.json, alongside other reviewed source hashes.

Message batching adopts the upstream debounced input idea, implemented independently
for our existing asyncio queue with an 8-message/4000-character cap and hard deadline.
We do not import their shared memory, installers, phone tools, or entire agent runtime.
Role samples are locally authored; the base role/identity/relationship remains ours.
