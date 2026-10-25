"""Adapted from ai-companion-kit Humanizer.split (MIT).

Upstream commit: f9d4d54b5abbf7ccd926bd0bf4fc40f384868578.
Copyright (c) 2026 AI Companion Kit contributors.
See third_party/ai-companion-kit/LICENSE and NOTICE.md.
"""
import re


def split_reply(text, max_parts=3, enabled=True):
    raw = str(text).strip()
    if not raw:
        return []
    if not 1 <= max_parts <= 4:
        raise ValueError('max_parts must be between 1 and 4')
    # Do not fragment code, links, or formatting where line breaks carry meaning.
    if not enabled or max_parts == 1 or re.search(r'https?://|```|^\s*(?:[-*]|\d+[.)])\s', raw, re.M):
        return [raw]
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if len(line) <= 40:
            out.append(line)
        else:
            # Unlike upstream, closing quotes remain attached and ellipses stay intact.
            out.extend(part.strip() for part in re.findall(r'.+?(?:[。！？!?]+[”’」』）)]*|…{2,}[”’」』）)]*|$)', line) if part.strip())
    merged = []
    for part in out:
        if merged and len(merged[-1]) < 6 and len(merged[-1]) + len(part) <= 24:
            merged[-1] += part
        else:
            merged.append(part)
    if len(merged) > max_parts:
        merged = merged[:max_parts - 1] + [' '.join(merged[max_parts - 1:])]
    return merged
