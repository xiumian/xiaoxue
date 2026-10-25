from pathlib import Path

DEFAULT = Path(__file__).resolve().parents[1] / 'characters' / 'xu_zhining.md'


def load_persona(path=DEFAULT):
    text = Path(path).read_text(encoding='utf-8-sig').strip()
    if not text or len(text) > 16000:
        raise ValueError('Invalid persona configuration')
    return text
