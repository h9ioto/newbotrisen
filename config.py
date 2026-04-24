import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    bot_token: str


def _read_env():
    out = {}
    for p in [Path(".env"), Path(".env.txt"), Path(__file__).resolve().parent / ".env"]:
        if not p.exists():
            continue
        for raw in p.read_text(encoding="utf-8-sig").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def load_settings():
    env = _read_env()
    token = os.getenv("BOT_TOKEN", "").strip() or env.get("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN не задан. Добавь .env с BOT_TOKEN=...")
    return Settings(bot_token=token)


def env_bool(key, default=False):
    env = _read_env()
    val = os.getenv(key, "").strip().lower() or env.get(key, "").strip().lower()
    if not val:
        return default
    return val in ("1", "true", "yes", "on")