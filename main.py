# ============================================================
# main.py - Risen Solo Bot | aiogram 3.x | Python 3.13
# ============================================================
import asyncio
import json
import logging
import os
import sys
import random
import re
import time
import traceback
import math
import textwrap
from html import escape
from datetime import datetime, date, timedelta, timezone
from io import BytesIO

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand, CallbackQuery, InlineKeyboardButton,
    InlineKeyboardMarkup, KeyboardButton, Message,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, BufferedInputFile,
    CopyTextButton,
)
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter

import db
import game_data as gd
from config import load_settings

# ─────────────────────────────────────────────
#  ЛОГИРОВАНИЕ
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("risen_solo")

# ─────────────────────────────────────────────
#  КОНСТАНТЫ
# ─────────────────────────────────────────────
SUPER_ADMINS: set[int] = {1406546170, 8256752341}
POOP_PRIVILEGED_TG_ID: int = 8140522040

ADMIN_ROLES: dict[int, str] = {
    1: "тестер",
    2: "мл. админ",
    3: "ст. админ",
    4: "зам создателя",
    5: "создатель",
}

VIP_NAMES: dict[int, str] = {
    0: "Нет",
    1: "[E] Пробужденный",
    2: "[C] Следопыт",
    3: "[A] Мастер",
    4: "[S] Превосходный",
    5: "[👑] Monarch",
}

VIP_COSTS: dict[int, int] = {
    1: 199,
    2: 499,
    3: 899,
    4: 1299,
    5: 9999,
}

DONATE_TIERS = [
    {
        "idx": 1,
        "name": "Пробужденный",
        "title": "[E] Пробужденный",
        "price": 199,
        "icon": "⭐",
        "vip_level": 1,
        "perks": [
            "• Лимит открытия кейсов: 50",
            "• +10% золота с боссов",
            "• +5% к урону",
            "• Авто-сбор ежедневного бонуса",
            "• +1 час к тренировке",
        ],
    },
    {
        "idx": 2,
        "name": "Следопыт",
        "title": "[C] Следопыт",
        "price": 499,
        "icon": "🧭",
        "vip_level": 2,
        "perks": [
            "• Лимит открытия кейсов: 100",
            "• +25% золота с боссов",
            "• +10% к урону",
            "• Тик тренировки: 5с",
            "• Авто-синтез: раз в 6 часов (можно отключить)",
            "• +2 часа к тренировке",
            "• Уведомление в ЛС о завершении тренировки",
        ],
    },
    {
        "idx": 3,
        "name": "Мастер",
        "title": "[A] Мастер",
        "price": 899,
        "icon": "🛡",
        "vip_level": 3,
        "perks": [
            "• Лимит открытия кейсов: 500",
            "• +50% золота с боссов",
            "• +15% к урону",
            "• Тик тренировки: 4с",
            "• Авто-синтез: каждые 3 часа",
            "• Авто-продажа мусора (настраиваемый порог)",
            "• +5 часов к тренировке",
            "• +1 слот для артефакта (всего 3)",
            "• Уведомление о завершении тренировки",
            "• Комиссия депозита: 3%",
        ],
    },
    {
        "idx": 4,
        "name": "Превосходный",
        "title": "[S] Превосходный",
        "price": 1299,
        "icon": "🔥",
        "vip_level": 4,
        "perks": [
            "• Лимит открытия кейсов: 1000",
            "• x2 золото с боссов",
            "• +30% к урону",
            "• Тик тренировки: 3с",
            "• Авто-синтез: каждый час",
            "• Авто-продажа мусора (настраиваемый порог)",
            "• Просмотр профиля по reply в чате",
            "• +5 часов к тренировке",
            "• 4-й слот артефакта",
            "• Комиссия депозита: 0%",
            "• Сохранение 1 артефакта при истинном ребёрте",
        ],
    },
    {
        "idx": 5,
        "name": "Monarch",
        "title": "[👑] Monarch",
        "price": 9999,
        "icon": "👑",
        "vip_level": 5,
        "perks": [
            "• Лимит открытия кейсов: 10000",
            "• x3 золото с боссов",
            "• +30% к урону",
            "• Лавка Джинна доступна 24/7",
            "• Тик тренировки: 2с",
            "• Авто-синтез: каждые 30 минут",
            "• Авто-продажа мусора (настраиваемый порог)",
            "• 4-й слот артефакта",
            "• Сохранение 2 артефактов при истинном ребёрте",
            "• Еженедельный бонус: 1 сумка артефактов",
            "• Префикс в профиле: 👑 Monarch",
        ],
    },
]

SEP = "···························"
SEP_BAR = "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰"
INVISIBLE_TEXT = "\u2063"

SHARD_WEIGHTS = [70, 18, 8, 3, 1]

DUNGEON_MODES = ["tomb", "grot", "greed", "chaos"]
DUNGEON_MODE_NAMES = {
    "tomb": "🪦 Гробница",
    "grot": "🕳 Грот",
    "greed": "💰 Жадность",
    "chaos": "🌪 Хаос",
}

DUNGEON_REWARD_DESC = {
    "tomb": "осколки колец",
    "grot": "магические монеты",
    "greed": "x2 золото с боссов",
    "chaos": "хаос (осколки или маг. монеты)",
}

DUNGEON_DIFFICULTY_NAMES = {
    "easy": "Легкая",
    "medium": "Средняя",
    "hard": "Сложная",
}

CASINO_COLORS = ["⚪", "🟢", "🔵", "🟣", "🔴"]
CASINO_COLOR_NAMES = {
    "⚪": "белый",
    "🟢": "зеленый",
    "🔵": "синий",
    "🟣": "фиолетовый",
    "🔴": "красный",
}

ADMIN_ID_FILE = "id.txt"
FRIENDLY_DUEL_START_HP = 300
FRIENDLY_DUEL_MIN_DMG = 10
FRIENDLY_DUEL_MAX_DMG = 90
BOT_STARTED_AT = int(time.time())
IGNORED_TG_IDS = {7581996418}
ADMIN_STATS_MODE_KEY = "admin_stats_mode"
ADMIN_SUPER_BONUS = 10 ** 12
ADMIN_HIDE_WEAPON_BONUS = 205_000
ADMIN_HIDE_PET_BONUS = 194_000
VIP_DONATE_WEAPON_NAME = "👑 VIP Оружие"
VIP_DONATE_PET_NAME = "👑 VIP Питомец"
ARTIFACT_BAG_STAT_KEY = "artifact:bag"

# Ключи для второго слота экипировки
SLOT2_WEAPON_KEY = "slot:weapon2"  # 1 = куплен
SLOT2_PET_KEY    = "slot:pet2"     # 1 = куплен
SLOT2_WEAPON_COST = 500   # эссенция
SLOT2_PET_COST    = 500   # эссенция

PROFILE_HIDDEN_KEY  = "settings:profile_hidden"   # 1=скрыт, 0=открыт
PROFILE_HIDDEN_COST = 300  # эссенция за разблокировку функции

ARTIFACT_TYPES = {
    "🔺": {"name": "Призма Регенерации", "weight": 3, "effect": "regen"},
    "👊": {"name": "Кулак Берсерка", "weight": 5, "effect": "dmg"},
    "💗": {"name": "Сердце Исцеления", "weight": 10, "effect": "heal"},
    "⚡": {"name": "Искра Уклонения", "weight": 25, "effect": "dodge"},
    "🥋": {"name": "Пояс Выносливости", "weight": 15, "effect": "train_time"},
    "🔩": {"name": "Ядро Мощи", "weight": 20, "effect": "train_power"},
    "💳": {"name": "Монета Алчности", "weight": 20, "effect": "coins"},
    "🏴‍☠️": {"name": "Флаг Мародёра", "weight": 3, "effect": "afk_loot"},
    "🪬": {"name": "Оберег Трофея", "weight": 5, "effect": "afk_case_chance"},
    "🎮": {"name": "Ключ Аркады", "weight": 0.5, "effect": "mini_any"},
    "🔮": {"name": "Сфера Грота", "weight": 5, "effect": "dungeon_magic"},
    "🎯": {"name": "Глаз Сокола", "weight": 4, "effect": "crit"},
    "🛡": {"name": "Эгида Отражения", "weight": 1, "effect": "reflect"},
    "🩸": {"name": "Вампирский Клык", "weight": 1, "effect": "lifesteal"},
    "🍀": {"name": "Подкова Удачи", "weight": 0.15, "effect": "artifact_luck"},
    "🗝": {"name": "Отмычка Хаоса", "weight": 0.5, "effect": "case_double"},
    "🕯": {"name": "Свеча Жизни", "weight": 2, "effect": "survive"},
}
ARTIFACT_SLOT_KEYS = ("artifact_slot_1", "artifact_slot_2", "artifact_slot_3", "artifact_slot_4")

VIP_AUTOSELL_THRESHOLD_KEY = "vip:autosell_threshold"
VIP_AUTOSELL_KEEP_PREFIX = "vip:autosell_keep:"
VIP_AUTOSYNTH_DISABLED_KEY = "vip:autosynth_disabled"
VIP_AUTOSYNTH_LAST_KEY = "vip:autosynth_last"
VIP_TRAIN_NOTIFY_MARK_KEY = "vip:train_notify_mark"

# Ключ для хранения дневного пула зачарований в лавке Джина
ENCHANT_SHOP_POOL_KEY = "enchant:shop_pool"   # значение: JSON строки "key:lvl,key:lvl,..."
ENCHANT_SHOP_POOL_DAY_KEY = "enchant:shop_day"  # значение: дата в формате YYYYMMDD (int)

# Человекочитаемые псевдонимы для ввода зачарований игроком
ENCHANT_NAME_ALIASES: dict[str, str] = {
    "сила": "power",       "сил": "power",      "урон": "power",     "power": "power",
    "реген": "regen",      "рег": "regen",       "лечение": "regen",  "regen": "regen",
    "мощь": "might",       "мощность": "might",  "трен": "might",     "might": "might",
    "уклон": "dodge",      "уклонение": "dodge", "dodge": "dodge",
}

# ─── Ключи настроек уведомлений (user_stats, 1=вкл, 0=выкл, default=1) ───
NOTIFY_PROMO_KEY        = "notify:promo"
NOTIFY_ADMIN_MSG_KEY    = "notify:admin_msg"
NOTIFY_CONTEST_KEY      = "notify:contest"
NOTIFY_FAST_CONTEST_KEY = "notify:fast_contest"
NOTIFY_GUILD_BOSS_KEY   = "notify:guild_boss"
NOTIFY_JINN_KEY         = "notify:jinn"

NOTIFY_SETTINGS = [
    (NOTIFY_PROMO_KEY,        "🎫 Промокоды"),
    (NOTIFY_ADMIN_MSG_KEY,    "📣 Сообщения от админа"),
    (NOTIFY_CONTEST_KEY,      "🏆 Конкурс"),
    (NOTIFY_FAST_CONTEST_KEY, "⚡ Фаст-конкурс"),
    (NOTIFY_GUILD_BOSS_KEY,   "🏰 Гильдийный рейд"),
    (NOTIFY_JINN_KEY,         "🧞 Лавка Джинна"),
]
VIP_WEEKLY_BAG_TS_KEY = "vip:weekly_bag_ts"
ARTIFACT_LEVEL_ICON = {
    1: "⚪",
    2: "🟢",
    3: "🔵",
    4: "🟣",
    5: "🟡",
    6: "🟠",
    7: "🔴",
    8: "🟤",
    9: "⚫",
    10: "🔘",
}


def _fmt_uptime(total_secs: int) -> str:
    total_secs = max(0, int(total_secs))
    d, rem = divmod(total_secs, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d > 0:
        return f"{d}д {h}ч {m}м"
    if h > 0:
        return f"{h}ч {m}м {s}с"
    if m > 0:
        return f"{m}м {s}с"
    return f"{s}с"


def _build_admin_item_catalog() -> dict[int, dict]:
    """Собирает скрытый каталог предметов для выдачи через /admin."""
    seen: set[tuple[str, str]] = set()
    weapons: list[tuple[str, int]] = []
    pets: list[tuple[str, int]] = []

    for edge, name in sorted(gd.WEAPON_NAME_TABLE, key=lambda x: (int(x[0]), str(x[1]))):
        key = ("weapon", str(name))
        if key in seen:
            continue
        seen.add(key)
        weapons.append((str(name), max(1, int(edge))))

    for edge, name in sorted(gd.PET_NAME_TABLE, key=lambda x: (int(x[0]), str(x[1]))):
        key = ("pet", str(name))
        if key in seen:
            continue
        seen.add(key)
        pets.append((str(name), max(1, int(edge))))

    for arena in sorted(gd.PET_CASE_POOLS.keys()):
        for name, lo, _hi, _w in gd.PET_CASE_POOLS[arena]:
            key = ("pet", str(name))
            if key in seen:
                continue
            seen.add(key)
            pets.append((str(name), max(1, int(lo))))

    catalog: dict[int, dict] = {}
    wid = 1001
    for name, bonus in weapons:
        catalog[wid] = {
            "type": "weapon",
            "name": name,
            "level": 1,
            "bonus": int(bonus),
        }
        wid += 1

    pid = 2001
    for name, bonus in pets:
        catalog[pid] = {
            "type": "pet",
            "name": name,
            "level": 1,
            "bonus": int(bonus),
        }
        pid += 1

    # Выделенные админ-предметы для выдачи через /admin.
    catalog[9001] = {
        "type": "weapon",
        "name": "admin weapon",
        "level": 1,
        "bonus": ADMIN_SUPER_BONUS,
    }
    catalog[9002] = {
        "type": "pet",
        "name": "admin pet",
        "level": 1,
        "bonus": ADMIN_SUPER_BONUS,
    }

    return catalog


ADMIN_ITEM_CATALOG = _build_admin_item_catalog()


def _parse_admin_item_value(raw: str) -> tuple[int, str, int]:
    """
    Форматы:
    - item_id
    - item_id:custom_id
    - item_id*qty
    - item_id:custom_id*qty
    """
    m = re.match(r"^\s*(\d+)(?:\s*:\s*([\w\-]+))?(?:\s*[xX*]\s*(\d+))?\s*$", str(raw or ""))
    if not m:
        raise ValueError("Формат: admin_id[:custom_id][*qty]")
    item_id = int(m.group(1))
    custom_id = str(m.group(2) or "").strip()
    qty = int(m.group(3) or 1)
    if qty < 1:
        raise ValueError("Количество должно быть >= 1")
    return item_id, custom_id, min(qty, 10000)


def _admin_item_ids_text() -> str:
    lines = ["Список ID предметов (админ)", SEP]
    lines.append("Оружие:")
    for item_id in sorted(k for k, v in ADMIN_ITEM_CATALOG.items() if v["type"] == "weapon"):
        it = ADMIN_ITEM_CATALOG[item_id]
        lines.append(f"{it['name']} = {item_id} (+{it['bonus']})")
    lines.append(SEP)
    lines.append("Питомцы:")
    for item_id in sorted(k for k, v in ADMIN_ITEM_CATALOG.items() if v["type"] == "pet"):
        it = ADMIN_ITEM_CATALOG[item_id]
        lines.append(f"{it['name']} = {item_id} (+{it['bonus']})")
    lines.append(SEP)
    lines.append("Выдача в /admin: admin_id[:custom_id][*qty] id_игрока")
    return "\n".join(lines)


def _write_admin_id_file():
    """Пишет локальный файл id.txt со списком скрытых админ-ID."""
    try:
        with open(ADMIN_ID_FILE, "w", encoding="utf-8") as f:
            f.write(_admin_item_ids_text())
    except Exception as e:
        log.warning(f"id file write error: {e}")


# Глобальные коэффициенты хардкор-профиля.
ECONOMY_COST_MULT = 1.55
BOSS_REWARD_MULT = 0.68
FINAL_BOSS_REWARD_MULT = 1.28
REBIRTH_MULT_STEP = 1.10
TRAIN_POWER_GAIN_MULT = 0.05625
WEAPON_EFFECT_MULT = 0.76
DAMAGE_GLOBAL_MULT = 1.06
PET_HP_EFFECT_MULT = 0.663
BASE_HP_MULT = 1.21
PLAYER_MAX_HP_MULT = 0.90
REGEN_MULT = 1.27
AURA_REGEN_MULT = 1.05
BATTLE_REGEN_INTERVAL_SEC = 2
BATTLE_REGEN_MULT = 1.2

WORLD_BOSS_NAME = "🌑 Повелитель Бездны"
WORLD_BOSS_MAX_HP = 500_000_000_000
WORLD_BOSS_DURATION_SEC = 7 * 24 * 3600
WORLD_BOSS_REGEN_SEC = 5
WORLD_BOSS_REGEN_HP = 5
WORLD_BOSS_HIT_CD_SEC = 2
WORLD_BOSS_DEAD_CD_SEC = 3600
WORLD_BOSS_PLAYER_REGEN_SEC = 3
WORLD_BOSS_TIER1_DAMAGE = 1_000_000_000
WORLD_BOSS_TIER2_DAMAGE = 1_000_000
WORLD_BOSS_HITS_MIN_REWARD = 100

# Античит по паттерну "авто-данж/авто-боссы 24/7".
ACTIVITY_BD_STREAK_SEC = 90 * 60
ACTIVITY_BD_GAP_SEC = 180
ACTIVITY_SURVEIL_SEC = 3600
ACTIVITY_REPORT_MIN_BD_ACTIONS = 30
ACTIVITY_STALE_FORGET_SEC = 6 * 3600
ACTIVITY_LOG_MAX = 3000
ACTIVITY_PARALLEL_MIN_BD_ACTIONS = 80
ACTIVITY_PARALLEL_MIN_NON_BD_ACTIONS = 15
ACTIVITY_RHYTHM_SAMPLE_MIN = 30
ACTIVITY_RHYTHM_STD_MAX = 0.22
ACTIVITY_RHYTHM_AVG_MIN = 1.6
ACTIVITY_RHYTHM_AVG_MAX = 3.2
ACTIVITY_RHYTHM_STD_STRICT = 0.12
ACTIVITY_ONLY_BD_ATTACK_TS_MIN = 45
ACTIVITY_ONLY_BD_UNIQUE_LABELS_MAX = 3
ACTIVITY_PICK_ATTACK_MIN_SAMPLES = 3
ACTIVITY_PICK_ATTACK_AVG_MIN = 0.7
ACTIVITY_PICK_ATTACK_AVG_MAX = 2.4
ACTIVITY_PICK_ATTACK_STD_MAX = 0.25
ACTIVITY_PICK_ATTACK_RANGE_MAX = 0.7
ACTIVITY_ATTACK_PICK_STD_MAX = 0.25
ACTIVITY_ATTACK_PICK_RANGE_MAX = 0.7
ACTIVITY_PICK_PAIR_AVG_DIFF_MAX = 0.25

BATTLE_STALE_SEC = 6 * 3600

TRAIN_CASE_UP_BASE = 30000
TRAIN_CASE_UP_GROWTH = 1.42
TRAIN_POWER_UP_BASE = 2500
TRAIN_POWER_UP_GROWTH = 1.16
TRAIN_TIME_UP_BASE = 100000
TRAIN_TIME_UP_GROWTH = 1.5
TRAIN_UPGRADE_MAX_LVL = 250

BIO_BONUS_TAG = "@risensolo_bot"
BIO_HP_MULT = 1.5
BIO_DMG_MULT = 1.5
BIO_TRAIN_POWER_MULT = 1.5
BIO_TRAIN_CASE_MULT = 1.5

REF_MIN_ARENA_FOR_REWARD = 3  # награда доступна с 3-й арены
REF_REFERRED_REWARD = {
    "coins": 50_000,
    "magic_coins": 20,
    "cases_per_type": 3,
}

VPN_TASK_URL = "https://t.me/VPNVezdehodBot?start=ref1406546170"
VPN_TASK_STAT_KEY = "task:vpn_vezdehod_done"
VPN_TASK_REWARD_ESSENCE = 5
VPN_TASK_REWARD_MAGIC = 25

# Поддерживаем оба формата Telegram chat id: обычный и supergroup (-100...)
AFK_CASE_BONUS_CHAT_IDS = {3615361482, 1003615361482}
AFK_CASE_BONUS_CHAT_MULT = 1.2

GUILD_UPGRADE_COSTS = {
    2: 800,
    3: 3500,
    4: 12000,
    5: 30000,
}

GUILD_LEVEL_BUFFS = {
    1: {"hp": 0.02, "case": 0.01, "dmg": 0.02, "coins": 0.03},
    2: {"hp": 0.05, "case": 0.03, "dmg": 0.05, "coins": 0.06},
    3: {"hp": 0.09, "case": 0.07, "dmg": 0.08, "coins": 0.10},
    4: {"hp": 0.21, "case": 0.15, "dmg": 0.13, "coins": 0.21},
    5: {"hp": 0.30, "case": 0.21, "dmg": 0.19, "coins": 0.30},
}

# Дополнительное усиление гильд-боссов: заметно выше, чтобы соло-убийство
# за пару минут на высоком уроне было практически невозможно.
GUILD_BOSS_HP_MULT = 15 * 3.5 * 30
GUILD_BOSS_TIMEOUT_SEC = 15 * 60
GUILD_BOSS_CD_DAYS = 2
DEPOSIT_LOCK_DAYS = 3
MINIGAME_CHAT_ID = -1003857387510
LADDER_CHANCES = [0.78, 0.68, 0.58, 0.48, 0.38]
LADDER_MULTS = [1.2, 1.6, 2.2, 3.5, 6.0]
SAFE_MULT = 2.4
RACE_MULT = 3.2
MINE_ROWS = 5
MINE_COLS = 5
MINE_CELLS = MINE_ROWS * MINE_COLS
MINE_MINES_MIN = 2
MINE_MINES_MAX = 6
RR_CHAMBERS = 10
EARN_EVENT_REWARDS = [
    {"coins": 67, "magic": 90, "essence": 10},
    {"coins": 67, "magic": 70, "essence": 5},
    {"coins": 67, "magic": 45, "essence": 5},
    {"coins": 67, "magic": 30, "essence": 3},
    {"coins": 67, "magic": 10, "essence": 1},
]

# ============ ТЕХРАБОТЫ КОМАНД ============
TECH_COMMANDS = {
    "профиль": False,
    "баланс": False,
    "боссы": False,
    "данж": False,
    "тренировка": False,
    "кейсы": False,
    "инвентарь": False,
    "синтез": False,
    "крафт": False,
    "экипировка": False,
    "арена": False,
    "ребёрты": False,
    "гильдия": False,
    "казино": False,
    "топ": False,
    "донат": False,
    "ивент": False,
    "лавка": False,
    "бонусы": False,
    "улучшения": False,
    "артефакты": False,
    "депозит": False,
    "миры": False,
    "настройки": False,
    "рефералы": False,
    "ежедневный": False,
    "стол зачарований": False,
    "мини-игры": False,
    "дуэль": False,
    "конкурс": False,
}
# ============================================


def _guild_member_limit(level: int) -> int:
    return max(10, min(50, int(level) * 10))


# День недели → режим данжа
def _today_dungeon_mode() -> str:
    wd = _today_msk().weekday()
    if wd in (0, 3):
        return "tomb"
    if wd in (1, 4):
        return "grot"
    if wd == 2:
        return "greed"
    return "chaos"


def _normalize_color_token(token: str) -> str:
    t = (token or "").strip().lower().replace("️", "")
    if t in CASINO_COLORS:
        return t
    aliases = {
        "белый": "⚪", "white": "⚪",
        "зеленый": "🟢", "зелёный": "🟢", "green": "🟢",
        "синий": "🔵", "blue": "🔵",
        "фиолетовый": "🟣", "purple": "🟣",
        "красный": "🔴", "red": "🔴",
    }
    return aliases.get(t, "")


def _duel_asset_from_token(token: str) -> tuple[str, str, str]:
    """
    Возвращает (kind, key, label):
    - kind=currency, key in users columns
    - kind=case, key in users columns (weapon_cases_aN / pet_cases_aN)
    """
    t = (token or "").strip().lower()
    currency_alias = {
        "монеты": ("currency", "coins", "🪙 Монеты"),
        "монета": ("currency", "coins", "🪙 Монеты"),
        "money": ("currency", "coins", "🪙 Монеты"),
        "coins": ("currency", "coins", "🪙 Монеты"),
        "маг": ("currency", "magic_coins", "🔯 Маг. монеты"),
        "маг монеты": ("currency", "magic_coins", "🔯 Маг. монеты"),
        "магмонеты": ("currency", "magic_coins", "🔯 Маг. монеты"),
        "magic": ("currency", "magic_coins", "🔯 Маг. монеты"),
        "magic_coins": ("currency", "magic_coins", "🔯 Маг. монеты"),
        "эссенция": ("currency", "essence", "💠 Эссенция"),
        "essence": ("currency", "essence", "💠 Эссенция"),
        "эсс": ("currency", "essence", "💠 Эссенция"),
    }
    if t in currency_alias:
        return currency_alias[t]

    m = re.match(r"^ко([1-9]|1[0-5])$", t)
    if m:
        a = int(m.group(1))
        return "case", f"weapon_cases_a{a}", f"🎫 ко{a}"
    m = re.match(r"^кп([1-9]|1[0-5])$", t)
    if m:
        a = int(m.group(1))
        return "case", f"pet_cases_a{a}", f"🐾 кп{a}"
    return "", "", ""


from typing import Any, Dict, Tuple, Optional

# Предполагается, что глобальные константы и объекты определены выше:
# db, MINIGAME_CHAT_ID, MINE_ROWS, MINE_COLS, RR_CHAMBERS, INVISIBLE_TEXT
# fmt_num, _row_get, _artifact_item_emoji, _artifact_effects, _display_name


def _mini_chat_allowed(chat_id: int) -> bool:
    """Проверяет, является ли чат разрешённым для мини-игр."""
    return chat_id == int(MINIGAME_CHAT_ID)


def _mini_has_arcade_key(tg_id: int) -> bool:
    """Проверяет наличие артефакта с ключом аркады (🎮) в инвентаре."""
    items = db.inventory_list(tg_id, limit=5000)
    for item in items:
        item_type = str(_row_get(item, "type", "") or "")
        if item_type != "artifact":
            continue

        name_raw = str(_row_get(item, "name", "") or "")
        count = int(_row_get(item, "count", 0) or 0)
        if count <= 0:
            continue

        emoji = _artifact_item_emoji(name_raw)
        # Условие: либо эмодзи 🎮, либо в названии есть "ключ аркады"
        if emoji == "🎮" or "ключ аркады" in name_raw.lower():
            return True
    return False


def _mini_allowed_for_user(user: Optional[Dict[str, Any]], chat_id: int) -> bool:
    """Проверяет, разрешён ли пользователю запуск мини-игры."""
    if not user:
        return False

    if _mini_chat_allowed(chat_id):
        return True

    effects = _artifact_effects(user)
    # Мини-игры вне игрового чата доступны только при реально надетом артефакте
    # с эффектом mini_any (🎮 в слоте), а не просто при наличии в инвентаре.
    return effects.get("mini_any", 0.0) > 0


def _mini_currency_from_token(token: str) -> Tuple[str, str]:
    """Возвращает (название поля в БД, иконку) по текстовому токену валюты."""
    aliases = {
        "монеты": ("coins", "🪙"),
        "монета": ("coins", "🪙"),
        "coins": ("coins", "🪙"),
        "маг": ("magic_coins", "🔯"),
        "магмонеты": ("magic_coins", "🔯"),
        "magic": ("magic_coins", "🔯"),
        "magic_coins": ("magic_coins", "🔯"),
        "эссенция": ("essence", "💠"),
        "эсс": ("essence", "💠"),
        "essence": ("essence", "💠"),
    }
    return aliases.get((token or "").strip().lower(), ("", ""))


def _mini_take_balance(user: Dict[str, Any], field: str, amount: int) -> bool:
    """Списывает валюту с баланса пользователя. Возвращает True при успехе."""
    uid = int(_row_get(user, "tg_id", 0) or 0)
    if uid <= 0:
        return False

    current = int(_row_get(user, field, 0) or 0)
    if current < amount:
        return False

    db.update_user(uid, **{field: current - amount})
    return True


def _mini_add_balance(user_id: int, field: str, amount: int) -> None:
    """Начисляет валюту на баланс пользователя."""
    user = db.get_user(user_id)
    if not user:
        return

    current = int(_row_get(user, field, 0) or 0)
    db.update_user(user_id, **{field: current + amount})


def _mini_mine_mult(safe_hits: int, mines: int) -> float:
    """Рассчитывает множитель выигрыша в игре «Мины»."""
    step = 0.06 + mines * 0.03
    return 1.0 + safe_hits * step


def _mini_mine_view(state: Dict[str, Any]) -> str:
    """Формирует текстовое представление текущего состояния игры «Мины»."""
    return (
        "💣 Mines\n"
        f"Ставка: {fmt_num(state['bet'])} {state['icon']} | Мин: {state['mines']}\n"
        f"Открыто: {len(state['opened'])} | x{state['mult']:.2f}"
    )


def _mini_mine_kb(state: Dict[str, Any], reveal_all: bool = False) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для игры «Мины»."""
    uid = state["uid"]
    mines_set = set(state["mines_set"])
    opened = set(state["opened"])
    rows = []

    for r in range(MINE_ROWS):
        row_buttons = []
        for c in range(MINE_COLS):
            idx = r * MINE_COLS + c
            if idx in opened:
                text = "🟩"
                callback = f"mini:mine:noop:{uid}"
            elif reveal_all and idx in mines_set:
                text = "💣"
                callback = f"mini:mine:noop:{uid}"
            elif reveal_all:
                text = INVISIBLE_TEXT
                callback = f"mini:mine:noop:{uid}"
            else:
                text = "ㅤ"
                callback = f"mini:mine:open:{uid}:{idx}"
            row_buttons.append(InlineKeyboardButton(text=text, callback_data=callback))
        rows.append(row_buttons)

    rows.append([InlineKeyboardButton(text="💰 Забрать", callback_data=f"mini:mine:take:{uid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _mini_rr_view(state: Dict[str, Any]) -> str:
    """Формирует текстовое представление состояния игры «Русская рулетка»."""
    p1 = _duel_user_label(state["p1"])
    p2 = _duel_user_label(state["p2"])
    lines = [
        "🔫 Русская рулетка",
        f"Ставка: {fmt_num(state['bet'])} {state['icon']} (каждый)",
        f"Барабан: {state['idx']}/{RR_CHAMBERS}",
        f"Игроки: {p1} vs {p2}",
    ]

    status = state.get("status", "pending")
    if status == "pending":
        lines.append("Ожидается подтверждение соперника.")
    elif status == "active":
        lines.append(f"Ход: {_duel_user_label(state['turn'])}")

    last_log = state.get("last_log")
    if last_log:
        lines.append("")
        lines.append(last_log)

    return "\n".join(lines)


def _mini_rr_kb_pending(rr_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для ожидающей игры в русскую рулетку."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"mini:rr:accept:{rr_id}"),
            InlineKeyboardButton(text="❌ Отказаться", callback_data=f"mini:rr:decline:{rr_id}"),
        ]
    ])


def _mini_rr_kb_active(rr_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для активной игры в русскую рулетку."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔫 Спустить", callback_data=f"mini:rr:fire:{rr_id}")],
    ])


def _duel_has_balance(user: Dict[str, Any], asset_key: str, amount: int) -> bool:
    """Проверяет, достаточно ли у пользователя указанного ресурса."""
    return int(_row_get(user, asset_key, 0) or 0) >= amount


def _duel_take_stake(tg_id: int, asset_key: str, amount: int) -> bool:
    """Списывает ставку. Возвращает True, если списание успешно."""
    user = db.get_user(tg_id)
    if not user:
        return False

    current = int(_row_get(user, asset_key, 0) or 0)
    if current < amount:
        return False

    db.update_user(tg_id, **{asset_key: current - amount})
    return True


def _duel_add_reward(tg_id: int, asset_key: str, amount: int) -> None:
    """Начисляет награду победителю дуэли."""
    user = db.get_user(tg_id)
    if not user:
        return

    current = int(_row_get(user, asset_key, 0) or 0)
    db.update_user(tg_id, **{asset_key: current + amount})


def _duel_user_label(tg_id: int) -> str:
    """Возвращает читаемое имя пользователя для отображения в дуэли."""
    user = db.get_user(tg_id)
    if not user:
        return f"id{tg_id}"
    return _display_name(user)


def _friendly_duel_kb_pending(duel_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для ожидающей дружеской дуэли."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"duel:accept:{duel_id}"),
            InlineKeyboardButton(text="❌ Отказаться", callback_data=f"duel:decline:{duel_id}"),
        ]
    ])


def _friendly_duel_kb_active(duel_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для активной дружеской дуэли."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚔️ Ударить", callback_data=f"duel:hit:{duel_id}")],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data=f"duel:close:{duel_id}")],
    ])


def _friendly_duel_view(ds: "FriendlyDuelState") -> str:
    p1 = _duel_user_label(ds.inviter_id)
    p2 = _duel_user_label(ds.target_id)
    lines = [
        "🤝 Дружеский бой",
        SEP,
        f"Ставка: {fmt_num(ds.amount)} {ds.asset_label}",
        f"{p1}: ❤️ {fmt_num(ds.hp[ds.inviter_id])}",
        f"{p2}: ❤️ {fmt_num(ds.hp[ds.target_id])}",
    ]
    if ds.status == "pending":
        lines.append("\nОжидается ответ соперника.")
    elif ds.status == "active":
        lines.append(f"\nХод: {_duel_user_label(ds.turn_id)}")
    if ds.last_log:
        lines.append("")
        lines.append(ds.last_log)
    return "\n".join(lines)


# ─────────────────────────────────────────────
#  ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ─────────────────────────────────────────────
GROUP_CLEANED: set[int] = set()


class BattleState:
    __slots__ = (
        "user_id", "arena", "boss_idx", "boss_hp", "boss_max_hp",
        "player_hp", "player_max_hp", "player_dmg", "regen_per_tick",
        "boss_atk", "last_regen", "last_action", "last_hit", "msg_id", "chat_id",
        "survive_used",
    )

    def __init__(self, user_id, arena, boss_idx, boss_hp, boss_max_hp,
                 player_hp, player_max_hp, player_dmg, regen_per_tick,
                 boss_atk, msg_id, chat_id):
        self.user_id = user_id
        self.arena = arena
        self.boss_idx = boss_idx
        self.boss_hp = boss_hp
        self.boss_max_hp = boss_max_hp
        self.player_hp = player_hp
        self.player_max_hp = player_max_hp
        self.player_dmg = player_dmg
        self.regen_per_tick = regen_per_tick
        self.boss_atk = boss_atk
        now = time.time()
        self.last_regen = now
        self.last_action = now
        self.last_hit = 0.0
        self.msg_id = msg_id
        self.chat_id = chat_id
        self.survive_used = False


class DungeonState:
    __slots__ = (
        "user_id", "mode", "wave", "max_waves", "gold", "magic",
        "shards", "started_at", "enemy_hp", "enemy_max_hp",
        "enemy_atk", "player_dmg", "msg_id", "chat_id", "arena", "note", "difficulty",
    )

    def __init__(self, user_id, mode, arena, msg_id, chat_id, difficulty="easy"):
        self.user_id = user_id
        self.mode = mode
        self.arena = arena
        self.difficulty = str(difficulty or "easy")
        self.wave = 0
        self.max_waves = 50
        self.gold = 0
        self.magic = 0
        self.shards: dict[int, int] = {}
        self.started_at = time.time()
        self.msg_id = msg_id
        self.chat_id = chat_id
        self.note = ""
        self.enemy_hp = 0
        self.enemy_max_hp = 0
        self.enemy_atk = 0
        self.player_dmg = 0


class GuildBattleState:
    __slots__ = (
        "guild_id", "arena", "boss_idx", "boss_name", "boss_hp", "boss_max_hp",
        "reward_base", "msg_id", "chat_id", "started_at", "updated_at",
    )

    def __init__(self, guild_id, arena, boss_idx, boss_name, boss_hp, boss_max_hp, reward_base, msg_id, chat_id):
        self.guild_id = guild_id
        self.arena = arena
        self.boss_idx = boss_idx
        self.boss_name = boss_name
        self.boss_hp = boss_hp
        self.boss_max_hp = boss_max_hp
        self.reward_base = reward_base
        self.msg_id = msg_id
        self.chat_id = chat_id
        now = int(time.time())
        self.started_at = now
        self.updated_at = now


class FriendlyDuelState:
    __slots__ = (
        "duel_id", "chat_id", "msg_id", "inviter_id", "target_id",
        "asset_kind", "asset_key", "asset_label", "amount", "status",
        "hp", "turn_id", "created_at", "last_action", "last_log",
    )

    def __init__(self, duel_id: int, chat_id: int, msg_id: int, inviter_id: int, target_id: int,
                 asset_kind: str, asset_key: str, asset_label: str, amount: int):
        self.duel_id = duel_id
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.inviter_id = inviter_id
        self.target_id = target_id
        self.asset_kind = asset_kind
        self.asset_key = asset_key
        self.asset_label = asset_label
        self.amount = amount
        self.status = "pending"  # pending|active|finished|cancelled
        self.hp = {inviter_id: FRIENDLY_DUEL_START_HP, target_id: FRIENDLY_DUEL_START_HP}
        self.turn_id = inviter_id
        now = int(time.time())
        self.created_at = now
        self.last_action = now
        self.last_log = ""


ACTIVE_BATTLES: dict[int, BattleState] = {}
ACTIVE_BATTLES_BY_MSG: dict[tuple[int, int], int] = {}
ACTIVE_DUNGEONS: dict[int, DungeonState] = {}
ACTIVE_DUNGEONS_BY_MSG: dict[tuple[int, int], int] = {}
ADMIN_CTX: dict[int, dict] = {}
PROMO_CTX: dict[int, dict] = {}
SHOP_CUSTOM_CTX: dict[int, dict] = {}
ENCHANT_TABLE_CTX: dict[int, dict] = {}  # {tg_id: {step, item_id, enchant_key, enchant_lvl, msg_id, chat_id}}
SHOP_CONFIRM_QTY = 50
DUNGEON_LOCKS: dict[int, float] = {}
JINN_PREALERT_SENT: set[tuple[int, str, int]] = set()
JINN_FORCED_UNTIL: int = 0
NICK_PENDING: set[int] = set()
CONTEST_STATE: dict = {}
CONTEST_ANSWERED: set[int] = set()
FAST_CONTEST_STATE: dict = {}
FAST_CONTEST_ANSWERED: set[int] = set()
FAST_CONTEST_LOCK = asyncio.Lock()
GUILD_ACTIVE_BATTLES: dict[int, GuildBattleState] = {}
GUILD_BOSS_LOCKS: dict[int, asyncio.Lock] = {}
GUILD_PENDING_DESC: dict[int, int] = {}
GUILD_PENDING_NAME: dict[int, int] = {}
ACTIVITY_MONITOR: dict[int, dict] = {}
WORLD_BOSS_LOCK = asyncio.Lock()
FRIENDLY_DUELS: dict[int, FriendlyDuelState] = {}
FRIENDLY_DUEL_COUNTER: int = 0
# Антидубль редактирования: не дергаем edit повторно с тем же контентом.
EDIT_FLOOD_GUARD: dict[tuple[int, int], tuple[str, float]] = {}
# Владелец callback-сообщения: защита от нажатия чужих кнопок в чатах.
CALLBACK_OWNER_BY_MSG: dict[tuple[int, int], int] = {}
MINI_LADDER: dict[int, dict] = {}
MINI_SAFE: dict[int, dict] = {}
MINI_MINE: dict[int, dict] = {}
MINI_RR: dict[int, dict] = {}
MINI_RR_COUNTER: int = 0
EARN_EVENT_STATE: dict = {}
EARN_EVENT_COUNTER: int = 0
ARTIFACT_TRUST_PENDING: dict[int, dict] = {}


def _is_shop_custom_input_message(m: Message) -> bool:
    if not m or not m.from_user or not m.text:
        return False
    ctx = SHOP_CUSTOM_CTX.get(int(m.from_user.id))
    if not ctx:
        return False
    if int(m.chat.id) != int(ctx.get("chat_id", 0) or 0):
        return False
    if not m.reply_to_message:
        return False
    if int(m.reply_to_message.message_id) != int(ctx.get("prompt_msg_id", 0) or 0):
        return False
    # Перехватываем только чистое число, чтобы другие команды (например, "отк ко10 50")
    # не блокировались, даже если пользователь случайно ответил на prompt.
    return bool(re.fullmatch(r"\s*\d+\s*", str(m.text or "")))


def _is_artifact_trust_input_message(m: Message) -> bool:
    if not m or not m.from_user or not m.text:
        return False
    ctx = ARTIFACT_TRUST_PENDING.get(int(m.from_user.id))
    if not ctx:
        return False
    if str(ctx.get("phase", "input")) != "input":
        return False
    if int(m.chat.id) != int(ctx.get("chat_id", 0) or 0):
        return False
    # Для ввода доверия принимаем только reply на карточку артефакта.
    if not m.reply_to_message:
        return False
    return int(m.reply_to_message.message_id) == int(ctx.get("artifact_msg_id", 0) or 0)


def _artifact_trust_confirm_kb(tg_id: int, item_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"art:{int(tg_id)}:trustok:{int(item_id)}:{int(page)}")],
        [InlineKeyboardButton(text="✖ Отмена", callback_data=f"art:{int(tg_id)}:trustcancel:{int(item_id)}:{int(page)}")],
    ])


def _artifact_return_confirm_kb(tg_id: int, item_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Вернуть досрочно", callback_data=f"art:{int(tg_id)}:returnok:{int(item_id)}:{int(page)}")],
        [InlineKeyboardButton(text="◀ Назад", callback_data=f"art:{int(tg_id)}:returncancel:{int(item_id)}:{int(page)}")],
    ])


# ─────────────────────────────────────────────
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────────────
def fmt_num(v) -> str:
    if isinstance(v, float):
        s = f"{v:,.2f}".replace(",", " ").rstrip("0").rstrip(".")
        return s
    return f"{int(v):,}".replace(",", " ")


async def _reply(message: Message, text: str, **kwargs):
    """Отправляет ответ реплаем на исходное сообщение пользователя."""
    try:
        return await message.reply(text, **kwargs)
    except Exception:
        return await message.answer(text, **kwargs)


def fmt_short_num(v: int | float) -> str:
    """Короткий формат чисел для профиля: k/m/b/t/q."""
    n = float(v or 0)
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n < 1000:
        return f"{sign}{int(n)}"

    suffixes = [
        (10**15, "q"),
        (10**12, "t"),
        (10**9, "b"),
        (10**6, "m"),
        (10**3, "k"),
    ]
    for div, suf in suffixes:
        if n >= div:
            val = n / div
            if val >= 100:
                body = f"{val:.0f}"
            elif val >= 10:
                body = f"{val:.1f}".rstrip("0").rstrip(".")
            else:
                body = f"{val:.2f}".rstrip("0").rstrip(".")
            return f"{sign}{body}{suf}"
    return f"{sign}{int(n)}"


MSK_TZ = timezone(timedelta(hours=3), name="MSK")


def _now_msk() -> datetime:
    return datetime.now(MSK_TZ)


def _today_msk() -> date:
    return _now_msk().date()


def _fmt_ts_msk(ts: int | float, fmt: str = "%d.%m.%Y %H:%M") -> str:
    return datetime.fromtimestamp(float(ts or 0), MSK_TZ).strftime(fmt)


_NUM_SUFFIX_MULT = {
    "k": 10**3,
    "к": 10**3,
    "m": 10**6,
    "м": 10**6,
    "b": 10**9,
    "б": 10**9,
    "t": 10**12,
    "т": 10**12,
    "q": 10**15,
    "кв": 10**15,
}


def _parse_amount_token(raw: str) -> int:
    """Парсит суммы вида 1500, 3m/3м, 3b/3б, 3t/3т, 3q/3кв."""
    token = str(raw or "").strip().lower().replace("_", "").replace(" ", "")
    if not token:
        return 0
    m = re.fullmatch(r"(\d+)([a-zа-я]+)?", token)
    if not m:
        return 0
    base = int(m.group(1))
    suffix = str(m.group(2) or "")
    if not suffix:
        return base
    mult = _NUM_SUFFIX_MULT.get(suffix)
    if not mult:
        return 0
    return max(0, int(base) * int(mult))


def _row_get(row, key: str, default=0):
    try:
        return row[key]
    except Exception:
        return default


def _is_dev_mode() -> bool:
    val = os.getenv("DEV_MODE", "").strip().lower()
    return val in ("1", "true", "yes")


def _parse_contest_duration(token: str) -> int:
    """Парсит длительность конкурса: 30м, 1ч, 2h, 45m, 1д."""
    t = (token or "").strip().lower()
    if not t:
        return 0
    aliases = {
        "м": 60,
        "m": 60,
        "мин": 60,
        "ч": 3600,
        "h": 3600,
        "час": 3600,
        "д": 86400,
        "d": 86400,
        "дн": 86400,
    }
    for suffix, mul in aliases.items():
        if t.endswith(suffix):
            num = t[: -len(suffix)].strip()
            if not num.isdigit():
                return 0
            return int(num) * mul
    if t.isdigit():
        # По умолчанию число трактуем как минуты.
        return int(t) * 60
    return 0


def _extract_start_referrer(text: str) -> int:
    raw = str(text or "").strip()
    m = re.search(r"^/start(?:@\w+)?\s+ref[_:\-]?(\d+)\s*$", raw, re.IGNORECASE)
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


def _ref_reward_tier(u) -> int:
    arena = int(u["arena"] or 1)
    if arena <= 5:
        return 1
    if arena <= 10:
        return 2
    return 3


def _ref_reward_options(u) -> list[dict]:
    tier = _ref_reward_tier(u)
    arena = int(u["arena"] or 1)
    case_arena = max(1, min(arena, gd.max_arena()))
    if tier == 1:
        return [
            {"key": "coins", "title": "🪙 Монеты", "desc": "+75 000 монет", "apply": {"coins": 75000}},
            {"key": "magic", "title": "🔯 Маг. монеты", "desc": "+25 маг. монет", "apply": {"magic_coins": 25}},
            {
                "key": "cases",
                "title": "📦 Набор кейсов",
                "desc": "+30 обычных +10 редких",
                "apply": {"afk_common": 30, "afk_rare": 10},
            },
        ]
    if tier == 2:
        return [
            {"key": "coins", "title": "🪙 Монеты", "desc": "+250 000 монет", "apply": {"coins": 250000}},
            {"key": "magic", "title": "🔯 Маг. монеты", "desc": "+80 маг. монет", "apply": {"magic_coins": 80}},
            {
                "key": "cases",
                "title": "🎫 Предметные кейсы",
                "desc": f"+3 ко{case_arena} и +3 кп{case_arena}",
                "apply": {f"weapon_cases_a{case_arena}": 3, f"pet_cases_a{case_arena}": 3},
            },
        ]
    return [
        {"key": "coins", "title": "🪙 Монеты", "desc": "+700 000 монет", "apply": {"coins": 700000}},
        {"key": "essence", "title": "💠 Эссенция", "desc": "+120 эссенции", "apply": {"essence": 120}},
        {
            "key": "combo",
            "title": "🔥 Комбо-награда",
            "desc": f"+150 🔯 и +5 ко{case_arena}/кп{case_arena}",
            "apply": {"magic_coins": 150, f"weapon_cases_a{case_arena}": 5, f"pet_cases_a{case_arena}": 5},
        },
    ]


def _ref_kb(u, pending_count: int) -> InlineKeyboardMarkup:
    rows = []
    if pending_count > 0:
        for opt in _ref_reward_options(u):
            rows.append([
                InlineKeyboardButton(
                    text=f"{opt['title']} ({opt['desc']})",
                    callback_data=f"ref:claim:{opt['key']}",
                )
            ])
    rows.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data="ref:refresh"),
        InlineKeyboardButton(text="✖ Закрыть", callback_data="ref:close"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _ref_text(u, pending_rows=None) -> str:
    pending_rows = pending_rows or []
    stats = db.referral_stats(int(u["tg_id"]))
    tier = _ref_reward_tier(u)
    lines = [
        "🤝 Реферальная система",
        SEP,
        f"Условие награды: приглашенный должен дойти до арены {REF_MIN_ARENA_FOR_REWARD}+.",
        f"Бонус приглашенному при достижении условия: +{fmt_num(REF_REFERRED_REWARD['coins'])} 🪙, +{fmt_num(REF_REFERRED_REWARD['magic_coins'])} 🔯, +{REF_REFERRED_REWARD['cases_per_type']} ко/кп своей арены",
        f"Твой уровень наград: T{tier} (по твоей прокачке).",
        "",
        f"👥 Всего приглашено: {fmt_num(stats.get('total', 0))}",
        f"✅ Дошли до арены {REF_MIN_ARENA_FOR_REWARD}+: {fmt_num(stats.get('qualified', 0))}",
        f"🎁 Доступно к выдаче: {fmt_num(stats.get('pending', 0))}",
        f"📦 Уже забрано: {fmt_num(stats.get('claimed', 0))}",
    ]
    if pending_rows:
        lines.append("")
        lines.append("Готовы к награде:")
        for row in pending_rows[:10]:
            nick = str(row["nickname"] or "").strip()
            if not nick:
                uname = str(row["username"] or "").strip()
                nick = f"@{uname}" if uname else f"id{row['referred_id']}"
            lines.append(f"• {nick} (id {row['referred_id']}, арена {row['arena']})")
    lines.append("")
    lines.append("Команда: /ref")
    return "\n".join(lines)


async def _maybe_complete_referral(user_id: int):
    uid = int(user_id)
    referrer_id = db.mark_referral_qualified_if_ready(uid, REF_MIN_ARENA_FOR_REWARD)

    # Одноразовая награда самому приглашенному за достижение условия.
    referred_reward_referrer_id = db.claim_referred_reward_if_qualified(uid)
    if referred_reward_referrer_id > 0:
        referred_arena = 1
        ru = db.get_user(uid)
        if ru:
            referred_arena = max(1, min(int(_row_get(ru, "arena", 1) or 1), gd.max_arena()))
            db.update_user(
                uid,
                coins=int(_row_get(ru, "coins", 0) or 0) + int(REF_REFERRED_REWARD["coins"]),
                magic_coins=int(_row_get(ru, "magic_coins", 0) or 0) + int(REF_REFERRED_REWARD["magic_coins"]),
                **{f"weapon_cases_a{referred_arena}": int(_row_get(ru, f"weapon_cases_a{referred_arena}", 0) or 0) + int(REF_REFERRED_REWARD["cases_per_type"]),
                   f"pet_cases_a{referred_arena}": int(_row_get(ru, f"pet_cases_a{referred_arena}", 0) or 0) + int(REF_REFERRED_REWARD["cases_per_type"])},
            )
        if bot_instance is not None:
            try:
                await bot_instance.send_message(
                    uid,
                    f"🎁 Реферальный бонус активирован!\n"
                    f"🪙 +{fmt_num(REF_REFERRED_REWARD['coins'])}\n"
                    f"🔯 +{fmt_num(REF_REFERRED_REWARD['magic_coins'])}\n"
                    f"🎫 +{REF_REFERRED_REWARD['cases_per_type']} ко{referred_arena} и +{REF_REFERRED_REWARD['cases_per_type']} кп{referred_arena}",
                )
            except Exception:
                pass

    if referrer_id > 0 and bot_instance is not None:
        try:
            await bot_instance.send_message(
                referrer_id,
                f"🎉 Один из твоих приглашенных дошел до арены {REF_MIN_ARENA_FOR_REWARD}. Награда доступна в /ref",
            )
        except Exception:
            pass


def _persist_battle_state(bs: BattleState):
    db.save_active_battle(
        tg_id=bs.user_id,
        arena=bs.arena,
        boss_idx=bs.boss_idx,
        boss_hp=bs.boss_hp,
        boss_max_hp=bs.boss_max_hp,
        player_hp=bs.player_hp,
        player_max_hp=bs.player_max_hp,
        player_dmg=bs.player_dmg,
        regen_per_tick=bs.regen_per_tick,
        boss_atk=bs.boss_atk,
        last_regen=bs.last_regen,
        last_action=bs.last_action,
        msg_id=bs.msg_id,
        chat_id=bs.chat_id,
    )


def _drop_battle_state(user_id: int):
    db.delete_active_battle(int(user_id))


def _restore_battle_from_db_row(row) -> BattleState | None:
    """Восстанавливает состояние боя из строки БД и регистрирует в памяти."""
    if not row:
        return None
    now = time.time()
    user_id = int(row["tg_id"])
    last_action = float(row["last_action"] or 0)
    if now - last_action > BATTLE_STALE_SEC:
        db.delete_active_battle(user_id)
        return None
    bs = BattleState(
        user_id=user_id,
        arena=int(row["arena"]),
        boss_idx=int(row["boss_idx"]),
        boss_hp=int(row["boss_hp"]),
        boss_max_hp=int(row["boss_max_hp"]),
        player_hp=int(row["player_hp"]),
        player_max_hp=int(row["player_max_hp"]),
        player_dmg=int(row["player_dmg"]),
        regen_per_tick=int(row["regen_per_tick"]),
        boss_atk=int(row["boss_atk"]),
        msg_id=int(row["msg_id"]),
        chat_id=int(row["chat_id"]),
    )
    bs.last_regen = float(row["last_regen"] or now)
    bs.last_action = last_action or now
    ACTIVE_BATTLES[user_id] = bs
    ACTIVE_BATTLES_BY_MSG[(bs.chat_id, bs.msg_id)] = user_id
    return bs


def _persist_dungeon_state(ds: DungeonState):
    db.save_active_dungeon(
        tg_id=ds.user_id,
        mode=ds.mode,
        difficulty=ds.difficulty,
        wave=ds.wave,
        max_waves=ds.max_waves,
        gold=ds.gold,
        magic=ds.magic,
        shards=ds.shards,
        started_at=ds.started_at,
        enemy_hp=ds.enemy_hp,
        enemy_max_hp=ds.enemy_max_hp,
        enemy_atk=ds.enemy_atk,
        player_dmg=ds.player_dmg,
        msg_id=ds.msg_id,
        chat_id=ds.chat_id,
        arena=ds.arena,
        note=ds.note,
    )


def _drop_dungeon_state(user_id: int):
    db.delete_active_dungeon(int(user_id))


def _clear_user_combat_states(user_id: int):
    """Полностью очищает активные PvE-состояния и мини-игры пользователя (RAM + DB)."""
    uid = int(user_id)

    bs = ACTIVE_BATTLES.pop(uid, None)
    if bs:
        ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
    db.delete_active_battle(uid)

    ds = ACTIVE_DUNGEONS.pop(uid, None)
    if ds:
        ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)
    db.delete_active_dungeon(uid)

    # Аннулируем мини-игры без возврата ставок (монеты всё равно обнуляются при ребёрте)
    MINI_LADDER.pop(uid, None)
    MINI_SAFE.pop(uid, None)
    MINI_MINE.pop(uid, None)
    # RR: находим и удаляем все игры где этот юзер участник
    rr_ids_to_remove = [rid for rid, st in list(MINI_RR.items()) if uid in (int(st.get("p1", 0)), int(st.get("p2", 0)))]
    for rid in rr_ids_to_remove:
        st = MINI_RR.pop(rid, None)
        if st:
            # Возвращаем ставку другому игроку если он уже заплатил
            other = int(st.get("p2", 0)) if uid == int(st.get("p1", 0)) else int(st.get("p1", 0))
            if str(st.get("status", "pending")) == "active" and other > 0:
                _mini_add_balance(other, str(st.get("field", "coins")), int(st.get("bet", 0)))
    # Очищаем ожидающий ввод в магазине
    SHOP_CUSTOM_CTX.pop(uid, None)
    # Убираем из earn event чтобы нельзя было сравнивать монеты до/после ребёрта
    if EARN_EVENT_STATE.get("active"):
        EARN_EVENT_STATE.get("start_coins", {}).pop(str(uid), None)
        EARN_EVENT_STATE.get("start_coins", {}).pop(uid, None)


def _restore_dungeon_from_db_row(row) -> DungeonState | None:
    """Восстанавливает состояние данжа из строки БД и регистрирует в памяти."""
    if not row:
        return None
    now = time.time()
    user_id = int(row["tg_id"])
    started_at = float(row["started_at"] or now)
    if now - started_at > 660:
        # Истекший данж начислим при обычной логике старта/очистки, здесь просто не возвращаем его.
        return None
    ds = DungeonState(
        user_id=user_id,
        mode=str(row["mode"] or _today_dungeon_mode()),
        arena=int(row["arena"]),
        msg_id=int(row["msg_id"]),
        chat_id=int(row["chat_id"]),
        difficulty=str(_row_get(row, "difficulty", "easy") or "easy"),
    )
    ds.wave = int(row["wave"])
    ds.max_waves = int(row["max_waves"])
    ds.gold = int(row["gold"])
    ds.magic = int(row["magic"])
    try:
        raw_shards = json.loads(str(row["shards_json"] or "{}"))
        ds.shards = {int(k): int(v) for k, v in dict(raw_shards).items()}
    except Exception:
        ds.shards = {}
    ds.started_at = started_at
    ds.enemy_hp = int(row["enemy_hp"])
    ds.enemy_max_hp = int(row["enemy_max_hp"])
    ds.enemy_atk = int(row["enemy_atk"])
    ds.player_dmg = int(row["player_dmg"])
    ds.note = str(row["note"] or "")
    ACTIVE_DUNGEONS[user_id] = ds
    ACTIVE_DUNGEONS_BY_MSG[(ds.chat_id, ds.msg_id)] = user_id
    return ds


def _persist_guild_battle_state(gb: GuildBattleState):
    db.save_active_guild_battle(
        guild_id=gb.guild_id,
        arena=gb.arena,
        boss_idx=gb.boss_idx,
        boss_name=gb.boss_name,
        boss_hp=gb.boss_hp,
        boss_max_hp=gb.boss_max_hp,
        reward_base=gb.reward_base,
        msg_id=gb.msg_id,
        chat_id=gb.chat_id,
        started_at=gb.started_at,
    )


def _drop_guild_battle_state(guild_id: int):
    db.delete_active_guild_battle(int(guild_id))


def _contest_restore_from_db() -> bool:
    row = db.get_active_contest()
    if not row:
        return False
    contest_id = int(row["contest_id"])
    answers_rows = db.list_contest_answers(contest_id)
    answers = {int(r["tg_id"]): str(r["answer"]) for r in answers_rows}
    CONTEST_STATE.clear()
    CONTEST_ANSWERED.clear()
    CONTEST_STATE.update({
        "id": contest_id,
        "owner_id": int(row["owner_id"]),
        "question": str(row["question"] or ""),
        "started_at": int(row["started_at"] or 0),
        "ends_at": int(row["ends_at"] or 0),
        "answers": answers,
    })
    CONTEST_ANSWERED.update(answers.keys())
    return True


def _is_bd_message(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    bd_tokens = {
        "боссы", "boss", "🏛️ боссы", "⛩️ данж", "данж", "данжен",
        "/dungeon", "/bosses",
    }
    if t in bd_tokens:
        return True
    if t.startswith("/dungeon") or t.startswith("/bosses"):
        return True
    return False


def _is_bd_callback(data: str) -> bool:
    d = str(data or "")
    return d.startswith((
        "battle:",
        "boss_",
        "boss:",
        "boss_pick:",
        "dungeon:",
        "guild:boss",
        "guild:bossatk:",
        "guild:bossref:",
    ))


def _is_attack_action(label: str) -> bool:
    return label in {
        "cb:battle:attack",
        "cb:dungeon:attack",
        "cb:guild:bossatk",
    }


def _monitor_rhythm_stats(ts_list: list[int]) -> tuple[float, float]:
    """Возвращает (avg_interval, std_interval) по ряду отметок ударов."""
    if len(ts_list) < 2:
        return 0.0, 0.0
    intervals = [max(0.0, float(ts_list[i] - ts_list[i - 1])) for i in range(1, len(ts_list))]
    if not intervals:
        return 0.0, 0.0
    avg = sum(intervals) / len(intervals)
    var = sum((x - avg) ** 2 for x in intervals) / len(intervals)
    return avg, math.sqrt(var)


def _monitor_log_stats(logs: list[tuple[int, str, int]]) -> tuple[int, int, float]:
    """Возвращает (unique_labels, expanded_actions, dominant_share)."""
    if not logs:
        return 0, 0, 0.0
    counts: dict[str, int] = {}
    total = 0
    for _ts, label, cnt in logs:
        c = max(1, int(cnt or 1))
        counts[str(label)] = counts.get(str(label), 0) + c
        total += c
    if total <= 0:
        return len(counts), 0, 0.0
    dominant = max(counts.values()) if counts else 0
    return len(counts), total, float(dominant) / float(total)


def _monitor_interval_profile(values: list[float]) -> tuple[float, float, float]:
    """Возвращает (avg, std, range) для набора интервалов."""
    if not values:
        return 0.0, 0.0, 0.0
    clean = [max(0.0, float(v)) for v in values]
    if not clean:
        return 0.0, 0.0, 0.0
    avg = sum(clean) / len(clean)
    var = sum((x - avg) ** 2 for x in clean) / len(clean)
    rng = max(clean) - min(clean)
    return avg, math.sqrt(var), rng


def _monitor_compact_append(logs: list[tuple[int, str, int]], ts: int, label: str):
    if logs and _is_attack_action(label) and logs[-1][1] == label:
        prev_ts, prev_label, prev_cnt = logs[-1]
        logs[-1] = (prev_ts, prev_label, prev_cnt + 1)
        return
    logs.append((ts, label, 1))
    if len(logs) > ACTIVITY_LOG_MAX:
        del logs[: len(logs) - ACTIVITY_LOG_MAX]


def _monitor_track_action(tg_id: int, label: str, is_bd: bool):
    now = time.time()
    # Persist raw activity so /active works for any player across restarts.
    try:
        db.add_user_activity_log(int(tg_id), str(label), bool(is_bd), int(now))
    except Exception:
        pass
    st = ACTIVITY_MONITOR.get(int(tg_id))
    if st is None:
        st = {
            "bd_start": 0.0,
            "last_bd": 0.0,
            "last_seen": now,
            "watch_started": 0,
            "watch_until": 0,
            "has_non_bd": False,
            "bd_actions": 0,
            "non_bd_actions": 0,
            "total_actions": 0,
            "trigger": "",
            "logs": [],
            "attack_ts": [],
            "boss_pick_ts": [],
            "pick_attack_dt": [],
            "attack_pick_dt": [],
            "last_pick_ts": 0.0,
            "last_boss_attack_ts": 0.0,
        }
        ACTIVITY_MONITOR[int(tg_id)] = st

    st["last_seen"] = now

    if is_bd:
        last_bd = float(st.get("last_bd", 0.0) or 0.0)
        if last_bd <= 0 or (now - last_bd) > ACTIVITY_BD_GAP_SEC:
            st["bd_start"] = now
        st["last_bd"] = now

        watch_until = int(st.get("watch_until", 0) or 0)
        bd_start = float(st.get("bd_start", now) or now)
        if watch_until <= int(now) and (now - bd_start) >= ACTIVITY_BD_STREAK_SEC:
            st["watch_started"] = int(now)
            st["watch_until"] = int(now) + ACTIVITY_SURVEIL_SEC
            st["has_non_bd"] = False
            st["bd_actions"] = 0
            st["non_bd_actions"] = 0
            st["total_actions"] = 0
            st["trigger"] = "2ч непрерывной активности босс/данж"
            st["logs"] = []
            st["attack_ts"] = []
            st["boss_pick_ts"] = []
            st["pick_attack_dt"] = []
            st["attack_pick_dt"] = []
            st["last_pick_ts"] = 0.0
            st["last_boss_attack_ts"] = 0.0
            _monitor_compact_append(st["logs"], int(now), "sys:watch_start")

    if int(st.get("watch_until", 0) or 0) > int(now):
        st["total_actions"] = int(st.get("total_actions", 0) or 0) + 1
        if is_bd:
            st["bd_actions"] = int(st.get("bd_actions", 0) or 0) + 1
        else:
            st["has_non_bd"] = True
            st["non_bd_actions"] = int(st.get("non_bd_actions", 0) or 0) + 1
        if _is_attack_action(label):
            hits = list(st.get("attack_ts", []))
            hits.append(int(now))
            if len(hits) > 600:
                hits = hits[-600:]
            st["attack_ts"] = hits
        if label == "cb:boss_pick":
            picks = list(st.get("boss_pick_ts", []))
            picks.append(int(now))
            if len(picks) > 300:
                picks = picks[-300:]
            st["boss_pick_ts"] = picks
            last_boss_attack_ts = float(st.get("last_boss_attack_ts", 0.0) or 0.0)
            if last_boss_attack_ts > 0:
                dt = float(now) - last_boss_attack_ts
                # Интервал между последним ударом и входом в нового босса.
                if 0.2 <= dt <= 12.0:
                    seq2 = list(st.get("attack_pick_dt", []))
                    seq2.append(dt)
                    if len(seq2) > 120:
                        seq2 = seq2[-120:]
                    st["attack_pick_dt"] = seq2
            st["last_pick_ts"] = float(now)
        elif label == "cb:battle:attack":
            last_pick_ts = float(st.get("last_pick_ts", 0.0) or 0.0)
            if last_pick_ts > 0:
                dt = float(now) - last_pick_ts
                # Интересует именно первый удар после входа в босса.
                if 0.2 <= dt <= 12.0:
                    seq = list(st.get("pick_attack_dt", []))
                    seq.append(dt)
                    if len(seq) > 120:
                        seq = seq[-120:]
                    st["pick_attack_dt"] = seq
                st["last_pick_ts"] = 0.0
            st["last_boss_attack_ts"] = float(now)
        _monitor_compact_append(st["logs"], int(now), label)


def _monitor_track_message(message: Message):
    if not message.from_user:
        return
    text = str(message.text or "").strip()
    if not text:
        return
    is_bd = _is_bd_message(text)
    if text.startswith("/"):
        cmd = text.split()[0].lower()
        label = f"msg:{cmd}"
    else:
        label = f"msg:{text.lower()[:32]}"
    _monitor_track_action(int(message.from_user.id), label, is_bd)


def _monitor_track_callback(cb: CallbackQuery):
    if not cb.from_user:
        return
    data = str(cb.data or "").strip()
    if not data:
        return
    is_bd = _is_bd_callback(data)
    # Для отчёта не нужен полный payload после gid/id, оставляем устойчивые префиксы.
    if data.startswith("guild:bossatk:"):
        label = "cb:guild:bossatk"
    elif data.startswith("boss_pick:"):
        label = "cb:boss_pick"
    elif data.startswith("battle:attack"):
        label = "cb:battle:attack"
    elif data.startswith("dungeon:attack"):
        label = "cb:dungeon:attack"
    else:
        label = f"cb:{data[:48]}"
    _monitor_track_action(int(cb.from_user.id), label, is_bd)


async def _activity_report_user(bot: Bot, tg_id: int, st: dict, mode: str):
    u = db.get_user(int(tg_id))
    nick = _display_name(u) if u else f"id{tg_id}"
    uname = str(_row_get(u, "username", "") or "").strip() if u else ""
    watch_started = int(st.get("watch_started", 0) or 0)
    watch_until = int(st.get("watch_until", 0) or 0)
    duration = max(0, watch_until - watch_started)
    bd_actions = int(st.get("bd_actions", 0) or 0)
    non_bd_actions = int(st.get("non_bd_actions", 0) or 0)
    total_actions = int(st.get("total_actions", 0) or 0)
    trigger = str(st.get("trigger", "-") or "-")
    avg_i, std_i = _monitor_rhythm_stats(list(st.get("attack_ts", [])))
    pick_avg, pick_std, pick_rng = _monitor_interval_profile(list(st.get("pick_attack_dt", [])))
    pick_samples = len(list(st.get("pick_attack_dt", [])))
    attack_pick_avg, attack_pick_std, attack_pick_rng = _monitor_interval_profile(list(st.get("attack_pick_dt", [])))
    attack_pick_samples = len(list(st.get("attack_pick_dt", [])))
    unique_labels, expanded_actions, dominant_share = _monitor_log_stats(list(st.get("logs", [])))

    mode_line = "⚠️ За час не обнаружены действия вне боссов/данжей."
    if mode == "parallel":
        mode_line = "⚠️ Зафиксирован параллельный паттерн: бои + прочие действия с ровным ритмом ударов."
    elif mode == "rhythm_only_bd":
        mode_line = "⚠️ Зафиксирован сверхмонотонный ритм ударов и однообразные действия в boss/dungeon."
    elif mode == "boss_pick_attack":
        mode_line = "⚠️ Зафиксирован повторяющийся паттерн: последний удар -> вход в босса -> первый удар с ровным интервалом."

    lines = [
        "🚨 Подозрительная активность",
        f"👤 Игрок: {escape(nick)}",
        f"🆔 ID: <a href=\"tg://user?id={tg_id}\">{tg_id}</a>",
        f"🔗 Username: @{escape(uname)}" if uname else "🔗 Username: нет",
        f"⏱ Мониторинг: {duration // 60} мин",
        f"🎯 Boss/Dungeon действий: {bd_actions}",
        f"🧭 Прочих действий: {non_bd_actions}",
        f"📌 Всего действий: {total_actions}",
        f"🧩 Уникальных действий: {unique_labels} | Доминирующее: {dominant_share * 100:.1f}%",
        f"🎼 Ритм ударов: avg {avg_i:.2f}s | std {std_i:.2f}s",
        f"🎯 Удар->вход: avg {attack_pick_avg:.2f}s | std {attack_pick_std:.2f}s | range {attack_pick_rng:.2f}s | n={attack_pick_samples}",
        f"🎯 Вход->удар: avg {pick_avg:.2f}s | std {pick_std:.2f}s | range {pick_rng:.2f}s | n={pick_samples}",
        f"📝 Причина старта слежки: {escape(trigger)}",
        mode_line,
    ]
    text = "\n".join(lines)

    log_lines = [
        "Activity report",
        f"user_id={tg_id}",
        f"nick={nick}",
        f"username={uname or '-'}",
        f"watch_started={watch_started}",
        f"watch_until={watch_until}",
        f"mode={mode}",
        f"non_bd_actions={non_bd_actions}",
        f"unique_labels={unique_labels}",
        f"expanded_actions={expanded_actions}",
        f"dominant_share={dominant_share:.4f}",
        f"attack_avg_interval={avg_i:.3f}",
        f"attack_std_interval={std_i:.3f}",
        f"attack_pick_samples={attack_pick_samples}",
        f"attack_pick_avg={attack_pick_avg:.3f}",
        f"attack_pick_std={attack_pick_std:.3f}",
        f"attack_pick_range={attack_pick_rng:.3f}",
        f"pick_attack_samples={pick_samples}",
        f"pick_attack_avg={pick_avg:.3f}",
        f"pick_attack_std={pick_std:.3f}",
        f"pick_attack_range={pick_rng:.3f}",
        "",
        "Logs:",
    ]
    for ts, label, cnt in st.get("logs", []):
        dt = _fmt_ts_msk(int(ts), "%Y-%m-%d %H:%M:%S")
        suffix = f" x{cnt}" if int(cnt) > 1 else ""
        log_lines.append(f"{dt} | {label}{suffix}")
    payload = "\n".join(log_lines)
    bio = BytesIO(payload.encode("utf-8"))
    bio.name = f"activity_{tg_id}_{int(time.time())}.txt"

    for admin_id in SUPER_ADMINS:
        try:
            await bot.send_message(int(admin_id), text, parse_mode=ParseMode.HTML)
            await bot.send_document(
                int(admin_id),
                BufferedInputFile(bio.getvalue(), filename=bio.name),
                caption=f"Лог активности за час: {tg_id}",
            )
        except Exception:
            continue


async def _activity_apply_penalty(bot: Bot, tg_id: int, mode: str):
    """Автосанкции отключены: античит только репортит администрации."""
    _ = (bot, tg_id, mode)
    return


async def _activity_monitor_worker(bot: Bot):
    while True:
        try:
            now = int(time.time())
            to_forget: list[int] = []
            for uid, st in list(ACTIVITY_MONITOR.items()):
                last_seen = int(st.get("last_seen", 0) or 0)
                watch_until = int(st.get("watch_until", 0) or 0)
                if watch_until > 0 and now >= watch_until:
                    bd_actions = int(st.get("bd_actions", 0) or 0)
                    non_bd_actions = int(st.get("non_bd_actions", 0) or 0)
                    has_non_bd = bool(st.get("has_non_bd", False))
                    avg_i, std_i = _monitor_rhythm_stats(list(st.get("attack_ts", [])))
                    unique_labels, _expanded_actions, dominant_share = _monitor_log_stats(list(st.get("logs", [])))
                    attack_samples = len(list(st.get("attack_ts", [])))
                    pick_attack_vals = list(st.get("pick_attack_dt", []))
                    pick_avg, pick_std, pick_rng = _monitor_interval_profile(pick_attack_vals)
                    pick_samples = len(pick_attack_vals)
                    attack_pick_vals = list(st.get("attack_pick_dt", []))
                    attack_pick_avg, attack_pick_std, attack_pick_rng = _monitor_interval_profile(attack_pick_vals)
                    attack_pick_samples = len(attack_pick_vals)

                    only_bd_suspicious = bd_actions >= ACTIVITY_REPORT_MIN_BD_ACTIONS and not has_non_bd
                    parallel_suspicious = (
                        has_non_bd
                        and bd_actions >= ACTIVITY_PARALLEL_MIN_BD_ACTIONS
                        and non_bd_actions >= ACTIVITY_PARALLEL_MIN_NON_BD_ACTIONS
                        and len(list(st.get("attack_ts", []))) >= ACTIVITY_RHYTHM_SAMPLE_MIN
                        and ACTIVITY_RHYTHM_AVG_MIN <= avg_i <= ACTIVITY_RHYTHM_AVG_MAX
                        and std_i <= ACTIVITY_RHYTHM_STD_MAX
                    )
                    # Дополнительный детект: сверхмонотонный ритм в boss/dungeon без побочных действий.
                    rhythm_only_bd_suspicious = (
                        not has_non_bd
                        and bd_actions >= ACTIVITY_PARALLEL_MIN_BD_ACTIONS
                        and attack_samples >= ACTIVITY_ONLY_BD_ATTACK_TS_MIN
                        and ACTIVITY_RHYTHM_AVG_MIN <= avg_i <= ACTIVITY_RHYTHM_AVG_MAX
                        and std_i <= ACTIVITY_RHYTHM_STD_STRICT
                        and unique_labels <= ACTIVITY_ONLY_BD_UNIQUE_LABELS_MAX
                        and dominant_share >= 0.85
                    )
                    boss_pick_attack_suspicious = (
                        not has_non_bd
                        and bd_actions >= ACTIVITY_REPORT_MIN_BD_ACTIONS
                        and pick_samples >= ACTIVITY_PICK_ATTACK_MIN_SAMPLES
                        and attack_pick_samples >= ACTIVITY_PICK_ATTACK_MIN_SAMPLES
                        and ACTIVITY_PICK_ATTACK_AVG_MIN <= pick_avg <= ACTIVITY_PICK_ATTACK_AVG_MAX
                        and ACTIVITY_PICK_ATTACK_AVG_MIN <= attack_pick_avg <= ACTIVITY_PICK_ATTACK_AVG_MAX
                        and pick_std <= ACTIVITY_PICK_ATTACK_STD_MAX
                        and attack_pick_std <= ACTIVITY_ATTACK_PICK_STD_MAX
                        and pick_rng <= ACTIVITY_PICK_ATTACK_RANGE_MAX
                        and attack_pick_rng <= ACTIVITY_ATTACK_PICK_RANGE_MAX
                        and abs(pick_avg - attack_pick_avg) <= ACTIVITY_PICK_PAIR_AVG_DIFF_MAX
                    )

                    if boss_pick_attack_suspicious:
                        await _activity_report_user(bot, int(uid), st, mode="boss_pick_attack")
                    elif rhythm_only_bd_suspicious:
                        await _activity_report_user(bot, int(uid), st, mode="rhythm_only_bd")
                    elif parallel_suspicious:
                        await _activity_report_user(bot, int(uid), st, mode="parallel")
                    elif only_bd_suspicious:
                        await _activity_report_user(bot, int(uid), st, mode="only_bd")
                    st["watch_started"] = 0
                    st["watch_until"] = 0
                    st["has_non_bd"] = False
                    st["bd_actions"] = 0
                    st["non_bd_actions"] = 0
                    st["total_actions"] = 0
                    st["trigger"] = ""
                    st["logs"] = []
                    st["attack_ts"] = []
                    st["boss_pick_ts"] = []
                    st["pick_attack_dt"] = []
                    st["attack_pick_dt"] = []
                    st["last_pick_ts"] = 0.0
                    st["last_boss_attack_ts"] = 0.0
                    # После отчёта начинаем новый детект только с нового окна активности.
                    st["bd_start"] = float(st.get("last_bd", now) or now)
                if (now - last_seen) > ACTIVITY_STALE_FORGET_SEC and int(st.get("watch_until", 0) or 0) <= 0:
                    to_forget.append(int(uid))
            for uid in to_forget:
                ACTIVITY_MONITOR.pop(uid, None)
        except Exception as e:
            log.warning(f"activity_monitor_worker error: {e}")
        await asyncio.sleep(15)


def _is_creator(u) -> bool:
    if u is None:
        return False
    return int(u["tg_id"]) in SUPER_ADMINS or int(u["admin_role"]) == 5


def _is_admin(u) -> bool:
    if u is None:
        return False
    return int(u["tg_id"]) in SUPER_ADMINS or int(u["admin_role"]) >= 1


def _can_moderate(u) -> bool:
    return _is_creator(u)


def _display_name(u) -> str:
    nick = str(u["nickname"] or "").strip()
    if nick:
        return nick
    uname = str(u["username"] or "").strip()
    return uname if uname else f"id{u['tg_id']}"


def _is_admin_item_name(name: str) -> bool:
    return "admin" in str(name or "").strip().lower()


def _is_vip_donate_item_name(name: str) -> bool:
    low = str(name or "").strip().lower()
    return low.startswith("👑 vip")


def _rebirth_mult_expected(rebirth_count: int) -> float:
    cnt = max(0, int(rebirth_count or 0))
    return float(REBIRTH_MULT_STEP ** cnt)


def _cb_stale_after_rebirth(cb: "CallbackQuery", u) -> bool:
    """
    Возвращает True если сообщение с кнопкой было отправлено ДО последнего ребёрта.
    Используется для блокировки финансовых операций из старых меню.
    """
    last_rebirth_at = int(_row_get(u, "last_rebirth_at", 0) or 0)
    if last_rebirth_at <= 0:
        return False
    try:
        msg_ts = int(cb.message.date.timestamp())
    except Exception:
        return False
    return msg_ts < last_rebirth_at


def _sync_rebirth_mult_for_user(u):
    """Синхронизирует rebirth_mult с rebirth_count для старых/ручных правок."""
    if not u:
        return u
    tg_id = int(_row_get(u, "tg_id", 0) or 0)
    if tg_id <= 0:
        return u
    count = int(_row_get(u, "rebirth_count", 0) or 0)
    expected = _rebirth_mult_expected(count)
    current = float(_row_get(u, "rebirth_mult", 1.0) or 1.0)
    if abs(current - expected) <= 1e-9:
        return u
    db.update_user(tg_id, rebirth_mult=expected)
    return db.get_user(tg_id) or u


def _arena_max_weapon_bonus(arena: int) -> int:
    a = max(1, min(int(arena), gd.max_arena()))
    tiers = gd.WEAPON_ROLLS.get(a, gd.WEAPON_ROLLS[1])
    return max(int(hi) for _lo, hi in tiers)


def _arena_max_pet_bonus(arena: int) -> int:
    a = max(1, min(int(arena), gd.max_arena()))
    if a in gd.PET_CASE_POOLS:
        return max(int(it[2]) for it in gd.PET_CASE_POOLS[a])
    tiers = gd.PET_ROLLS.get(a, gd.PET_ROLLS[1])
    return max(int(hi) for _lo, hi in tiers)


def _vip_target_bonus(item_type: str, arena: int) -> int:
    # VIP-экип держим на x4 от максимального бонуса арены.
    if item_type == "weapon":
        return max(1, int(_arena_max_weapon_bonus(arena) * 4.0))
    return max(1, int(_arena_max_pet_bonus(arena) * 4.0))


def _sync_vip_donate_items_for_user(tg_id: int, arena: int):
    """Подтягивает бонус VIP-экипировки к текущей арене владельца."""
    import sqlite3 as _sq

    uid = int(tg_id)
    a = max(1, min(int(arena), gd.max_arena()))
    specs = [
        ("weapon", VIP_DONATE_WEAPON_NAME),
        ("pet", VIP_DONATE_PET_NAME),
    ]
    with _sq.connect("bot.db", timeout=30) as con:
        con.row_factory = _sq.Row
        for item_type, item_name in specs:
            need_bonus = _vip_target_bonus(item_type, a)
            rows = con.execute(
                "SELECT id, bonus FROM inventory WHERE tg_id = ? AND type = ? AND name = ? ORDER BY id ASC",
                (uid, item_type, item_name),
            ).fetchall()
            if not rows:
                continue
            keep_id = int(rows[0]["id"])
            con.execute(
                "UPDATE inventory SET level = 1, bonus = ?, count = 1 WHERE id = ?",
                (need_bonus, keep_id),
            )
            for r in rows[1:]:
                rid = int(r["id"])
                con.execute("DELETE FROM inventory WHERE id = ?", (rid,))
                con.execute("DELETE FROM saved_items WHERE item_id = ?", (rid,))
                con.execute("DELETE FROM item_enchants WHERE item_id = ?", (rid,))


def _grant_vip_donate_item(target_id: int, item_type: str) -> tuple[bool, str]:
    import sqlite3 as _sq

    uid = int(target_id)
    u = db.get_user(uid)
    if not u:
        return False, "Игрок не найден."
    if item_type not in ("weapon", "pet"):
        return False, "Неизвестный тип предмета."

    item_name = VIP_DONATE_WEAPON_NAME if item_type == "weapon" else VIP_DONATE_PET_NAME
    need_bonus = _vip_target_bonus(item_type, int(_row_get(u, "arena", 1) or 1))

    with _sq.connect("bot.db", timeout=30) as con:
        con.row_factory = _sq.Row
        rows = con.execute(
            "SELECT id FROM inventory WHERE tg_id = ? AND type = ? AND name = ? ORDER BY id ASC",
            (uid, item_type, item_name),
        ).fetchall()
        if rows:
            keep_id = int(rows[0]["id"])
            con.execute(
                "UPDATE inventory SET level = 1, bonus = ?, count = 1, in_bank = 0 WHERE id = ?",
                (need_bonus, keep_id),
            )
            for r in rows[1:]:
                rid = int(r["id"])
                con.execute("DELETE FROM inventory WHERE id = ?", (rid,))
                con.execute("DELETE FROM saved_items WHERE item_id = ?", (rid,))
                con.execute("DELETE FROM item_enchants WHERE item_id = ?", (rid,))
        else:
            cur = con.execute(
                "INSERT INTO inventory (tg_id, type, name, level, bonus, count, in_bank) VALUES (?, ?, ?, 1, ?, 1, 0)",
                (uid, item_type, item_name, need_bonus),
            )
            keep_id = int(cur.lastrowid)

    db.set_equipped_item(uid, item_type, keep_id)
    return True, f"{item_name} выдан и надет (bonus {fmt_num(need_bonus)})."


def _admin_stats_enabled(u) -> bool:
    return int(db.get_stat(int(u["tg_id"]), ADMIN_STATS_MODE_KEY, 1) or 0) == 1


def _equipped_admin_items(u) -> tuple[bool, bool]:
    tg_id = int(u["tg_id"])
    eq_w_id = int(_row_get(u, "equipped_weapon_id", 0) or 0)
    eq_p_id = int(_row_get(u, "equipped_pet_id", 0) or 0)
    w_item = db.get_inventory_item(tg_id, eq_w_id) if eq_w_id > 0 else None
    p_item = db.get_inventory_item(tg_id, eq_p_id) if eq_p_id > 0 else None
    w_admin = bool(w_item and _is_admin_item_name(str(_row_get(w_item, "name", "") or "")))
    p_admin = bool(p_item and _is_admin_item_name(str(_row_get(p_item, "name", "") or "")))
    return w_admin, p_admin


def _admin_mode_active(u) -> bool:
    w_admin, p_admin = _equipped_admin_items(u)
    return _admin_stats_enabled(u) and (w_admin or p_admin)


def _set_admin_stats_enabled(tg_id: int, enabled: bool):
    cur = int(db.get_stat(int(tg_id), ADMIN_STATS_MODE_KEY, 1) or 0)
    target = 1 if enabled else 0
    if cur == target:
        return
    db.add_stat(int(tg_id), ADMIN_STATS_MODE_KEY, target - cur)


def _artifact_slot_count(u) -> int:
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip >= 4:
        return 4
    if vip >= 3:
        return 3
    return 2


def _artifact_bag_count(tg_id: int) -> int:
    return max(0, int(db.get_stat(int(tg_id), ARTIFACT_BAG_STAT_KEY, 0) or 0))


def _artifact_add_bags(tg_id: int, delta: int):
    cur = _artifact_bag_count(int(tg_id))
    db.set_stat_value(int(tg_id), ARTIFACT_BAG_STAT_KEY, max(0, cur + int(delta)))


def _artifact_item_emoji(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    # Для составных emoji (например, 🏴‍☠️) берем токен до первого пробела.
    return raw.split(" ", 1)[0]


def _artifact_items_for_user(tg_id: int) -> list:
    items = db.inventory_list(int(tg_id), limit=5000)
    return [it for it in items if str(_row_get(it, "type", "")) == "artifact" and int(_row_get(it, "in_bank", 0) or 0) == 0]


def _artifact_sort_key(it) -> tuple:
    lvl = int(_row_get(it, "level", 1) or 1)
    emo = _artifact_item_emoji(str(_row_get(it, "name", "") or ""))
    cnt = int(_row_get(it, "count", 0) or 0)
    return (-lvl, emo, -cnt, int(_row_get(it, "id", 0) or 0))


def _artifact_item_short(it) -> str:
    lvl = max(1, min(10, int(_row_get(it, "level", 1) or 1)))
    icon = ARTIFACT_LEVEL_ICON.get(lvl, "⚪")
    emo = _artifact_item_emoji(str(_row_get(it, "name", "") or ""))
    cnt = int(_row_get(it, "count", 1) or 1)
    trust_mark = "🤝" if _artifact_trust_info(int(_row_get(it, "id", 0) or 0), int(_row_get(it, "tg_id", 0) or 0)) else ""
    return f"{emo}{icon}{trust_mark} x{cnt}"


def _artifact_clean_title_name(it, emo: str, cfg: dict | None) -> str:
    """Нормализует заголовок артефакта (убирает дубли emoji в старых предметах)."""
    def _norm(e: str) -> str:
        return str(e or "").replace("\ufe0f", "").strip()

    if cfg and str(cfg.get("name", "")).strip():
        base = str(cfg["name"]).strip()
        if base.startswith(emo):
            base = base[len(emo):].strip()
        while base.startswith(emo):
            base = base[len(emo):].strip()
        return f"{emo} {base}".strip()
    raw = str(_row_get(it, "name", "") or "").strip()
    if not raw:
        return f"{emo} Артефакт"

    # Убираем повторяющиеся leading-emoji у старых записей вида "🔮 🔮 Сфера ...".
    parts = raw.split()
    while parts and _norm(parts[0]) == _norm(emo):
        parts.pop(0)
    tail = " ".join(parts).strip()
    return f"{emo} {tail}".strip() if tail else f"{emo} Артефакт"


def _artifact_open_bags(u, count: int) -> str:
    tg_id = int(u["tg_id"])
    have = _artifact_bag_count(tg_id)
    cnt = max(1, int(count or 1))
    if have < cnt:
        return f"❌ Недостаточно сумок артефактов. Есть: {have}, нужно: {cnt}."

    _artifact_add_bags(tg_id, -cnt)
    emojis = list(ARTIFACT_TYPES.keys())
    weights = [float(ARTIFACT_TYPES[e]["weight"]) for e in emojis]
    # 🍀 Подкова Удачи: повышает шанс выпадения любого другого артефакта.
    luck_mult = 1.0 + float(_artifact_effects(u).get("artifact_luck", 0.0)) * 0.2
    if luck_mult > 1.0:
        boosted = []
        for emo, w in zip(emojis, weights):
            if emo == "🍀":
                boosted.append(float(w))
            else:
                boosted.append(float(w) * luck_mult)
        weights = boosted
    got: dict[str, int] = {e: 0 for e in emojis}

    for _ in range(cnt):
        emo = random.choices(emojis, weights=weights, k=1)[0]
        cfg = ARTIFACT_TYPES[emo]
        name = f"{emo} {cfg['name']}"
        db.add_inventory_item(tg_id, "artifact", name, 1, 0, 1)
        got[emo] += 1

    lines = [f"👜 Открыто сумок артефактов: {cnt}"]
    for emo in emojis:
        if got[emo] > 0:
            lines.append(f"• {emo} {ARTIFACT_TYPES[emo]['name']}: +{got[emo]}")
    return "\n".join(lines)


def _artifact_slot_label(u, slot_idx: int) -> str:
    key = ARTIFACT_SLOT_KEYS[slot_idx - 1]
    item_id = int(db.get_stat(int(u["tg_id"]), key, 0) or 0)
    if item_id <= 0:
        return "пусто"
    it = db.get_inventory_item(int(u["tg_id"]), item_id)
    if not it or str(_row_get(it, "type", "")) != "artifact":
        return "пусто"
    return _artifact_item_short(it)


def _artifact_slot_brief(u, slot_idx: int) -> str:
    key = ARTIFACT_SLOT_KEYS[slot_idx - 1]
    item_id = int(db.get_stat(int(u["tg_id"]), key, 0) or 0)
    if item_id <= 0:
        return "пусто"
    it = db.get_inventory_item(int(u["tg_id"]), item_id)
    if not it or str(_row_get(it, "type", "")) != "artifact":
        return "пусто"
    emo = _artifact_item_emoji(str(_row_get(it, "name", "") or ""))
    lvl = max(1, min(10, int(_row_get(it, "level", 1) or 1)))
    icon = ARTIFACT_LEVEL_ICON.get(lvl, "⚪")
    trust_mark = "🤝" if _artifact_trust_info(int(item_id), int(u["tg_id"])) else ""
    return f"{emo}{icon}{trust_mark}"


# Стоимость апа артефакта по уровням (L1→L2, ..., L9→L10)
ARTIFACT_MERGE_COIN_COST = {
    1: 10_000_000,
    2: 25_000_000,
    3: 60_000_000,
    4: 140_000_000,
    5: 300_000_000,
    6: 600_000_000,
    7: 1_100_000_000,
    8: 1_900_000_000,
    9: 3_250_000_000,
}


def _artifact_merge_one(tg_id: int, item_id: int) -> tuple[bool, str]:
    it = db.get_inventory_item(int(tg_id), int(item_id))
    if not it or str(_row_get(it, "type", "")) != "artifact":
        return False, "Артефакт не найден."
    if _artifact_trust_info(int(item_id), int(tg_id)):
        return False, "Доверенный артефакт нельзя улучшать. Сначала верни его владельцу."
    emo = _artifact_item_emoji(str(_row_get(it, "name", "") or ""))
    if emo == "🎮":
        return False, "Ключ Аркады не прокачивается."
    lvl = int(_row_get(it, "level", 1) or 1)
    if lvl >= 10:
        return False, "Максимальный уровень (10)."
    cnt = int(_row_get(it, "count", 0) or 0)
    if cnt < 2:
        return False, "Нужно минимум 2 одинаковых артефакта этого уровня."

    coin_cost = ARTIFACT_MERGE_COIN_COST.get(lvl, 3_250_000_000)
    u = db.get_user(int(tg_id))
    coins = int(_row_get(u, "coins", 0) or 0) if u else 0
    if coins < coin_cost:
        return False, f"Недостаточно монет. Нужно {fmt_num(coin_cost)} 🪙 для апа L{lvl}→L{lvl+1}."

    name = str(_row_get(it, "name", "") or "")
    bonus = int(_row_get(it, "bonus", 0) or 0)
    db.update_user(int(tg_id), coins=coins - coin_cost)
    db.consume_inventory_item(int(tg_id), int(item_id), 2)
    db.add_inventory_item(int(tg_id), "artifact", name, lvl + 1, bonus, 1)
    return True, f"✅ {name} улучшен до L{lvl + 1} | -{fmt_num(coin_cost)} 🪙"


def _artifact_prepare_true_rebirth_keep(tg_id: int, vip_lvl: int):
    """Сохраняет только выбранные в слотах артефакты для true rebirth."""
    if int(vip_lvl) >= 5:
        keep_limit = 2
    elif int(vip_lvl) >= 4:
        keep_limit = 1
    else:
        keep_limit = 0

    keep_ids: set[int] = set()
    keys = list(ARTIFACT_SLOT_KEYS[:_artifact_slot_count({"vip_lvl": vip_lvl})])
    stats = db.get_stats(int(tg_id), keys)
    for k in keys:
        item_id = int(stats.get(k, 0) or 0)
        if item_id <= 0:
            continue
        it = db.get_inventory_item(int(tg_id), item_id)
        if it and str(_row_get(it, "type", "")) == "artifact":
            keep_ids.add(int(item_id))
        if len(keep_ids) >= keep_limit:
            break

    for it in _artifact_items_for_user(int(tg_id)):
        item_id = int(_row_get(it, "id", 0) or 0)
        if item_id <= 0:
            continue
        if keep_limit > 0 and item_id in keep_ids:
            db.save_item_by_id(item_id)
        else:
            db.unsave_item_by_id(item_id)


def _artifact_effects(u) -> dict[str, float]:
    tg_id = int(u["tg_id"])
    slot_count = _artifact_slot_count(u)
    keys = list(ARTIFACT_SLOT_KEYS[:slot_count])
    stats = db.get_stats(tg_id, keys)
    effects = {
        "regen": 0.0,
        "dmg": 0.0,
        "heal": 0.0,
        "dodge": 0.0,
        "crit": 0.0,
        "reflect": 0.0,
        "lifesteal": 0.0,
        "artifact_luck": 0.0,
        "case_double": 0.0,
        "train_time": 0.0,
        "train_power": 0.0,
        "coins": 0.0,
        "afk_loot": 0.0,
        "afk_case_chance": 0.0,
        "mini_any": 0.0,
        "dungeon_magic": 0.0,
        "survive": 0.0,
    }
    seen_item_ids: set[int] = set()
    for k in keys:
        item_id = int(stats.get(k, 0) or 0)
        if item_id <= 0:
            continue
        # Один и тот же артефакт не должен стакаться через несколько слотов.
        if item_id in seen_item_ids:
            continue
        it = db.get_inventory_item(tg_id, item_id)
        if not it or str(_row_get(it, "type", "")) != "artifact":
            continue
        seen_item_ids.add(item_id)
        emo = _artifact_item_emoji(str(_row_get(it, "name", "") or ""))
        cfg = ARTIFACT_TYPES.get(emo)
        if not cfg:
            continue
        lvl = max(1, min(10, int(_row_get(it, "level", 1) or 1)))
        effect_key = str(cfg["effect"])
        if effect_key == "mini_any":
            effects[effect_key] = 1.0
        else:
            effects[effect_key] += 0.10 * lvl
    return effects


def _artifact_coin_mult(u) -> float:
    return 1.0 + float(_artifact_effects(u).get("coins", 0.0))


def _artifact_case_double_chance(u) -> float:
    return min(0.95, float(_artifact_effects(u).get("case_double", 0.0)))


def _artifact_lifesteal_pct(u) -> float:
    # 🩸 5% за уровень.
    return min(0.95, float(_artifact_effects(u).get("lifesteal", 0.0)) * 0.5)


def _artifact_reflect_pct(u) -> float:
    # 🛡 10% за уровень.
    return min(2.5, float(_artifact_effects(u).get("reflect", 0.0)))


def _artifact_dodge_chance(u) -> float:
    base = 0.10
    return min(0.85, base * (1.0 + float(_artifact_effects(u).get("dodge", 0.0))) )


def _artifact_crit_chance(u) -> float:
    base = 0.01
    return min(0.75, base + float(_artifact_effects(u).get("crit", 0.0)) * 0.5)


def _artifact_survive_chance(u) -> float:
    return min(0.95, float(_artifact_effects(u).get("survive", 0.0)) * 0.5)


def _parse_dungeon_difficulty(text: str) -> str:
    t = str(text or "").strip().lower()
    if not t:
        return "easy"
    aliases = {
        "лег": "easy", "легкая": "easy", "л": "easy", "easy": "easy",
        "сред": "medium", "средняя": "medium", "с": "medium", "medium": "medium",
        "слож": "hard", "сложная": "hard", "хард": "hard", "hard": "hard",
    }
    parts = t.split()
    for p in parts[1:]:
        if p in aliases:
            return aliases[p]
    return "easy"


def _dungeon_diff_min_arena(diff: str) -> int:
    key = str(diff or "easy")
    if key == "medium":
        return 6
    if key == "hard":
        return 11
    return 1


def _dungeon_diff_unlocked(arena: int, diff: str) -> bool:
    return int(arena or 1) >= _dungeon_diff_min_arena(diff)


def _dungeon_diff_select_text(arena: int) -> str:
    return "\n".join([
        "⛩ Данж",
        SEP,
        "Выбери сложность:",
        f"• Легкая: доступно с 1 арены {'✅' if _dungeon_diff_unlocked(arena, 'easy') else '🔒'}",
        f"• Средняя: доступно с 6 арены {'✅' if _dungeon_diff_unlocked(arena, 'medium') else '🔒'}",
        f"• Сложная: доступно с 11 арены {'✅' if _dungeon_diff_unlocked(arena, 'hard') else '🔒'}",
    ])


def _dungeon_diff_kb(arena: int) -> InlineKeyboardMarkup:
    def _btn(diff: str, label: str) -> InlineKeyboardButton:
        if _dungeon_diff_unlocked(arena, diff):
            return InlineKeyboardButton(text=label, callback_data=f"dungeon:diff:{diff}")
        return InlineKeyboardButton(text=f"🔒 {label}", callback_data=f"dungeon:diff_locked:{diff}")

    return InlineKeyboardMarkup(inline_keyboard=[
        [_btn("easy", "Легкая")],
        [_btn("medium", "Средняя")],
        [_btn("hard", "Сложная")],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="dungeon:close")],
    ])


def _dungeon_hp_mult_by_difficulty(diff: str, wave: int, arena: int) -> float:
    """Возвращает множитель HP волн по сложности данжа."""
    key = str(diff or "easy")
    wave_i = max(1, int(wave))
    arena_i = max(1, int(arena))

    # Легкая: базовая геометрия волн без дополнительного множителя.
    if key == "easy":
        return 1.0

    # Средняя: заметно плотнее легкой, с плавным ростом от арены и волны.
    if key == "medium":
        return max(1.0, 2.5 + arena_i * 0.15 + wave_i * 0.10)

    if key == "hard":
        # Якорим сложность так, чтобы 20-я волна была около HP финального босса 15 арены.
        target_hp = float(int(gd.ARENAS[15][2].hp * _enemy_hp_mult(15)))
        base_wave20_hp = float(
            int((1800 + (20 ** 2.25) * 180 + arena_i * 2200) * _enemy_hp_mult(arena_i))
        )
        anchor_mult = target_hp / max(1.0, base_wave20_hp)

        # До 20-й волны плавно подводим к якорю, после — аккуратно усиливаем.
        if wave_i < 20:
            curve = 0.60 + (wave_i / 20.0) * 0.40  # 0.60 -> 1.00
        elif wave_i == 20:
            curve = 1.0
        else:
            curve = 1.0 + (wave_i - 20) * 0.03
        return max(1.0, anchor_mult * curve)

    return 1.0


def _dungeon_reward_mult_by_difficulty(diff: str) -> float:
    """Множитель наград по сложности.

    База в текущем коде считается как прежний "базовый" доход волны.
    - easy: -10% к текущей базе
    - medium: +30% к easy
    - hard: +90% к easy
    """
    key = str(diff or "easy")
    easy_mult = 0.90
    if key == "medium":
        return easy_mult * 1.30
    if key == "hard":
        return easy_mult * 1.90
    return easy_mult


def _guild_level_for_user(tg_id: int) -> int:
    g = db.get_user_guild(int(tg_id))
    if not g:
        return 0
    return int(_row_get(g, "level", 1) or 1)


def _guild_buffs_for_user(u) -> dict:
    lvl = _guild_level_for_user(int(u["tg_id"]))
    if lvl <= 0:
        return {"hp": 0.0, "case": 0.0, "dmg": 0.0, "coins": 0.0}
    return GUILD_LEVEL_BUFFS.get(min(5, lvl), GUILD_LEVEL_BUFFS[1])


def _guild_role_for_user(tg_id: int) -> str:
    return str(db.get_user_guild_role(int(tg_id)) or "")


def _is_guild_owner(guild_row, tg_id: int) -> bool:
    return bool(guild_row and int(_row_get(guild_row, "owner_id", 0) or 0) == int(tg_id))


def _is_guild_deputy(guild_id: int, tg_id: int) -> bool:
    m = db.get_guild_member(int(guild_id), int(tg_id))
    return bool(m and str(_row_get(m, "role", "")) == "deputy")


def _is_guild_manager(guild_row, tg_id: int) -> bool:
    if _is_guild_owner(guild_row, tg_id):
        return True
    if not guild_row:
        return False
    return _is_guild_deputy(int(_row_get(guild_row, "id", 0) or 0), int(tg_id))


def _guild_dmg_mult(u) -> float:
    return 1.0 + float(_guild_buffs_for_user(u).get("dmg", 0.0))


def _guild_hp_mult(u) -> float:
    return 1.0 + float(_guild_buffs_for_user(u).get("hp", 0.0))


def _guild_case_mult(u) -> float:
    return 1.0 + float(_guild_buffs_for_user(u).get("case", 0.0))


def _guild_coin_mult(u) -> float:
    return 1.0 + float(_guild_buffs_for_user(u).get("coins", 0.0))


def _true_rebirth_stage(u) -> int:
    return max(0, min(2, int(_row_get(u, "true_rebirth_count", 0) or 0)))


def _true_dmg_mult(u) -> float:
    stage = _true_rebirth_stage(u)
    return {0: 1.0, 1: 2.0, 2: 3.0}.get(stage, 1.0)


def _true_coin_mult(u) -> float:
    stage = _true_rebirth_stage(u)
    return {0: 1.0, 1: 3.0, 2: 5.0}.get(stage, 1.0)


def _true_hp_mult(u) -> float:
    stage = _true_rebirth_stage(u)
    return {0: 1.0, 1: 1.5, 2: 2.0}.get(stage, 1.0)


def _true_train_power_mult(u) -> float:
    stage = _true_rebirth_stage(u)
    return {0: 1.0, 1: 5.0, 2: 10.0}.get(stage, 1.0)


def _true_train_case_mult(u) -> float:
    stage = _true_rebirth_stage(u)
    return {0: 1.0, 1: 1.5, 2: 3.0}.get(stage, 1.0)


def _bio_bonus_active(u) -> bool:
    return int(_row_get(u, "bio_bonus_active", 0) or 0) == 1


def _bio_hp_mult(u) -> float:
    return BIO_HP_MULT if _bio_bonus_active(u) else 1.0


def _bio_dmg_mult(u) -> float:
    return BIO_DMG_MULT if _bio_bonus_active(u) else 1.0


def _bio_train_power_mult(u) -> float:
    return BIO_TRAIN_POWER_MULT if _bio_bonus_active(u) else 1.0


def _bio_train_case_mult(u) -> float:
    return BIO_TRAIN_CASE_MULT if _bio_bonus_active(u) else 1.0


def _ring_bonus_mult(u) -> float:
    pct = gd.RING_BONUS_PCT.get(int(u["active_ring_level"] or 0), 0)
    return 1.0 + pct / 100.0


def _aura_gold_mult(u) -> float:
    return 1.5 if str(u["active_aura"] or "") == "fortune" else 1.0


def _vip_gold_mult(u) -> float:
    vip = int(u["vip_lvl"] or 0)
    return {1: 1.1, 2: 1.25, 3: 1.5, 4: 2.0, 5: 3.0}.get(vip, 1.0)


def _vip_dmg_mult(u) -> float:
    vip = int(u["vip_lvl"] or 0)
    return {1: 1.05, 2: 1.10, 3: 1.15, 4: 1.3, 5: 1.3}.get(vip, 1.0)


def _aura_power_mult(u) -> float:
    return 1.3 if str(u["active_aura"] or "") == "master" else 1.0


def _aura_hunter(u) -> bool:
    return str(u["active_aura"] or "") == "hunter"


def _aura_regen_bonus(u, base_regen: int) -> int:
    if str(u["active_aura"] or "") == "regen":
        boosted = base_regen + max(1, int(u["hp_boost"] or 0) // 20)
        return max(1, int(boosted * AURA_REGEN_MULT))
    return base_regen


def _aura_wrath(u) -> float:
    return 1.5 if str(u["active_aura"] or "") == "wrath" else 1.0


def _has_slot2_weapon(tg_id: int) -> bool:
    return int(db.get_stat(int(tg_id), SLOT2_WEAPON_KEY, 0) or 0) == 1

def _has_slot2_pet(tg_id: int) -> bool:
    return int(db.get_stat(int(tg_id), SLOT2_PET_KEY, 0) or 0) == 1


def _player_weapon_bonus(u) -> int:
    tg_id = int(u["tg_id"])
    eq_w_id = int(_row_get(u, "equipped_weapon_id", 0) or 0)
    eq_w_id2 = int(_row_get(u, "equipped_weapon_id_2", 0) or 0) if _has_slot2_weapon(tg_id) else 0

    def _get_bonus(item_id: int) -> int:
        if item_id <= 0:
            return 0
        it = db.get_inventory_item(tg_id, item_id)
        if not it:
            return 0
        if _is_admin_item_name(str(_row_get(it, "name", "") or "")) and not _admin_stats_enabled(u):
            return ADMIN_HIDE_WEAPON_BONUS
        return int(_row_get(it, "bonus", 0) or 0)

    b1 = _get_bonus(eq_w_id)
    b2 = _get_bonus(eq_w_id2)
    if b1 == 0 and eq_w_id <= 0:
        # fallback: найти лучшее из инвентаря
        b1 = db.inventory_equipped_bonus(tg_id, "weapon")
    return b1 + b2


def _player_pet_bonus(u) -> int:
    tg_id = int(u["tg_id"])
    eq_p_id = int(_row_get(u, "equipped_pet_id", 0) or 0)
    eq_p_id2 = int(_row_get(u, "equipped_pet_id_2", 0) or 0) if _has_slot2_pet(tg_id) else 0

    def _get_bonus(item_id: int) -> int:
        if item_id <= 0:
            return 0
        it = db.get_inventory_item(tg_id, item_id)
        if not it:
            return 0
        if _is_admin_item_name(str(_row_get(it, "name", "") or "")) and not _admin_stats_enabled(u):
            return ADMIN_HIDE_PET_BONUS
        return int(_row_get(it, "bonus", 0) or 0)

    b1 = _get_bonus(eq_p_id)
    b2 = _get_bonus(eq_p_id2)
    if b1 == 0 and eq_p_id <= 0:
        b1 = db.inventory_equipped_bonus(tg_id, "pet")
    return b1 + b2


def _calc_player_damage(u) -> int:
    wb = _player_weapon_bonus(u)
    power = int(u["power"] or 0)
    arena = int(u["arena"] or 1)
    rebirth_mult = _rebirth_mult_expected(int(_row_get(u, "rebirth_count", 0) or 0))
    power_mult = 1 + (math.sqrt(power) / 108) + (math.log10(power + 1) * 0.015)
    dmg = int((14 + wb * WEAPON_EFFECT_MULT) * power_mult * rebirth_mult)
    dmg = int(dmg * DAMAGE_GLOBAL_MULT)
    if arena <= 5:
        dmg = int(dmg * 1.05)
    elif arena <= 10:
        dmg = int(dmg * 1.02)
    dmg = int(dmg * _aura_wrath(u))
    dmg = int(dmg * _vip_dmg_mult(u))
    dmg = int(dmg * _guild_dmg_mult(u))
    dmg = int(dmg * _true_dmg_mult(u))
    dmg = int(dmg * _bio_dmg_mult(u))
    dmg = int(dmg * (1.0 + float(_artifact_effects(u).get("dmg", 0.0))) )
    dmg = int(dmg * _enchant_dmg_mult(u))
    return max(1, dmg)


def _calc_player_max_hp(u) -> int:
    arena = int(u["arena"] or 1)
    pet_bonus = _player_pet_bonus(u)
    rebirth_count = int(u["rebirth_count"] or 0)
    hp_boost = int(u["hp_boost"] or 0)
    base_raw = 120 + arena * 55 + int(pet_bonus * PET_HP_EFFECT_MULT * 4) + rebirth_count * 90
    base = max(1, int(base_raw * BASE_HP_MULT))
    total = int((base + hp_boost) * _guild_hp_mult(u))
    total = int(total * _true_hp_mult(u))
    total = int(total * _bio_hp_mult(u))
    return max(1, int(total * PLAYER_MAX_HP_MULT))


def _world3_weapon_scale(arena: int) -> float:
    """Скалирование мира 3 по росту оружия относительно 10 арены."""
    a = max(1, min(int(arena), gd.max_arena()))
    if a <= 10:
        return 1.0
    w10 = max(1, _arena_max_weapon_bonus(10))
    wa = max(1, _arena_max_weapon_bonus(a))
    return max(1.0, wa / w10)


def _world3_pet_scale(arena: int) -> float:
    """Скалирование мира 3 по росту питомцев относительно 10 арены."""
    a = max(1, min(int(arena), gd.max_arena()))
    if a <= 10:
        return 1.0
    p10 = max(1, _arena_max_pet_bonus(10))
    pa = max(1, _arena_max_pet_bonus(a))
    return max(1.0, pa / p10)


def _calc_regen(u) -> int:
    arena = int(u["arena"] or 1)
    pet_bonus = _player_pet_bonus(u)
    max_hp = _calc_player_max_hp(u)
    regen = max(2, int(max_hp * 0.01) + arena + pet_bonus // 35)
    regen = max(2, int(regen * REGEN_MULT))
    regen = _aura_regen_bonus(u, regen)
    regen = max(2, int(regen * (1.0 + float(_artifact_effects(u).get("regen", 0.0)))))
    regen = max(2, int(regen * _enchant_regen_mult(u)))
    return regen


# ─────────────────────────────────────────────
#  ЗАЧАРОВАНИЯ — ХЕЛПЕРЫ
# ─────────────────────────────────────────────

def _get_equipped_enchants(u) -> dict:
    """
    Возвращает суммарные эффекты зачарований с экипированного оружия и пета.
    Формат: {'enchant_dmg': pct_float, 'enchant_regen': pct_float,
             'enchant_train_power': pct_float, 'enchant_dodge': pct_float}
    где pct_float — доля (0.05 = 5%).
    Результат кэшируется в объекте u как _enchant_cache для повторных вызовов.
    """
    # Простой кэш: если u — dict и уже содержит _enchant_cache, возвращаем его
    if isinstance(u, dict) and "_enchant_cache" in u:
        return u["_enchant_cache"]

    eq_w_id = int(_row_get(u, "equipped_weapon_id", 0) or 0)
    eq_p_id = int(_row_get(u, "equipped_pet_id", 0) or 0)

    item_ids = [i for i in (eq_w_id, eq_p_id) if i > 0]
    all_enchants = db.get_enchants_for_items(item_ids) if item_ids else {}

    result = {}
    for item_id, enchants in all_enchants.items():
        for key, lvl in enchants.items():
            if key not in gd.ENCHANT_CATALOG:
                continue
            lvl_idx = max(0, min(int(lvl), 3)) - 1
            if lvl_idx < 0:
                continue
            pct = gd.ENCHANT_CATALOG[key]["levels"][lvl_idx]["bonus_pct"] / 100.0
            effect = gd.ENCHANT_CATALOG[key]["effect"]
            result[effect] = result.get(effect, 0.0) + pct

    # Кэшируем в словаре u если возможно
    try:
        if isinstance(u, dict):
            u["_enchant_cache"] = result
    except Exception:
        pass
    return result


def _enchant_dmg_mult(u) -> float:
    return 1.0 + _get_equipped_enchants(u).get("enchant_dmg", 0.0)


def _enchant_regen_mult(u) -> float:
    return 1.0 + _get_equipped_enchants(u).get("enchant_regen", 0.0)


def _enchant_train_power_mult(u) -> float:
    return 1.0 + _get_equipped_enchants(u).get("enchant_train_power", 0.0)


def _enchant_dodge_chance(u) -> float:
    """Шанс уклонения от зачарования (0.0–0.25)."""
    return min(0.25, _get_equipped_enchants(u).get("enchant_dodge", 0.0))


def _enchant_item_label(item_id: int) -> str:
    """Строка с перечнем зачарований предмета для инвентаря."""
    enchants = db.get_item_enchants(item_id)
    if not enchants:
        return ""
    parts = []
    for key, lvl in enchants.items():
        data = gd.ENCHANT_CATALOG.get(key)
        if not data:
            continue
        parts.append(f"{data['emoji']} {data['name']} Ур.{lvl}")
    return "  📖 " + " | ".join(parts) if parts else ""


def _get_enchant_shop_pool(tg_id: int) -> list[tuple[str, int]]:
    """
    Возвращает дневной пул зачарований для пользователя [(key, lvl), ...].
    Если пул устарел или отсутствует — генерирует новый.
    """
    today_int = int(_today_msk().strftime("%Y%m%d"))
    stored_day = int(db.get_stat(tg_id, ENCHANT_SHOP_POOL_DAY_KEY, 0) or 0)
    if stored_day == today_int:
        raw = db.get_text_stat(tg_id, ENCHANT_SHOP_POOL_KEY, "")
        if raw:
            try:
                pool = []
                for part in raw.split(","):
                    k, lv = part.strip().split(":")
                    if k in gd.ENCHANT_CATALOG:
                        pool.append((k, int(lv)))
                if len(pool) == gd.ENCHANT_SHOP_DAILY_COUNT:
                    return pool
            except Exception:
                pass
    # Генерируем новый пул
    pool = gd.get_enchant_shop_pool()
    encoded = ",".join(f"{k}:{lv}" for k, lv in pool)
    db.set_text_stat(tg_id, ENCHANT_SHOP_POOL_KEY, encoded)
    db.set_stat_value(tg_id, ENCHANT_SHOP_POOL_DAY_KEY, today_int)
    return pool


def _jinn_open_for_user(u) -> bool:
    global JINN_FORCED_UNTIL
    now = int(time.time())
    if int(u["vip_lvl"] or 0) >= 5:
        return True
    if JINN_FORCED_UNTIL > now:
        return True
    day_key = _today_msk().isoformat()
    hour = db.ensure_trader_hour(int(u["tg_id"]), day_key)
    if hour < 0:
        return False
    cur_hour = _now_msk().hour
    return cur_hour == hour


def _sell_price(bonus: int, level: int, qty: int) -> int:
    if bonus <= 6:
        arena_ref, pct = 1, 0.17
    elif bonus <= 35:
        arena_ref, pct = 2, 0.145
    elif bonus <= 130:
        arena_ref, pct = 3, 0.115
    elif bonus <= 500:
        arena_ref, pct = 4, 0.085
    else:
        arena_ref, pct = 5, 0.07
    base_price = gd.get_weapon_price(arena_ref)
    lvl_mult = {1: 1.0, 2: 2.5, 3: 6.0}.get(level, 1.0)
    raw_price = int(base_price * pct * lvl_mult * qty)
    # Даже после бафа продажа не должна окупать стоимость открытия кейсов.
    hard_cap = int(_case_price(arena_ref) * 0.70 * lvl_mult * qty)
    return max(1, min(raw_price, hard_cap))


def _case_price(arena: int) -> int:
    # Цена = 2.5 убийства последнего босса этой арены.
    a = max(1, min(int(arena), gd.max_arena()))
    bosses = gd.ARENAS.get(a, gd.ARENAS[1])
    last_boss = bosses[-1]
    last_reward = int(last_boss.reward * BOSS_REWARD_MULT * _boss_reward_arena_mult(a))
    price = int(max(150, last_reward * 2.5))
    if 11 <= a <= 15:
        price = int(price * 0.98)
    return max(150, price)


def _is_afk_bonus_chat(chat_id: int | None) -> bool:
    if chat_id is None:
        return False
    try:
        cid = int(chat_id)
    except Exception:
        return False
    abs_cid = abs(cid)
    variants = {cid, abs_cid}
    s = str(abs_cid)
    if s.startswith("100") and len(s) > 3:
        variants.add(int(s[3:]))
    else:
        variants.add(int("100" + s))
    return any(v in AFK_CASE_BONUS_CHAT_IDS for v in variants)


def _train_duration_secs(u) -> int:
    if int(u["vip_lvl"] or 0) >= 5:
        return 0
    vip = int(u["vip_lvl"] or 0)
    bonus_hours = {1: 1, 2: 2, 3: 5, 4: 5, 5: 6}.get(vip, 0)
    time_lvl = int(_row_get(u, "train_time_lvl", 0) or 0)
    base = (1 + bonus_hours) * 3600 + time_lvl * 300
    return int(base * (1.0 + float(_artifact_effects(u).get("train_time", 0.0))))


def _train_tick_seconds(u) -> int:
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip >= 5:
        return 2
    if vip >= 4:
        return 3
    if vip >= 3:
        return 4
    if vip >= 2:
        return 5
    return 6


def _deposit_fee_pct(u) -> int:
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip >= 4:
        return 0
    if vip >= 3:
        return 3
    return 5


def _autosynth_interval_secs(u) -> int:
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip >= 5:
        return 30 * 60
    if vip >= 4:
        return 60 * 60
    if vip >= 3:
        return 3 * 3600
    if vip >= 2:
        return 6 * 3600
    return 0


def _autosell_keep_patterns(tg_id: int) -> list[str]:
    return []


def _autosell_should_keep_item(tg_id: int, item_name: str) -> bool:
    low = str(item_name or "").strip().lower()
    if not low:
        return False
    for pat in _autosell_keep_patterns(int(tg_id)):
        if pat and pat in low:
            return True
    return False


def _vip_auto_daily_claim(u) -> int:
    if int(_row_get(u, "vip_lvl", 0) or 0) < 1:
        return 0
    now = int(time.time())
    last = int(_row_get(u, "last_daily_claim", 0) or 0)
    if now - last < 86400:
        return 0
    rank_idx = int(_row_get(u, "rank_idx", 0) or 0)
    reward = int(500 * (1 + rank_idx * 0.5) * _true_coin_mult(u) * _artifact_coin_mult(u))
    db.update_user(int(u["tg_id"]), coins=int(_row_get(u, "coins", 0) or 0) + reward, last_daily_claim=now)
    return reward


def _vip_weekly_bag_claim(u) -> bool:
    if int(_row_get(u, "vip_lvl", 0) or 0) < 5:
        return False
    tg_id = int(u["tg_id"])
    now = int(time.time())
    last = int(db.get_stat(tg_id, VIP_WEEKLY_BAG_TS_KEY, 0) or 0)
    if now - last < 7 * 86400:
        return False
    _artifact_add_bags(tg_id, 1)
    db.set_stat_value(tg_id, VIP_WEEKLY_BAG_TS_KEY, now)
    return True


def _train_case_mult(u) -> float:
    lvl = int(_row_get(u, "train_case_lvl", 0) or 0)
    return 1.0 + lvl * 0.01


def _train_power_mult(u) -> float:
    lvl = int(_row_get(u, "train_power_lvl", 0) or 0)
    base = 1.0 + lvl * 0.0225
    return base * (1.0 + float(_artifact_effects(u).get("train_power", 0.0))) * _enchant_train_power_mult(u)


def _train_up_cost(kind: str, level: int) -> int:
    lvl = max(0, int(level))
    if kind == "case":
        return int(TRAIN_CASE_UP_BASE * (TRAIN_CASE_UP_GROWTH ** lvl))
    if kind == "power":
        return int(TRAIN_POWER_UP_BASE * (TRAIN_POWER_UP_GROWTH ** lvl))
    return int(TRAIN_TIME_UP_BASE * (TRAIN_TIME_UP_GROWTH ** lvl))


def _buy_train_upgrade(tg_id: int, kind: str) -> tuple[bool, str]:
    u = db.get_user(int(tg_id))
    if not u:
        return False, "❌ Профиль не найден."
    field_map = {
        "case": "train_case_lvl",
        "power": "train_power_lvl",
        "time": "train_time_lvl",
    }
    title_map = {
        "case": "Шанс кейсов (ШК)",
        "power": "Сила тренировки (СТ)",
        "time": "Время тренировки (ВТ)",
    }
    if kind not in field_map:
        return False, "❌ Неизвестный тип улучшения."
    field = field_map[kind]
    level = int(_row_get(u, field, 0) or 0)
    if level >= TRAIN_UPGRADE_MAX_LVL:
        return False, f"❌ Достигнут лимит уровня ({TRAIN_UPGRADE_MAX_LVL})."
    cost = _train_up_cost(kind, level)
    coins = int(_row_get(u, "coins", 0) or 0)
    if coins < cost:
        return False, f"💸 Недостаточно монет. Нужно: {fmt_num(cost)} 🪙"
    db.update_user(int(tg_id), coins=coins - cost, **{field: level + 1})
    return True, (
        f"✅ Улучшено: {title_map[kind]}\n"
        f"📈 Уровень: {level} -> {level + 1}\n"
        f"🪙 Потрачено: {fmt_num(cost)}"
    )


def _enemy_hp_mult(arena: int) -> float:
    # Мягче ранний прогресс, хардкор остается в поздней игре.
    base = min(1.45, 1.12 + arena * 0.016)
    if arena <= 5:
        return base * 0.82
    if arena <= 10:
        return base * 1.02
    # Мир 3 (11-15): HP боссов растет пропорционально росту питомцев.
    return base * 1.18 * _world3_pet_scale(arena)


def _boss_reward_arena_mult(arena: int) -> float:
    if arena <= 5:
        return 1.22
    if arena <= 10:
        return 1.10
    # Арены 11-15: +5% монет с боссов.
    return 1.05


def _enemy_atk_mult(arena: int) -> float:
    base = min(1.42, 1.10 + arena * 0.014)
    if arena <= 5:
        return base * 0.85
    if arena <= 10:
        return base * 0.95
    # Мир 3 (11-15): урон боссов растет пропорционально росту оружия.
    return base * _world3_weapon_scale(arena)


def _item_case_bonus_scale(arena: int) -> float:
    if arena <= 5:
        return 0.88
    if arena <= 10:
        return 0.80
    return 0.72


def _rebirth_power_required(rebirth_count: int) -> int:
    """Требование по мощности для ребёрта растет плавно вместе с прогрессом."""
    return int(950 * (2.45 ** max(0, rebirth_count)) * ECONOMY_COST_MULT)


def _true_rebirth_requirements(stage: int) -> dict:
    if stage <= 1:
        return {
            "arena": 15,
            "coins": 500_000_000,
            "power": 50_000_000,
            "kills_total": 500,
            "rebirths": 15,
            "kills_world23": 0,
        }
    return {
        "arena": 15,
        "coins": 1_000_000_000,
        "power": 100_000_000,
        "kills_total": 0,
        "rebirths": 50,
        "kills_world23": 2000,
    }


def _next_true_rebirth_stage(u) -> int:
    cur = _true_rebirth_stage(u)
    if cur >= 2:
        return 0
    return cur + 1


def _true_rebirth_status(u, stage: int) -> tuple[bool, list[str]]:
    tg_id = int(u["tg_id"])
    req = _true_rebirth_requirements(stage)
    arena = int(u["arena"] or 1)
    coins = int(u["coins"] or 0)
    power = int(u["power"] or 0)
    rebirths = int(u["rebirth_count"] or 0)
    kills_total = int(u["total_boss_kills"] or 0)
    kills_world23 = int(db.get_stat(tg_id, "boss_kill:world23", 0))

    lines = [f"⚡ Истинное перерождение #{stage}"]
    checks = []
    checks.append((arena >= req["arena"], f"Арена: {arena}/{req['arena']}"))
    checks.append((coins >= req["coins"], f"Монеты: {fmt_num(coins)}/{fmt_num(req['coins'])}"))
    checks.append((power >= req["power"], f"Мощность: {fmt_num(power)}/{fmt_num(req['power'])}"))
    checks.append((rebirths >= req["rebirths"], f"Обычных ребёртов: {rebirths}/{req['rebirths']}"))
    if req["kills_total"] > 0:
        checks.append((kills_total >= req["kills_total"], f"Убито боссов (все арены): {fmt_num(kills_total)}/{fmt_num(req['kills_total'])}"))
    if req["kills_world23"] > 0:
        checks.append((kills_world23 >= req["kills_world23"], f"Убито боссов (арены 6-15): {fmt_num(kills_world23)}/{fmt_num(req['kills_world23'])}"))

    ok = True
    for passed, text in checks:
        ok = ok and passed
        lines.append(("✅ " if passed else "❌ ") + text)
    return ok, lines


def _rebirths_hub_text(u) -> str:
    lines = [
        "♻️ Ребёрты",
        SEP,
        "Слева: обычный ребёрт (мягкий сброс + рост множителя урона).",
        "Справа: ⚡ Истинное перерождение (жесткий сброс и мощные постоянные бонусы).",
    ]
    true_count = _true_rebirth_stage(u)
    lines.append(f"Текущий прогресс: обычных {int(u['rebirth_count'] or 0)} | истинных {true_count}/2")
    if true_count >= 2:
        lines.append("⚡ Истинное перерождение: максимум достигнут.")
    else:
        stage = true_count + 1
        ok, status = _true_rebirth_status(u, stage)
        lines.append(SEP)
        lines.extend(status[:3])
        lines.append("✅ Готово к активации" if ok else "⏳ Требования пока не выполнены")
    return "\n".join(lines)


def _rebirths_hub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="♻️ Обычный", callback_data="rebirth:open_normal"),
            InlineKeyboardButton(text="⚡ Истинный", callback_data="rebirth:open_true"),
        ],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="rebirth:close")],
    ])


def _normal_rebirth_offer_text(u) -> tuple[str, InlineKeyboardMarkup]:
    rebirth_count = int(u["rebirth_count"] or 0)
    cost = int(220000 * (3.0 ** rebirth_count) * ECONOMY_COST_MULT)
    power_required = _rebirth_power_required(rebirth_count)
    coins = int(u["coins"] or 0)
    power_now = int(u["power"] or 0)
    lack_lines = []
    if coins < cost:
        lack_lines.append(f"🪙 Не хватает монет: {fmt_num(cost - coins)}")
    if power_now < power_required:
        lack_lines.append(f"⚙️ Не хватает мощности: {fmt_num(power_required - power_now)}")

    text = (
        f"♻️ Обычный ребёрт #{rebirth_count + 1}\n"
        f"Стоимость: {fmt_num(cost)} 🪙 + {fmt_num(power_required)} ⚙️\n"
        f"Сброс: монеты, арена, мощность, прогресс арены.\n"
        f"Останется: коллекции, ауры, кольца, инвентарь.\n"
        f"Новый множитель урона: x{_rebirth_mult_expected(rebirth_count + 1):.2f}"
    )
    if lack_lines:
        text += "\n\n" + "\n".join(lack_lines)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="rebirth:confirm_normal")],
        [InlineKeyboardButton(text="◀ Назад", callback_data="rebirth:hub")],
    ])
    return text, kb


def _true_rebirth_offer_text(u) -> tuple[str, InlineKeyboardMarkup]:
    stage = _next_true_rebirth_stage(u)
    if stage == 0:
        text = "⚡ Истинное перерождение\n\nТы уже достиг максимума: 2/2."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀ Назад", callback_data="rebirth:hub")],
        ])
        return text, kb

    ok, status_lines = _true_rebirth_status(u, stage)
    if stage == 1:
        bonus_lines = [
            "• x2 к урону",
            "• x3 к монетам",
            "• x1.5 к HP",
            "• x5 к мощности с тренировки",
            "• x1.5 к шансу кейсов с тренировки",
        ]
    else:
        bonus_lines = [
            "• x3 к урону",
            "• x5 к монетам",
            "• x2 к HP",
            "• x10 к мощности с тренировки",
            "• x3 к шансу кейсов с тренировки",
        ]

    text = "\n".join([
        f"⚡ Истинное перерождение #{stage}",
        SEP,
        *status_lines,
        SEP,
        "Будет полностью сброшено:",
        "• монеты и маг. монеты",
        "• арена и прогресс арен",
        "• мощность, кейсы, инвентарь, экип",
        "• ауры и кольца",
        "• крафт-осколки и временные прогрессы",
        "",
        "Не будет сброшено:",
        "• ник и дата регистрации",
        "• гильдия",
        "• VIP, эссенция и админ-права",
        "• обычные ребёрты и их множитель",
        "• общий счетчик убийств боссов",
        "",
        "Ты получишь:",
        *bonus_lines,
    ])

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"rebirth:confirm_true:{stage}")],
        [InlineKeyboardButton(text="◀ Назад", callback_data="rebirth:hub")],
    ])
    if not ok:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀ Назад", callback_data="rebirth:hub")],
        ])
    return text, kb


def _max_case_open_count(u) -> int:
    vip = int(u["vip_lvl"] or 0)
    return {0: 20, 1: 50, 2: 100, 3: 500, 4: 1000, 5: 10000}.get(vip, 20)


def _boss_unique_progress(u) -> int:
    """Возвращает количество уникально убитых боссов на текущей арене."""
    mask = int(_row_get(u, "boss_kill_mask", 0) or 0)
    return min(3, mask.bit_count())


def _stat_boss_key(arena: int, boss_idx: int) -> str:
    return f"boss_kill:{arena}:{boss_idx}"


def _stat_open_key(item_type: str, arena: int) -> str:
    return f"open:{item_type}:{arena}"


def _track_synth_stats(tg_id: int, synth_result: dict):
    new_level = int(synth_result.get("new_level", 0) or 0)
    created_count = int(synth_result.get("created_count", 0) or 0)
    if created_count <= 0:
        return
    if new_level == 2:
        db.add_stat(tg_id, "synth:l2", created_count)
    elif new_level == 3:
        db.add_stat(tg_id, "synth:l3", created_count)


def _quest_mult_for_arena(arena: int) -> int:
    """Мир 3 = базовая сложность, мир 2 = проще в 1.5 раза, мир 1 = проще в 2 раза."""
    base_mult = min(15, arena + 1)
    if arena <= 5:
        return max(1, math.ceil(base_mult / 2.0))
    if arena <= 10:
        return max(1, math.ceil(base_mult / 1.5))
    return base_mult


def _arena_requirements_status(u) -> tuple[bool, list[str]]:
    """Возвращает (готов_к_переходу, строки_прогресса) для текущей арены."""
    arena = int(u["arena"] or 1)
    if arena >= gd.max_arena():
        return True, ["🏟 Максимальная арена достигнута."]

    tg_id = int(u["tg_id"])
    # Прогрессивная сложность квестов перехода с послаблением для миров 1-2.
    quest_mult = _quest_mult_for_arena(arena)
    lines: list[str] = [f"🏟 Условия перехода {arena} -> {arena + 1}"]
    world_tag = "(мир 1: x0.5 от мира 3)" if arena <= 5 else "(мир 2: x0.67 от мира 3)" if arena <= 10 else "(мир 3: базовый)"
    lines.append(f"📈 Модификатор квестов: x{quest_mult} {world_tag}")
    checks: list[bool] = []

    # Базовое условие: 3 уникальных босса текущей арены.
    unique_bosses = _boss_unique_progress(u)
    unique_ok = unique_bosses >= 3
    checks.append(unique_ok)
    lines.append(f"{'✅' if unique_ok else '❌'} 1) Убить всех 3 боссов: {unique_bosses}/3")

    def each_boss_need(need: int) -> bool:
        keys = [_stat_boss_key(arena, i) for i in range(3)]
        stats = db.get_stats(tg_id, keys)
        ok = True
        parts = []
        for i, boss in enumerate(gd.ARENAS[arena]):
            cur = int(stats.get(_stat_boss_key(arena, i), 0) or 0)
            parts.append(f"{boss.name}: {cur}/{need}")
            if cur < need:
                ok = False
        lines.append(f"{'✅' if ok else '❌'} 2) Каждый босс x{need}")
        lines.extend([f"   - {p}" for p in parts])
        return ok

    def boss_need(boss_idx: int, need: int, caption: str) -> bool:
        cur = db.get_stat(tg_id, _stat_boss_key(arena, boss_idx), 0)
        ok = cur >= need
        lines.append(f"{'✅' if ok else '❌'} 2) {caption}: {cur}/{need}")
        return ok

    def power_need(need: int, slot_idx: int = 3) -> bool:
        cur = int(u["power"] or 0)
        ok = cur >= need
        lines.append(f"{'✅' if ok else '❌'} {slot_idx}) Мощность: {fmt_num(cur)}/{fmt_num(need)}")
        return ok

    def open_need(item_type: str, need: int, slot_idx: int, label: str) -> bool:
        cur = db.get_stat(tg_id, _stat_open_key(item_type, arena), 0)
        ok = cur >= need
        lines.append(f"{'✅' if ok else '❌'} {slot_idx}) {label}: {cur}/{need}")
        return ok

    def synth_need(l2_need: int, l3_need: int, slot_idx: int) -> bool:
        l2 = db.get_stat(tg_id, "synth:l2", 0)
        l3 = db.get_stat(tg_id, "synth:l3", 0)
        ok = l2 >= l2_need or l3 >= l3_need
        lines.append(
            f"{'✅' if ok else '❌'} {slot_idx}) Синтез: L2 {l2}/{l2_need} или L3 {l3}/{l3_need}"
        )
        return ok

    # Дополнительные 2 условия по аренам 1-14.
    if arena == 1:
        checks.append(each_boss_need(2 * quest_mult))
        checks.append(power_need(500 * quest_mult))
    elif arena == 2:
        checks.append(boss_need(1, 3 * quest_mult, f"Убить Волка Тьмы x{3 * quest_mult}"))
        checks.append(open_need("weapon", 5 * quest_mult, 3, "Открыть кейсы оружия арены 2"))
    elif arena == 3:
        checks.append(boss_need(2, 4 * quest_mult, f"Убить Хозяина Шахт x{4 * quest_mult}"))
        checks.append(open_need("pet", 5 * quest_mult, 3, "Открыть кейсы питомцев арены 3"))
    elif arena == 4:
        checks.append(each_boss_need(2 * quest_mult))
        checks.append(synth_need(1 * quest_mult, 1 * quest_mult, 3))
    elif arena == 5:
        checks.append(boss_need(2, 5 * quest_mult, f"Убить Аватара Разлома x{5 * quest_mult}"))
        checks.append(power_need(5000 * quest_mult))
    elif arena == 6:
        checks.append(each_boss_need(2 * quest_mult))
        checks.append(open_need("weapon", 10 * quest_mult, 3, "Открыть кейсы оружия арены 6"))
    elif arena == 7:
        checks.append(boss_need(2, 6 * quest_mult, f"Убить Феникса Возрожденного x{6 * quest_mult}"))
        checks.append(open_need("pet", 10 * quest_mult, 3, "Открыть кейсы питомцев арены 7"))
    elif arena == 8:
        checks.append(each_boss_need(3 * quest_mult))
        checks.append(power_need(25000 * quest_mult))
    elif arena == 9:
        checks.append(boss_need(2, 7 * quest_mult, f"Убить Титана Лавы x{7 * quest_mult}"))
        checks.append(synth_need(2 * quest_mult, 1 * quest_mult, 3))
    elif arena == 10:
        checks.append(each_boss_need(3 * quest_mult))
        checks.append(open_need("weapon", 20 * quest_mult, 3, "Открыть кейсы оружия арены 10"))
    elif arena == 11:
        checks.append(boss_need(2, 8 * quest_mult, f"Убить Морозного Гиганта x{8 * quest_mult}"))
        checks.append(power_need(100000 * quest_mult))
    elif arena == 12:
        checks.append(each_boss_need(4 * quest_mult))
        checks.append(open_need("pet", 25 * quest_mult, 3, "Открыть кейсы питомцев арены 12"))
    elif arena == 13:
        checks.append(boss_need(2, 10 * quest_mult, f"Убить Йети-Исполина x{10 * quest_mult}"))
        checks.append(synth_need(3 * quest_mult, 2 * quest_mult, 3))
    elif arena == 14:
        checks.append(each_boss_need(5 * quest_mult))
        checks.append(power_need(500000 * quest_mult))

    return all(checks), lines


async def _safe_edit(msg: Message, text: str, reply_markup=None, parse_mode=None):
    try:
        await msg.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            log.warning(f"edit error: {e}")
    except Exception as e:
        log.warning(f"safe_edit error: {e}")


def _edit_fingerprint(text: str, reply_markup=None, parse_mode=None) -> str:
    return f"{parse_mode or ''}|{text}|{repr(reply_markup)}"


def _edit_guard_key(chat_id: int, message_id: int) -> tuple[int, int]:
    return int(chat_id), int(message_id)


def _edit_should_skip(key: tuple[int, int], fingerprint: str) -> bool:
    prev = EDIT_FLOOD_GUARD.get(key)
    if not prev:
        return False
    prev_fp, prev_ts = prev
    _ = prev_ts
    if prev_fp == fingerprint:
        return True
    return False


def _edit_guard_remember(key: tuple[int, int], fingerprint: str):
    EDIT_FLOOD_GUARD[key] = (fingerprint, time.time())
    if len(EDIT_FLOOD_GUARD) > 5000:
        EDIT_FLOOD_GUARD.pop(next(iter(EDIT_FLOOD_GUARD)))


def _cb_msg_key(chat_id: int, message_id: int) -> tuple[int, int]:
    return int(chat_id), int(message_id)


def _set_cb_owner(chat_id: int, message_id: int, owner_id: int):
    CALLBACK_OWNER_BY_MSG[_cb_msg_key(chat_id, message_id)] = int(owner_id)
    if len(CALLBACK_OWNER_BY_MSG) > 15000:
        CALLBACK_OWNER_BY_MSG.pop(next(iter(CALLBACK_OWNER_BY_MSG)))


def _get_cb_owner(chat_id: int, message_id: int) -> int:
    return int(CALLBACK_OWNER_BY_MSG.get(_cb_msg_key(chat_id, message_id), 0) or 0)


def _cb_owner_check_skipped(cb_data: str) -> bool:
    data = str(cb_data or "")
    # Исключения для механик, где одну кнопку могут нажимать несколько участников.
    return data.startswith("duel:") or data.startswith("mini:rr:")


async def _safe_edit_cb(cb: CallbackQuery, text: str, reply_markup=None, parse_mode=None) -> bool:
    """Безопасно редактирует callback-сообщение.

    Если редактирование невозможно (старое сообщение/ограничения клиента),
    отправляет новый ответ в чат, чтобы у пользователя не было ощущения,
    что кнопка "не работает".
    """
    _set_cb_owner(cb.message.chat.id, cb.message.message_id, int(cb.from_user.id))
    key = _edit_guard_key(cb.message.chat.id, cb.message.message_id)
    fp = _edit_fingerprint(text, reply_markup, parse_mode)
    if _edit_should_skip(key, fp):
        return True

    try:
        await cb.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        _edit_guard_remember(key, fp)
        return True
    except TelegramRetryAfter as e:
        retry = int(getattr(e, "retry_after", 0) or 0)
        log.warning(f"safe_edit_cb floodwait: retry_after={retry}s")
        try:
            await cb.answer()
        except Exception:
            pass
        return False
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            _edit_guard_remember(key, fp)
            return True
        log.warning(f"edit_cb error: {e}")
        try:
            new_msg = await cb.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
            _set_cb_owner(new_msg.chat.id, new_msg.message_id, int(cb.from_user.id))
            return True
        except Exception as e2:
            log.warning(f"safe_edit_cb fallback send error: {e2}")
            return False
    except Exception as e:
        # Сетевые/редкие клиентские ошибки не должны «ломать» кнопки.
        log.warning(f"safe_edit_cb unknown error: {e}")
        try:
            new_msg = await cb.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
            _set_cb_owner(new_msg.chat.id, new_msg.message_id, int(cb.from_user.id))
            return True
        except Exception as e2:
            log.warning(f"safe_edit_cb final fallback send error: {e2}")
            return False


async def _render_battle_cb(cb: CallbackQuery, bs: "BattleState", text: str, reply_markup=None, parse_mode=None) -> bool:
    """Рендер боя с авто-перепривязкой к новому сообщению, если старое больше не редактируется."""
    _set_cb_owner(cb.message.chat.id, cb.message.message_id, int(cb.from_user.id))
    key = _edit_guard_key(cb.message.chat.id, cb.message.message_id)
    fp = _edit_fingerprint(text, reply_markup, parse_mode)
    if _edit_should_skip(key, fp):
        return True

    try:
        await cb.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        _edit_guard_remember(key, fp)
        return True
    except TelegramRetryAfter as e:
        retry = int(getattr(e, "retry_after", 0) or 0)
        log.warning(f"render_battle floodwait: retry_after={retry}s")
        try:
            await cb.answer()
        except Exception:
            pass
        return False
    except TelegramBadRequest as e:
        low = str(e).lower()
        if "message is not modified" in low:
            _edit_guard_remember(key, fp)
            return True
        # Старое сообщение/кнопки могли устареть: выдаем новый якорь боя.
        if any(x in low for x in (
            "message to edit not found",
            "message can't be edited",
            "message identifier is not specified",
            "message is too old",
        )):
            try:
                new_msg = await cb.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
                ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
                bs.chat_id = new_msg.chat.id
                bs.msg_id = new_msg.message_id
                bs.last_action = time.time()
                ACTIVE_BATTLES_BY_MSG[(bs.chat_id, bs.msg_id)] = int(bs.user_id)
                _set_cb_owner(new_msg.chat.id, new_msg.message_id, int(cb.from_user.id))
                _persist_battle_state(bs)
                _edit_guard_remember(_edit_guard_key(new_msg.chat.id, new_msg.message_id), fp)
                return True
            except Exception as e2:
                log.warning(f"render_battle fallback send error: {e2}")
                return False
        log.warning(f"render_battle edit error: {e}")
        try:
            new_msg = await cb.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            bs.chat_id = new_msg.chat.id
            bs.msg_id = new_msg.message_id
            bs.last_action = time.time()
            ACTIVE_BATTLES_BY_MSG[(bs.chat_id, bs.msg_id)] = int(bs.user_id)
            _set_cb_owner(new_msg.chat.id, new_msg.message_id, int(cb.from_user.id))
            _persist_battle_state(bs)
            _edit_guard_remember(_edit_guard_key(new_msg.chat.id, new_msg.message_id), fp)
            return True
        except Exception as e2:
            log.warning(f"render_battle generic fallback send error: {e2}")
            return False
    except Exception as e:
        log.warning(f"render_battle unknown error: {e}")
        try:
            new_msg = await cb.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            bs.chat_id = new_msg.chat.id
            bs.msg_id = new_msg.message_id
            bs.last_action = time.time()
            ACTIVE_BATTLES_BY_MSG[(bs.chat_id, bs.msg_id)] = int(bs.user_id)
            _set_cb_owner(new_msg.chat.id, new_msg.message_id, int(cb.from_user.id))
            _persist_battle_state(bs)
            _edit_guard_remember(_edit_guard_key(new_msg.chat.id, new_msg.message_id), fp)
            return True
        except Exception as e2:
            log.warning(f"render_battle final fallback send error: {e2}")
            return False


async def _switch_keyboard(message: Message, kb: ReplyKeyboardMarkup):
    await message.answer("Открыто меню.", reply_markup=kb)


async def _send_text_as_quote_or_file(
    message: Message,
    text: str,
    *,
    file_prefix: str,
    file_caption: str,
):
    """Отправляет текст в свёрнутой цитате реплаем, а длинный — файлом."""
    if len(text) > 3500:
        bio = BytesIO(text.encode("utf-8"))
        bio.name = f"{file_prefix}_{message.from_user.id}.txt"
        try:
            await message.reply_document(
                BufferedInputFile(bio.read(), filename=bio.name),
                caption=file_caption,
            )
        except Exception:
            bio.seek(0)
            await message.answer_document(
                BufferedInputFile(bio.read(), filename=bio.name),
                caption=file_caption,
            )
        return
    quote = f"<blockquote expandable>{escape(text)}</blockquote>"
    await _reply(message, quote, parse_mode=ParseMode.HTML)


def main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[
            KeyboardButton(text="Профиль"),
            KeyboardButton(text="🥊 Тренировка"),
            KeyboardButton(text="📦 Кейсы"),
            KeyboardButton(text="💾 Меню"),
        ]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎒 Снаряжение"), KeyboardButton(text="⚔️ Приключения")],
            [KeyboardButton(text="🎁 Бонусы"), KeyboardButton(text="🏰 Гильдия"), KeyboardButton(text="🎰 Казино")],
            [KeyboardButton(text="♻️ Ребёрты"), KeyboardButton(text="🧪 Улучшения")],
            [KeyboardButton(text="⚙️ Дополнительно")],
            [KeyboardButton(text="Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def gear_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎒 Инвентарь"), KeyboardButton(text="🌀 Синтез")],
            [KeyboardButton(text="🛠️ Крафт"), KeyboardButton(text="👔 Экипировка")],
            [KeyboardButton(text="◀ В меню")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def adventures_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏛️ Боссы"), KeyboardButton(text="⛩️ Данж")],
            [KeyboardButton(text="🏟️ Арена"), KeyboardButton(text="🌍 Миры")],
            [KeyboardButton(text="🛟 Floodwait")],
            [KeyboardButton(text="◀ В меню")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def fw_battle_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="FW ⚔️ Атаковать"), KeyboardButton(text="FW 🔄 Обновить")],
            [KeyboardButton(text="FW ◀ К боссам"), KeyboardButton(text="FW ✖ Закрыть")],
            [KeyboardButton(text="FW ◀ В приключения")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def fw_dungeon_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="FW ⚔️ Волна"), KeyboardButton(text="FW 🔄 Обновить")],
            [KeyboardButton(text="FW 🚪 Выйти")],
            [KeyboardButton(text="FW ◀ В приключения")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def extras_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Топ"), KeyboardButton(text="📜 Гайд")],
            [KeyboardButton(text="💸 Топ донат"), KeyboardButton(text="💳 Донат")],
            [KeyboardButton(text="⚙️ Настройки")],
            [KeyboardButton(text="◀ В меню")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def _guide_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📖 Открыть гайд",
            url="https://telegra.ph/Gajd-po-Risen-Solo-04-04",
        )
    ]])


def remove_kb() -> ReplyKeyboardRemove:
    return ReplyKeyboardRemove()


def _bonuses_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎁 Ежедневный бонус", callback_data="bonuses:daily"),
            InlineKeyboardButton(text="🎫 Промокоды", callback_data="bonuses:promos"),
        ],
        [
            InlineKeyboardButton(text="🤝 Рефералы", callback_data="bonuses:ref"),
            InlineKeyboardButton(text="📅 Ивент дня", callback_data="bonuses:event"),
        ],
        [
            InlineKeyboardButton(text="✏️ Описание", callback_data="bonuses:bio"),
        ],
        [
            InlineKeyboardButton(text="📌 Задания", callback_data="bonuses:tasks"),
        ],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="bonuses:close")],
    ])


def _bonuses_text() -> str:
    return (
        "🎁 Бонусы\n"
        f"{SEP}\n"
        "Выбери раздел:\n"
        "• Ежедневный бонус\n"
        "• Список активных промокодов\n"
        "• Реферальные награды\n"
        "• Ивент дня\n"
        "• Бонус за описание профиля\n"
        "• Задания"
    )


def _bonus_tasks_text(u) -> str:
    done = int(db.get_stat(int(u["tg_id"]), VPN_TASK_STAT_KEY, 0) or 0) > 0
    return "\n".join([
        "📌 Задания",
        SEP,
        "1) VPN-партнер",
        "• Перейди по кнопке и нажми Старт в боте.",
        f"• Награда сразу: +{VPN_TASK_REWARD_MAGIC} 🔯",
        f"• Эссенция +{VPN_TASK_REWARD_ESSENCE} 💠 будет выдана администрацией в течение дня.",
        f"• Статус: {'✅ Получено' if done else '⏳ Не получено'}",
    ])


def _bonus_tasks_kb(u) -> InlineKeyboardMarkup:
    done = int(db.get_stat(int(u["tg_id"]), VPN_TASK_STAT_KEY, 0) or 0) > 0
    rows = [
        [InlineKeyboardButton(text="🔗 Открыть VPN-бота", url=VPN_TASK_URL)],
    ]
    if not done:
        rows.append([InlineKeyboardButton(text=f"🎁 Получить +{VPN_TASK_REWARD_MAGIC} 🔯", callback_data="bonuses:task_claim_vpn")])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="bonuses:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _train_upgrades_kb(u) -> InlineKeyboardMarkup:
    case_lvl = int(_row_get(u, "train_case_lvl", 0) or 0)
    power_lvl = int(_row_get(u, "train_power_lvl", 0) or 0)
    time_lvl = int(_row_get(u, "train_time_lvl", 0) or 0)
    rows = [
        [InlineKeyboardButton(
            text=f"🎯 ШК +0.01 ({fmt_num(_train_up_cost('case', case_lvl))} 🪙)",
            callback_data="trainup:buy:case",
        )],
        [InlineKeyboardButton(
            text=f"💪 СТ +0.0225 ({fmt_num(_train_up_cost('power', power_lvl))} 🪙)",
            callback_data="trainup:buy:power",
        )],
        [InlineKeyboardButton(
            text=f"⏱ ВТ +5м ({fmt_num(_train_up_cost('time', time_lvl))} 🪙)",
            callback_data="trainup:buy:time",
        )],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="trainup:refresh"),
            InlineKeyboardButton(text="✖ Закрыть", callback_data="trainup:close"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _train_upgrades_text(u) -> str:
    case_lvl = int(_row_get(u, "train_case_lvl", 0) or 0)
    power_lvl = int(_row_get(u, "train_power_lvl", 0) or 0)
    time_lvl = int(_row_get(u, "train_time_lvl", 0) or 0)
    duration = _train_duration_secs(u)
    dur_str = "∞ (до ручного завершения)" if duration <= 0 else f"{duration // 3600}ч {(duration % 3600) // 60}м"
    return "\n".join([
        "🧪 Улучшения тренировки",
        SEP,
        f"🕒 Базовый тик тренировки: {_train_tick_seconds(u)}с",
        f"🎯 Шанс кейсов (ШК): ур. {case_lvl} | x{_train_case_mult(u):.2f}",
        f"💪 Сила тренировки (СТ): ур. {power_lvl} | x{_train_power_mult(u):.2f}",
        f"⏱ Время тренировки (ВТ): ур. {time_lvl} | +{time_lvl * 5}м",
        f"🕒 Текущее время тренировки: {dur_str}",
        SEP,
        "Каждое улучшение ШК даёт +0.01 к множителю.",
        "Каждое улучшение СТ даёт +0.0225 к множителю.",
        "ВТ даёт +5 минут за уровень.",
        "Алиасы: шк, ст, вт",
    ])


def _bio_bonus_text(u=None) -> str:
    active = _bio_bonus_active(u) if u is not None else False
    status = "🟢 Активен" if active else "🔴 Не активен"
    return "\n".join([
        "✏️ Бонус за описание",
        SEP,
        "Добавь @risensolo_bot в описание профиля и получи усиления:",
        "• x1.5 к HP",
        "• x1.5 к урону",
        "• x1.5 к мощности с тренировки",
        "• x1.5 к шансу кейсов с тренировки",
        "",
        f"Статус: {status}",
        "Проверка выполняется автоматически. Если уберешь тег — бонус отключится.",
    ])


async def _refresh_bio_bonus_for_user(bot: Bot, tg_id: int, notify: bool = False) -> int | None:
    """Проверяет bio пользователя и синхронизирует флаг бонуса.
    Возвращает 1/0 при успешной проверке или None при ошибке запроса.
    """
    u = db.get_user(tg_id)
    if not u:
        return None
    old_active = int(_row_get(u, "bio_bonus_active", 0) or 0)
    try:
        chat = await bot.get_chat(tg_id)
        bio_text = str(getattr(chat, "bio", "") or "").lower()
    except Exception:
        return None

    new_active = 1 if BIO_BONUS_TAG in bio_text else 0
    if new_active != old_active:
        db.update_user(
            tg_id,
            bio_bonus_active=new_active,
            bio_bonus_checked_at=int(time.time()),
        )
        if notify:
            try:
                if new_active:
                    await bot.send_message(
                        tg_id,
                        "✅ Бонус за описание активирован: найден тег @risensolo_bot.",
                    )
                else:
                    await bot.send_message(
                        tg_id,
                        "⚠️ Бонус за описание отключен: тег @risensolo_bot не найден.",
                    )
            except Exception:
                pass
    else:
        db.update_user(tg_id, bio_bonus_checked_at=int(time.time()))
    return new_active


def _guild_level_cost(level: int) -> int:
    return int(GUILD_UPGRADE_COSTS.get(int(level), 0))


def _guild_level_buff_text(level: int) -> str:
    buffs = GUILD_LEVEL_BUFFS.get(int(level), GUILD_LEVEL_BUFFS[1])
    return (
        f"❤️ HP +{int(buffs['hp'] * 100)}% | "
        f"📦 Шанс кейсов +{int(buffs['case'] * 100)}% | "
        f"💢 Урон +{int(buffs['dmg'] * 100)}% | "
        f"🪙 Монеты +{int(buffs['coins'] * 100)}%"
    )


def _guild_level_buff_lines(level: int) -> list[str]:
    buffs = GUILD_LEVEL_BUFFS.get(int(level), GUILD_LEVEL_BUFFS[1])
    return [
        f"❤️ Выносливость: +{int(buffs['hp'] * 100)}%",
        f"📦 Шанс кейсов: +{int(buffs['case'] * 100)}%",
        f"💢 Урон: +{int(buffs['dmg'] * 100)}%",
        f"🪙 Монеты: +{int(buffs['coins'] * 100)}%",
    ]


def _guild_desc_preview(text: str, width: int = 34, max_lines: int = 5) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "Без описания"
    wrapped: list[str] = []
    for part in raw.splitlines():
        part = part.strip()
        if not part:
            continue
        wrapped.extend(textwrap.wrap(part, width=width, break_long_words=False, break_on_hyphens=False) or [part])
        if len(wrapped) >= max_lines:
            break
    wrapped = wrapped[:max_lines]
    if len(raw) > 220:
        wrapped.append("...")
    return "\n".join(wrapped)


def _guild_panel_text(g, member_count: int, is_owner: bool) -> str:
    level = int(g["level"] or 1)
    join_mode = "Открытый" if int(g["open_join"] or 0) else "Закрытый"
    limit = _guild_member_limit(level)
    created_ts = int(_row_get(g, "created_at", 0) or 0)
    created_str = _fmt_ts_msk(created_ts, "%d.%m.%Y") if created_ts else "—"
    owner_id = int(_row_get(g, "owner_id", 0) or 0)
    owner_u = db.get_user(owner_id) if owner_id else None
    owner_name = _display_name(owner_u) if owner_u else f"id{owner_id}"
    buff_lines = _guild_level_buff_lines(level)
    lines = [
        f"🏯 {g['name']}",
        SEP,
        f"🆔 ID: {g['id']} | 👑 Лидер: {owner_name}",
        f"🏰 Уровень гильдии: {level}",
        f"👥 Состав: {member_count}/{limit}",
        f"🔐 Набор: {join_mode}",
        f"🛡 Осколки единства: {fmt_num(int(g['unity_shards'] or 0))}",
        SEP,
        "📜 Описание",
        _guild_desc_preview(g["description"]),
        SEP,
        "✨ Бонусы гильдии",
    ]
    lines.extend(buff_lines)
    lines += [
        SEP,
        f"⏳ Основана: {created_str}",
        "⚙️ Управление: кнопки ниже",
    ]
    if is_owner:
        lines.append("👑 Тебе доступны: описание, настройки и заявки")
    return "\n".join(lines)


def _clan_info_text(g, member_count: int) -> str:
    level = int(g["level"] or 1)
    limit = _guild_member_limit(level)
    join_mode = "Открытый" if int(g["open_join"] or 0) else "Закрытый"
    created_ts = int(_row_get(g, "created_at", 0) or 0)
    created_str = _fmt_ts_msk(created_ts, "%d.%m.%Y") if created_ts else "—"
    return "\n".join([
        f"🏯 {g['name']}",
        SEP,
        f"🆔 ID: {g['id']}",
        f"🏰 Уровень: {level}",
        f"👥 Участники: {member_count}/{limit}",
        f"🔐 Тип набора: {join_mode}",
        f"🛡 Осколки единства: {fmt_num(int(g['unity_shards'] or 0))}",
        SEP,
        "📜 Описание",
        _guild_desc_preview(g["description"]),
        SEP,
        "✨ Бонусы",
        *_guild_level_buff_lines(level),
        SEP,
        f"⏳ Основана: {created_str}",
    ])


def _clan_admin_info_text(g, member_rows: list) -> str:
    level = int(g["level"] or 1)
    limit = _guild_member_limit(level)
    join_mode = "Открытый" if int(g["open_join"] or 0) else "Закрытый"
    created_ts = int(_row_get(g, "created_at", 0) or 0)
    created_str = _fmt_ts_msk(created_ts, "%d.%m.%Y") if created_ts else "—"
    lines = [
        f"🛡 Админ-проверка клана #{int(g['id'])}",
        SEP,
        f"🏯 Название: {g['name']}",
        f"👑 Владелец ID: {int(_row_get(g, 'owner_id', 0) or 0)}",
        f"🏰 Уровень: {level}",
        f"👥 Участники: {len(member_rows)}/{limit}",
        f"🔐 Набор: {join_mode}",
        f"🛡 Осколки единства: {fmt_num(int(g['unity_shards'] or 0))}",
        SEP,
        "📈 Бонусы уровня:",
    ]
    lines.extend(_guild_level_buff_lines(level))
    lines += [
        SEP,
        "👥 Состав:",
    ]
    if not member_rows:
        lines.append("— нет участников")
    else:
        for idx, row in enumerate(member_rows, start=1):
            role_key = str(row["role"])
            role = "Лидер" if role_key == "owner" else ("Заместитель" if role_key == "deputy" else "Участник")
            nick = str(_row_get(row, "nickname", "") or "").strip()
            uname = str(_row_get(row, "username", "") or "").strip()
            name = nick if nick else (f"@{uname}" if uname else f"id{int(row['tg_id'])}")
            lines.append(
                f"{idx}. {name} | ID {int(row['tg_id'])} | {role} | Арена {int(_row_get(row, 'arena', 1) or 1)}"
            )
    lines += [
        SEP,
        f"⏳ Основана: {created_str}",
    ]
    return "\n".join(lines)


def _guild_top_text(limit: int = 5) -> str:
    rows = db.list_top_guilds(limit=max(1, int(limit)))
    if not rows:
        return "📊 Топ кланов\n\nПока нет созданных гильдий."
    lines = ["📊 Топ кланов", SEP]
    for i, g in enumerate(rows, start=1):
        lines.append(
            f"{i}. {g['name']} | Ур. {int(g['level'] or 1)} | Участники: {int(g['members'] or 0)}"
        )
    lines.append(SEP)
    lines.append("Сортировка: уровень -> участники")
    return "\n".join(lines)


def _top_don_text(limit: int = 10) -> str:
    rows = db.list_top_donators(limit=max(1, int(limit)))
    lines = ["💸 Топ донатеров", SEP]
    for i in range(1, max(1, int(limit)) + 1):
        if i <= len(rows):
            row = rows[i - 1]
            nick = str(row["nickname"] or "").strip()
            if not nick:
                uname = str(row["username"] or "").strip()
                nick = f"@{uname}" if uname else f"id{int(row['tg_id'])}"
            lines.append(f"{i}. {nick} — {fmt_num(int(row['donate_rub'] or 0))} ₽")
        else:
            lines.append(f"{i}. -")
    lines.append(SEP)
    lines.append("Команда: /topdon")
    return "\n".join(lines)


def _top_name_from_row(row) -> str:
    nick = str(_row_get(row, "nickname", "") or "").strip()
    if nick:
        return nick
    uname = str(_row_get(row, "username", "") or "").strip()
    return f"@{uname}" if uname else f"id{int(_row_get(row, 'tg_id', 0) or 0)}"


def _top_coins_text(limit: int = 10) -> str:
    rows = db.list_top_coins(limit=max(1, int(limit)))
    lines = ["🪙 Топ по монетам", SEP]
    for i in range(1, max(1, int(limit)) + 1):
        if i <= len(rows):
            row = rows[i - 1]
            lines.append(f"{i}. {_top_name_from_row(row)} — {fmt_num(int(_row_get(row, 'coins', 0) or 0))}")
        else:
            lines.append(f"{i}. -")
    return "\n".join(lines)


def _top_level_text(limit: int = 10) -> str:
    rows = db.list_top_level(limit=max(1, int(limit)))
    lines = ["⭐ Топ по уровню", SEP]
    for i in range(1, max(1, int(limit)) + 1):
        if i <= len(rows):
            row = rows[i - 1]
            lvl = int(_row_get(row, "level_value", 0) or 0)
            arena = int(_row_get(row, "arena", 1) or 1)
            reb = int(_row_get(row, "rebirth_count", 0) or 0)
            lines.append(f"{i}. {_top_name_from_row(row)} — Ур. {lvl} (A{arena} | реб {reb})")
        else:
            lines.append(f"{i}. -")
    return "\n".join(lines)


def _top_damage_text(limit: int = 10) -> str:
    try:
        users = db.list_users_for_damage_top(limit=120)
    except Exception as e:
        log.warning(f"top_damage source error: {e}")
        users = []
    scored = []
    for row in users:
        try:
            power = max(0, int(_row_get(row, "power", 0) or 0))
            wb = max(0, int(_row_get(row, "best_weapon_bonus", 0) or 0))
            arena = int(_row_get(row, "arena", 1) or 1)
            rebirth_count = int(_row_get(row, "rebirth_count", 0) or 0)
            rebirth_mult = _rebirth_mult_expected(rebirth_count)
            power_mult = 1 + (math.sqrt(power) / 108) + (math.log10(power + 1) * 0.015)
            dmg = int((14 + wb * WEAPON_EFFECT_MULT) * power_mult * rebirth_mult)
            dmg = int(dmg * DAMAGE_GLOBAL_MULT)
            if arena <= 5:
                dmg = int(dmg * 1.05)
            elif arena <= 10:
                dmg = int(dmg * 1.02)
            # Для топа считаем быстрый и стабильный estimate без тяжелых запросов к инвентарю/артефактам.
            dmg = int(dmg * _aura_wrath(row))
            dmg = int(dmg * _vip_dmg_mult(row))
            dmg = int(dmg * _true_dmg_mult(row))
            dmg = int(dmg * _bio_dmg_mult(row))
        except Exception:
            dmg = 0
        scored.append((dmg, row))
    scored.sort(key=lambda x: (-int(x[0]), int(_row_get(x[1], "tg_id", 0) or 0)))
    lines = ["💢 Топ по урону", SEP]
    top_rows = scored[: max(1, int(limit))]
    for i in range(1, max(1, int(limit)) + 1):
        if i <= len(top_rows):
            dmg, row = top_rows[i - 1]
            lines.append(f"{i}. {_top_name_from_row(row)} — {fmt_num(dmg)}")
        else:
            lines.append(f"{i}. -")
    return "\n".join(lines)


def _top_kills_text(limit: int = 10) -> str:
    rows = db.list_top_kills(limit=max(1, int(limit)))
    lines = ["☠️ Топ по убийствам", SEP]
    for i in range(1, max(1, int(limit)) + 1):
        if i <= len(rows):
            row = rows[i - 1]
            lines.append(f"{i}. {_top_name_from_row(row)} — {fmt_num(int(_row_get(row, 'total_boss_kills', 0) or 0))}")
        else:
            lines.append(f"{i}. -")
    return "\n".join(lines)


def _tops_hub_text() -> str:
    return "\n".join([
        "📊 Топы",
        SEP,
        "Выбери нужный рейтинг:",
        "• Монеты",
        "• Уровень",
        "• Урон",
        "• Убийства",
        "• Донат",
        "• Кланы",
    ])


def _tops_hub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🪙 Монеты", callback_data="tops:coins"),
            InlineKeyboardButton(text="⭐ Уровень", callback_data="tops:level"),
        ],
        [
            InlineKeyboardButton(text="💢 Урон", callback_data="tops:damage"),
            InlineKeyboardButton(text="☠️ Убийства", callback_data="tops:kills"),
        ],
        [
            InlineKeyboardButton(text="💸 Донат", callback_data="tops:donate"),
            InlineKeyboardButton(text="🏰 Кланы", callback_data="tops:guilds"),
        ],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="tops:close")],
    ])


def _guild_panel_kb(guild_id: int, is_owner: bool, is_manager: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👥 Участники", callback_data=f"guild:members:{guild_id}:0")],
        [InlineKeyboardButton(text="⚔️ Гильд-босс", callback_data=f"guild:boss:{guild_id}")],
        [InlineKeyboardButton(text="⬆️ Прокачка", callback_data=f"guild:upgrade:{guild_id}")],
        [InlineKeyboardButton(text="📊 Топ", callback_data=f"guild:top:{guild_id}")],
    ]
    if is_manager:
        rows.append([InlineKeyboardButton(text="⚙️ Настройки", callback_data=f"guild:settings:{guild_id}")])
    if is_owner:
        rows.append([InlineKeyboardButton(text="📥 Заявки", callback_data=f"guild:reqs:{guild_id}")])
        rows.append([InlineKeyboardButton(text="🗑 Удалить гильдию", callback_data=f"guild:delask:{guild_id}")])
    rows.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="guild:close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _guild_settings_kb(guild_id: int, open_join: int, can_manage_open: bool = True) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="📝 Изменить описание", callback_data=f"guild:desc:{guild_id}"),
            InlineKeyboardButton(text="✏️ Изменить название", callback_data=f"guild:rename:{guild_id}"),
        ],
    ]
    if can_manage_open:
        rows.append([InlineKeyboardButton(text=("✅ Открытый" if int(open_join) else "Открыть"), callback_data=f"guild:setopen:{guild_id}:1")])
        rows.append([InlineKeyboardButton(text=("✅ Закрытый" if not int(open_join) else "Закрыть"), callback_data=f"guild:setopen:{guild_id}:0")])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{guild_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _guild_delete_confirm_kb(guild_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить удаление", callback_data=f"guild:delok:{guild_id}")],
        [InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{guild_id}")],
    ])


def _guild_members_kb(guild_id: int, members_rows, page: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    for row in members_rows:
        nick = str(row["nickname"] or "").strip() or str(row["username"] or f"id{row['tg_id']}")
        role_name = str(row["role"])
        role = "👑" if role_name == "owner" else ("🛡" if role_name == "deputy" else "👤")
        rows.append([InlineKeyboardButton(text=f"{role} {nick}", callback_data=f"guild:member:{guild_id}:{row['tg_id']}:{page}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀", callback_data=f"guild:members:{guild_id}:{page - 1}"))
    if (page + 1) * 10 < total:
        nav.append(InlineKeyboardButton(text="▶", callback_data=f"guild:members:{guild_id}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{guild_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _guild_requests_kb(guild_id: int, req_rows) -> InlineKeyboardMarkup:
    rows = []
    for r in req_rows:
        nick = str(r["nickname"] or "").strip() or str(r["username"] or f"id{r['tg_id']}")
        rows.append([
            InlineKeyboardButton(text=f"✅ {nick}", callback_data=f"guild:req:ok:{r['id']}:{guild_id}"),
            InlineKeyboardButton(text=f"❌ {nick}", callback_data=f"guild:req:no:{r['id']}:{guild_id}"),
        ])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{guild_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _guild_boss_arena_kb(guild_id: int) -> InlineKeyboardMarkup:
    rows = []
    for a in range(1, gd.max_arena() + 1):
        rows.append([
            InlineKeyboardButton(
                text=f"Арена {a}: {gd.arena_title(a)}",
                callback_data=f"guild:bossarena:{guild_id}:{a}",
            )
        ])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{guild_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _guild_boss_pick_kb(guild_id: int, arena: int) -> InlineKeyboardMarkup:
    rows = []
    for i, b in enumerate(gd.ARENAS[arena]):
        hp = int(b.hp * _enemy_hp_mult(arena) * GUILD_BOSS_HP_MULT)
        cd_key = db.get_guild_boss_cooldown(guild_id, arena, i)
        locked = _guild_boss_on_cooldown(cd_key)
        left = _guild_boss_cooldown_left_text(cd_key)
        lock_tail = f" | ⏳ {left}" if locked and left else ""
        label = f"{'🔒 ' if locked else ''}{b.name} | HP {fmt_num(hp)}{lock_tail}"
        cb_data = f"guild:bosspick:{guild_id}:{arena}:{i}" if not locked else f"guild:bosspick_locked:{guild_id}:{arena}:{i}"
        rows.append([InlineKeyboardButton(text=label, callback_data=cb_data)])
    rows.append([InlineKeyboardButton(text="◀ К аренам", callback_data=f"guild:boss:{guild_id}")])
    rows.append([InlineKeyboardButton(text="✖ Закрыть", callback_data=f"guild:panel:{guild_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _guild_boss_view(gb: GuildBattleState) -> str:
    remain = max(0, GUILD_BOSS_TIMEOUT_SEC - (int(time.time()) - int(gb.started_at)))
    mm, ss = divmod(remain, 60)
    return (
        f"🏰 Гильд-босс: {gb.boss_name}\n"
        f"{SEP_BAR}\n"
        f"👾 HP: [{fmt_num(gb.boss_hp)}/{fmt_num(gb.boss_max_hp)}]\n"
        f"⏱ Осталось: {mm:02d}:{ss:02d}\n"
        f"🎁 Награда в хранилище: +{fmt_num(gb.reward_base)} 🛡"
    )


def _guild_boss_kb(guild_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚔️ Удар", callback_data=f"guild:bossatk:{guild_id}")],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"guild:bossref:{guild_id}"),
            InlineKeyboardButton(text="🚪 Выход", callback_data=f"guild:panel:{guild_id}"),
        ],
    ])


def _guild_boss_cooldown_end_ts(cd_key: str) -> int:
    key = str(cd_key or "").strip()
    if not key:
        return 0
    try:
        dt = datetime.fromisoformat(key)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK_TZ)
        return int(dt.timestamp())
    except Exception:
        pass
    try:
        d = date.fromisoformat(key)
        dt = datetime(d.year, d.month, d.day, tzinfo=MSK_TZ)
        return int(dt.timestamp())
    except Exception:
        return 0


def _guild_boss_on_cooldown(cd_key: str) -> bool:
    end_ts = _guild_boss_cooldown_end_ts(cd_key)
    if end_ts <= 0:
        return False
    return int(time.time()) < end_ts


def _guild_boss_cooldown_left_text(cd_key: str) -> str:
    end_ts = _guild_boss_cooldown_end_ts(cd_key)
    if end_ts <= 0:
        return ""
    left = max(0, int(end_ts - int(time.time())))
    if left <= 0:
        return ""
    d, rem = divmod(left, 86400)
    h, rem = divmod(rem, 3600)
    m, _s = divmod(rem, 60)
    if d > 0:
        return f"{d}д {h}ч {m}м"
    return f"{h}ч {m}м"


def _profile_text(u, is_admin_view: bool = False) -> str:
    tg_id = int(u["tg_id"])
    nick = _display_name(u)
    rank_name = gd.RANKS[min(int(u["rank_idx"] or 0), len(gd.RANKS) - 1)]
    rebirth_count = int(u["rebirth_count"] or 0)
    true_rebirth_count = int(_row_get(u, "true_rebirth_count", 0) or 0)
    rebirth_mult = _rebirth_mult_expected(rebirth_count)
    arena = int(u["arena"] or 1)
    coins = int(u["coins"] or 0)
    magic_coins = int(u["magic_coins"] or 0)
    essence = int(u["essence"] or 0)
    vip_lvl = int(u["vip_lvl"] or 0)
    power = int(u["power"] or 0)
    active_ring = int(u["active_ring_level"] or 0)
    active_aura = str(u["active_aura"] or "")
    hp_boost = int(u["hp_boost"] or 0)
    total_kills = int(u["total_boss_kills"] or 0)
    admin_role = int(u["admin_role"] or 0)
    profile_title = str(_row_get(u, "profile_title", "") or "").strip()
    profile_note = str(u["profile_note"] or "").strip()
    created_at = int(u["created_at"] or 0)
    reg_label = str(_row_get(u, "reg_label", "") or "").strip()
    cur_hp_field = _calc_player_max_hp(u)
    base_hp_raw = 120 + arena * 55 + int(_player_pet_bonus(u) * PET_HP_EFFECT_MULT * 4) + rebirth_count * 90
    base_hp = max(1, int(base_hp_raw * BASE_HP_MULT))

    weapon_bonus = _player_weapon_bonus(u)
    pet_bonus = _player_pet_bonus(u)
    damage = _calc_player_damage(u)
    hp = _calc_player_max_hp(u)
    admin_mode = _admin_mode_active(u)

    eq_w_id = int(u["equipped_weapon_id"] or 0)
    eq_p_id = int(u["equipped_pet_id"] or 0)
    w_item = db.get_inventory_item(tg_id, eq_w_id) if eq_w_id else None
    p_item = db.get_inventory_item(tg_id, eq_p_id) if eq_p_id else None

    # Для профиля показываем фактическое имя надетого предмета, а не вычисленное по bonus.
    weapon_name = str(w_item["name"]) if w_item else (gd.get_weapon_name(weapon_bonus) if weapon_bonus > 0 else "Нет")
    pet_name = str(p_item["name"]) if p_item else (gd.get_pet_name(pet_bonus, arena) if pet_bonus > 0 else "Нет")
    if w_item and _is_admin_item_name(str(_row_get(w_item, "name", "") or "")):
        weapon_name = "admin"
    elif w_item and _is_vip_donate_item_name(str(_row_get(w_item, "name", "") or "")):
        weapon_name = "👑 VIP"
    if p_item and _is_admin_item_name(str(_row_get(p_item, "name", "") or "")):
        pet_name = "admin"
    elif p_item and _is_vip_donate_item_name(str(_row_get(p_item, "name", "") or "")):
        pet_name = "👑 VIP"
    elif p_item and db.get_item_enchants(int(_row_get(p_item, "id", 0) or 0)):
        pet_name = f"{pet_name} ✨"

    ring_name = gd.RING_NAMES.get(active_ring, "Нет кольца")
    aura_name = gd.AURA_CATALOG[active_aura]["name"] if active_aura in gd.AURA_CATALOG else "Нет ауры"
    vip_name = VIP_NAMES.get(vip_lvl, "Нет")

    reg_str = reg_label if reg_label else (_fmt_ts_msk(created_at, "%d.%m.%Y") if created_at else "-")
    guild = db.get_user_guild(tg_id)
    guild_name = f"«{guild['name']}»" if guild else "Нет"
    guild_id = int(_row_get(guild, "id", 0) or 0) if guild else 0

    lines = [
        "👤 <b>Профиль</b>",
        "<code>···················</code>",
    ]

    if admin_role > 0 or tg_id in SUPER_ADMINS:
        role_str = ADMIN_ROLES.get(admin_role, "")
        if tg_id in SUPER_ADMINS and admin_role == 0:
            role_str = "создатель"
        if role_str:
            lines.append("👑 <b>АДМИНИСТРАЦИЯ ПРОЕКТА</b>")
            lines.append(f"└ <i>{escape(role_str)}</i>")
            lines.append("")

    if profile_title:
        title_text = escape(str(profile_title).strip().upper())
        lines.append(f"👑 | <b>{title_text}</b>")
        lines.append("<code>···················</code>")
        lines.append("")

    if profile_note:
        lines.append(f"📝 <i>{escape(profile_note)}</i>")
        lines.append("")

    lines += [
        f"🏷 | <b>Ник в боте:</b> <code>{escape(nick)}</code>",
        f"🏅 | <b>Ранг:</b> {escape(rank_name)} (<i>обыч. реб: {rebirth_count} | истин. реб: {true_rebirth_count} | x{rebirth_mult:.2f}</i>)",
        f"🏟 | <b>Арена:</b> {arena}",
        f"⭐ | <b>Уровень:</b> {arena + rebirth_count}",
        f"🏰 | <b>Гильдия:</b> {escape(guild_name)}" + (f" (ID: {guild_id})" if guild_id > 0 else ""),
        "",
        f"🪙 | <b>Монеты:</b> {fmt_short_num(coins)}",
        f"🔯 | <b>Маг. монеты:</b> {fmt_short_num(magic_coins)}",
        f"💠 | <b>Эссенция тьмы:</b> {fmt_short_num(essence)}",
        f"🌟 | <b>Привилегия:</b> {escape(vip_name)}",
        f"⚙️ | <b>Мощность:</b> {fmt_num(power)}",
        f"💍 | <b>Кольцо:</b> {escape(ring_name)}",
        f"✨ | <b>Аура:</b> {escape(aura_name)}",
        "",
        f"🗡 | <b>Оружие:</b> {escape(weapon_name)}",
        f"🐾 | <b>Питомец:</b> {escape(pet_name)}",
        f"💢 | <b>Урон:</b> {'admin' if admin_mode else fmt_num(damage)} | ❤️ <b>ХП:</b> {'admin' if admin_mode else fmt_num(hp)}",
        f"❤️ | <b>Текущий HP:</b> {'admin' if admin_mode else fmt_num(cur_hp_field)}",
        "",
        f"☠ | <b>Убито боссов:</b> {fmt_num(total_kills)}",
        f"🗓 | <i>Дата регистрации: {escape(reg_str)}</i>",
        "<code>···················</code>",
    ]
    if is_admin_view:
        train_case_lvl = int(_row_get(u, "train_case_lvl", 0) or 0)
        train_power_lvl = int(_row_get(u, "train_power_lvl", 0) or 0)
        train_time_lvl = int(_row_get(u, "train_time_lvl", 0) or 0)
        dep_amount = int(_row_get(u, "deposit_amount", 0) or 0)
        lines.append(f"🧪 <b>Базовый HP:</b> {fmt_num(base_hp)}")
        lines.append(
            f"🧪 <b>Улучшения:</b> ШК {train_case_lvl} | СТ {train_power_lvl} | ВТ {train_time_lvl}"
        )
        slot_parts = [f"{i}) {_artifact_slot_label(u, i)}" for i in range(1, _artifact_slot_count(u) + 1)]
        lines.append(f"🧿 <b>Артефакты:</b> " + " | ".join(slot_parts))
        lines.append(f"🏦 <b>Депозит:</b> {fmt_num(dep_amount)}")
        lines.append(f"🆔 <b>TG ID:</b> <code>{tg_id}</code>")
        lines.append(f"🚫 <b>Бан:</b> {'Да' if int(u['banned'] or 0) else 'Нет'}")
        muted = int(u["muted_until"] or 0)
        if muted > time.time():
            mins = int((muted - time.time()) / 60)
            lines.append(f"🔇 <b>Мут:</b> {mins} мин.")
    return "\n".join(lines)


def _cases_text(u) -> str:
    nick = escape(_display_name(u))
    max_open = _max_case_open_count(u)
    lines = [f"📦 Кейсы игрока {nick}", SEP]
    lines.append(f"📦 | {u['afk_common']} шт. (Обычные)")
    lines.append(f"🔮 | {u['afk_rare']} шт. (Редкие)")
    lines.append(f"💎 | {u['afk_epic']} шт. (Эпические)")
    lines.append(f"🔱 | {u['afk_legendary']} шт. (Легендарные)")
    lines.append(f"🌌 | {u['afk_mythic']} шт. (Мифические)")
    lines.append(f"👜 | {_artifact_bag_count(int(u['tg_id']))} шт. (Сумка артефактов)")
    lines.append(SEP)

    item_case_lines = []
    for a in range(1, gd.max_arena() + 1):
        wc = int(u[f"weapon_cases_a{a}"] or 0)
        pc = int(u[f"pet_cases_a{a}"] or 0)
        if wc > 0:
            item_case_lines.append(f"🎫 Кейсы оружия (Арена {a}): {wc} шт.")
        if pc > 0:
            item_case_lines.append(f"🐾 Кейсы питомцев (Арена {a}): {pc} шт.")
    if item_case_lines:
        lines.extend(item_case_lines)
        lines.append(SEP)

    how_to = "\n".join([
        "Как открыть:",
        "• AFK‑кейсы: отк об [кол], ред, эп, лег, мифч, са",
        "• Оружие арены N: отк коN [кол]",
        "• Питомец арены N: отк кпN [кол]",
        "",
        "Продажа: продать коN [кол] или кпN [кол]",
        f"Лимит: {max_open} за раз.",
        "ко — оружие, кп — питомец, N = номер арены 1..15",
    ])
    lines.append(f"<blockquote expandable>{how_to}</blockquote>")
    return "\n".join(lines)


def _inventory_text(u) -> str:
    tg_id = int(u["tg_id"])
    # Артефакты выводятся только в разделе "Артефакты", чтобы не засорять общий инвентарь.
    items = [it for it in db.inventory_list(tg_id, limit=5000) if str(_row_get(it, "type", "")) != "artifact"]
    eq_w = int(u["equipped_weapon_id"] or 0)
    eq_p = int(u["equipped_pet_id"] or 0)

    shard_lines = []
    for i in range(1, 6):
        cnt = int(u[f"shard_{i}"] or 0)
        if cnt > 0:
            shard_lines.append(f"• {gd.SHARD_NAMES[i]} x{cnt}")

    lines = ["🎒 Инвентарь:"]
    if shard_lines:
        lines.append("🧩 Осколки:")
        lines.extend(shard_lines)
        lines.append("")

    if not items:
        lines.append("Инвентарь пуст.")
    else:
        # Batch-загрузка всех зачарований за один запрос
        wp_ids = [int(it["id"]) for it in items if it["type"] in ("weapon", "pet")]
        all_enchants = db.get_enchants_for_items(wp_ids) if wp_ids else {}

        for it in items:
            if it["type"] == "weapon":
                icon = "🗡"
            elif it["type"] == "pet":
                icon = "🐾"
            else:
                icon = "🧿"
            marker = ""
            is_admin_item = _is_admin_item_name(str(it["name"] or ""))
            is_vip_item = _is_vip_donate_item_name(str(it["name"] or ""))
            bonus_text = "admin" if is_admin_item else f"+{it['bonus']}"
            if is_vip_item and not is_admin_item:
                bonus_text = f"+{it['bonus']} 👑 VIP"
            if it["in_bank"]:
                marker = " [В БАНКЕ]"
            elif it["type"] == "weapon" and it["id"] == eq_w:
                marker = " [НАДЕТО: оружие]"
            elif it["type"] == "pet" and it["id"] == eq_p:
                marker = " [НАДЕТО: питомец]"
            item_enchants = all_enchants.get(int(it["id"]), {})
            enchant_label = ""
            if item_enchants and it["type"] in ("weapon", "pet"):
                parts_e = []
                for ekey, elvl in item_enchants.items():
                    edata = gd.ENCHANT_CATALOG.get(ekey)
                    if edata:
                        parts_e.append(f"{edata['emoji']} {edata['name']} Ур.{elvl}")
                if parts_e:
                    enchant_label = "  📖 " + " | ".join(parts_e)
            enchant_mark = " ✨" if it["type"] == "pet" and item_enchants else ""
            lines.append(f"{icon} ID {it['id']} | {it['name']}{enchant_mark} | L{it['level']} | {bonus_text} | x{it['count']}{marker}{enchant_label}")

    lines += [
        "",
        "Продажа: прод [id] [кол-во]",
        "Массовая продажа: сел о | сел п",
        "Банк: банк | банк положить [id] | банк снять [id]",
        "Экип: надеть оружие [id] | надеть пета [id]",
    ]
    return "\n".join(lines)


def _loadout_text(u) -> str:
    active_ring = int(u["active_ring_level"] or 0)
    active_aura = str(u["active_aura"] or "")

    ring_name = gd.RING_NAMES.get(active_ring, "Нет кольца")
    aura_name = gd.AURA_CATALOG[active_aura]["name"] if active_aura in gd.AURA_CATALOG else "Нет ауры"

    tg_id = int(u["tg_id"])
    has_w2 = _has_slot2_weapon(tg_id)
    has_p2 = _has_slot2_pet(tg_id)

    eq_w_id  = int(_row_get(u, "equipped_weapon_id",   0) or 0)
    eq_w_id2 = int(_row_get(u, "equipped_weapon_id_2", 0) or 0)
    eq_p_id  = int(_row_get(u, "equipped_pet_id",      0) or 0)
    eq_p_id2 = int(_row_get(u, "equipped_pet_id_2",    0) or 0)

    def _item_name(item_id: int, fallback: str) -> str:
        if item_id <= 0:
            return fallback
        it = db.get_inventory_item(tg_id, item_id)
        return str(_row_get(it, "name", "") or fallback) if it else fallback

    weapon1 = _item_name(eq_w_id, "Нет оружия")
    weapon2 = _item_name(eq_w_id2, "Пусто") if has_w2 else None
    pet1    = _item_name(eq_p_id,  "Нет питомца")
    pet2    = _item_name(eq_p_id2, "Пусто") if has_p2 else None

    lines = ["🧩 Экипировка", SEP,
             f"💍 Активное кольцо: {ring_name}",
             f"✨ Активная аура: {aura_name}",
             f"🗡 Оружие 1: {weapon1}"]
    if has_w2:
        lines.append(f"🗡 Оружие 2: {weapon2}")
    lines.append(f"🐾 Питомец 1: {pet1}")
    if has_p2:
        lines.append(f"🐾 Питомец 2: {pet2}")
    return "\n".join(lines)


def _loadout_kb(u) -> InlineKeyboardMarkup:
    ring_level = int(u["ring_level"] or 0)
    active_ring = int(u["active_ring_level"] or 0)
    active_aura = str(u["active_aura"] or "")

    rows = []

    # Кольца: всегда показываем "без кольца" + доступные уровни.
    ring_buttons = [
        InlineKeyboardButton(
            text=("✅ " if active_ring == 0 else "") + "💍 Без кольца",
            callback_data="ring:0",
        )
    ]
    for i in range(1, ring_level + 1):
        ring_buttons.append(
            InlineKeyboardButton(
                text=("✅ " if active_ring == i else "") + f"💍 {i}",
                callback_data=f"ring:{i}",
            )
        )
    for i in range(0, len(ring_buttons), 4):
        rows.append(ring_buttons[i:i + 4])

    # Ауры: показываем только купленные + вариант "без ауры".
    aura_specs = [
        ("regen", "✨ Реген"),
        ("fortune", "🍀 Фортуна"),
        ("master", "⚙️ Мастер"),
        ("hunter", "🎯 Ловец"),
        ("wrath", "💀 Гнев"),
    ]
    aura_buttons = [
        InlineKeyboardButton(
            text=("✅ " if not active_aura else "") + "✨ Без ауры",
            callback_data="aura:",
        )
    ]
    for key, label in aura_specs:
        if int(_row_get(u, f"aura_{key}", 0) or 0) <= 0:
            continue
        aura_buttons.append(
            InlineKeyboardButton(
                text=("✅ " if active_aura == key else "") + label,
                callback_data=f"aura:{key}",
            )
        )
    for i in range(0, len(aura_buttons), 2):
        rows.append(aura_buttons[i:i + 2])

    rows.append([InlineKeyboardButton(text="⚡ Надеть лучшее", callback_data="loadout:best")])

    # Второй слот оружия
    tg_id_kb = int(u["tg_id"])
    if _has_slot2_weapon(tg_id_kb):
        eq_w2 = int(_row_get(u, "equipped_weapon_id_2", 0) or 0)
        rows.append([
            InlineKeyboardButton(text="🗡 Снять оружие 2" if eq_w2 else "🗡 Надеть оружие 2",
                                 callback_data="loadout:w2:unequip" if eq_w2 else "loadout:w2:equip"),
        ])
    if _has_slot2_pet(tg_id_kb):
        eq_p2 = int(_row_get(u, "equipped_pet_id_2", 0) or 0)
        rows.append([
            InlineKeyboardButton(text="🐾 Снять питомца 2" if eq_p2 else "🐾 Надеть питомца 2",
                                 callback_data="loadout:p2:unequip" if eq_p2 else "loadout:p2:equip"),
        ])

    rows.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data="loadout:refresh"),
        InlineKeyboardButton(text="✖ Закрыть", callback_data="loadout:close"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _artifact_menu_text(u, page: int = 0, per_page: int = 21) -> str:
    tg_id = int(u["tg_id"])
    items = sorted(_artifact_items_for_user(tg_id), key=_artifact_sort_key)
    total = len(items)
    pages = max(1, (total + per_page - 1) // per_page)
    p = max(0, min(int(page), pages - 1))
    start = p * per_page
    subset = items[start:start + per_page]
    lines = [
        "🧿 Артефакты",
        SEP,
        f"Твои артефакты: {total}",
        f"Страница: {p + 1}/{pages}",
    ]
    for i in range(1, _artifact_slot_count(u) + 1):
        lines.append(f"{i}) {_artifact_slot_label(u, i)}")
    if not subset:
        lines.append("\nИнвентарь артефактов пуст.")
    else:
        lines.append("\nВыбери артефакт кнопкой ниже.")
    return "\n".join(lines)


def _artifact_menu_kb(u, page: int = 0, per_page: int = 21) -> InlineKeyboardMarkup:
    tg_id = int(u["tg_id"])
    items = sorted(_artifact_items_for_user(tg_id), key=_artifact_sort_key)
    total = len(items)
    pages = max(1, (total + per_page - 1) // per_page)
    p = max(0, min(int(page), pages - 1))
    start = p * per_page
    subset = items[start:start + per_page]

    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for it in subset:
        row.append(InlineKeyboardButton(
            text=_artifact_item_short(it),
            callback_data=f"art:{tg_id}:item:{int(it['id'])}:{p}",
        ))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav = []
    if p > 0:
        nav.append(InlineKeyboardButton(text="◀", callback_data=f"art:{tg_id}:page:{p - 1}"))
    if (p + 1) < pages:
        nav.append(InlineKeyboardButton(text="▶", callback_data=f"art:{tg_id}:page:{p + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="✖ Закрыть", callback_data=f"art:{tg_id}:close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _artifact_detail_text(u, item_id: int) -> str:
    it = db.get_inventory_item(int(u["tg_id"]), int(item_id))
    if not it or str(_row_get(it, "type", "")) != "artifact":
        return "❌ Артефакт не найден."
    emo = _artifact_item_emoji(str(_row_get(it, "name", "") or ""))
    cfg = ARTIFACT_TYPES.get(emo)
    lvl = max(1, min(10, int(_row_get(it, "level", 1) or 1)))
    cnt = int(_row_get(it, "count", 1) or 1)
    effect = ""
    if cfg:
        effect = str(cfg["effect"])
    effect_map = {
        "regen": "Реген HP",
        "dmg": "Урон",
        "heal": "Сила хила",
        "dodge": "Уклонение",
        "crit": "Шанс крита",
        "reflect": "Отражение урона",
        "lifesteal": "Вампиризм",
        "artifact_luck": "Шанс других артефактов",
        "case_double": "x2 награда из кейса",
        "train_time": "Время тренировки",
        "train_power": "Мощность с тренировки",
        "coins": "Доход монет",
        "afk_loot": "Лут с AFK-кейсов",
        "afk_case_chance": "Шанс AFK-кейсов",
        "mini_any": "Мини-игры в любых чатах",
        "dungeon_magic": "Маг. монеты в данже",
        "survive": "Шанс выжить с 1 HP (раз в бой)",
    }
    pct_per_level = {
        "crit": 5,
        "lifesteal": 5,
        "artifact_luck": 2,
        "survive": 5,
    }
    effect_line = f"Эффект: {effect_map.get(effect, effect)} +{lvl * pct_per_level.get(effect, 10)}%"
    if effect == "mini_any":
        effect_line = f"Эффект: {effect_map.get(effect, effect)} (без прокачки)"
    title_name = _artifact_clean_title_name(it, emo, cfg)
    coin_cost = ARTIFACT_MERGE_COIN_COST.get(lvl, 3_250_000_000)
    if lvl >= 10 or emo == "🎮":
        merge_line = "Для апа: максимальный уровень." if lvl >= 10 else "Ключ Аркады не прокачивается."
    else:
        merge_line = f"Для апа: 2 одинаковых арта + {fmt_num(coin_cost)} 🪙 (макс L10)."
    return "\n".join([
        title_name,
        SEP,
        f"ID: {int(it['id'])}",
        f"Уровень: {ARTIFACT_LEVEL_ICON.get(lvl, '⚪')} {lvl}",
        f"Количество: {cnt}",
        effect_line,
        "",
        merge_line,
    ])


def _artifact_detail_kb(u, item_id: int, page: int) -> InlineKeyboardMarkup:
    tg_id = int(u["tg_id"])
    rows = []
    slots = _artifact_slot_count(u)
    equip_row = []
    for i in range(1, slots + 1):
        equip_row.append(InlineKeyboardButton(text=_artifact_slot_brief(u, i), callback_data=f"art:{tg_id}:equip:{item_id}:{i}:{page}"))
    rows.append(equip_row)
    trust_info = _artifact_trust_info(int(item_id), int(tg_id))
    if trust_info and int(trust_info.get("holder_id", 0)) == int(tg_id):
        rows.append([
            InlineKeyboardButton(text="⬆️ Улучшить", callback_data=f"art:{tg_id}:merge:{item_id}:{page}"),
            InlineKeyboardButton(text="↩️ Вернуть", callback_data=f"art:{tg_id}:return:{item_id}:{page}"),
        ])
    else:
        rows.append([
            InlineKeyboardButton(text="⬆️ Улучшить", callback_data=f"art:{tg_id}:merge:{item_id}:{page}"),
            InlineKeyboardButton(text="🤝 Доверить", callback_data=f"art:{tg_id}:trust:{item_id}:{page}"),
        ])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data=f"art:{tg_id}:page:{page}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _artifact_trust_info(item_id: int, holder_id: int) -> dict | None:
    """Возвращает данные доверия для артефакта у текущего держателя."""
    iid = int(item_id)
    hid = int(holder_id)
    if iid <= 0 or hid <= 0:
        return None
    row = db.get_artifact_trust(iid)
    if not row:
        return None
    if int(_row_get(row, "holder_id", 0) or 0) != hid:
        return None
    return {
        "owner_id": int(_row_get(row, "owner_id", 0) or 0),
        "holder_id": int(_row_get(row, "holder_id", 0) or 0),
        "expires_at": int(_row_get(row, "expires_at", 0) or 0),
        "item_id": int(_row_get(row, "item_id", 0) or 0),
    }


def _artifact_trust_duration_secs(token: str) -> int:
    raw = str(token or "").strip().lower().replace(" ", "")
    m = re.match(r"^(\d+)([mhdмчд])$", raw)
    if not m:
        return 0
    num = int(m.group(1))
    unit = m.group(2)
    mult = {
        "m": 60,
        "h": 3600,
        "d": 86400,
        "м": 60,
        "ч": 3600,
        "д": 86400,
    }.get(unit, 0)
    secs = num * mult
    if secs < 60 or secs > 7 * 86400:
        return 0
    return secs


def _artifact_trust_nick(nick: str) -> str:
    value = str(nick or "").strip()
    if value.startswith("@"):
        value = value[1:]
    # Разрешаем буквы/цифры/подчеркивание/дефис/точку, чтобы не ломать привычные ники.
    if not re.fullmatch(r"[A-Za-z0-9А-Яа-яЁё_.\-]{2,32}", value):
        return ""
    return value


def _artifact_find_target_by_nick(nick: str) -> tuple[int, str]:
    needle = str(nick or "").strip().lower()
    if not needle:
        return 0, "bad_nick"
    rows = db.find_users_by_nickname_or_username(needle, limit=50)
    nick_exact = [r for r in rows if str(_row_get(r, "nickname", "") or "").strip().lower() == needle]
    user_exact = [r for r in rows if str(_row_get(r, "username", "") or "").strip().lower() == needle]
    if len(nick_exact) == 1:
        return int(_row_get(nick_exact[0], "tg_id", 0) or 0), "ok"
    if len(nick_exact) > 1:
        return 0, "ambiguous_nickname"
    if len(user_exact) == 1:
        return int(_row_get(user_exact[0], "tg_id", 0) or 0), "ok"
    if len(user_exact) > 1:
        return 0, "ambiguous_username"
    return 0, "not_found"


def _artifact_transfer_full_stack(item_id: int, from_tg_id: int, to_tg_id: int) -> tuple[bool, str, int]:
    if int(from_tg_id) == int(to_tg_id):
        return False, "Нельзя доверить артефакт самому себе.", 0
    it = db.get_inventory_item(int(from_tg_id), int(item_id))
    if not it or str(_row_get(it, "type", "")) != "artifact":
        return False, "Артефакт не найден у владельца.", 0
    if int(_row_get(it, "in_bank", 0) or 0) == 1:
        return False, "Артефакт в банке, сначала сними его из банка.", 0

    import sqlite3 as _sq

    with _sq.connect("bot.db", timeout=30) as con:
        con.row_factory = _sq.Row
        row = con.execute(
            "SELECT id, tg_id, type, name, level, bonus, count FROM inventory WHERE id = ? AND tg_id = ?",
            (int(item_id), int(from_tg_id)),
        ).fetchone()
        if not row or str(row["type"] or "") != "artifact":
            return False, "Артефакт не найден у владельца.", 0

        src_count = int(row["count"] or 0)
        if src_count <= 0:
            return False, "Артефакт не найден у владельца.", 0

        # Передаем ровно 1 копию: для доверия создаем отдельную строку count=1.
        if src_count > 1:
            con.execute(
                "UPDATE inventory SET count = ? WHERE id = ?",
                (src_count - 1, int(item_id)),
            )
            cur = con.execute(
                """
                INSERT INTO inventory (tg_id, type, name, level, bonus, count, in_bank)
                VALUES (?, 'artifact', ?, ?, ?, 1, 0)
                """,
                (int(to_tg_id), str(row["name"] or ""), int(row["level"] or 1), int(row["bonus"] or 0)),
            )
            trusted_item_id = int(cur.lastrowid)
        else:
            con.execute(
                "UPDATE inventory SET tg_id = ?, in_bank = 0, count = 1 WHERE id = ?",
                (int(to_tg_id), int(item_id)),
            )
            trusted_item_id = int(item_id)
            con.execute("DELETE FROM saved_items WHERE item_id = ?", (int(item_id),))

    # Если исходная строка у владельца исчезла (count было 1), снимаем со слотов.
    if int(trusted_item_id) == int(item_id):
        for slot_key in ARTIFACT_SLOT_KEYS:
            if int(db.get_stat(int(from_tg_id), slot_key, 0) or 0) == int(item_id):
                db.set_stat_value(int(from_tg_id), slot_key, 0)
    return True, "ok", int(trusted_item_id)


def _artifact_return_one_copy_in_con(con, trust_row) -> tuple[bool, int, int]:
    """Возвращает ровно 1 копию доверенного артефакта владельцу в текущей транзакции.

    Возвращает (ok, owner_id, holder_id).
    """
    item_id = int(_row_get(trust_row, "item_id", 0) or 0)
    owner_id = int(_row_get(trust_row, "owner_id", 0) or 0)
    holder_id = int(_row_get(trust_row, "holder_id", 0) or 0)
    item_name = str(_row_get(trust_row, "item_name", "") or "")
    item_level = max(1, int(_row_get(trust_row, "item_level", 1) or 1))
    item_bonus = int(_row_get(trust_row, "item_bonus", 0) or 0)

    if item_id <= 0 or owner_id <= 0:
        return False, owner_id, holder_id

    inv = con.execute(
        "SELECT id, tg_id, type, name, level, bonus, count FROM inventory WHERE id = ?",
        (item_id,),
    ).fetchone()

    if inv and str(inv["type"] or "") == "artifact":
        current_holder = int(inv["tg_id"] or 0)
        count = max(1, int(inv["count"] or 1))
        name = str(inv["name"] or item_name)
        lvl = max(1, int(inv["level"] or item_level))
        bonus = int(inv["bonus"] or item_bonus)

        if count > 1:
            # Если доверенный артефакт оказался в стеке, возвращаем только 1 копию владельцу.
            con.execute("UPDATE inventory SET count = ? WHERE id = ?", (count - 1, item_id))
            owner_row = con.execute(
                """
                SELECT id, count
                FROM inventory
                WHERE tg_id = ? AND type = 'artifact' AND name = ? AND level = ? AND bonus = ? AND in_bank = 0
                ORDER BY id DESC
                LIMIT 1
                """,
                (owner_id, name, lvl, bonus),
            ).fetchone()
            if owner_row:
                con.execute(
                    "UPDATE inventory SET count = ? WHERE id = ?",
                    (int(owner_row["count"] or 0) + 1, int(owner_row["id"])),
                )
            else:
                con.execute(
                    "INSERT INTO inventory (tg_id, type, name, level, bonus, count, in_bank) VALUES (?, 'artifact', ?, ?, ?, 1, 0)",
                    (owner_id, name, lvl, bonus),
                )
        else:
            if current_holder != owner_id:
                con.execute(
                    "UPDATE inventory SET tg_id = ?, in_bank = 0, count = 1 WHERE id = ?",
                    (owner_id, item_id),
                )
    else:
        # Предмет исчез из инвентаря: восстановим 1 копию владельцу из снапшота доверия.
        owner_row = con.execute(
            """
            SELECT id, count
            FROM inventory
            WHERE tg_id = ? AND type = 'artifact' AND name = ? AND level = ? AND bonus = ? AND in_bank = 0
            ORDER BY id DESC
            LIMIT 1
            """,
            (owner_id, item_name, item_level, item_bonus),
        ).fetchone()
        if owner_row:
            con.execute(
                "UPDATE inventory SET count = ? WHERE id = ?",
                (int(owner_row["count"] or 0) + 1, int(owner_row["id"])),
            )
        else:
            con.execute(
                "INSERT INTO inventory (tg_id, type, name, level, bonus, count, in_bank) VALUES (?, 'artifact', ?, ?, ?, 1, 0)",
                (owner_id, item_name, item_level, item_bonus),
            )

    # Снимаем предмет из слотов держателя.
    con.executemany(
        """
        UPDATE user_stats
        SET stat_value = 0
        WHERE tg_id = ? AND stat_key = ? AND stat_value = ?
        """,
        [(holder_id, slot_key, item_id) for slot_key in ARTIFACT_SLOT_KEYS],
    )
    con.execute("DELETE FROM artifact_trust WHERE item_id = ?", (item_id,))
    return True, owner_id, holder_id


# ─────────────────────────────────────────────
#  ТЕКСТ ПОМОЩИ
# ─────────────────────────────────────────────
def _help_text() -> str:
    arena_summary = []
    total_hp = {a: sum(int(b.hp * _enemy_hp_mult(a)) for b in gd.ARENAS[a]) for a in gd.ARENAS}
    total_atk = {a: sum(int(b.atk * _enemy_atk_mult(a)) for b in gd.ARENAS[a]) for a in gd.ARENAS}
    total_rew = {
        a: sum(int(b.reward * BOSS_REWARD_MULT * _boss_reward_arena_mult(a)) for b in gd.ARENAS[a])
        for a in gd.ARENAS
    }
    for a in range(1, gd.max_arena() + 1):
        arena_summary.append(
            f"{a}) {gd.arena_title(a)} | HP {fmt_num(total_hp[a])} | ATK {fmt_num(total_atk[a])} | 🪙 {fmt_num(total_rew[a])}"
        )
    lines = [
        "📘 Помощь",
        SEP,
        "🎯 Шансы AFK-кейсов за тик:",
        "📦 10% | 🔮 5% | 💎 1% | 🔱 0.1% | 🌌 0.06%",
        "",
        "🛡 Бои с боссами:",
    ] + arena_summary + [
        "",
        "💍 Кольца:",
        "• 💍 Кольцо Пепла (+2%) -> +2% 🪙",
        "• 💍 Кольцо Луны (+5%) -> +5% 🪙",
        "• 💍 Кольцо Крови (+10%) -> +10% 🪙",
        "• 💍 Кольцо Пустоты (+20%) -> +20% 🪙",
        "• 💍 Кольцо Владыки (+35%) -> +35% 🪙",
        "",
        "✨ Ауры (Лавка Джинна):",
        "• ✨ Аура Регена — 70 🔯 (+5% HP к регену)",
        "• 🍀 Аура Фортуны — 315 🔯 (x1.5 золото)",
        "• ⚙️ Аура Мастера — 560 🔯 (+30% AFK мощность)",
        "• 🎯 Аура Ловца — 1500 🔯 (x2 шансы AFK-кейсов)",
        "• 💀 Аура Гнева — 3400 🔯 (+50% урон)",
        "",
        "🗺 Данж: пошаговый, 50 волн, 10 минут.",
        "Режимы: 🪦Гробница (пн/чт), 🕳Грот (вт/пт), 💰Жадность (ср), 🌪Хаос (сб/вс)",
        "",
        "⚔️ Синтез: 3 одинаковых предмета → 1 улучшенный (макс L3)",
        "🔨 Крафт: 10 одинаковых осколков → кольцо",
        "",
        SEP,
        "Команды: профиль, кейсы, инвентарь, трен, боссы, данж, крафт, казино, ивент, помощь",
    ]
    return "\n".join(lines)


def _admin_commands_text() -> str:
    return "\n".join([
        "🛡 Админ-команды",
        SEP,
        "Creator only:",
        "/admin - панель администратора",
        "/item_ids - список admin item id",
        "/set ... - ручная установка значений",
        "/prof [id|reply] - админ-профиль",
        "/soo текст - массовая рассылка",
        "/promo - конструктор промокодов",
        "/con вопрос время - конкурс",
        "/congive ... - выдать награду конкурса",
        "/fk ... - fast-конкурс",
        "/donpet [id|reply] - выдать 👑 VIP питомца",
        "/donekip [id|reply] - выдать 👑 VIP оружие",
        "/donequip [id|reply] - алиас VIP оружия",
        "/donwep [id|reply] - алиас VIP оружия",
        "/donfull [id|reply] - выдать полный 👑 VIP набор",
        "/donset [id|reply] - алиас полного VIP набора",
        "",
        "Admin+:",
        "/debag (/debug) - диагностический отчет",
        "/ping - пинг до Telegram API",
        "/online - онлайн 1ч/12ч/24ч",
        "/active [1m|1h|1d] [id|reply] - выгрузка активности игрока",
        "/command - список админ-команд",
        "",
        "Управление и сервис:",
        "/check [id|ник] - быстрый поиск профиля",
        "/clan_down [id] [уровень] - понизить/задать уровень клана",
        "/clan [id] - полный отчёт по клану (для админов)",
        "/topdon [id] [+/-сумма|сумма] - правка донат-топа",
        "/reset - глобальный вайп профилей (с сохранением core-полей)",
        "/rollback [id|all] [5m|30m|1h|10h|24h] - откат по снапшоту",
        "/transferacc [old_id] [new_id] - перенос полного прогресса",
        "/startivent [время] - ивент заработка монет",
        "/endpromos - завершить все активные промокоды",
        "",
        "Кланы и бои:",
        "/hide_clan [id] - скрыть клан из топа",
        "/open_clan [id] (/show_clan) - вернуть клан в топ",
        "/show_clan [id] - алиас вернуть клан в топ",
        "/stop [id|reply] - остановить активный бой/данж",
        "",
        "Артефакты и предметы:",
        "/delart [item_id] - удалить артефакт по ID",
        "/giveart [тип] [уровень] [кол-во] [id|reply] - выдать артефакт",
        "/title [id|reply] [текст] - выдать титул в профиль",
        "/cleartitle [id|reply] - убрать титул",
        "/save id[,id2,...] - сохранить предметы для true rebirth",
        "/unsave id[,id2,...] - убрать из сохранения",
        "",
        "Режимы отображения:",
        "/hidestats - скрыть admin-статы",
        "/openstats - включить admin-статы",
        "",
        "Модерация:",
        "/bb [id|reply] - бан",
        "/bub [id|reply] - разбан",
        "/mute [id|reply] [10m|2h|1d] - мут",
        "/unmute [id|reply] - размут",
        "/notifyoff [id] - отключить все рассылки игроку",
        "/notifyon [id] - включить рассылки обратно",
        "",
        "Команда: /command",
    ])


# ─────────────────────────────────────────────
#  ТЕКСТ ИВЕНТА
# ─────────────────────────────────────────────
def _daily_event_text(u) -> str:
    mode = _today_dungeon_mode()
    mode_name = DUNGEON_MODE_NAMES.get(mode, mode)
    reward_desc = DUNGEON_REWARD_DESC.get(mode, "")
    day_key = _today_msk().isoformat()
    tg_id = int(u["tg_id"])
    hour = db.ensure_trader_hour(tg_id, day_key)
    cur_hour = _now_msk().hour
    if JINN_FORCED_UNTIL > int(time.time()):
        jinn_status = "🟢 Открыта (принудительно)"
    elif hour >= 0 and cur_hour == hour:
        jinn_status = "🟢 Открыта"
    elif hour >= 0:
        jinn_status = "🕒 Откроется позже сегодня"
    else:
        jinn_status = "🔴 Закрыта"
    lines = [
        "📅 Ивент дня",
        SEP,
        f"🗺 Данж сегодня: {mode_name}",
        f"🎁 Награды: {reward_desc}",
        f"🛒 Лавка Джинна: {jinn_status}",
    ]
    return "\n".join(lines)


def _ensure_world_boss_event() -> object:
    row = db.get_world_boss_event()
    if row:
        # Если конфиг босса изменился (например, подняли максимум HP), синхронизируем его.
        if int(row["max_hp"] or 0) != WORLD_BOSS_MAX_HP:
            row = db.world_boss_set_max_hp(WORLD_BOSS_MAX_HP, refill_current=True) or row
        return row
    now_ts = int(time.time())
    db.create_world_boss_event(
        name=WORLD_BOSS_NAME,
        max_hp=WORLD_BOSS_MAX_HP,
        started_at=now_ts,
        ends_at=now_ts + WORLD_BOSS_DURATION_SEC,
    )
    return db.get_world_boss_event()


def _world_boss_time_left(ends_at: int) -> str:
    left = max(0, int(ends_at) - int(time.time()))
    d = left // 86400
    h = (left % 86400) // 3600
    m = (left % 3600) // 60
    return f"{d}д {h}ч {m}м"


def _world_boss_kb(admin_mode: bool = False, owner_id: int | None = None) -> InlineKeyboardMarkup:
    def _cb(action: str) -> str:
        if owner_id is None:
            return f"wboss:{action}"
        return f"wboss:{action}:{int(owner_id)}"

    first_row = [InlineKeyboardButton(text="⚔️ Удар", callback_data=_cb("hit"))]
    if admin_mode:
        first_row = [
            InlineKeyboardButton(text="🔥 Испепелить", callback_data=_cb("burn")),
            InlineKeyboardButton(text="🤲 Пощадить", callback_data=_cb("mercy")),
        ]
    return InlineKeyboardMarkup(inline_keyboard=[
        first_row,
        [InlineKeyboardButton(text="📊 Топ ивент", callback_data=_cb("top"))],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=_cb("refresh")),
            InlineKeyboardButton(text="✖ Закрыть", callback_data=_cb("close")),
        ],
    ])


def _world_boss_text(u, state, hit_row=None, admin_mode: bool = False) -> str:
    if not state:
        return "⚠️ Ивент-босс пока не запущен."
    hp = int(state["hp"] or 0)
    max_hp = int(state["max_hp"] or 0)
    ends_at = int(state["ends_at"] or 0)
    is_finished = int(state["is_finished"] or 0)
    winner_id = int(state["winner_id"] or 0)
    my_damage = int(_row_get(hit_row, "damage", 0) or 0)
    my_hits = int(_row_get(hit_row, "hits", 0) or 0)
    dead_until = int(_row_get(hit_row, "dead_until", 0) or 0)
    last_hit_at = int(_row_get(hit_row, "last_hit_at", 0) or 0)
    player_max_hp = _calc_player_max_hp(u)
    player_cur_hp = int(_row_get(hit_row, "current_hp", 0) or 0)
    if player_cur_hp <= 0 or player_cur_hp > player_max_hp:
        player_cur_hp = player_max_hp
    lines = [
        WORLD_BOSS_NAME,
        SEP_BAR,
        f"❤️ HP: [{fmt_num(hp)}/{fmt_num(max_hp)}]",
    ]
    if is_finished:
        lines.append("🏁 Босс повержен!")
        if winner_id > 0:
            lines.append(f"🏆 Последний удар: {winner_id}")
    elif int(time.time()) >= ends_at:
        lines.append("⏳ Ивент завершен по времени.")
    else:
        lines.append(f"⏱ До конца: {_world_boss_time_left(ends_at)}")

    # Показываем HP игрока в шапке всегда, в том числе по кнопке "Обновить".
    if admin_mode:
        lines.append("❤️ Твой HP в ивенте: ∞/∞")
    else:
        lines.append(f"❤️ Твой HP в ивенте: {fmt_num(player_cur_hp)}/{fmt_num(player_max_hp)}")

    lines += [
        SEP,
        f"📊 Твой урон: {fmt_num(my_damage)}",
        f"🗡 Твои удары: {fmt_num(my_hits)}",
    ]
    if dead_until > int(time.time()):
        mins = max(1, (dead_until - int(time.time())) // 60)
        lines.append(f"☠️ Ты повержен. Вход через ~{mins} мин.")
    elif last_hit_at > 0:
        cd = max(0, WORLD_BOSS_HIT_CD_SEC - (int(time.time()) - last_hit_at))
        if cd > 0:
            lines.append(f"⏳ КД удара: {cd}с")
    lines.append(SEP)
    return "\n".join(lines)


def _event_text(u) -> str:
    state = _ensure_world_boss_event()
    hit = db.get_world_boss_hit(int(u["tg_id"]))
    return f"{_world_boss_text(u, state, hit)}\n\n{_daily_event_text(u)}"


def _world_boss_top_text(limit: int = 5) -> str:
    rows = db.list_world_boss_hits(limit=max(1, int(limit)))
    lines = ["📊 Топ ивент-босса", SEP_BAR]
    if not rows:
        lines.append("Пока нет участников.")
        return "\n".join(lines)

    for idx, r in enumerate(rows, start=1):
        nick = str(_row_get(r, "nickname", "") or "").strip()
        uname = str(_row_get(r, "username", "") or "").strip()
        display = nick if nick else (f"@{uname}" if uname else f"id{int(r['tg_id'])}")
        dmg = int(_row_get(r, "damage", 0) or 0)
        hits = int(_row_get(r, "hits", 0) or 0)
        lines.append(f"{idx}. {display} — {fmt_num(dmg)} урона | {fmt_num(hits)} уд.")
    return "\n".join(lines)


def _append_profile_note(u, extra_line: str):
    raw = str(u["profile_note"] or "").strip()
    if extra_line in raw:
        return raw
    if not raw:
        return extra_line
    return f"{raw}\n{extra_line}"[:1800]


def _append_profile_title(u, extra_line: str):
    raw = str(_row_get(u, "profile_title", "") or "").strip()
    if extra_line in raw:
        return raw
    if not raw:
        return extra_line
    return f"{raw}\n{extra_line}"[:300]


async def _world_boss_distribute_rewards(state):
    if not state:
        return
    if not db.mark_world_boss_rewards_done():
        return
    winner_id = int(state["winner_id"] or 0)
    hits = db.list_world_boss_hits(limit=200000)
    for row in hits:
        uid = int(row["tg_id"])
        u = db.get_user(uid)
        if not u:
            continue
        dmg = int(row["damage"] or 0)
        hits_cnt = int(row["hits"] or 0)
        updates = {}
        notify_lines = ["🎁 Награды за ивент-босса:"]

        if uid == winner_id:
            title_note = "🏆 Гроза боссов"
            updates["profile_title"] = _append_profile_title(u, title_note)
            updates["admin_role"] = max(2, int(u["admin_role"] or 0))
            notify_lines.append("• Последний удар: титул «Гроза боссов»")
            notify_lines.append("• Выдана роль администратора 2 уровня")
        elif dmg >= WORLD_BOSS_TIER1_DAMAGE:
            cur_coins = int(u["coins"] or 0)
            bonus_coins = int(cur_coins * 10)
            updates["coins"] = cur_coins + bonus_coins
            updates["magic_coins"] = int(u["magic_coins"] or 0) + 1000
            updates["essence"] = int(u["essence"] or 0) + 1000
            updates["profile_title"] = _append_profile_title(u, "🌑 Покоритель Бездны")
            notify_lines.append("• +1000 эссенции")
            notify_lines.append(f"• +{fmt_num(bonus_coins)} монет (+1000% от баланса)")
            notify_lines.append("• +1000 маг. монет")
        elif dmg >= WORLD_BOSS_TIER2_DAMAGE:
            cur_coins = int(u["coins"] or 0)
            cur_magic = int(u["magic_coins"] or 0)
            bonus_coins = int(cur_coins * 3)
            bonus_magic = int(cur_magic * 1)
            updates["coins"] = cur_coins + bonus_coins
            updates["magic_coins"] = cur_magic + bonus_magic
            updates["essence"] = int(u["essence"] or 0) + 100
            max_a = max(1, min(int(u["arena"] or 1), gd.max_arena()))
            updates[f"weapon_cases_a{max_a}"] = int(u[f"weapon_cases_a{max_a}"] or 0) + 50
            updates[f"pet_cases_a{max_a}"] = int(u[f"pet_cases_a{max_a}"] or 0) + 50
            notify_lines.append("• +100 эссенции")
            notify_lines.append(f"• +{fmt_num(bonus_coins)} монет (+300% от баланса)")
            notify_lines.append(f"• +{fmt_num(bonus_magic)} маг. монет (+100% от баланса)")
            notify_lines.append(f"• +50 ко{max_a} и +50 кп{max_a}")
        elif hits_cnt >= WORLD_BOSS_HITS_MIN_REWARD:
            updates["essence"] = int(u["essence"] or 0) + 10
            updates["magic_coins"] = int(u["magic_coins"] or 0) + 100
            notify_lines.append("• +10 эссенции")
            notify_lines.append("• +100 маг. монет")

        if updates:
            db.update_user(uid, **updates)
            if bot_instance is not None and _notify_enabled(uid, NOTIFY_GUILD_BOSS_KEY):
                try:
                    await bot_instance.send_message(uid, "\n".join(notify_lines))
                except Exception:
                    pass


# ─────────────────────────────────────────────
#  БОЙ С БОССОМ — VIEW
# ─────────────────────────────────────────────
def _battle_view(bs: BattleState, log_lines: list[str] = None, admin_mode: bool = False, regen_heal: int = 0) -> str:
    boss = gd.ARENAS[bs.arena][bs.boss_idx]
    hp_line = f"❤️ {fmt_num(bs.player_hp)}/{fmt_num(bs.player_max_hp)}"
    if admin_mode:
        hp_line = "❤️ ∞/∞"
    elif regen_heal > 0:
        hp_line += f" (💞+{fmt_num(regen_heal)})"
    lines = [
        f"⚔️ {boss.name}",
        hp_line,
        f"👾 {fmt_num(bs.boss_hp)}/{fmt_num(bs.boss_max_hp)}",
    ]
    if log_lines:
        lines.append("")
        lines.extend(log_lines)
    return "\n".join(lines)


def _battle_kb(admin_mode: bool = False) -> InlineKeyboardMarkup:
    first_row = [InlineKeyboardButton(text="⚔️ Атаковать", callback_data="battle:attack")]
    if admin_mode:
        first_row = [
            InlineKeyboardButton(text="🔥 Испепелить", callback_data="battle:burn"),
            InlineKeyboardButton(text="🤲 Пощадить", callback_data="battle:mercy"),
        ]
    return InlineKeyboardMarkup(inline_keyboard=[
        first_row,
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="battle:refresh"),
            InlineKeyboardButton(text="◀ Назад к боссам", callback_data="battle:back"),
        ],
        [
            InlineKeyboardButton(text="✖ Закрыть", callback_data="battle:close"),
        ],
    ])


# ─────────────────────────────────────────────
#  ДАНЖ — VIEW
# ─────────────────────────────────────────────
def _dungeon_view(ds: DungeonState) -> str:
    mode_name = DUNGEON_MODE_NAMES.get(ds.mode, ds.mode)
    diff_name = DUNGEON_DIFFICULTY_NAMES.get(str(ds.difficulty or "easy"), "Легкая")
    elapsed = int(time.time() - ds.started_at)
    remaining = max(0, 600 - elapsed)
    shard_str = ""
    if ds.shards:
        parts = [f"{gd.SHARD_NAMES[k]} x{v}" for k, v in sorted(ds.shards.items())]
        shard_str = "\n🧩 " + " | ".join(parts)
    lines = [f"{mode_name} [{diff_name}] | {ds.wave}/{ds.max_waves} | {remaining}с"]
    if ds.enemy_max_hp > 0:
        lines.append(f"👹 {fmt_num(ds.enemy_hp)}/{fmt_num(ds.enemy_max_hp)}")
    lines.append(f"🪙 {fmt_num(ds.gold)} | 🔯 {fmt_num(ds.magic)}{shard_str}")
    if ds.note:
        lines.append(f"\n{ds.note}")
    return "\n".join(lines)


def _dungeon_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚔️ Атаковать волну", callback_data="dungeon:attack")],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data="dungeon:refresh"),
            InlineKeyboardButton(text="🚪 Выйти", callback_data="dungeon:exit"),
        ],
    ])


def _dungeon_finish_text(ds: DungeonState) -> str:
    mode_name = DUNGEON_MODE_NAMES.get(ds.mode, ds.mode)
    diff_name = DUNGEON_DIFFICULTY_NAMES.get(str(ds.difficulty or "easy"), "Легкая")
    shard_lines = []
    for k, v in sorted(ds.shards.items()):
        shard_lines.append(f"  {gd.SHARD_NAMES[k]} x{v}")
    cleared_waves = min(int(ds.wave), int(ds.max_waves))
    lines = [
        "🏁 ДАНЖ ЗАВЕРШЕН",
        f"🗺 Режим: {mode_name} [{diff_name}]",
        SEP_BAR,
        "⏱ Лимит забега: 10 минут",
        f"🌊 Пройдено волн: {cleared_waves}/50",
        f"🪙 Золото: +{fmt_num(ds.gold)}",
        f"🔯 Маг. монеты: +{fmt_num(ds.magic)}",
        "🧩 Осколки:",
    ]
    if shard_lines:
        lines.extend(shard_lines)
    else:
        lines.append("  Нет")
    return "\n".join(lines)


# ─────────────────────────────────────────────
#  ВЫБОР БОССА — ИНЛАЙН
# ─────────────────────────────────────────────
def _boss_arena_kb(user_arena: int) -> InlineKeyboardMarkup:
    rows = []
    for a in range(1, gd.max_arena() + 1):
        locked = a > user_arena
        rows.append([
            InlineKeyboardButton(
                text=f"{'🔒 ' if locked else ''}Арена {a}: {gd.arena_title(a)}",
                callback_data=f"boss_arena:{a}" if not locked else "boss_arena_locked",
            )
        ])
    rows.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="boss_close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _boss_arenas_text(user_arena: int) -> str:
    _ = user_arena
    return "⚔️ Выбери арену"


def _boss_select_kb(arena: int) -> InlineKeyboardMarkup:
    rows = []
    for i, boss in enumerate(gd.ARENAS[arena]):
        reward = int(boss.reward * BOSS_REWARD_MULT * _boss_reward_arena_mult(arena))
        if arena == gd.max_arena() and i == len(gd.ARENAS[arena]) - 1:
            reward = int(reward * FINAL_BOSS_REWARD_MULT)
        rows.append([InlineKeyboardButton(
            text=f"{boss.name} | 🪙{fmt_num(reward)}",
            callback_data=f"boss_pick:{arena}:{i}",
        )])
    rows.append([
        InlineKeyboardButton(text="◀ К аренам", callback_data="boss_arenas"),
        InlineKeyboardButton(text="✖ Закрыть", callback_data="boss_close"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────────────────────────
#  МАГАЗИН КЕЙСОВ — ИНЛАЙН
# ─────────────────────────────────────────────
def _shop_arena_kb(user_arena: int) -> InlineKeyboardMarkup:
    rows = []
    for a in range(1, user_arena + 1):
        price = _case_price(a)
        rows.append([
            InlineKeyboardButton(text=f"Арена {a} — {fmt_num(price)} 🪙", callback_data=f"shop_arena:{a}"),
        ])
    rows.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="shop_close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _shop_buy_kb(arena: int) -> InlineKeyboardMarkup:
    price = _case_price(arena)
    rows = [
        [
            InlineKeyboardButton(text=f"🗡 Оружие x1 ({fmt_num(price)}🪙)", callback_data=f"shop_buy:weapon:{arena}:1"),
            InlineKeyboardButton(text=f"🗡 x3 ({fmt_num(price * 3)}🪙)", callback_data=f"shop_buy:weapon:{arena}:3"),
            InlineKeyboardButton(text=f"🗡 x10 ({fmt_num(price * 10)}🪙)", callback_data=f"shop_buy:weapon:{arena}:10"),
        ],
        [
            InlineKeyboardButton(text=f"🐾 Питомец x1 ({fmt_num(price)}🪙)", callback_data=f"shop_buy:pet:{arena}:1"),
            InlineKeyboardButton(text=f"🐾 x3 ({fmt_num(price * 3)}🪙)", callback_data=f"shop_buy:pet:{arena}:3"),
            InlineKeyboardButton(text=f"🐾 x10 ({fmt_num(price * 10)}🪙)", callback_data=f"shop_buy:pet:{arena}:10"),
        ],
        [
            InlineKeyboardButton(text="🗡 Свое кол-во", callback_data=f"shop_custom:weapon:{arena}"),
            InlineKeyboardButton(text="🐾 Свое кол-во", callback_data=f"shop_custom:pet:{arena}"),
        ],
        [InlineKeyboardButton(text="◀ Назад", callback_data="shop_back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _shop_confirm_text(u, item_type: str, arena: int, qty: int) -> str:
    icon = "🗡" if item_type == "weapon" else "🐾"
    one = _case_price(arena)
    total = one * int(qty)
    return (
        f"🛒 Подтверждение покупки\n"
        f"{icon} Тип: {'Оружие' if item_type == 'weapon' else 'Питомец'}\n"
        f"🏟 Арена: {arena} ({gd.arena_title(arena)})\n"
        f"📦 Кол-во: {fmt_num(qty)}\n"
        f"💵 Цена за 1: {fmt_num(one)} 🪙\n"
        f"💰 Итого: {fmt_num(total)} 🪙\n"
        f"👛 Твои монеты: {fmt_num(int(_row_get(u, 'coins', 0) or 0))} 🪙"
    )


def _shop_confirm_kb(item_type: str, arena: int, qty: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"shop_confirm:{item_type}:{arena}:{qty}")],
        [InlineKeyboardButton(text="◀ Отмена", callback_data=f"shop_confirm_cancel:{arena}")],
    ])


def _shop_apply_purchase(u, item_type: str, arena: int, qty: int) -> tuple[bool, str]:
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        return False, "🔒 Арена не открыта."
    if item_type not in ("weapon", "pet"):
        return False, "❌ Неизвестный тип кейса."

    total_price = _case_price(arena) * int(qty)
    coins = int(_row_get(u, "coins", 0) or 0)
    if coins < total_price:
        return False, f"💸 Недостаточно монет. Нужно: {fmt_num(total_price)}"

    tg_id = int(u["tg_id"])
    col = f"weapon_cases_a{arena}" if item_type == "weapon" else f"pet_cases_a{arena}"
    cur = int(_row_get(u, col, 0) or 0)
    db.update_user(tg_id, coins=coins - total_price, **{col: cur + int(qty)})
    icon = "🗡" if item_type == "weapon" else "🐾"
    return True, f"✅ Куплено {qty} {icon} кейс(ов) арены {arena}!"


# ─────────────────────────────────────────────
#  ЛАВКА ДЖИННА — ИНЛАЙН
# ─────────────────────────────────────────────
def _jinn_kb(u) -> InlineKeyboardMarkup:
    rows = []
    for key, data in gd.AURA_CATALOG.items():
        owned = int(_row_get(u, f"aura_{key}", 0) or 0)
        label = f"{'✅' if owned else '🔮'} {data['name']} — {data['cost']} 🔯"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"jinn_buy:{key}")])
    rows.append([InlineKeyboardButton(text="📖 Зачарования", callback_data="jinn_enchants")])
    rows.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="jinn_close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────────────────────────
#  ПРОМО — ИНЛАЙН (только создатель)
# ─────────────────────────────────────────────
PROMO_TYPES = [
    ("coins", "🪙 Монеты"),
    ("magic_coins", "🔯 Маг.монеты"),
    ("essence", "💠 Эссенция"),
    ("afk_common", "📦 Об.кейс"),
    ("afk_rare", "🔮 Ред.кейс"),
    ("afk_epic", "💎 Эп.кейс"),
    ("afk_legendary", "🔱 Лег.кейс"),
    ("afk_mythic", "🌌 Миф.кейс"),
    ("weapon_cases_a1", "🗡Кейс-О А1"),
    ("weapon_cases_a2", "🗡Кейс-О А2"),
    ("weapon_cases_a3", "🗡Кейс-О А3"),
    ("weapon_cases_a4", "🗡Кейс-О А4"),
    ("weapon_cases_a5", "🗡Кейс-О А5"),
    ("weapon_cases_a6", "🗡Кейс-О А6"),
    ("weapon_cases_a7", "🗡Кейс-О А7"),
    ("weapon_cases_a8", "🗡Кейс-О А8"),
    ("weapon_cases_a9", "🗡Кейс-О А9"),
    ("weapon_cases_a10", "🗡Кейс-О А10"),
    ("weapon_cases_a11", "🗡Кейс-О А11"),
    ("weapon_cases_a12", "🗡Кейс-О А12"),
    ("weapon_cases_a13", "🗡Кейс-О А13"),
    ("weapon_cases_a14", "🗡Кейс-О А14"),
    ("weapon_cases_a15", "🗡Кейс-О А15"),
    ("pet_cases_a1", "🐾Кейс-П А1"),
    ("pet_cases_a2", "🐾Кейс-П А2"),
    ("pet_cases_a3", "🐾Кейс-П А3"),
    ("pet_cases_a4", "🐾Кейс-П А4"),
    ("pet_cases_a5", "🐾Кейс-П А5"),
    ("pet_cases_a6", "🐾Кейс-П А6"),
    ("pet_cases_a7", "🐾Кейс-П А7"),
    ("pet_cases_a8", "🐾Кейс-П А8"),
    ("pet_cases_a9", "🐾Кейс-П А9"),
    ("pet_cases_a10", "🐾Кейс-П А10"),
    ("pet_cases_a11", "🐾Кейс-П А11"),
    ("pet_cases_a12", "🐾Кейс-П А12"),
    ("pet_cases_a13", "🐾Кейс-П А13"),
    ("pet_cases_a14", "🐾Кейс-П А14"),
    ("pet_cases_a15", "🐾Кейс-П А15"),
    ("percent_coins", "% от баланса монет"),
    ("percent_magic", "% от баланса маг.монет"),
]

PROMO_DURATIONS = [
    (3600, "1 час"),
    (6 * 3600, "6 часов"),
    (12 * 3600, "12 часов"),
    (24 * 3600, "1 день"),
    (3 * 24 * 3600, "3 дня"),
    (7 * 24 * 3600, "7 дней"),
    (30 * 24 * 3600, "30 дней"),
]

AUTO_PROMO_MSK_HOUR = 15
AUTO_PROMO_USES = 50
AUTO_PROMO_DURATION_SEC = 6 * 3600
# Конструктор рандом-наград для авто-промо (без жестко заданных наборов).
AUTO_PROMO_REWARD_POOL = [
    ("coins", 8),
    ("magic_coins", 6),
    ("essence", 4),
    ("afk_rare", 4),
    ("afk_epic", 2),
]
AUTO_PROMO_REWARD_RANGES = {
    "coins": (300, 1200, 50),
    "magic_coins": (5, 40, 1),
    "essence": (1, 4, 1),
    "afk_rare": (1, 6, 1),
    "afk_epic": (1, 2, 1),
}
# Жесткие лимиты наград для авто-промо: защита от случайного завышения.
AUTO_PROMO_REWARD_CAPS = {
    "coins": 1200,
    "magic_coins": 40,
    "essence": 4,
    "afk_rare": 6,
    "afk_epic": 2,
}
AUTO_PROMO_LAST_DAY = ""


def _promo_reward_text(rt: str, amount: int) -> str:
    label = dict(PROMO_TYPES).get(rt, rt)
    if rt.startswith("percent"):
        return f"{label}: {amount}%"
    return f"{label}: +{fmt_num(amount)}"


def _normalize_promo_code(raw: str) -> str:
    code = (raw or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9_-]{3,24}", code):
        return ""
    return code


def _auto_promo_make_code() -> str:
    import string

    for _ in range(100):
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        if not db.get_promo(code):
            return code
    return f"AUTO{int(time.time())}"


def _auto_promo_pick_rewards() -> list[dict]:
    # 2-3 уникальных награды за раз. Набор и значения каждый день случайные.
    reward_count = random.choices([2, 3], weights=[70, 30], k=1)[0]
    reward_types = random.choices(
        [it[0] for it in AUTO_PROMO_REWARD_POOL],
        weights=[it[1] for it in AUTO_PROMO_REWARD_POOL],
        k=reward_count * 2,
    )
    uniq_types: list[str] = []
    for rt in reward_types:
        if rt not in uniq_types:
            uniq_types.append(rt)
        if len(uniq_types) >= reward_count:
            break
    if not uniq_types:
        uniq_types = ["coins"]

    out = []
    for rt in uniq_types:
        lo, hi, step = AUTO_PROMO_REWARD_RANGES.get(rt, (1, 2, 1))
        raw = random.randint(int(lo), int(hi))
        val = max(int(lo), (raw // int(max(1, step))) * int(max(1, step)))
        cap = int(AUTO_PROMO_REWARD_CAPS.get(rt, val))
        val = max(int(lo), min(int(val), cap))
        out.append({
            "reward_type": str(rt),
            "reward_value": int(val),
            "reward_percent": 0,
        })
    return out


async def _create_daily_auto_promo(bot: Bot):
    code = _auto_promo_make_code()
    rewards = _auto_promo_pick_rewards()
    now_ts = int(time.time())
    expires = now_ts + AUTO_PROMO_DURATION_SEC

    promo_id = db.create_promo(
        code,
        expires,
        AUTO_PROMO_USES,
        0,
        reward_type="coins",
        reward_value=0,
        reward_percent=0,
    )
    reward_lines = []
    for rw in rewards:
        db.add_promo_reward(
            promo_id,
            str(rw["reward_type"]),
            int(rw["reward_value"]),
            int(rw["reward_percent"]),
        )
        reward_lines.append(_promo_reward_text(str(rw["reward_type"]), int(rw["reward_value"])))

    reward_preview = ", ".join(reward_lines)
    await _broadcast_promo_and_pin(
        bot,
        promo_id,
        code,
        reward_preview,
        AUTO_PROMO_USES,
        expires,
    )


def _active_promos_text(limit: int = 20) -> str:
    rows = db.list_active_promos(int(time.time()), limit=limit)
    lines = ["🎫 Активные промокоды", SEP]
    if not rows:
        lines.append("Сейчас нет активных промокодов.")
        return "\n".join(lines)
    for row in rows:
        code = str(row["code"])
        used = int(row["used_count"] or 0)
        max_uses = int(row["max_uses"] or 0)
        left = max(0, max_uses - used)
        exp = _fmt_ts_msk(int(row["expires_at"] or 0), "%d.%m %H:%M")
        lines.append(f"• {code} | осталось: {left}/{max_uses} | до: {exp}")
    lines.append("")
    lines.append("Активация: промо КОД")
    return "\n".join(lines)


def _apply_promo_rewards(u, rewards) -> tuple[dict, list[str]]:
    fields = [
        "coins", "magic_coins", "essence",
        "afk_common", "afk_rare", "afk_epic", "afk_legendary", "afk_mythic",
    ]
    for i in range(1, 16):
        fields.append(f"weapon_cases_a{i}")
        fields.append(f"pet_cases_a{i}")

    state = {k: int(_row_get(u, k, 0) or 0) for k in fields}
    lines: list[str] = []

    for rw in rewards:
        rt = str(_row_get(rw, "reward_type", "coins") or "coins")
        rv = int(_row_get(rw, "reward_value", 0) or 0)
        rp = int(_row_get(rw, "reward_percent", 0) or 0)

        if rt == "percent_coins":
            bonus = int(state["coins"] * rp / 100)
            state["coins"] += bonus
            lines.append(f"🪙 +{fmt_num(bonus)} ({rp}% от баланса монет)")
            continue

        if rt == "percent_magic":
            bonus = int(state["magic_coins"] * rp / 100)
            state["magic_coins"] += bonus
            lines.append(f"🔯 +{fmt_num(bonus)} ({rp}% от баланса маг. монет)")
            continue

        if rt in state:
            state[rt] += rv
            lines.append(f"🎁 {_promo_reward_text(rt, rv)}")

    return state, lines


async def _broadcast_promo_and_pin(bot: Bot, promo_id: int, code: str, reward_line: str, uses: int, expires_ts: int):
    users = db.list_users_for_notify()
    text = (
        "📣 Новый промокод!\n"
        f"📋 Код: <code>промо {escape(code)}</code>\n"
        f"🎁 {escape(reward_line)}\n"
        f"👥 Использований: {uses}\n"
        f"⏱ Истекает: {_fmt_ts_msk(expires_ts, '%d.%m.%Y %H:%M')}"
    )
    for row in users:
        tg_id = int(row["tg_id"])
        if int(row["banned"] or 0):
            continue
        if not _notify_enabled(tg_id, NOTIFY_PROMO_KEY):
            continue
        try:
            try:
                await bot.pin_chat_message(tg_id, msg.message_id, disable_notification=True)
                db.add_promo_broadcast(promo_id, tg_id, msg.message_id)
            except Exception:
                pass
        except Exception:
            continue


def _promo_type_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, (key, label) in enumerate(PROMO_TYPES):
        row.append(InlineKeyboardButton(text=label, callback_data=f"promo_type:{key}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="✖ Отмена", callback_data="promo_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _promo_amount_kb(type_key: str) -> InlineKeyboardMarkup:
    amounts = [1, 5, 10, 25, 50, 100, 500, 1000, 5000, 10000]
    if type_key.startswith("percent"):
        amounts = [5, 10, 15, 20, 25, 30, 40, 50]
    rows = []
    row = []
    for a in amounts:
        row.append(InlineKeyboardButton(text=str(a), callback_data=f"promo_amount:{a}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="✍️ Свое значение", callback_data="promo_amount_custom")])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="promo_back_type")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _promo_uses_kb(type_key: str, amount: int) -> InlineKeyboardMarkup:
    uses = [1, 5, 10, 25, 50, 100, 999]
    rows = []
    row = []
    for u in uses:
        row.append(InlineKeyboardButton(text=str(u), callback_data=f"promo_uses:{u}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="promo_back_amount")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _promo_finalize_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить еще награду", callback_data="promo_more")],
        [InlineKeyboardButton(text="➡️ К использованию", callback_data="promo_next_uses")],
        [InlineKeyboardButton(text="✖ Отмена", callback_data="promo_cancel")],
    ])


def _promo_duration_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for secs, label in PROMO_DURATIONS:
        row.append(InlineKeyboardButton(text=label, callback_data=f"promo_dur:{secs}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🏷 Свой код", callback_data="promo_set_code")])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="promo_back_uses")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────────────────────────
#  АДМИН ПАНЕЛЬ
# ─────────────────────────────────────────────
ADM_ACTIONS = [
    ("coins", "Монеты"),
    ("essence", "Эссенция"),
    ("guild_unity", "🛡 ОЕ клана"),
    ("cases", "Кейсы"),
    ("rank_idx", "Ранг"),
    ("vip_lvl", "VIP"),
    ("rebirth_count", "Ребёрты"),
    ("arena", "Арена"),
    ("power", "Мощность"),
    ("hp_boost", "HP (итог)"),
    ("admin_role", "Админ роль"),
    ("ring_level", "Кольцо"),
    ("aura", "Аура"),
    ("grant_weapon", "Оружие ID"),
    ("grant_pet", "Питомец ID"),
    ("delete_guild", "🗑 Удалить гильдию"),
    ("update_all", "🆕 Обновление"),
    ("jinn_on", "🧞 Джинн ВКЛ 1ч"),
    ("jinn_off", "🌑 Джинн ВЫКЛ"),
    ("reg_label", "Дата рег."),
    ("profile_title", "Титул"),
    ("profile_note", "Текст профиля"),
    ("ban", "Бан"),
    ("unban", "Разбан"),
    ("mute", "Мут"),
    ("unmute", "Размут"),
    ("reset", "🔄 Полный сброс"),
]


def _admin_kb() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for key, label in ADM_ACTIONS:
        row.append(InlineKeyboardButton(text=label, callback_data=f"adm_act:{key}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton(text="✅ Подтвердить", callback_data="adm_confirm"),
        InlineKeyboardButton(text="🔄 Сброс", callback_data="adm_reset"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _admin_view_text(ctx: dict) -> str:
    action = ctx.get("action", "-")
    value = ctx.get("value", "-")
    target = ctx.get("target_id", "-")
    hint = {
        "coins": "Формат: значение id (пример: 50000 123456)",
        "essence": "Формат: значение id",
        "guild_unity": "Формат: значение id_клана (пример: 5000 77, можно отрицательное)",
        "cases": "Формат: коN=кол-во id / кпN=кол-во id / об=кол-во id",
        "rank_idx": "Формат: значение id (ранг 0-8)",
        "vip_lvl": "Формат: значение id (VIP 0-6)",
        "rebirth_count": "Формат: значение id",
        "arena": "Формат: значение id (арена 1-15)",
        "power": "Формат: значение id (мощность >= 0)",
        "hp_boost": "Формат: значение id",
        "admin_role": "Формат: значение id (роль 0-5)",
        "ring_level": "Формат: значение id (кольцо 0-5)",
        "aura": "Формат: aura_key id (regen/fortune/master/hunter/wrath/clear)",
        "grant_weapon": "Формат: admin_id[:custom_id][*qty] id (список: id.txt или /item_ids)",
        "grant_pet": "Формат: admin_id[:custom_id][*qty] id (список: id.txt или /item_ids)",
        "delete_guild": "Формат: id_гильдии. Нажми Подтвердить для удаления гильдии целиком.",
        "update_all": "Нажми Подтвердить: всем игрокам арена=1, монеты=0, прогресс боссов/квестов очищается.",
        "profile_title": "Введи титул профиля (отдельно от заметки)",
        "profile_note": "Введи текст заметки профиля",
        "ban": "Введи причину (или '-'). Затем подтверди.",
        "unban": "Подтверди разбан.",
        "mute": "Формат: время id (пример: 10m 123456)",
        "unmute": "Подтверди размут.",
        "jinn_on": "Нажми Подтвердить — Джинн откроется на 1 час.",
        "jinn_off": "Нажми Подтвердить — Джинн закроется.",
        "reg_label": "Формат: олд id или clear id (можно и свой текст: value id)",
        "reset": "⚠️ ПОЛНЫЙ СБРОС. Нажми Подтвердить для сброса.",
    }.get(action, "Выбери действие и цель.")
    action_label = dict(ADM_ACTIONS).get(action, action)
    return (
        f"⚙️ Админ панель\n"
        f"Действие: {action_label if action != '-' else '-'}\n"
        f"Ввод: {value}\n"
        f"Цель ID: {target}\n\n"
        f"💡 {hint}"
    )


# ─────────────────────────────────────────────
#  РОУТЕР И ПРОВЕРКИ ДОСТУПА
# ─────────────────────────────────────────────
router = Router()


async def _check_access(message: Message) -> tuple[bool, object]:
    _monitor_track_message(message)
    tg_id = message.from_user.id
    if int(tg_id) in IGNORED_TG_IDS:
        return False, None
    username = message.from_user.username or str(tg_id)
    db.create_user(tg_id, username)
    db.touch_user_activity(tg_id)
    u = db.get_user(tg_id)
    if u is None:
        return False, None
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _sync_vip_donate_items_for_user, tg_id, int(_row_get(u, "arena", 1) or 1))
    u = db.get_user(tg_id) or u
    u = _sync_rebirth_mult_for_user(u)

    # ============ ПРОВЕРКА ТЕХРАБОТ ============
    text = (message.text or "").strip().lower()
    
    cmd_detect = {
        "профиль": "профиль", "проф": "профиль",
        "баланс": "баланс", "б": "баланс",
        "боссы": "боссы", "boss": "боссы",
        "данж": "данж", "dungeon": "данж",
        "тренировка": "тренировка", "трен": "тренировка",
        "кейсы": "кейсы", "к": "кейсы",
        "инвентарь": "инвентарь", "инв": "инвентарь",
        "синтез": "синтез", "син": "синтез",
        "крафт": "крафт",
        "экипировка": "экипировка", "экип": "экипировка",
        "арена": "арена",
        "ребёрты": "ребёрты", "реберт": "ребёрты",
        "гильдия": "гильдия", "guild": "гильдия",
        "казино": "казино", "casino": "казино", "каз": "казино",
        "топ": "топ",
        "донат": "донат", "donate": "донат",
        "ивент": "ивент", "event": "ивент",
        "лавка": "лавка", "джинн": "лавка",
        "бонусы": "бонусы",
        "улучшения": "улучшения", "улуч": "улучшения", "апы": "улучшения",
        "артефакты": "артефакты", "арт": "артефакты",
        "депозит": "депозит", "деп": "депозит",
        "миры": "миры",
        "настройки": "настройки", "settings": "настройки",
        "рефералы": "рефералы", "реф": "рефералы", "рефка": "рефералы",
        "ежедневный бонус": "ежедневный", "ежедневка": "ежедневный", "еб": "ежедневный", "еж": "ежедневный", "бонус": "ежедневный",
        "стол зачарований": "стол зачарований", "стол": "стол зачарований",
        "/coin": "мини-игры", "/dice": "мини-игры", "/ladder": "мини-игры",
        "/safe": "мини-игры", "/mine": "мини-игры", "/rr": "мини-игры",
        "дружеский бой": "дуэль", "бой": "дуэль",
        "/con": "конкурс", "/fc": "конкурс", "/fk": "конкурс",
    }
    
    command_name = None
    for key, name in cmd_detect.items():
        if text == key or text.startswith(key + " "):
            command_name = name
            break
    
    # Если команда на техработе и пользователь не создатель
    if command_name and TECH_COMMANDS.get(command_name, False):
        if not _is_creator(u):
            await message.answer(
                f"🔧 Команда «{command_name}» находится на технических работах.\n"
                f"Пожалуйста, попробуйте позже."
            )
            return False, u
    # ============================================

    auto_daily_reward = _vip_auto_daily_claim(u)
    if auto_daily_reward > 0:
        u = db.get_user(tg_id) or u
        if message.chat.type == ChatType.PRIVATE:
            try:
                await message.answer(f"🎁 VIP-автосбор: +{fmt_num(auto_daily_reward)} 🪙")
            except Exception:
                pass

    if _vip_weekly_bag_claim(u):
        if message.chat.type == ChatType.PRIVATE:
            try:
                await message.answer("👜 Еженедельный VIP-бонус: +1 сумка артефактов")
            except Exception:
                pass

    is_su = tg_id in SUPER_ADMINS
    if int(u["banned"] or 0) and not is_su:
        await message.answer("🚫 Ты забанен в боте.")
        return False, u
    muted = int(u["muted_until"] or 0)
    if muted > time.time() and not is_su:
        mins = int((muted - time.time()) / 60)
        if message.chat.type == ChatType.PRIVATE:
            await message.answer(f"🔇 Ты замучен ещё на {mins} мин.")
        return False, u

    # Просим обязательную установку ника для всех новых профилей.
    txt = (message.text or "").strip().lower()
    allow_without_nick = txt.startswith("/start") or txt == "сменить ник"
    current_nick = str(_row_get(u, "nickname", "") or "").strip()
    nick_missing = not current_nick
    nick_invalid = current_nick and not _NICK_RE.match(current_nick)
    if (nick_missing or nick_invalid) and not allow_without_nick:
        NICK_PENDING.add(tg_id)
        if nick_invalid:
            notice = (
                f"✏️ Ник «{escape(current_nick)}» не соответствует правилам.\n"
                "Допустимы только рус/англ буквы, цифры и _ (3–16 символов, без пробелов).\n"
                "Напиши мне в ЛС чтобы сменить ник."
            )
        else:
            notice = "✏️ Сначала установи ник в боте. Напиши мне в ЛС."
        if message.chat.type == ChatType.PRIVATE:
            if nick_invalid:
                notice = (
                    f"✏️ Твой ник «{escape(current_nick)}» не соответствует правилам.\n"
                    "Допустимы только рус/англ буквы, цифры и _ (3–16 символов, без пробелов).\n"
                    "Введи новый ник:"
                )
            else:
                notice = "✏️ Сначала установи ник в боте.\nВведи новый ник (3–16 символов):"
        try:
            await message.answer(notice, parse_mode=ParseMode.HTML)
        except Exception:
            pass
        return False, u
    return True, u


async def _check_cb_access(cb: CallbackQuery) -> tuple[bool, object]:
    _monitor_track_callback(cb)
    tg_id = cb.from_user.id
    if int(tg_id) in IGNORED_TG_IDS:
        return False, None
    username = cb.from_user.username or str(tg_id)
    db.create_user(tg_id, username)
    db.touch_user_activity(tg_id)
    u = db.get_user(tg_id)
    if u is None:
        await cb.answer("Профиль не найден.", show_alert=True)
        return False, None

    if cb.message and not _cb_owner_check_skipped(str(cb.data or "")):
        owner_id = _get_cb_owner(cb.message.chat.id, cb.message.message_id)
        if owner_id > 0 and owner_id != int(tg_id) and int(tg_id) not in SUPER_ADMINS:
            await cb.answer("Это кнопки другого игрока.", show_alert=True)
            return False, u
        if owner_id <= 0:
            _set_cb_owner(cb.message.chat.id, cb.message.message_id, int(tg_id))

    # Тяжёлые синхронные операции запускаем в executor чтобы не блокировать event loop
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _sync_vip_donate_items_for_user, tg_id, int(_row_get(u, "arena", 1) or 1))
    u = db.get_user(tg_id) or u
    u = _sync_rebirth_mult_for_user(u)
    if _vip_auto_daily_claim(u) > 0:
        u = db.get_user(tg_id) or u
    _vip_weekly_bag_claim(u)
    if int(u["banned"] or 0) and tg_id not in SUPER_ADMINS:
        await cb.answer("🚫 Ты забанен.", show_alert=True)
        return False, u

    return True, u

@router.message(CommandStart())
async def cmd_start(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type == ChatType.PRIVATE:
        referrer_id = _extract_start_referrer(message.text or "")
        if referrer_id > 0:
            created, reason = db.bind_referral(referrer_id, int(u["tg_id"]))
            if created:
                await message.answer(
                    f"🤝 Реферальная связь сохранена. Награда будет, когда ты дойдешь до арены {REF_MIN_ARENA_FOR_REWARD}."
                )
                if bot_instance is not None:
                    try:
                        await bot_instance.send_message(referrer_id, f"🎉 Новый приглашенный: id {u['tg_id']}")
                    except Exception:
                        pass
            elif reason == "self":
                await message.answer("❌ Нельзя пригласить самого себя.")

    await _maybe_complete_referral(int(u["tg_id"]))
    nick = _display_name(u)
    if message.chat.type == ChatType.PRIVATE:
        await message.answer(
            f"⚔️ Добро пожаловать в Risen Solo, {nick}!\n\nИспользуй кнопки ниже или команды.",
            reply_markup=main_kb(),
        )
    else:
        if message.chat.id not in GROUP_CLEANED:
            GROUP_CLEANED.add(message.chat.id)
            await message.answer("⚔️ Risen Solo активен в этом чате.", reply_markup=remove_kb())
        else:
            await message.answer("⚔️ Risen Solo активен.")


@router.message(F.text.lower().in_({"меню", "💾 меню"}))
async def cmd_menu(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    await _switch_keyboard(message, menu_kb())


@router.message(F.text.lower() == "🎒 снаряжение")
async def cmd_menu_gear(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    await _switch_keyboard(message, gear_kb())


@router.message(F.text.lower() == "⚔️ приключения")
async def cmd_menu_adventures(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    await _switch_keyboard(message, adventures_kb())


@router.message(F.text.lower().in_({"🛟 floodwait", "floodwait", "флудвейт"}))
async def cmd_floodwait_panel(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    bs = ACTIVE_BATTLES.get(tg_id) or _restore_battle_from_db_row(db.get_active_battle(tg_id))
    if bs:
        await message.answer("🛟 Floodwait-режим: бой", reply_markup=fw_battle_kb())
        return
    ds = ACTIVE_DUNGEONS.get(tg_id) or _restore_dungeon_from_db_row(db.get_active_dungeon(tg_id))
    if ds:
        await message.answer("🛟 Floodwait-режим: данж", reply_markup=fw_dungeon_kb())
        return
    await message.answer("ℹ️ Нет активного боя или данжа.", reply_markup=adventures_kb())


@router.message(F.text.lower() == "⚙️ дополнительно")
async def cmd_menu_extras(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    await _switch_keyboard(message, extras_kb())


@router.message(Command("upgrades"))
@router.message(F.text.lower().in_({"🧪 улучшения", "улучшения", "улуч", "апы"}))
async def cmd_train_upgrades(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("🧪 Улучшения тренировки доступны только в ЛС.")
        return
    await message.answer(_train_upgrades_text(u), reply_markup=_train_upgrades_kb(u))


@router.message(F.text.lower().regexp(r"^\s*(ул|ап|улуч)\s+(шк|ст|вт)\s*$"))
async def cmd_train_upgrade_alias_buy(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    m = re.match(r"^\s*(ул|ап|улуч)\s+(шк|ст|вт)\s*$", str(message.text or "").strip().lower())
    if not m:
        return
    alias = m.group(2)
    kind = {"шк": "case", "ст": "power", "вт": "time"}[alias]
    ok2, text = _buy_train_upgrade(int(u["tg_id"]), kind)
    await message.answer(text, parse_mode=ParseMode.HTML)
    if ok2 and message.chat.type == ChatType.PRIVATE:
        u2 = db.get_user(int(u["tg_id"])) or u
        await message.answer(_train_upgrades_text(u2), reply_markup=_train_upgrades_kb(u2))


@router.message(F.text.lower().in_({"шк", "ст", "вт"}))
async def cmd_train_upgrade_alias_hint(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("🧪 Открой ЛС: там доступны улучшения тренировки.")
        return
    await message.answer(_train_upgrades_text(u), reply_markup=_train_upgrades_kb(u))


@router.message(Command("autosynth"))
async def cmd_autosynth(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip < 2:
        await message.answer("❌ Авто-синтез доступен с VIP 2.")
        return
    parts = (message.text or "").strip().split()
    if len(parts) <= 1:
        disabled = int(db.get_stat(int(u["tg_id"]), VIP_AUTOSYNTH_DISABLED_KEY, 0) or 0)
        interval = _autosynth_interval_secs(u)
        await message.answer(
            "🌀 Авто-синтез\n"
            f"Статус: {'выключен' if disabled else 'включен'}\n"
            f"Интервал: {_fmt_uptime(interval)}\n"
            "Команды: /autosynth on | /autosynth off"
        )
        return
    mode = parts[1].strip().lower()
    if mode in ("on", "вкл", "enable"):
        db.set_stat_value(int(u["tg_id"]), VIP_AUTOSYNTH_DISABLED_KEY, 0)
        await message.answer("✅ Авто-синтез включен.")
        return
    if mode in ("off", "выкл", "disable"):
        db.set_stat_value(int(u["tg_id"]), VIP_AUTOSYNTH_DISABLED_KEY, 1)
        await message.answer("✅ Авто-синтез выключен.")
        return
    await message.answer("Использование: /autosynth on|off")


@router.message(Command("autosell"))
async def cmd_autosell(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip < 3:
        await message.answer("❌ Авто-продажа доступна с VIP 3.")
        return
    parts = (message.text or "").strip().split()
    if len(parts) <= 1:
        cur = int(db.get_stat(int(u["tg_id"]), VIP_AUTOSELL_THRESHOLD_KEY, 0) or 0)
        await message.answer(
            "♻️ Авто-продажа\n"
            f"Порог бонуса: {fmt_num(cur)}\n"
            "Команды: /autosell off | /autosell [порог]"
        )
        return
    token = parts[1].strip().lower()
    if token in ("off", "выкл", "0"):
        db.set_stat_value(int(u["tg_id"]), VIP_AUTOSELL_THRESHOLD_KEY, 0)
        await message.answer("✅ Авто-продажа выключена.")
        return
    if not token.isdigit():
        await message.answer("❌ Укажи число порога. Пример: /autosell 15000")
        return
    val = max(0, int(token))
    db.set_stat_value(int(u["tg_id"]), VIP_AUTOSELL_THRESHOLD_KEY, val)
    await message.answer(f"✅ Порог авто-продажи установлен: {fmt_num(val)}")


@router.message(F.text.lower() == "◀ в меню")
async def cmd_back_to_menu(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    await _switch_keyboard(message, menu_kb())


@router.message(F.text.lower() == "назад")
async def cmd_back_to_main(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    await _switch_keyboard(message, main_kb())


@router.message(Command("bonuses"))
@router.message(F.text.lower().in_({"бонусы", "🎁 бонусы"}))
async def cmd_bonuses(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("🎁 Раздел бонусов доступен только в ЛС.")
        return
    await message.answer(_bonuses_text(), reply_markup=_bonuses_kb())


@router.message(Command("ref"))
@router.message(F.text.lower().in_({"реф", "рефка", "рефералы", "реферал"}))
async def cmd_ref(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("🤝 Рефералы доступны только в ЛС с ботом.")
        return
    pending = db.list_pending_referrals(int(u["tg_id"]), limit=10)
    me = await message.bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{u['tg_id']}" if me and me.username else "(не удалось получить username бота)"
    text = _ref_text(u, pending)
    await message.answer(f"{text}\n\n🔗 Твоя ссылка:\n<code>{escape(link)}</code>", parse_mode=ParseMode.HTML, reply_markup=_ref_kb(u, len(pending)))


@router.message(Command("id"))
async def cmd_my_id(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("🆔 Команда доступна только в ЛС с ботом.")
        return
    await message.answer(f"🆔 Твой Telegram ID: <code>{message.from_user.id}</code>", parse_mode=ParseMode.HTML)


@router.message(F.text.lower().regexp(r"^\s*/?(покакать|какать)\s*$"))
async def cmd_poop_coin(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return

    tg_id = int(u["tg_id"])
    # Команда доступна всем админ-уровням и отдельному пользователю.
    if not (_is_admin(u) or tg_id == POOP_PRIVILEGED_TG_ID):
        await message.answer("❌ Команда недоступна.")
        return

    coins = int(u["coins"] or 0) + 1
    db.update_user(tg_id, coins=coins)
    await message.answer("💩Ты нашел в какашке монету\n+1🪙")


def _nick_copy_kb(nick: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Скопировать", copy_text=CopyTextButton(text=nick))],
    ])


@router.message(Command("nick"))
@router.message(F.text.lower().in_({"н", "ник"}))
async def cmd_nick_show(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return

    target_id = int(u["tg_id"])
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = int(message.reply_to_message.from_user.id)
        target_username = message.reply_to_message.from_user.username or str(target_id)
        db.create_user(target_id, target_username)

    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Профиль не найден.")
        return

    nick = _display_name(tu)
    await message.answer(
        f"🏷 Ник: <code>{escape(nick)}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=_nick_copy_kb(nick),
    )


@router.callback_query(F.data.startswith("nickcopy:"))
async def cb_nick_copy(cb: CallbackQuery):
    # Совместимость со старыми сообщениями
    await cb.answer()


@router.callback_query(F.data == "bonuses:daily")
async def cb_bonuses_daily(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    text = _daily_claim_text(u)
    await _safe_edit_cb(cb, text, reply_markup=_bonuses_kb())
    await cb.answer("🎁 Обновлено")


@router.callback_query(F.data == "bonuses:promos")
async def cb_bonuses_promos(cb: CallbackQuery):
    ok, _u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _active_promos_text(), reply_markup=_bonuses_kb())
    await cb.answer()


@router.callback_query(F.data == "bonuses:home")
async def cb_bonuses_home(cb: CallbackQuery):
    ok, _u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _bonuses_text(), reply_markup=_bonuses_kb())
    await cb.answer()


@router.callback_query(F.data == "bonuses:tasks")
async def cb_bonuses_tasks(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _bonus_tasks_text(u), reply_markup=_bonus_tasks_kb(u))
    await cb.answer()


@router.callback_query(F.data == "bonuses:task_claim_vpn")
async def cb_bonuses_task_claim_vpn(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    if int(db.get_stat(tg_id, VPN_TASK_STAT_KEY, 0) or 0) > 0:
        await cb.answer("Награда уже получена.", show_alert=True)
        return
    db.add_stat(tg_id, VPN_TASK_STAT_KEY, 1)
    db.update_user(tg_id, magic_coins=int(_row_get(u, "magic_coins", 0) or 0) + int(VPN_TASK_REWARD_MAGIC))
    u2 = db.get_user(tg_id) or u
    await _safe_edit_cb(cb, _bonus_tasks_text(u2), reply_markup=_bonus_tasks_kb(u2))
    await cb.answer(f"+{VPN_TASK_REWARD_MAGIC} 🔯 зачислено", show_alert=True)


# ─────────────────────────────────────────────
#  ДРУЖЕСКИЙ БОЙ (только чаты)
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
#  ЛАВКА ДЖИННА
# ─────────────────────────────────────────────
@router.message(Command("jinn"))
@router.message(F.text.lower().in_({"лавка", "джинн", "🛒 лавка джинна"}))
async def cmd_jinn(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _jinn_open_for_user(u):
        await message.answer(
            f"🧞 Лавка Джинна закрыта.\n"
            f"Откроется сегодня на 1 час (точное время скрыто).\n"
            f"📣 За 3 минуты придёт уведомление."
        )
        return
    magic = int(u["magic_coins"] or 0)
    text = f"🧞 Лавка Джинна\n{SEP}\n🔯 Маг. монет: {fmt_num(magic)}\nВыбери ауру:"
    await message.answer(text, reply_markup=_jinn_kb(u))


@router.callback_query(F.data.startswith("jinn_buy:"))
async def cb_jinn_buy(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _jinn_open_for_user(u):
        await cb.answer("🧞 Лавка закрыта.", show_alert=True)
        return
    aura_key = cb.data.split(":")[1]
    if aura_key not in gd.AURA_CATALOG:
        await cb.answer("❌ Аура не найдена.", show_alert=True)
        return
    data = gd.AURA_CATALOG[aura_key]
    tg_id = int(u["tg_id"])
    if int(_row_get(u, f"aura_{aura_key}", 0) or 0):
        await cb.answer("✅ Аура уже куплена.", show_alert=True)
        return
    magic = int(u["magic_coins"] or 0)
    if magic < data["cost"]:
        await cb.answer(f"❌ Нужно {data['cost']} 🔯, у тебя {magic}.", show_alert=True)
        return
    db.update_user(tg_id, **{
        f"aura_{aura_key}": 1,
        "magic_coins": magic - data["cost"],
    })
    u2 = db.get_user(tg_id)
    magic2 = int(u2["magic_coins"] or 0)
    await _safe_edit_cb(cb,
                        f"🧞 Лавка Джинна\n{SEP}\n🔯 Маг. монет: {fmt_num(magic2)}\nВыбери ауру:",
                        reply_markup=_jinn_kb(u2),
                        )
    await cb.answer(f"✅ Куплена {data['name']}!", show_alert=True)


@router.callback_query(F.data == "jinn_close")
async def cb_jinn_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "🧞 Лавка закрыта.")
    await cb.answer()


# ─────────────────────────────────────────────
#  ЗАЧАРОВАНИЯ — ЛАВКА ДЖИНА
# ─────────────────────────────────────────────

def _jinn_enchant_kb(tg_id: int) -> InlineKeyboardMarkup:
    pool = _get_enchant_shop_pool(tg_id)
    rows = []
    for i, (key, lvl) in enumerate(pool):
        data = gd.ENCHANT_CATALOG.get(key)
        if not data:
            continue
        cost = data["levels"][lvl - 1]["cost"]
        label = f"{data['emoji']} {data['name']} Ур.{lvl} — {cost} 🔯"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"jinn_enchant_buy:{i}")])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="jinn_enchant_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _jinn_enchant_text(tg_id: int, magic: int) -> str:
    pool = _get_enchant_shop_pool(tg_id)
    lines = [
        "📖 Зачарования — Лавка Джинна",
        SEP,
        f"🔯 Маг. монет: {fmt_num(magic)}",
        "",
        "Сегодня доступны:",
    ]
    for key, lvl in pool:
        data = gd.ENCHANT_CATALOG.get(key)
        if not data:
            continue
        cost = data["levels"][lvl - 1]["cost"]
        pct = data["levels"][lvl - 1]["bonus_pct"]
        target_label = "🗡 оружие" if data["target"] == "weapon" else "🐾 питомец"
        lines.append(f"• {data['emoji']} {data['name']} Ур.{lvl} | +{pct}% | {target_label} | {cost} 🔯")
    lines += [
        "",
        "После покупки применяй зачарование командой:",
        "/enchant [id предмета] [id зачарования]",
        "",
        "Купленные зачарования хранятся в /enchant_list",
    ]
    return "\n".join(lines)


@router.callback_query(F.data == "jinn_enchants")
async def cb_jinn_enchants(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _jinn_open_for_user(u):
        await cb.answer("🧞 Лавка закрыта.", show_alert=True)
        return
    tg_id = int(u["tg_id"])
    magic = int(u["magic_coins"] or 0)
    await _safe_edit_cb(cb, _jinn_enchant_text(tg_id, magic), reply_markup=_jinn_enchant_kb(tg_id))
    await cb.answer()


@router.callback_query(F.data.startswith("jinn_enchant_buy:"))
async def cb_jinn_enchant_buy(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _jinn_open_for_user(u):
        await cb.answer("🧞 Лавка закрыта.", show_alert=True)
        return
    tg_id = int(u["tg_id"])
    try:
        slot_idx = int(cb.data.split(":")[1])
    except Exception:
        await cb.answer("❌ Ошибка.", show_alert=True)
        return

    pool = _get_enchant_shop_pool(tg_id)
    if slot_idx < 0 or slot_idx >= len(pool):
        await cb.answer("❌ Зачарование не найдено.", show_alert=True)
        return

    key, lvl = pool[slot_idx]
    data = gd.ENCHANT_CATALOG.get(key)
    if not data:
        await cb.answer("❌ Зачарование не найдено.", show_alert=True)
        return

    cost = data["levels"][lvl - 1]["cost"]
    magic = int(u["magic_coins"] or 0)
    if magic < cost:
        await cb.answer(f"❌ Нужно {cost} 🔯, у тебя {magic}.", show_alert=True)
        return

    # Сохраняем купленное зачарование как stat: enchant:owned:{key}:{lvl} += 1
    stat_key = f"enchant:owned:{key}:{lvl}"
    cur = int(db.get_stat(tg_id, stat_key, 0) or 0)
    db.set_stat_value(tg_id, stat_key, cur + 1)
    db.update_user(tg_id, magic_coins=magic - cost)

    u2 = db.get_user(tg_id)
    magic2 = int(u2["magic_coins"] or 0)
    await _safe_edit_cb(cb, _jinn_enchant_text(tg_id, magic2), reply_markup=_jinn_enchant_kb(tg_id))
    await cb.answer(f"✅ Куплено: {data['emoji']} {data['name']} Ур.{lvl}!", show_alert=True)


@router.callback_query(F.data == "jinn_enchant_back")
async def cb_jinn_enchant_back(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _jinn_open_for_user(u):
        await cb.answer("🧞 Лавка закрыта.", show_alert=True)
        return
    magic = int(u["magic_coins"] or 0)
    text = f"🧞 Лавка Джинна\n{SEP}\n🔯 Маг. монет: {fmt_num(magic)}\nВыбери ауру:"
    await _safe_edit_cb(cb, text, reply_markup=_jinn_kb(u))
    await cb.answer()


# ─────────────────────────────────────────────
#  ЗАЧАРОВАНИЯ — ПРИМЕНЕНИЕ (/enchant)
# ─────────────────────────────────────────────

def _owned_enchants_text(tg_id: int) -> str:
    lines = ["📖 Твои зачарования:", SEP]
    found = False
    for key, data in gd.ENCHANT_CATALOG.items():
        for lvl in range(1, 4):
            stat_key = f"enchant:owned:{key}:{lvl}"
            cnt = int(db.get_stat(tg_id, stat_key, 0) or 0)
            if cnt > 0:
                found = True
                target_label = "🗡 оружие" if data["target"] == "weapon" else "🐾 питомец"
                lines.append(f"• ID: {key}:{lvl} | {data['emoji']} {data['name']} Ур.{lvl} | {target_label} | x{cnt}")
    if not found:
        lines.append("Зачарований нет. Купи их в лавке Джинна.")
    lines += [
        "",
        "Применить: /enchant [id_предмета] [ключ:уровень]",
        "Пример:    /enchant 42 power:2",
    ]
    return "\n".join(lines)


@router.message(Command("enchant_list"))
@router.message(F.text.lower() == "мои зачарования")
async def cmd_enchant_list(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    await message.answer(_owned_enchants_text(tg_id))


@router.message(Command("enchant"))
async def cmd_enchant(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])

    args = (message.text or "").split()
    # /enchant без аргументов — показать список + инструкцию
    if len(args) < 3:
        await message.answer(_owned_enchants_text(tg_id))
        return

    # /enchant [item_id] [enchant_key:level]
    try:
        item_id = int(args[1])
    except ValueError:
        await message.answer("❌ Неверный ID предмета. Пример: /enchant 42 power:2")
        return

    enchant_arg = args[2].strip().lower()
    parts = enchant_arg.split(":")
    if len(parts) != 2:
        await message.answer("❌ Формат: ключ:уровень, например power:2")
        return

    enchant_key = parts[0]
    try:
        enchant_lvl = int(parts[1])
    except ValueError:
        await message.answer("❌ Уровень зачарования от 1 до 3.")
        return

    if enchant_key not in gd.ENCHANT_CATALOG:
        known = ", ".join(gd.ENCHANT_CATALOG.keys())
        await message.answer(f"❌ Неизвестное зачарование. Доступные: {known}")
        return

    if enchant_lvl < 1 or enchant_lvl > 3:
        await message.answer("❌ Уровень зачарования от 1 до 3.")
        return

    # Проверяем предмет
    it = db.get_inventory_item(tg_id, item_id)
    if not it:
        await message.answer("❌ Предмет с таким ID не найден в твоём инвентаре.")
        return

    item_type = str(_row_get(it, "type", "") or "")
    if item_type not in ("weapon", "pet"):
        await message.answer("❌ Зачарования применяются только на оружие и питомцев.")
        return

    # Запрет на VIP-предметы
    if _is_vip_donate_item_name(str(_row_get(it, "name", "") or "")):
        await message.answer("❌ На VIP-экипировку нельзя накладывать зачарования.")
        return

    enchant_data = gd.ENCHANT_CATALOG[enchant_key]
    expected_target = enchant_data["target"]
    if item_type != expected_target:
        target_ru = "оружие" if expected_target == "weapon" else "питомца"
        await message.answer(
            f"❌ {enchant_data['emoji']} {enchant_data['name']} применяется только на {target_ru}."
        )
        return

    # Одно зачарование на предмет — проверяем любое существующее
    existing_enchants = db.get_item_enchants(item_id)
    if existing_enchants:
        existing_key = list(existing_enchants.keys())[0]
        existing_lvl = existing_enchants[existing_key]
        ex_data = gd.ENCHANT_CATALOG.get(existing_key, {})
        ex_name = ex_data.get("name", existing_key) if ex_data else existing_key
        ex_emoji = ex_data.get("emoji", "📖") if ex_data else "📖"
        await message.answer(
            f"❌ На этом предмете уже есть зачарование: {ex_emoji} {ex_name} Ур.{existing_lvl}.\n"
            "Снять зачарование нельзя — одному предмету одно зачарование."
        )
        return

    # Проверяем, есть ли зачарование у игрока в инвентаре
    stat_key = f"enchant:owned:{enchant_key}:{enchant_lvl}"
    owned = int(db.get_stat(tg_id, stat_key, 0) or 0)
    if owned <= 0:
        await message.answer(
            f"❌ У тебя нет {enchant_data['emoji']} {enchant_data['name']} Ур.{enchant_lvl}.\n"
            "Купи в лавке Джинна → раздел 📖 Зачарования."
        )
        return

    # Применяем зачарование
    db.set_stat_value(tg_id, stat_key, owned - 1)
    db.set_item_enchant(tg_id, item_id, enchant_key, enchant_lvl)
    pct = enchant_data["levels"][enchant_lvl - 1]["bonus_pct"]
    item_name = str(_row_get(it, "name", "") or "")
    await message.answer(
        f"✅ Зачарование применено!\n"
        f"{enchant_data['emoji']} {enchant_data['name']} Ур.{enchant_lvl} (+{pct}%)\n"
        f"Предмет: {item_name} (ID {item_id})\n\n"
        f"{_enchant_item_label(item_id)}"
    )


# ─────────────────────────────────────────────
#  ПРАВИЛА


# ─────────────────────────────────────────────
#  НАСТРОЙКИ
# ─────────────────────────────────────────────

def _notify_enabled(tg_id: int, key: str) -> bool:
    """Уведомление включено по умолчанию (1). 0 = отключено."""
    val = db.get_stat(tg_id, key, 1)
    return int(val) != 0


def _set_notify(tg_id: int, key: str, enabled: bool):
    db.set_stat_value(tg_id, key, 1 if enabled else 0)


def _autosell_enabled(tg_id: int) -> bool:
    return int(db.get_stat(tg_id, VIP_AUTOSELL_THRESHOLD_KEY, 0) or 0) > 0


def _autosynth_enabled(tg_id: int) -> bool:
    return int(db.get_stat(tg_id, VIP_AUTOSYNTH_DISABLED_KEY, 0) or 0) == 0


def _profile_hidden(tg_id: int) -> bool:
    return int(db.get_stat(tg_id, PROFILE_HIDDEN_KEY, 0) or 0) == 1

def _profile_hide_unlocked(tg_id: int) -> bool:
    """Функция скрытия разблокирована (оплачена)."""
    return int(db.get_stat(tg_id, "settings:profile_hide_unlocked", 0) or 0) == 1


def _settings_text(u) -> str:
    tg_id = int(u["tg_id"])
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    essence = int(u["essence"] or 0)
    lines = ["⚙️ Настройки", SEP, "", "🔔 Уведомления:"]
    for key, label in NOTIFY_SETTINGS:
        icon = "✅" if _notify_enabled(tg_id, key) else "🔕"
        lines.append(f"  {icon} {label}")

    # Скрытие профиля
    lines += ["", "👤 Профиль:"]
    if _profile_hide_unlocked(tg_id):
        hidden = _profile_hidden(tg_id)
        icon = "🔒" if hidden else "🔓"
        lines.append(f"  {icon} Скрытый профиль: {'включён' if hidden else 'выключен'}")
    else:
        lines.append(f"  🔓 Скрыть профиль — {PROFILE_HIDDEN_COST} 💠 (разблокировать)")

    if vip >= 2:
        lines += ["", "🎖 Привилегии:"]
        if vip >= 3:
            threshold = int(db.get_stat(tg_id, VIP_AUTOSELL_THRESHOLD_KEY, 0) or 0)
            sell_icon = "✅" if threshold > 0 else "❌"
            sell_label = f"порог: +{fmt_num(threshold)}" if threshold > 0 else "выкл"
            lines.append(f"  {sell_icon} Авто-продажа ({sell_label})")
        synth_icon = "✅" if _autosynth_enabled(tg_id) else "❌"
        lines.append(f"  {synth_icon} Авто-синтез")

    lines += ["", f"💠 Эссенция: {fmt_num(essence)}", "", "Нажми кнопку ниже чтобы изменить."]
    return "\n".join(lines)


def _settings_kb(u) -> InlineKeyboardMarkup:
    tg_id = int(u["tg_id"])
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    rows = []

    rows.append([InlineKeyboardButton(text="── 🔔 Уведомления ──", callback_data="settings:noop")])
    for key, label in NOTIFY_SETTINGS:
        enabled = _notify_enabled(tg_id, key)
        icon = "✅" if enabled else "🔕"
        action = "off" if enabled else "on"
        rows.append([InlineKeyboardButton(
            text=f"{icon} {label}",
            callback_data=f"settings:notify:{key}:{action}",
        )])

    # Скрытие профиля
    rows.append([InlineKeyboardButton(text="── 👤 Профиль ──", callback_data="settings:noop")])
    if _profile_hide_unlocked(tg_id):
        hidden = _profile_hidden(tg_id)
        icon = "🔒" if hidden else "🔓"
        rows.append([InlineKeyboardButton(
            text=f"{icon} Скрыть профиль",
            callback_data="settings:profile_hide:toggle",
        )])
    else:
        rows.append([InlineKeyboardButton(
            text=f"🔓 Разблокировать скрытие профиля ({PROFILE_HIDDEN_COST} 💠)",
            callback_data="settings:profile_hide:unlock",
        )])

    if vip >= 2:
        rows.append([InlineKeyboardButton(text="── 🎖 Привилегии ──", callback_data="settings:noop")])
        if vip >= 3:
            threshold = int(db.get_stat(tg_id, VIP_AUTOSELL_THRESHOLD_KEY, 0) or 0)
            sell_on = threshold > 0
            sell_icon = "✅" if sell_on else "❌"
            rows.append([InlineKeyboardButton(
                text=f"{sell_icon} Авто-продажа",
                callback_data="settings:autosell:toggle",
            )])
        synth_on = _autosynth_enabled(tg_id)
        synth_icon = "✅" if synth_on else "❌"
        rows.append([InlineKeyboardButton(
            text=f"{synth_icon} Авто-синтез",
            callback_data="settings:autosynth:toggle",
        )])

    rows.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="settings:close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(Command("settings"))
@router.message(F.text.lower().in_({"⚙️ настройки", "настройки", "settings"}))
async def cmd_settings(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await message.answer(
        _settings_text(u),
        reply_markup=_settings_kb(u),
    )


@router.callback_query(F.data == "settings:noop")
async def cb_settings_noop(cb: CallbackQuery):
    await cb.answer()


@router.callback_query(F.data == "settings:close")
async def cb_settings_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "⚙️ Настройки закрыты.")
    await cb.answer()


@router.callback_query(F.data.startswith("settings:notify:"))
async def cb_settings_notify(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    # Формат: settings:notify:{key}:{on|off}
    # key сам содержит ':', поэтому берём action с конца, key — всё между
    raw = cb.data  # "settings:notify:notify:promo:off"
    prefix = "settings:notify:"
    rest = raw[len(prefix):]  # "notify:promo:off"
    # action — последний сегмент, key — всё до него
    last_colon = rest.rfind(":")
    if last_colon == -1:
        await cb.answer("❌ Ошибка.", show_alert=True)
        return
    key = rest[:last_colon]    # "notify:promo"
    action = rest[last_colon + 1:]  # "off"

    valid_keys = {k for k, _ in NOTIFY_SETTINGS}
    if key not in valid_keys:
        await cb.answer("❌ Неизвестная настройка.", show_alert=True)
        return

    enabled = action == "on"
    _set_notify(tg_id, key, enabled)
    label = next((lbl for k, lbl in NOTIFY_SETTINGS if k == key), key)
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _settings_text(u2), reply_markup=_settings_kb(u2))
    await cb.answer(f"{'✅ Включено' if enabled else '🔕 Выключено'}: {label}", show_alert=False)


@router.callback_query(F.data == "settings:autosell:toggle")
async def cb_settings_autosell(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip < 3:
        await cb.answer("❌ Авто-продажа доступна с VIP 3.", show_alert=True)
        return
    threshold = int(db.get_stat(tg_id, VIP_AUTOSELL_THRESHOLD_KEY, 0) or 0)
    if threshold > 0:
        # Выключаем
        db.set_stat_value(tg_id, VIP_AUTOSELL_THRESHOLD_KEY, 0)
        await cb.answer("❌ Авто-продажа выключена.", show_alert=False)
    else:
        # Включаем с дефолтным порогом (минимальный бонус арены)
        arena = int(_row_get(u, "arena", 1) or 1)
        default_threshold = max(1, _arena_max_weapon_bonus(arena) // 4)
        db.set_stat_value(tg_id, VIP_AUTOSELL_THRESHOLD_KEY, default_threshold)
        await cb.answer(
            f"✅ Авто-продажа включена (порог: +{fmt_num(default_threshold)}).\n"
            "Изменить порог: /autosell [число]",
            show_alert=True,
        )
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _settings_text(u2), reply_markup=_settings_kb(u2))


@router.callback_query(F.data == "settings:autosynth:toggle")
async def cb_settings_autosynth(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip < 2:
        await cb.answer("❌ Авто-синтез доступен с VIP 2.", show_alert=True)
        return
    currently_on = _autosynth_enabled(tg_id)
    db.set_stat_value(tg_id, VIP_AUTOSYNTH_DISABLED_KEY, 1 if currently_on else 0)
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _settings_text(u2), reply_markup=_settings_kb(u2))
    await cb.answer(
        f"{'❌ Авто-синтез выключен.' if currently_on else '✅ Авто-синтез включён.'}",
        show_alert=False,
    )


@router.callback_query(F.data == "settings:profile_hide:unlock")
async def cb_settings_profile_hide_unlock(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    if _profile_hide_unlocked(tg_id):
        await cb.answer("✅ Уже разблокировано.", show_alert=True)
        return
    essence = int(u["essence"] or 0)
    if essence < PROFILE_HIDDEN_COST:
        await cb.answer(
            f"❌ Нужно {PROFILE_HIDDEN_COST} 💠, у тебя {fmt_num(essence)}.",
            show_alert=True,
        )
        return
    db.update_user(tg_id, essence=essence - PROFILE_HIDDEN_COST)
    db.set_stat_value(tg_id, "settings:profile_hide_unlocked", 1)
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _settings_text(u2), reply_markup=_settings_kb(u2))
    await cb.answer("✅ Разблокировано! Теперь можешь скрыть профиль.", show_alert=True)


@router.callback_query(F.data == "settings:profile_hide:toggle")
async def cb_settings_profile_hide_toggle(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    if not _profile_hide_unlocked(tg_id):
        await cb.answer("❌ Сначала разблокируй функцию.", show_alert=True)
        return
    currently_hidden = _profile_hidden(tg_id)
    db.set_stat_value(tg_id, PROFILE_HIDDEN_KEY, 0 if currently_hidden else 1)
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _settings_text(u2), reply_markup=_settings_kb(u2))
    await cb.answer(
        "🔓 Профиль открыт." if currently_hidden else "🔒 Профиль скрыт.",
        show_alert=False,
    )


# ─────────────────────────────────────────────
#  СТАТА БОССОВ (VIP 3+)
# ─────────────────────────────────────────────

def _boss_stats_text(u) -> str:
    tg_id = int(u["tg_id"])
    arena_cur = int(u["arena"] or 1)
    total_kills = int(u["total_boss_kills"] or 0)
    lines = [
        "📊 Детальная стата боссов",
        SEP,
        f"Всего убийств: {fmt_num(total_kills)}",
        "",
    ]
    for arena in range(1, arena_cur + 1):
        bosses = gd.ARENAS.get(arena, [])
        if not bosses:
            continue
        arena_lines = []
        for idx, boss in enumerate(bosses):
            key = _stat_boss_key(arena, idx)
            kills = int(db.get_stat(tg_id, key, 0) or 0)
            if kills > 0:
                arena_lines.append(f"  {boss.name}: {fmt_num(kills)}")
        if arena_lines:
            lines.append(f"🏟 Арена {arena}:")
            lines.extend(arena_lines)
    if len(lines) <= 4:
        lines.append("Убийств пока нет.")
    return "\n".join(lines)


@router.message(Command("bossstat"))
@router.message(F.text.lower().in_({"стата", "📊 стата боссов"}))
async def cmd_boss_stat(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    vip = int(_row_get(u, "vip_lvl", 0) or 0)
    if vip < 3 and not _is_admin(u):
        await message.answer("❌ Детальная стата боссов доступна с VIP 3 (Мастер).")
        return
    await _reply(message, _boss_stats_text(u))
RULES_TEXT: dict[str, str] = {
    "1.1": (
        "1.1. Запрет на многозадачность (мультиаккаунтинг).\n"
        "Нельзя создавать больше двух дополнительных профилей для накрутки реферальных бонусов и тд."
    ),
    "1.2": (
        "1.2. Мошенничество и обман других игроков.\n"
        "Любые действия с целью хищения чужих ресурсов (валюты, предметов, аккаунтов) — например, ложные "
        "обещания обмена, поддельные скриншоты переводов или жульничество в игровых мини-играх — ведут к "
        "блокировке от временной до вечной.\n"
        "Администрация не гарантирует безопасность любых сделок между игроками. Отправленные по ошибке "
        "(неправильный ID) монеты, предметы или кланы возврату не подлежат — вы действуете на свой страх и риск.\n"
        "При техническом сбое (деньги списаны, а ресурс не пришел) администрация вручную зачислит его после "
        "предоставления чека. Ошибки самого пользователя (не туда перевел, не то купил) не компенсируются."
    ),
    "1.3": (
        "1.3. Вымогательство и принуждение.\n"
        "Запрещено угрожать игрокам киком из клана, накруткой жалоб или травлей, чтобы вынудить их отдать свои ресурсы."
    ),
    "1.4": (
        "1.4. Передача ценностей между аккаунтами.\n"
        "Администрация не гарантирует безопасность любых сделок между игроками. Отправленные по ошибке "
        "(неправильный ID) монеты, предметы или кланы возврату не подлежат — вы действуете на свой страх и риск."
    ),
    "1.5": (
        "1.5. Продажа игрового имущества за реальные деньги (RMT).\n"
        "Запрещены любые попытки продать или купить внутриигровые ресурсы (валюту, кейсы, кланы, аккаунты) "
        "за валюту других проектов или настоящие деньги вне официального донат-сервиса."
    ),
    "1.6": (
        "1.6. Передача лидерства в гильдии.\n"
        "Если лидер гильдии удаляет аккаунт, право управления может перейти самому активному заместителю "
        "по запросу в техподдержку."
    ),
    "1.7": (
        "1.7. Добровольность платежей и возвраты.\n"
        "Все покупки в боте — это пожертвования. Поскольку цифровой товар начисляется мгновенно, "
        "возврат средств после получения ресурсов невозможен."
    ),
    "1.8": (
        "1.8. Ошибочные списания.\n"
        "При техническом сбое (деньги списаны, а ресурс не пришел) администрация вручную зачислит ресурс "
        "после предоставления чека. Ошибки самого пользователя (не туда перевел, не то купил) не компенсируются."
    ),
    "1.9": (
        "1.9. Дополнительно\n"
        "Если в игре нет механик которые прописаны в правилах, это не значит что правила недействительны. "
        "Все правила вступают в силу 04.04.2026 и в течении времени могут как добавляться так и убавляться "
        "по усмотрению администрации."
    ),
    "2.1": (
        "2.1. Оскорбления и грубость.\n"
        "Нельзя унижать других участников, использовать ненормативную лексику или разжигать ненависть по любым признакам."
    ),
    "2.2": (
        "2.2. Нежелательный контент.\n"
        "Запрещены порнография, шокирующие изображения, а также распространение личных данных других людей "
        "(доксинг) — за это сразу перманентный бан."
    ),
    "2.3": (
        "2.3. Реклама и флуд.\n"
        "Нельзя рекламировать сторонние проекты, ссылаться на другие боты или засорять чат однотипными "
        "сообщениями, не связанными с игрой.\n"
        "(Исключение MineEVO и Neko)"
    ),
    "2.4": (
        "2.4. Самозванство.\n"
        "Запрещено использовать никнеймы, аватарки или манеру общения, чтобы выдавать себя за администрацию."
    ),
    "2.5": (
        "2.5. Токсичность и провокации.\n"
        "Нельзя намеренно разжигать конфликты, троллить участников или массово отмечать (@) людей без причины."
    ),
    "2.6": (
        "2.6. Попрошайничество.\n"
        "Систематические просьбы выдать донат, ресурсы или привилегии у админов или других игроков расцениваются как спам."
    ),
    "3.1": (
        "3.1. Эксплуатация багов (багоюз).\n"
        "Несмотря на разрешенную автоматизацию, намеренное использование программных ошибок, ломающих игровую "
        "экономику, строго запрещено. Если вы нашли баг или сбой, сразу сообщите администрации: @B3ZDAPH0CTb."
    ),
    "3.2": (
        "3.2. Безопасность аккаунта.\n"
        "Вы сами отвечаете за сохранность своего Telegram-профиля. При взломе или потере доступа перенос "
        "прогресса на новый ID не гарантируется."
    ),
    "3.3": (
        "3.3. Бета-тестирование.\n"
        "Бот находится в активной разработке. Откаты базы данных при критических сбоях — форс-мажор и не "
        "компенсируются, если администрация не решит иначе."
    ),
    "3.4": (
        "3.4. Изменение цен доната.\n"
        "Стоимость ресурсов (эссенции, привилегий и т.д.) может меняться. Компенсация разницы за старые покупки "
        "не предусмотрена."
    ),
    "3.5": (
        "3.5. Взлом и дестабилизация.\n"
        "Запрещены любые попытки взлома инфраструктуры бота, нарушения его работы или распространение ложной "
        "информации от имени администрации."
    ),
    "3.6": (
        "3.6. Автовыбор боссов и автоданж\n"
        "В отличие от автоатаки как боссов так и данжей автовыбор будет нечестен по отношению к игрокам без "
        "юзерботов поэтому автовыбор, автоданж, а так же автофк (/fc) будет наказываться сначала предупреждением "
        "а потом баном. Автотрен, автопромо, автобонус и тому подобное разрешено."
    ),
    "4.1": (
        "4.1. Автоматизация действий.\n"
        "Разрешено использовать автокликеры и юзерботы для упрощения рутинных операций (например, бой с боссом)."
    ),
    "4.2": (
        "4.2. Нестандартные нарушения.\n"
        "Администрация может наказать за действие, прямо не описанное в правилах, но которое явно вредит "
        "сообществу или экономике проекта."
    ),
    "4.3": (
        "4.3. Мера наказания.\n"
        "В зависимости от тяжести проступка возможны: предупреждение, мут или бан. Решение принимает администрация."
    ),
    "4.4": (
        "4.4. Контакты поддержки.\n"
        "По всем вопросам, ошибкам и предложениям обращайтесь к @B3ZDAPH0CTb."
    ),
}


def _rules_index_text() -> str:
    keys = ", ".join(sorted(RULES_TEXT.keys(), key=lambda x: tuple(map(int, x.split(".")))))
    return (
        "🧩 Правила Risen Solo\n"
        f"{SEP}\n"
        "Запрос пункта: п1.1 или п 1.1\n"
        f"Доступные пункты: {keys}"
    )


@router.message(Command("rules"))
@router.message(F.text.lower().in_({"правила", "пункт", "п"}))
async def cmd_rules_index(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    await message.answer(_rules_index_text())


@router.message(F.text.lower().regexp(r"^\s*[пp]\s*([1-4]\.\d)\s*$"))
async def cmd_rules_point(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    txt = (message.text or "").strip().lower().replace(" ", "")
    txt = txt.replace("p", "п")
    point = txt[1:] if txt.startswith("п") else txt
    data = RULES_TEXT.get(point)
    if not data:
        await message.answer("❌ Пункт не найден. Напиши: правила")
        return
    await message.answer(data)


# ─────────────────────────────────────────────
#  ГАЙД
# ─────────────────────────────────────────────
@router.message(Command("guide"))
@router.message(F.text.lower().in_({"гайд", "📜 гайд"}))
async def cmd_guide(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    await message.answer(
        "Здесь собран полный гайд по боту Risen Solo",
        reply_markup=_guide_kb(),
    )


# ─────────────────────────────────────────────
#  ПОМОЩЬ
# ─────────────────────────────────────────────
@router.message(Command("help"))
@router.message(F.text.lower().in_({"помощь", "хелп", "🆘 помощь"}))
async def cmd_help(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    help_html = f"<blockquote expandable>{escape(_help_text())}</blockquote>"
    await message.answer(help_html, parse_mode=ParseMode.HTML)


@router.message(Command("command"))
async def cmd_admin_commands(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        await message.answer("❌ Команда доступна только администрации.")
        return
    txt = f"<blockquote expandable>{escape(_admin_commands_text())}</blockquote>"
    await message.answer(txt, parse_mode=ParseMode.HTML)


# ─────────────────────────────────────────────
#  ИВЕНТ
# ─────────────────────────────────────────────
@router.message(Command("event"))
@router.message(F.text.lower().in_({"ивент", "событие", "🪩 ивент", "🌑 ивент босс"}))
async def cmd_event(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await message.answer(_daily_event_text(u), reply_markup=_bonuses_kb())


@router.message(Command("event_top"))
@router.message(F.text.lower().in_({"топ ивент", "ивент топ"}))
async def cmd_event_top(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return


@router.message(Command("wbhp"))
async def cmd_world_boss_hp_delta(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return
    await message.answer("🌑 Ивент-босс удален из бота.")


@router.message(Command("wbset"))
async def cmd_world_boss_hp_set(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return
    await message.answer("🌑 Ивент-босс удален из бота.")


# ─────────────────────────────────────────────
#  ДОНАТ
# ─────────────────────────────────────────────
def _donate_main_text() -> str:
    return "\n".join([
        "💠 Донат-магазин",
        SEP_BAR,
        "Здесь можно усилить аккаунт и открыть редкие возможности.",
        "Покупки выполняются за эссенцию тьмы.",
        "Донат осуществляется только через @B3ZDAPH0CTb.",
        "Ознакомиться с ценами: https://telegra.ph/Donat-VIP-04-04",
        "",
        "Выбери раздел ниже:",
        "• Джинн: ауры за магические монеты",
        "• Привилегии: постоянные бонусы аккаунта",
        "• VIP-экип: отдельные донат-предметы",
    ])


def _donate_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🧞 Джинн", callback_data="donate:jinn"),
            InlineKeyboardButton(text="⚜ Привилегии", callback_data="donate:vip"),
        ],
        [InlineKeyboardButton(text="👑 VIP-экипировка", callback_data="donate:vipgear")],
        [InlineKeyboardButton(text="⚔️ Слоты экипировки", callback_data="donate:slots")],
        [InlineKeyboardButton(text="🧩 Специальное приложение", callback_data="donate:special")],
        [InlineKeyboardButton(text="✖ Закрыть", callback_data="donate:close")],
    ])


def _vip_gear_donate_text() -> str:
    return "\n".join([
        "👑 VIP-экипировка",
        SEP_BAR,
        "Стоимость:",
        "• VIP-питомец: 150 ₽",
        "• VIP-оружие: 150 ₽",
        "• Комплект: 275 ₽",
        "",
        "Как работает:",
        "• Предмет помечается в профиле как 👑 VIP",
        "• Сила адаптируется под текущую арену владельца",
        "• Бонус = x2 от максимального обычного предмета этой арены",
        "• При переходе арены VIP-предмет автоматически усиливается",
        "",
        "Гарантии:",
        "• Не продается через сел о/сел п и обычную продажу",
        "• Не пропадает при истинном ребёрте",
        "",
        "Покупка через ЛС: @B3ZDAPH0CTb",
    ])


def _vip_tier_by_idx(idx: int):
    for tier in DONATE_TIERS:
        if int(tier["idx"]) == int(idx):
            return tier
    return None


def _vip_shop_text() -> str:
    lines = [
        "👑 VIP-привилегии",
        SEP_BAR,
        f"⭐ VIP 1 — Пробужденный ({VIP_COSTS[1]} 💠)",
        "",
        f"🧭 VIP 2 — Следопыт ({VIP_COSTS[2]} 💠)",
        "",
        f"🛡 VIP 3 — Мастер ({VIP_COSTS[3]} 💠)",
        "",
        f"🔥 VIP 4 — Превосходный ({VIP_COSTS[4]} 💠)",
        "",
        f"👑 VIP 5 — Monarch ({VIP_COSTS[5]} 💠)",
        "",
    ]
    return "\n".join(lines)


def _vip_shop_kb() -> InlineKeyboardMarkup:
    rows = []
    for tier in DONATE_TIERS:
        rows.append([
            InlineKeyboardButton(text=str(tier.get("title") or tier["name"]), callback_data=f"donate:vip_tier:{tier['idx']}")
        ])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="donate:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _vip_tier_details_text(u, tier: dict) -> str:
    cur_vip = int(u["vip_lvl"] or 0)
    cur_rank = min(cur_vip, 5)
    need_rank = int(tier["vip_level"])
    balance = int(u["essence"] or 0)
    if cur_rank >= need_rank:
        cost_line = "Привилегия уже имеется, либо она выше этой."
    else:
        cost_line = f"{tier['price']} эссенции"

    lines = [
        "❕ Требуется подтверждение.",
        "",
        "Ты получишь:",
        f"{tier['icon']} Привилегия:  {tier.get('title') or tier['name']}",
    ]
    lines.extend(tier["perks"])
    lines += [
        "",
        f"💳 Стоимость: {cost_line}",
        f"💳 Баланс: {fmt_num(balance)}",
    ]
    return "\n".join(lines)


def _vip_tier_kb(tier_idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"donate:vip_confirm:{tier_idx}")],
        [InlineKeyboardButton(text="◀ Назад", callback_data="donate:vip")],
    ])


SPECIAL_APP_PRICE_ESSENCE = 50
SPECIAL_APP_BOUGHT_STAT_KEY = "donate:special_app_bought"


def _donate_special_text(u) -> str:
    balance = int(_row_get(u, "essence", 0) or 0)
    bought = int(db.get_stat(int(u["tg_id"]), SPECIAL_APP_BOUGHT_STAT_KEY, 0) or 0)
    return "\n".join([
        "🧩 Специальное приложение",
        f"🔥 Цена:  {fmt_num(SPECIAL_APP_PRICE_ESSENCE)} 💠",
        f"💳 Баланс: {fmt_num(balance)} 💠",
        SEP,
        "Включено в набор:",
        "👜 Сумка артефактов х1",
        SEP,
        f"💰 Приобретено: {fmt_num(bought)} из ∞",
        "⚠️ Всего осталось: Не ограничено",
    ])


def _donate_special_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Купить", callback_data="donate:special_buy")],
        [InlineKeyboardButton(text="◀ Назад", callback_data="donate:back")],
    ])


@router.message(Command("donate"))
@router.message(F.text.lower().in_({"донат", "💳 донат"}))
async def cmd_donate(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await message.answer(_donate_main_text(), reply_markup=_donate_main_kb())


@router.message(Command("sp"))
@router.message(F.text.lower().in_({"сп"}))
async def cmd_donate_special_alias(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await message.answer(_donate_special_text(u), reply_markup=_donate_special_kb())


@router.callback_query(F.data == "donate:jinn")
async def cb_donate_jinn(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    status = "🟢 Открыта" if _jinn_open_for_user(u) else "🔴 Закрыта"
    text = (
        f"🧞 Лавка Джинна\n{SEP_BAR}\n"
        f"Статус: {status}\n"
        "В лавке покупаются ауры за магические монеты.\n"
        "Команда для входа: лавка"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Назад", callback_data="donate:back")],
    ])
    await _safe_edit_cb(cb, text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "donate:vip")
async def cb_donate_vip(cb: CallbackQuery):
    ok, _u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _vip_shop_text(), reply_markup=_vip_shop_kb())
    await cb.answer()


@router.callback_query(F.data == "donate:vipgear")
async def cb_donate_vipgear(cb: CallbackQuery):
    ok, _u = await _check_cb_access(cb)
    if not ok:
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Назад", callback_data="donate:back")],
    ])
    await _safe_edit_cb(cb, _vip_gear_donate_text(), reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "donate:special")
async def cb_donate_special(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _donate_special_text(u), reply_markup=_donate_special_kb())
    await cb.answer()


@router.callback_query(F.data == "donate:special_buy")
async def cb_donate_special_buy(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    balance = int(_row_get(u, "essence", 0) or 0)
    if balance < SPECIAL_APP_PRICE_ESSENCE:
        await cb.answer(
            f"Недостаточно эссенции: нужно {fmt_num(SPECIAL_APP_PRICE_ESSENCE)}, у тебя {fmt_num(balance)}",
            show_alert=True,
        )
        return

    db.update_user(tg_id, essence=balance - SPECIAL_APP_PRICE_ESSENCE)
    _artifact_add_bags(tg_id, 1)
    db.add_stat(tg_id, SPECIAL_APP_BOUGHT_STAT_KEY, 1)

    u2 = db.get_user(tg_id) or u
    await _safe_edit_cb(cb, _donate_special_text(u2), reply_markup=_donate_special_kb())
    await cb.answer("✅ Покупка успешна: +1 👜 Сумка артефактов", show_alert=True)


@router.callback_query(F.data.startswith("donate:vip_tier:"))
async def cb_donate_vip_tier(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    try:
        tier_idx = int(cb.data.split(":")[2])
    except Exception:
        await cb.answer("Неверный выбор.", show_alert=True)
        return
    tier = _vip_tier_by_idx(tier_idx)
    if not tier:
        await cb.answer("Привилегия не найдена.", show_alert=True)
        return
    await _safe_edit_cb(cb, _vip_tier_details_text(u, tier), reply_markup=_vip_tier_kb(tier_idx))
    await cb.answer()


@router.callback_query(F.data.startswith("donate:vip_confirm:"))
async def cb_donate_vip_confirm(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    try:
        tier_idx = int(cb.data.split(":")[2])
    except Exception:
        await cb.answer("Неверный выбор.", show_alert=True)
        return
    tier = _vip_tier_by_idx(tier_idx)
    if not tier:
        await cb.answer("Привилегия не найдена.", show_alert=True)
        return

    cur_vip = int(u["vip_lvl"] or 0)
    need_rank = int(tier["vip_level"])
    if min(cur_vip, 5) >= need_rank:
        await cb.answer("У тебя уже есть эта привилегия или выше.", show_alert=True)
        return

    balance = int(u["essence"] or 0)
    price = int(tier["price"])
    if balance < price:
        await cb.answer(f"Недостаточно эссенции: нужно {price}, у тебя {balance}", show_alert=True)
        return

    db.update_user(int(u["tg_id"]), essence=balance - price, vip_lvl=max(cur_vip, need_rank))
    u2 = db.get_user(int(u["tg_id"]))
    await _safe_edit_cb(cb, _vip_tier_details_text(u2, tier), reply_markup=_vip_tier_kb(tier_idx))
    await cb.answer("✅ Привилегия активирована!", show_alert=True)


def _slots_shop_text(u) -> str:
    tg_id = int(u["tg_id"])
    has_w2 = _has_slot2_weapon(tg_id)
    has_p2 = _has_slot2_pet(tg_id)
    essence = int(u["essence"] or 0)
    lines = [
        "⚔️ Слоты экипировки",
        SEP_BAR,
        "Второй слот позволяет надеть ещё одно оружие или питомца.",
        "Бонусы обоих предметов суммируются.",
        "",
        f"💠 Твоя эссенция: {fmt_num(essence)}",
        "",
        f"🗡 Второй слот оружия — {fmt_num(SLOT2_WEAPON_COST)} 💠",
        f"   {'✅ Куплен' if has_w2 else '❌ Не куплен'}",
        "",
        f"🐾 Второй слот питомца — {fmt_num(SLOT2_PET_COST)} 💠",
        f"   {'✅ Куплен' if has_p2 else '❌ Не куплен'}",
    ]
    return "\n".join(lines)


def _slots_shop_kb(u) -> InlineKeyboardMarkup:
    tg_id = int(u["tg_id"])
    has_w2 = _has_slot2_weapon(tg_id)
    has_p2 = _has_slot2_pet(tg_id)
    rows = []
    if not has_w2:
        rows.append([InlineKeyboardButton(
            text=f"🗡 Купить слот оружия ({fmt_num(SLOT2_WEAPON_COST)} 💠)",
            callback_data="donate:buy_slot_w2",
        )])
    if not has_p2:
        rows.append([InlineKeyboardButton(
            text=f"🐾 Купить слот питомца ({fmt_num(SLOT2_PET_COST)} 💠)",
            callback_data="donate:buy_slot_p2",
        )])
    rows.append([InlineKeyboardButton(text="◀ Назад", callback_data="donate:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "donate:slots")
async def cb_donate_slots(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _slots_shop_text(u), reply_markup=_slots_shop_kb(u))
    await cb.answer()


@router.callback_query(F.data.in_({"donate:buy_slot_w2", "donate:buy_slot_p2"}))
async def cb_donate_buy_slot(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    is_weapon = cb.data == "donate:buy_slot_w2"
    stat_key = SLOT2_WEAPON_KEY if is_weapon else SLOT2_PET_KEY
    cost = SLOT2_WEAPON_COST if is_weapon else SLOT2_PET_COST
    label = "слот оружия" if is_weapon else "слот питомца"

    if int(db.get_stat(tg_id, stat_key, 0) or 0) == 1:
        await cb.answer("✅ Уже куплен.", show_alert=True)
        return

    essence = int(u["essence"] or 0)
    if essence < cost:
        await cb.answer(f"❌ Нужно {fmt_num(cost)} 💠, у тебя {fmt_num(essence)}.", show_alert=True)
        return

    db.update_user(tg_id, essence=essence - cost)
    db.set_stat_value(tg_id, stat_key, 1)
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _slots_shop_text(u2), reply_markup=_slots_shop_kb(u2))
    await cb.answer(f"✅ Куплен второй {label}!", show_alert=True)


@router.callback_query(F.data == "donate:back")
async def cb_donate_back(cb: CallbackQuery):
    ok, _ = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _donate_main_text(), reply_markup=_donate_main_kb())
    await cb.answer()


@router.callback_query(F.data == "donate:close")
async def cb_donate_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "💠 Донат-меню закрыто.")
    await cb.answer()


# ─────────────────────────────────────────────
#  ПРОМОКОД
# ─────────────────────────────────────────────
@router.message(Command("promo"))
@router.message(F.text.lower().startswith("промо "))
async def cmd_promo(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])

    text_raw = (message.text or "").strip()
    parts = text_raw.split()
    is_slash_promo = text_raw.lower().startswith("/promo")

    # Создатель — открывает меню создания промо
    if _is_creator(u):
        if len(parts) == 1:  # просто /promo
            PROMO_CTX[tg_id] = {"step": "type", "rewards": []}
            await message.answer(
                "🎫 Создание промокода\nВыбери тип награды:",
                reply_markup=_promo_type_kb(),
            )
            return

        # /promo MYCODE -> старт конструктора с кастомным кодом, если код еще не существует.
        if is_slash_promo and len(parts) == 2:
            custom_code = _normalize_promo_code(parts[1])
            if custom_code:
                if db.get_promo(custom_code):
                    # Если код уже есть, оставляем поведение активации.
                    pass
                else:
                    PROMO_CTX[tg_id] = {
                        "step": "type",
                        "rewards": [],
                        "custom_code": custom_code,
                    }
                    await message.answer(
                        f"🎫 Создание промокода\n🏷 Код: {custom_code}\nВыбери тип награды:",
                        reply_markup=_promo_type_kb(),
                    )
                    return

    # Активация промокода
    code = parts[-1].upper() if len(parts) >= 2 else ""
    if not code:
        await message.answer("Использование: промо КОД или /promo КОД")
        return

    promo = db.get_promo(code)
    now = int(time.time())
    if not promo:
        await message.answer("❌ Промокод не найден.")
        return
    if promo["expires_at"] < now:
        await message.answer("❌ Промокод истёк.")
        return
    uses = db.get_promo_uses_count(promo["id"])
    if uses >= promo["max_uses"]:
        await message.answer("❌ Промокод использован максимальное количество раз.")
        return
    if not db.mark_promo_use(promo["id"], tg_id):
        await message.answer("❌ Ты уже использовал этот промокод.")
        return

    rewards = db.get_promo_rewards(int(promo["id"]))
    updates, reward_lines = _apply_promo_rewards(u, rewards)
    db.update_user(tg_id, **updates)
    if reward_lines:
        await message.answer("✅ Промокод активирован!\n" + "\n".join(reward_lines))
    else:
        await message.answer("✅ Промокод активирован!")


@router.message(Command("endpromos"))
async def cmd_end_all_promos(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    ended = db.expire_all_active_promos(int(time.time()))
    await message.answer(
        "🛑 Все активные промокоды завершены.\n"
        f"Завершено: {fmt_num(ended)}"
    )


# Коллбэки создания промо
@router.callback_query(F.data.startswith("promo_type:"))
async def cb_promo_type(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        await cb.answer("Нет доступа.", show_alert=True)
        return
    type_key = cb.data.split(":")[1]
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.get(tg_id, {"rewards": []})
    ctx.update({"step": "amount", "type_key": type_key})
    PROMO_CTX[tg_id] = ctx
    type_label = dict(PROMO_TYPES).get(type_key, type_key)
    await _safe_edit_cb(cb,
                        f"🎫 Промокод — {type_label}\nВыбери количество:",
                        reply_markup=_promo_amount_kb(type_key),
                        )
    await cb.answer()


@router.callback_query(F.data.startswith("promo_amount:"))
async def cb_promo_amount(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    amount = int(cb.data.split(":")[1])
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.get(tg_id, {})
    type_key = ctx.get("type_key", "coins")
    reward_value = int(amount) if not type_key.startswith("percent") else 0
    reward_percent = int(amount) if type_key.startswith("percent") else 0
    reward_obj = {
        "reward_type": type_key,
        "reward_value": reward_value,
        "reward_percent": reward_percent,
    }
    rewards = list(ctx.get("rewards", []))
    rewards.append(reward_obj)
    ctx["rewards"] = rewards
    ctx["step"] = "post_reward"
    PROMO_CTX[tg_id] = ctx
    type_label = dict(PROMO_TYPES).get(type_key, type_key)
    await _safe_edit_cb(cb,
                        f"✅ Добавлено: {_promo_reward_text(type_key, amount)}\n"
                        f"Сейчас наград: {len(rewards)}\n\n"
                        "Что дальше?",
                        reply_markup=_promo_finalize_kb(),
                        )
    await cb.answer()


@router.callback_query(F.data == "promo_amount_custom")
async def cb_promo_amount_custom(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.get(tg_id, {})
    type_key = str(ctx.get("type_key", "coins") or "coins")
    ctx["step"] = "custom_amount"
    PROMO_CTX[tg_id] = ctx
    tip = "Введи процент (1-100)." if type_key.startswith("percent") else "Введи целое число награды (>0)."
    await _safe_edit_cb(
        cb,
        f"✍️ Свое значение для {dict(PROMO_TYPES).get(type_key, type_key)}\n{tip}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀ Назад", callback_data="promo_back_amount")],
            [InlineKeyboardButton(text="✖ Отмена", callback_data="promo_cancel")],
        ]),
    )
    await cb.answer()


@router.message(lambda m: bool(m.text) and m.from_user is not None and PROMO_CTX.get(m.from_user.id, {}).get("step") == "custom_amount")
async def handle_promo_custom_amount(message: Message):
    u = db.get_user(message.from_user.id)
    if not u or not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.get(tg_id, {})
    type_key = str(ctx.get("type_key", "coins") or "coins")
    raw = (message.text or "").strip()
    if not re.fullmatch(r"\d+", raw):
        await message.answer("❌ Введи число.")
        return
    amount = int(raw)
    if amount <= 0:
        await message.answer("❌ Значение должно быть больше 0.")
        return
    if type_key.startswith("percent"):
        amount = min(amount, 100)
    reward_obj = {
        "reward_type": type_key,
        "reward_value": int(amount) if not type_key.startswith("percent") else 0,
        "reward_percent": int(amount) if type_key.startswith("percent") else 0,
    }
    rewards = list(ctx.get("rewards", []))
    rewards.append(reward_obj)
    ctx["rewards"] = rewards
    ctx["step"] = "post_reward"
    PROMO_CTX[tg_id] = ctx
    await message.answer(
        f"✅ Добавлено: {_promo_reward_text(type_key, amount)}\n"
        f"Сейчас наград: {len(rewards)}",
        reply_markup=_promo_finalize_kb(),
    )


@router.callback_query(F.data == "promo_more")
async def cb_promo_more(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    ctx = PROMO_CTX.get(cb.from_user.id, {"rewards": []})
    ctx["step"] = "type"
    PROMO_CTX[cb.from_user.id] = ctx
    await _safe_edit_cb(cb, "🎫 Добавь следующую награду:\nВыбери тип:", reply_markup=_promo_type_kb())
    await cb.answer()


@router.callback_query(F.data == "promo_set_code")
async def cb_promo_set_code(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.get(tg_id, {})
    if not ctx.get("rewards"):
        await cb.answer("Сначала добавь хотя бы одну награду.", show_alert=True)
        return
    ctx["step"] = "custom_code"
    PROMO_CTX[tg_id] = ctx
    await _safe_edit_cb(
        cb,
        "🏷 Введи код промо в чат\nРазрешено: A-Z, 0-9, _, - (длина 3-24)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀ Назад", callback_data="promo_back_uses")],
            [InlineKeyboardButton(text="✖ Отмена", callback_data="promo_cancel")],
        ]),
    )
    await cb.answer()


@router.message(lambda m: bool(m.text) and m.from_user is not None and PROMO_CTX.get(m.from_user.id, {}).get("step") == "custom_code")
async def handle_promo_custom_code(message: Message):
    u = db.get_user(message.from_user.id)
    if not u or not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.get(tg_id, {})
    code = _normalize_promo_code(message.text)
    if not code:
        await message.answer("❌ Неверный код. Используй A-Z, 0-9, _, - (3-24 символа).")
        return
    if db.get_promo(code):
        await message.answer("❌ Такой код уже существует.")
        return
    ctx["custom_code"] = code
    ctx["step"] = "duration"
    PROMO_CTX[tg_id] = ctx
    await message.answer(f"✅ Код установлен: {code}\nТеперь выбери срок действия в панели промо.")


@router.callback_query(F.data == "promo_next_uses")
async def cb_promo_next_uses(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    ctx = PROMO_CTX.get(cb.from_user.id, {})
    rewards = ctx.get("rewards", [])
    if not rewards:
        await cb.answer("Добавь хотя бы одну награду.", show_alert=True)
        return
    ctx["step"] = "uses"
    PROMO_CTX[cb.from_user.id] = ctx
    await _safe_edit_cb(
        cb,
        f"🎫 Наград в промо: {len(rewards)}\nВыбери макс. кол-во использований:",
        reply_markup=_promo_uses_kb("coins", 1),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("promo_uses:"))
async def cb_promo_uses(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    uses = int(cb.data.split(":")[1])
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.get(tg_id, {})
    rewards = list(ctx.get("rewards", []))
    if not rewards:
        await cb.answer("Добавь хотя бы одну награду.", show_alert=True)
        return
    ctx["uses"] = uses
    ctx["step"] = "duration"
    PROMO_CTX[tg_id] = ctx
    await _safe_edit_cb(cb,
                        f"🎫 Наград: {len(rewards)}\n"
                        f"👥 Использований: {uses}\n"
                        f"Выбери время действия промокода:",
                        reply_markup=_promo_duration_kb(),
                        )
    await cb.answer()


@router.callback_query(F.data.startswith("promo_dur:"))
async def cb_promo_duration(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    ctx = PROMO_CTX.pop(tg_id, {})
    rewards = list(ctx.get("rewards", []))
    uses = int(ctx.get("uses", 1) or 1)
    if not rewards:
        await cb.answer("Нет наград для промокода.", show_alert=True)
        return

    dur = int(cb.data.split(":")[1])
    import string
    code = str(ctx.get("custom_code") or "").strip().upper()
    if not code:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
    expires = int(time.time()) + dur

    # Базовая запись + набор наград (новая схема promo_rewards)
    if db.get_promo(code):
        await cb.answer("❌ Такой код уже существует, выбери другой.", show_alert=True)
        return
    promo_id = db.create_promo(code, expires, uses, tg_id, reward_type="coins", reward_value=0, reward_percent=0)
    for rw in rewards:
        db.add_promo_reward(
            promo_id,
            str(_row_get(rw, "reward_type", "coins") or "coins"),
            int(_row_get(rw, "reward_value", 0) or 0),
            int(_row_get(rw, "reward_percent", 0) or 0),
        )

    reward_lines = []
    for rw in rewards:
        rt = str(_row_get(rw, "reward_type", "coins") or "coins")
        val = int(_row_get(rw, "reward_percent", 0) or 0) if rt.startswith("percent") else int(_row_get(rw, "reward_value", 0) or 0)
        reward_lines.append(_promo_reward_text(rt, val))
    preview = "\n".join(f"• {x}" for x in reward_lines)

    await _safe_edit_cb(
        cb,
        f"✅ Промокод создан!\n"
        f"📋 Код: <code>промо {code}</code>\n"
        f"👥 Использований: {uses}\n"
        f"⏱ До: {_fmt_ts_msk(expires, '%d.%m.%Y %H:%M')}\n\n"
        f"🎁 Награды:\n{preview}",
        parse_mode=ParseMode.HTML,
    )

    if bot_instance is not None:
        broadcast_reward = ", ".join(reward_lines[:3]) + (" ..." if len(reward_lines) > 3 else "")
        await _broadcast_promo_and_pin(bot_instance, promo_id, code, broadcast_reward, uses, expires)
    await cb.answer("Промокод создан")


@router.callback_query(F.data == "promo_cancel")
async def cb_promo_cancel(cb: CallbackQuery):
    PROMO_CTX.pop(cb.from_user.id, None)
    await _safe_edit_cb(cb, "🎫 Создание промокода отменено.")
    await cb.answer()


@router.callback_query(F.data == "promo_back_type")
async def cb_promo_back_type(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    ctx = PROMO_CTX.get(cb.from_user.id, {"rewards": []})
    ctx["step"] = "type"
    PROMO_CTX[cb.from_user.id] = ctx
    await _safe_edit_cb(cb, "🎫 Создание промокода\nВыбери тип:", reply_markup=_promo_type_kb())
    await cb.answer()


@router.callback_query(F.data == "promo_back_amount")
async def cb_promo_back_amount(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    ctx = PROMO_CTX.get(cb.from_user.id, {})
    type_key = ctx.get("type_key", "coins")
    await _safe_edit_cb(cb,
                        f"🎫 Выбери количество:",
                        reply_markup=_promo_amount_kb(type_key),
                        )
    await cb.answer()


@router.callback_query(F.data == "promo_back_uses")
async def cb_promo_back_uses(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    ctx = PROMO_CTX.get(cb.from_user.id, {})
    rewards = list(ctx.get("rewards", []))
    await _safe_edit_cb(
        cb,
        f"🎫 Наград в промо: {len(rewards)}\nВыбери макс. кол-во использований:",
        reply_markup=_promo_uses_kb("coins", 1),
    )
    await cb.answer()


# ─────────────────────────────────────────────
#  ВОССТАНОВЛЕНИЕ КЛАВИАТУРЫ
# ─────────────────────────────────────────────
@router.message(Command("restore_keyboard"))
async def cmd_restore_kb(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        return
    await message.answer("⌨️ Клавиатура восстановлена.", reply_markup=main_kb())


# ─────────────────────────────────────────────
#  РАССЫЛКА СОЗДАТЕЛЯ: /soo
# ─────────────────────────────────────────────
@router.message(Command("soo"))
async def cmd_soo(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Использование: /soo текст сообщения")
        return

    text = parts[1].strip()
    sent = 0
    failed = 0
    users = db.list_users_for_notify()
    for row in users:
        tg_id = int(row["tg_id"])
        if int(row["banned"] or 0):
            continue
        if not _notify_enabled(tg_id, NOTIFY_ADMIN_MSG_KEY):
            continue
        try:
            await message.bot.send_message(tg_id, text)
            sent += 1
        except Exception:
            failed += 1

    await message.answer(
        f"📣 Рассылка завершена.\n"
        f"✅ Отправлено: {fmt_num(sent)}\n"
        f"⚠️ Ошибок доставки: {fmt_num(failed)}"
    )


# ─────────────────────────────────────────────
#  ДИАГНОСТИКА ДЛЯ АДМИНОВ: /ping /online
# ─────────────────────────────────────────────
@router.message(Command("debag"))
@router.message(Command("debug"))
@router.message(F.text.lower().in_({"дебаг", "debag", "debug"}))
async def cmd_debag(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    # Быстрый сетевой чек до Telegram API.
    ping_ms = -1
    t0 = time.perf_counter()
    try:
        await message.bot.get_me()
        ping_ms = int((time.perf_counter() - t0) * 1000)
    except Exception:
        ping_ms = -1

    users_total = 0
    try:
        users_total = len(db.list_users_for_notify())
    except Exception:
        users_total = 0

    now_ts = int(time.time())
    online_1h = db.count_online_since(now_ts - 3600)
    online_24h = db.count_online_since(now_ts - 24 * 3600)

    persisted_battles = 0
    persisted_dungeons = 0
    total_coins = 0
    total_magic = 0
    total_essence = 0
    total_currency = 0
    total_power = 0
    total_afk_cases = 0
    total_item_cases = 0
    total_inventory_units = 0
    total_guild_unity = 0
    db_size = 0
    wal_size = 0
    try:
        import sqlite3 as _sq
        con = _sq.connect("bot.db", timeout=30)
        persisted_battles = int(con.execute("SELECT COUNT(*) FROM active_battles").fetchone()[0] or 0)
        persisted_dungeons = int(con.execute("SELECT COUNT(*) FROM active_dungeons").fetchone()[0] or 0)
        eco = con.execute(
            """
            SELECT
                COALESCE(SUM(coins), 0) AS s_coins,
                COALESCE(SUM(magic_coins), 0) AS s_magic,
                COALESCE(SUM(essence), 0) AS s_essence,
                COALESCE(SUM(power), 0) AS s_power,
                COALESCE(SUM(
                    afk_common + afk_rare + afk_epic + afk_legendary + afk_mythic
                ), 0) AS s_afk,
                COALESCE(SUM(
                    weapon_cases_a1 + weapon_cases_a2 + weapon_cases_a3 + weapon_cases_a4 + weapon_cases_a5 +
                    weapon_cases_a6 + weapon_cases_a7 + weapon_cases_a8 + weapon_cases_a9 + weapon_cases_a10 +
                    weapon_cases_a11 + weapon_cases_a12 + weapon_cases_a13 + weapon_cases_a14 + weapon_cases_a15 +
                    pet_cases_a1 + pet_cases_a2 + pet_cases_a3 + pet_cases_a4 + pet_cases_a5 +
                    pet_cases_a6 + pet_cases_a7 + pet_cases_a8 + pet_cases_a9 + pet_cases_a10 +
                    pet_cases_a11 + pet_cases_a12 + pet_cases_a13 + pet_cases_a14 + pet_cases_a15
                ), 0) AS s_item_cases
            FROM users
            """
        ).fetchone()
        total_coins = int(eco[0] or 0)
        total_magic = int(eco[1] or 0)
        total_essence = int(eco[2] or 0)
        total_power = int(eco[3] or 0)
        total_afk_cases = int(eco[4] or 0)
        total_item_cases = int(eco[5] or 0)
        total_currency = total_coins + total_magic + total_essence

        inv = con.execute("SELECT COALESCE(SUM(count), 0) FROM inventory").fetchone()
        total_inventory_units = int((inv[0] if inv else 0) or 0)

        gu = con.execute("SELECT COALESCE(SUM(unity_shards), 0) FROM guilds").fetchone()
        total_guild_unity = int((gu[0] if gu else 0) or 0)
        con.close()
    except Exception:
        pass
    try:
        db_size = int(os.path.getsize("bot.db"))
    except Exception:
        db_size = 0
    try:
        wal_size = int(os.path.getsize("bot.db-wal"))
    except Exception:
        wal_size = 0

    contest = db.get_active_contest()

    warn_lines = []
    if len(ACTIVE_BATTLES) != persisted_battles:
        warn_lines.append(
            f"⚠️ Бои: RAM {len(ACTIVE_BATTLES)} / DB {persisted_battles}"
        )
    if len(ACTIVE_DUNGEONS) != persisted_dungeons:
        warn_lines.append(
            f"⚠️ Данжи: RAM {len(ACTIVE_DUNGEONS)} / DB {persisted_dungeons}"
        )

    lines = [
        "🛠 Дебаг-отчёт",
        SEP,
        f"👥 Игроков: {fmt_num(users_total)}",
        f"🪙 Всего монет: {fmt_num(total_coins)}",
        f"🔯 Всего маг. монет: {fmt_num(total_magic)}",
        f"💠 Всего эссенции: {fmt_num(total_essence)}",
        f"💰 Всего валюты: {fmt_num(total_currency)}",
        f"⚙️ Суммарная мощность: {fmt_num(total_power)}",
        f"📦 AFK-кейсов всего: {fmt_num(total_afk_cases)}",
        f"🎫 Предметных кейсов всего: {fmt_num(total_item_cases)}",
        f"🎒 Предметов в инвентарях: {fmt_num(total_inventory_units)}",
        f"🛡 Осколков единства в кланах: {fmt_num(total_guild_unity)}",
        SEP,
        f"⏱ Аптайм: {_fmt_uptime(now_ts - BOT_STARTED_AT)}",
        f"🏓 Ping Telegram API: {'ошибка' if ping_ms < 0 else f'{ping_ms} мс'}",
        f"🟢 Онлайн 1ч: {fmt_num(online_1h)} | 24ч: {fmt_num(online_24h)}",
        f"⚔️ Активные бои: RAM {fmt_num(len(ACTIVE_BATTLES))} | DB {fmt_num(persisted_battles)}",
        f"⛩ Активные данжи: RAM {fmt_num(len(ACTIVE_DUNGEONS))} | DB {fmt_num(persisted_dungeons)}",
        f"🎫 Конкурс: {'активен' if contest else 'нет'}",
        f"💾 bot.db: {fmt_num(db_size)} байт | wal: {fmt_num(wal_size)} байт",
    ]
    if warn_lines:
        lines.append(SEP)
        lines.append("Потенциальные проблемы:")
        lines.extend(warn_lines)

    await message.answer("\n".join(lines))


@router.message(Command("ping"))
async def cmd_ping(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    t0 = time.perf_counter()
    try:
        await message.bot.get_me()
    except Exception as e:
        await message.answer(f"🏓 Ping: ошибка сети\n{e}")
        return
    ms = int((time.perf_counter() - t0) * 1000)
    await message.answer(f"🏓 Пинг хоста до Telegram API: {ms} мс")


@router.message(Command("online"))
async def cmd_online(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    now_ts = int(time.time())
    c1 = db.count_online_since(now_ts - 3600)
    c12 = db.count_online_since(now_ts - 12 * 3600)
    c24 = db.count_online_since(now_ts - 24 * 3600)
    await message.answer(
        "👥 Онлайн в боте\n"
        f"За 1 час: {fmt_num(c1)}\n"
        f"За 12 часов: {fmt_num(c12)}\n"
        f"За 24 часа: {fmt_num(c24)}"
    )


@router.message(Command("active"))
async def cmd_active_window(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /active [1m|1h|1d] [id|reply]")
        return

    window_token = str(parts[1]).strip().lower()
    window_secs = _parse_contest_duration(window_token)
    if window_secs <= 0:
        await message.answer("❌ Неверный интервал. Пример: /active 1h 123456789")
        return
    window_secs = max(60, min(window_secs, 24 * 3600))

    target_id = 0
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = int(message.reply_to_message.from_user.id)
    elif len(parts) >= 3 and parts[2].isdigit():
        target_id = int(parts[2])

    if target_id <= 0:
        await message.answer("❌ Укажи ID игрока или используй reply на его сообщение.")
        return

    now_ts = int(time.time())
    since_ts = now_ts - int(window_secs)
    rows = db.list_user_activity_log(int(target_id), since_ts, now_ts, limit=100000)
    filtered_logs: list[tuple[int, str, int]] = []
    attack_ts: list[int] = []
    for row in rows:
        try:
            ts = int(_row_get(row, "ts", 0) or 0)
            label = str(_row_get(row, "label", "") or "")
        except Exception:
            continue
        if ts <= 0 or not label:
            continue
        filtered_logs.append((ts, label, 1))
        if _is_attack_action(label):
            attack_ts.append(ts)

    avg_i, std_i = _monitor_rhythm_stats(list(attack_ts))
    u_target = db.get_user(int(target_id))
    nick = _display_name(u_target) if u_target else f"id{target_id}"
    uname = str(_row_get(u_target, "username", "") or "").strip() if u_target else ""

    lines = [
        "Activity export",
        f"target_id={target_id}",
        f"nick={nick}",
        f"username={uname or '-'}",
        f"window_secs={window_secs}",
        f"from_ts={since_ts}",
        f"to_ts={now_ts}",
        f"logs_count={len(filtered_logs)}",
        f"attack_samples={len(attack_ts)}",
        f"attack_avg_interval={avg_i:.3f}",
        f"attack_std_interval={std_i:.3f}",
        "",
        "Logs:",
    ]

    if not filtered_logs:
        lines.append("<no logs in selected window>")
    else:
        for ts, label, cnt in filtered_logs:
            dt = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
            suffix = f" x{cnt}" if int(cnt) > 1 else ""
            lines.append(f"{dt} | {label}{suffix}")

    payload = "\n".join(lines)
    bio = BytesIO(payload.encode("utf-8"))
    bio.name = f"active_{target_id}_{window_token}.txt"
    await message.answer_document(
        BufferedInputFile(bio.getvalue(), filename=bio.name),
        caption=f"Активность {target_id} за {window_token}",
    )


# ─────────────────────────────────────────────
#  ИВЕНТ ЗАРАБОТКА МОНЕТ: /startivent
# ─────────────────────────────────────────────
def _earn_event_top_rows(st: dict, limit: int = 5) -> list[tuple[int, int]]:
    start_coins = dict(st.get("start_coins", {}))
    rows: list[tuple[int, int]] = []
    for uid, start_val in start_coins.items():
        tu = db.get_user(int(uid))
        if not tu:
            continue
        gain = max(0, int(_row_get(tu, "coins", 0) or 0) - int(start_val or 0))
        if gain > 0:
            rows.append((int(uid), int(gain)))
    rows.sort(key=lambda x: (-x[1], x[0]))
    return rows[:max(1, int(limit))]


def _earn_event_top_text(st: dict, title: str) -> str:
    rows = _earn_event_top_rows(st, limit=5)
    lines = [title, SEP]
    if not rows:
        lines.append("Никто не заработал монеты за время ивента.")
        return "\n".join(lines)
    for idx, (uid, gain) in enumerate(rows, start=1):
        tu = db.get_user(int(uid))
        nick = _display_name(tu) if tu else f"id{uid}"
        lines.append(f"{idx}. {nick} — +{fmt_num(gain)} 🪙")
    return "\n".join(lines)


async def _earn_event_finish(event_id: int):
    st = EARN_EVENT_STATE
    if not st or not st.get("active") or int(st.get("id", 0) or 0) != int(event_id):
        return

    st["active"] = False
    rows = _earn_event_top_rows(st, limit=5)

    for pos, row in enumerate(rows, start=1):
        uid, _gain = row
        reward = EARN_EVENT_REWARDS[pos - 1]
        tu = db.get_user(int(uid))
        if not tu:
            continue
        db.update_user(
            int(uid),
            coins=int(_row_get(tu, "coins", 0) or 0) + int(reward["coins"]),
            magic_coins=int(_row_get(tu, "magic_coins", 0) or 0) + int(reward["magic"]),
            essence=int(_row_get(tu, "essence", 0) or 0) + int(reward["essence"]),
        )
        if bot_instance is not None:
            try:
                await bot_instance.send_message(
                    int(uid),
                    "🏁 Ивент заработка монет завершен!\n"
                    f"Твое место: {pos}\n"
                    f"Награда: +{fmt_num(reward['coins'])} 🪙, +{fmt_num(reward['magic'])} 🔯, +{fmt_num(reward['essence'])} 💠",
                )
            except Exception:
                pass

    result_text = _earn_event_top_text(st, "🏁 Ивент завершен. Топ-5 по заработку монет:")
    result_text += "\n\n🎁 Награды выданы победителям."
    st["result_text"] = result_text
    st["finished_at"] = int(time.time())

    if bot_instance is not None and int(st.get("chat_id", 0) or 0) != 0:
        try:
            await bot_instance.send_message(int(st["chat_id"]), result_text)
        except Exception:
            pass


async def _earn_event_worker(event_id: int, delay_secs: int):
    await asyncio.sleep(max(1, int(delay_secs)))
    await _earn_event_finish(int(event_id))


@router.message(Command("startivent"))
async def cmd_start_earn_event(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):         # ← только создатели (admin_role=5 или SUPER_ADMINS)
        await message.answer("❌ Запускать ивент могут только создатели проекта.")
        return

    global EARN_EVENT_COUNTER
    if EARN_EVENT_STATE.get("active"):
        left = max(0, int(EARN_EVENT_STATE.get("ends_at", 0) or 0) - int(time.time()))
        await message.answer(f"⏳ Ивент уже идет. Осталось: {_fmt_uptime(left)}")
        return

    parts = (message.text or "").split(maxsplit=1)
    duration = 600
    if len(parts) >= 2:
        parsed = _parse_contest_duration(parts[1])
        if parsed > 0:
            duration = parsed
    duration = max(60, min(duration, 24 * 3600))

    start_coins: dict[int, int] = {}
    for row in db.list_users_for_notify():
        if int(_row_get(row, "banned", 0) or 0):
            continue
        uid = int(_row_get(row, "tg_id", 0) or 0)
        if uid <= 0:
            continue
        tu = db.get_user(uid)
        if not tu:
            continue
        start_coins[uid] = int(_row_get(tu, "coins", 0) or 0)

    EARN_EVENT_COUNTER += 1
    event_id = int(EARN_EVENT_COUNTER)
    now_ts = int(time.time())
    EARN_EVENT_STATE.clear()
    EARN_EVENT_STATE.update({
        "id": event_id,
        "active": True,
        "chat_id": int(message.chat.id),
        "started_at": now_ts,
        "ends_at": now_ts + duration,
        "start_coins": start_coins,
    })

    await message.answer(
        "🚀 Ивент заработка монет стартовал!\n"
        f"⏱ Длительность: {_fmt_uptime(duration)}\n"
        "🏁 В конце будет топ-5 по приросту монет."
    )
    asyncio.create_task(_earn_event_worker(event_id, duration))


# ─────────────────────────────────────────────
#  КОНКУРСЫ: /con и /ответ
# ─────────────────────────────────────────────
async def _contest_end_worker(contest_id: int, delay_secs: int):
    await asyncio.sleep(max(1, int(delay_secs)))
    row = db.get_active_contest()
    if not row or int(row["contest_id"] or 0) != int(contest_id):
        return

    owner_id = int(row["owner_id"] or 0)
    question = str(row["question"] or "")
    answers_rows = db.list_contest_answers(contest_id)
    answers = {int(r["tg_id"]): str(r["answer"]) for r in answers_rows}
    db.clear_active_contest()
    CONTEST_STATE.clear()
    CONTEST_ANSWERED.clear()

    if bot_instance is None or owner_id <= 0:
        return

    lines = [
        "🏁 Конкурс завершен.",
        f"❓ Вопрос: {question}",
        f"👥 Ответов: {fmt_num(len(answers))}",
    ]
    if answers:
        lines.append("\nОтветы:")
        for uid, ans in answers.items():
            lines.append(f"• {uid}: {ans}")
    try:
        await bot_instance.send_message(owner_id, "\n".join(lines))
    except Exception:
        pass


@router.message(Command("con"))
async def cmd_con(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return

    raw = (message.text or "").strip()
    parts = raw.split()
    if len(parts) < 3:
        await message.answer(
            "Использование: /con [текст вопроса] [время]\n"
            "Пример: /con кто угадает число pi? 1ч"
        )
        return

    duration_token = parts[-1]
    duration_secs = _parse_contest_duration(duration_token)
    if duration_secs <= 0:
        await message.answer("❌ Неверное время. Примеры: 30м, 1ч, 2ч, 1д")
        return
    question = " ".join(parts[1:-1]).strip()
    if len(question) < 3:
        await message.answer("❌ Вопрос слишком короткий.")
        return

    contest_id = int(time.time() * 1000)
    ends_at = int(time.time()) + duration_secs
    started_at = int(time.time())
    owner_id = int(u["tg_id"])
    CONTEST_STATE.clear()
    CONTEST_ANSWERED.clear()
    CONTEST_STATE.update({
        "id": contest_id,
        "owner_id": owner_id,
        "question": question,
        "started_at": started_at,
        "ends_at": ends_at,
        "answers": {},
    })
    db.set_active_contest(contest_id, owner_id, question, started_at, ends_at)

    users = db.list_users_for_notify()
    sent = 0
    failed = 0
    text = (
        "Конкурс начался🎉\n"
        f"Вопрос: {question}\n"
        f"Время на ответ: {duration_token}\n"
        "Пример: /ответ ваш ответ"
    )
    for row in users:
        tg_id = int(row["tg_id"])
        if int(row["banned"] or 0):
            continue
        if not _notify_enabled(tg_id, NOTIFY_CONTEST_KEY):
            continue
        try:
            await message.bot.send_message(tg_id, text)
            sent += 1
        except Exception:
            failed += 1

    asyncio.create_task(_contest_end_worker(contest_id, duration_secs))
    await message.answer(
        "✅ Конкурс запущен.\n"
        f"👥 Уведомлено: {fmt_num(sent)}\n"
        f"⚠️ Ошибок доставки: {fmt_num(failed)}"
    )


@router.message(Command("answer"))
@router.message(F.text.lower().startswith("/ответ"))
async def cmd_answer(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return

    state = CONTEST_STATE
    if not state:
        if not _contest_restore_from_db():
            await message.answer("❌ Сейчас нет активного конкурса.")
            return
        state = CONTEST_STATE
    now = int(time.time())
    if now >= int(state.get("ends_at", 0)):
        db.clear_active_contest()
        CONTEST_STATE.clear()
        CONTEST_ANSWERED.clear()
        await message.answer("⏱ Конкурс уже завершен.")
        return

    tg_id = int(u["tg_id"])
    if tg_id in CONTEST_ANSWERED:
        await message.answer("❌ Ответ можно отправить только один раз.")
        return

    answer = ""
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        answer = parts[1].strip()
    if answer.lower().startswith("ответ "):
        answer = answer[6:].strip()
    if not answer:
        await message.answer("❌ Формат ответа: /ответ ваш ответ")
        return

    CONTEST_ANSWERED.add(tg_id)
    answers: dict[int, str] = state.setdefault("answers", {})
    answers[tg_id] = answer[:300]
    db.add_contest_answer(int(state.get("id", 0)), tg_id, answer[:300])
    owner_id = int(state.get("owner_id", 0))
    if bot_instance is not None and owner_id > 0:
        try:
            uname = (message.from_user.username or "").strip() if message.from_user else ""
            user_link = f"<a href=\"tg://user?id={tg_id}\">{tg_id}</a>"
            owner_text = (
                "📩 Ответ на конкурс\n"
                f"🆔 Профиль: {user_link}\n"
                f"ID: <code>{tg_id}</code>\n"
                f"Юз: {'@' + escape(uname) if uname else 'нет username'}\n"
                f"Ответ: {escape(answer[:300])}"
            )
            await bot_instance.send_message(owner_id, owner_text, parse_mode=ParseMode.HTML)
        except Exception:
            pass
    await message.answer("✅ Ответ принят. Удачи!")


@router.callback_query(F.data == "bonuses:ref")
async def cb_bonuses_ref(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    pending = db.list_pending_referrals(int(u["tg_id"]), limit=10)
    me = await cb.bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{u['tg_id']}" if me and me.username else "(не удалось получить username бота)"
    text = _ref_text(u, pending)
    await _safe_edit_cb(cb, f"{text}\n\n🔗 Твоя ссылка:\n<code>{escape(link)}</code>", parse_mode=ParseMode.HTML, reply_markup=_ref_kb(u, len(pending)))
    await cb.answer()


@router.callback_query(F.data == "bonuses:event")
async def cb_bonuses_event(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _daily_event_text(u), reply_markup=_bonuses_kb())
    await cb.answer()


@router.callback_query(F.data.startswith("wboss:"))
async def cb_world_boss(cb: CallbackQuery):
    ok, _u = await _check_cb_access(cb)
    if not ok:
        return
    await cb.answer("Ивент-босс удален из бота.", show_alert=True)


@router.callback_query(F.data == "bonuses:bio")
async def cb_bonuses_bio(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await _refresh_bio_bonus_for_user(cb.bot, int(u["tg_id"]), notify=False)
    u2 = db.get_user(int(u["tg_id"])) or u
    await _safe_edit_cb(cb, _bio_bonus_text(u2), reply_markup=_bonuses_kb())
    await cb.answer()


@router.callback_query(F.data == "trainup:refresh")
async def cb_trainup_refresh(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _train_upgrades_text(u), reply_markup=_train_upgrades_kb(u))
    await cb.answer()


@router.callback_query(F.data == "trainup:close")
async def cb_trainup_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "🧪 Улучшения закрыты.")
    await cb.answer()


@router.callback_query(F.data.startswith("trainup:buy:"))
async def cb_trainup_buy(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    kind = cb.data.split(":")[-1]
    success, msg = _buy_train_upgrade(int(u["tg_id"]), kind)
    u2 = db.get_user(int(u["tg_id"])) or u
    await _safe_edit_cb(cb, _train_upgrades_text(u2), reply_markup=_train_upgrades_kb(u2))
    await cb.answer(msg, show_alert=not success)


@router.callback_query(F.data == "ref:refresh")
async def cb_ref_refresh(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    pending = db.list_pending_referrals(int(u["tg_id"]), limit=10)
    me = await cb.bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{u['tg_id']}" if me and me.username else "(не удалось получить username бота)"
    text = _ref_text(u, pending)
    await _safe_edit_cb(cb, f"{text}\n\n🔗 Твоя ссылка:\n<code>{escape(link)}</code>", parse_mode=ParseMode.HTML, reply_markup=_ref_kb(u, len(pending)))
    await cb.answer("Обновлено")


@router.callback_query(F.data == "ref:close")
async def cb_ref_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "🤝 Раздел рефералов закрыт.")
    await cb.answer()


@router.callback_query(F.data.startswith("ref:claim:"))
async def cb_ref_claim(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    reward_key = cb.data.split(":")[-1]
    options = {opt["key"]: opt for opt in _ref_reward_options(u)}
    chosen = options.get(reward_key)
    if not chosen:
        await cb.answer("Награда недоступна на твоём уровне", show_alert=True)
        return
    pending = db.list_pending_referrals(int(u["tg_id"]), limit=1)
    if not pending:
        await cb.answer("Нет доступных реферальных наград", show_alert=True)
        await cb_ref_refresh(cb)
        return
    referred_id = int(pending[0]["referred_id"])
    if not db.claim_pending_referral(int(u["tg_id"]), referred_id, reward_key):
        await cb.answer("Награда уже выдана или недоступна", show_alert=True)
        await cb_ref_refresh(cb)
        return

    updates = {}
    reward_apply = dict(chosen.get("apply", {}))
    for field, delta in reward_apply.items():
        updates[field] = int(_row_get(u, field, 0) or 0) + int(delta)
    db.update_user(int(u["tg_id"]), **updates)

    u2 = db.get_user(int(u["tg_id"]))
    pending2 = db.list_pending_referrals(int(u["tg_id"]), limit=10)
    me = await cb.bot.get_me()
    link = f"https://t.me/{me.username}?start=ref_{u2['tg_id']}" if me and me.username else "(не удалось получить username бота)"
    text = _ref_text(u2, pending2)
    text += f"\n\n✅ Получено: {chosen['title']} ({chosen['desc']})\nЗа приглашенного: {referred_id}"
    await _safe_edit_cb(cb, f"{text}\n\n🔗 Твоя ссылка:\n<code>{escape(link)}</code>", parse_mode=ParseMode.HTML, reply_markup=_ref_kb(u2, len(pending2)))
    await cb.answer("Награда зачислена")


@router.callback_query(F.data == "bonuses:close")
async def cb_bonuses_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "🎁 Раздел бонусов закрыт.")
    await cb.answer()


@router.message(Command("worlds"))
@router.message(F.text.lower().in_({"миры", "🌍 миры"}))
async def cmd_worlds(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    lines = ["🏟 Список арен", SEP]
    for a in range(1, gd.max_arena() + 1):
        lines.append(f"{a}) {gd.arena_title(a)}")
    lines.extend([SEP, "Команда: боссы"])
    await message.answer("\n".join(lines))


# ─────────────────────────────────────────────
#  ГИЛЬДИИ
# ─────────────────────────────────────────────
@router.message(Command("clan"))
@router.message(F.text.lower().in_({"клан", "клан инфо", "🏰 клан"}))
async def cmd_clan(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)

    # /clan ID — расширенный просмотр для админов
    if len(parts) >= 2 and parts[0].startswith("/") and parts[1].strip().isdigit():
        if not _is_admin(u):
            await message.answer("❌ Просмотр клана по ID доступен только администрации.")
            return
        gid = int(parts[1].strip())
        g = db.get_guild(gid)
        if not g:
            await message.answer("❌ Клан с таким ID не найден.")
            return
        members = db.list_guild_members(gid, limit=200, offset=0)
        report = _clan_admin_info_text(g, members)
        if len(report) > 3500:
            bio = BytesIO(report.encode("utf-8"))
            await message.answer_document(
                BufferedInputFile(bio.read(), filename=f"clan_{gid}.txt"),
                caption=f"Отчёт по клану {gid}",
            )
        else:
            await message.answer(report)
        return

    g = db.get_user_guild(int(u["tg_id"]))
    if not g:
        await message.answer("🏰 Клан\nТы пока не состоишь в клане.")
        return
    members = db.guild_member_count(int(g["id"]))
    await message.answer(_clan_info_text(g, members))


@router.message(Command("clan_top"))
@router.message(F.text.lower().in_({"топ клан", "клан топ"}))
async def cmd_clan_top(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    await message.answer(_guild_top_text(5))


@router.message(Command("tops"))
@router.message(F.text.lower().in_({"📊 топ", "топы", "топ"}))
async def cmd_tops_hub(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    msg = await message.answer(_tops_hub_text(), reply_markup=_tops_hub_kb())
    _set_cb_owner(msg.chat.id, msg.message_id, int(message.from_user.id))


@router.message(Command("topdon"))
@router.message(F.text.lower().in_({"топ донат", "топ дон", "💸 топ донат"}))
async def cmd_topdon(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    text = str(message.text or "").strip()
    parts = text.split()
    first = parts[0].lower() if parts else ""
    is_command_mode = first.startswith("/topdon")

    # Текстовые алиасы/кнопка всегда показывают только топ.
    # Режим изменения доступен только через команду /topdon ...
    if not is_command_mode:
        await message.answer(_top_don_text(10))
        return

    # Просто показать топ (команда без аргументов).
    if len(parts) <= 1:
        await message.answer(_top_don_text(10))
        return

    # Изменять топ можно только администрации.
    if not _is_admin(u):
        await message.answer(_top_don_text(10))
        return

    if len(parts) != 3:
        await message.answer(
            "Использование:\n"
            "/topdon [id] [сумма] — установить\n"
            "/topdon [id] [+/-сумма] — изменить"
        )
        return

    try:
        target_id = int(parts[1])
    except Exception:
        await message.answer("❌ Неверный ID.")
        return

    amount_token = str(parts[2]).strip()
    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Игрок не найден.")
        return

    if amount_token.startswith(("+", "-")):
        try:
            delta = int(amount_token)
        except Exception:
            await message.answer("❌ Неверная сумма.")
            return
        new_total, ok2 = db.add_donate_total(target_id, delta)
        if not ok2:
            await message.answer("❌ Не удалось изменить сумму.")
            return
        action_text = f"изменено на {delta:+d}"
    else:
        try:
            new_total = max(0, int(amount_token))
        except Exception:
            await message.answer("❌ Неверная сумма.")
            return
        if not db.set_donate_total(target_id, new_total):
            await message.answer("❌ Не удалось установить сумму.")
            return
        action_text = "установлено"

    nick = _display_name(tu)
    await message.answer(
        f"✅ Топ-донат: {action_text}\n"
        f"👤 Игрок: {nick} (id {target_id})\n"
        f"💸 Сумма: {fmt_num(new_total)} ₽\n\n"
        f"{_top_don_text(10)}"
    )


@router.message(Command("topmon"))
@router.message(F.text.lower().in_({"топ монеты", "топ мон"}))
async def cmd_top_coins(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    await message.answer(_top_coins_text(10))


@router.message(Command("toplvl"))
@router.message(F.text.lower().in_({"топ ур", "топ уровень"}))
async def cmd_top_level(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    await message.answer(_top_level_text(10))


@router.message(Command("topdmg"))
@router.message(F.text.lower().in_({"топ урон"}))
async def cmd_top_damage(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    try:
        await message.answer(_top_damage_text(10))
    except Exception as e:
        log.warning(f"topdmg cmd error: {e}")
        await message.answer("❌ Не удалось построить топ по урону. Попробуй позже.")


@router.message(Command("topkills"))
@router.message(F.text.lower().in_({"топ килл", "топ киллы"}))
async def cmd_top_kills(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    await message.answer(_top_kills_text(10))


@router.callback_query(F.data.startswith("tops:"))
async def cb_tops_hub(cb: CallbackQuery):
    ok, _u = await _check_cb_access(cb)
    if not ok:
        return
    action = str(cb.data or "").split(":", 1)[1]
    if action == "coins":
        await _safe_edit_cb(cb, _top_coins_text(10), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data="tops:back")]]))
        await cb.answer()
        return
    if action == "level":
        await _safe_edit_cb(cb, _top_level_text(10), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data="tops:back")]]))
        await cb.answer()
        return
    if action == "damage":
        try:
            txt = _top_damage_text(10)
        except Exception as e:
            log.warning(f"tops damage error: {e}")
            txt = "❌ Не удалось построить топ по урону. Попробуй еще раз."
        await _safe_edit_cb(cb, txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data="tops:back")]]))
        await cb.answer()
        return
    if action == "kills":
        await _safe_edit_cb(cb, _top_kills_text(10), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data="tops:back")]]))
        await cb.answer()
        return
    if action == "donate":
        await _safe_edit_cb(cb, _top_don_text(10), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data="tops:back")]]))
        await cb.answer()
        return
    if action == "guilds":
        await _safe_edit_cb(cb, _guild_top_text(5), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data="tops:back")]]))
        await cb.answer()
        return
    if action == "back":
        await _safe_edit_cb(cb, _tops_hub_text(), reply_markup=_tops_hub_kb())
        await cb.answer()
        return
    if action == "close":
        await _safe_edit_cb(cb, "📊 Раздел топов закрыт.")
        await cb.answer()
        return
    await cb.answer("Неизвестный раздел", show_alert=True)


@router.message(Command("guild"))
@router.message(F.text.lower().in_({"гильдия", "🏰 гильдия"}))
async def cmd_guild(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    g = db.get_user_guild(tg_id)
    if not g:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Топ", callback_data="guild:top:0")],
        ])
        await message.answer(
            "🏰 Гильдия\n"
            f"{SEP}\n"
            "Ты пока не состоишь в гильдии.\n"
            "Команды:\n"
            "Создать клан [название]\n"
            "Вступить клан [id клана]",
            reply_markup=kb,
        )
        return
    members = db.guild_member_count(int(g["id"]))
    is_owner = _is_guild_owner(g, tg_id)
    is_manager = _is_guild_manager(g, tg_id)
    await message.answer(_guild_panel_text(g, members, is_owner), reply_markup=_guild_panel_kb(int(g["id"]), is_owner, is_manager))


@router.message(F.text.lower().regexp(r"^создать\s+клан\s+.+$"))
async def cmd_guild_create(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    name = message.text.split(maxsplit=2)[2].strip()
    try:
        gid = db.create_guild(int(u["tg_id"]), name)
    except Exception as e:
        await message.answer(f"❌ {e}")
        return
    g = db.get_guild(gid)
    await message.answer(
        f"✅ Гильдия создана!\nID: {gid}\nНазвание: {g['name']}\n"
        "Теперь можно открывать раздел «🏰 Гильдия»."
    )


@router.message(F.text.lower().regexp(r"^вступить\s+клан\s+\d+$"))
async def cmd_guild_join(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    if db.get_user_guild(tg_id):
        await message.answer("❌ Ты уже состоишь в гильдии.")
        return
    gid = int(message.text.strip().split()[-1])
    g = db.get_guild(gid)
    if not g:
        await message.answer("❌ Гильдия не найдена.")
        return
    if int(g["open_join"] or 0):
        cur_members = db.guild_member_count(gid)
        cap = _guild_member_limit(int(g["level"] or 1))
        if cur_members >= cap:
            await message.answer(f"❌ Гильдия заполнена: {cur_members}/{cap}.")
            return
        if not db.add_guild_member(gid, tg_id, role="member"):
            await message.answer("❌ Не удалось вступить в гильдию.")
            return
        await message.answer(f"✅ Ты вступил в гильдию «{g['name']}».")
        return

    created, reason = db.create_join_request(gid, tg_id)
    if not created:
        msg = "❌ Не удалось создать заявку."
        if reason == "already_pending":
            msg = "⏳ Заявка уже отправлена и ожидает решения владельца."
        elif reason == "already_member":
            msg = "❌ Ты уже состоишь в гильдии."
        await message.answer(msg)
        return

    await message.answer("📨 Заявка отправлена владельцу гильдии.")
    try:
        reqs = db.list_join_requests(gid, limit=1)
        req_id = int(reqs[-1]["id"]) if reqs else 0
        if req_id > 0:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Принять", callback_data=f"guild:req:ok:{req_id}:{gid}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"guild:req:no:{req_id}:{gid}"),
            ]])
            await bot_instance.send_message(
                int(g["owner_id"]),
                f"🏰 Новая заявка в гильдию «{g['name']}»\n"
                f"Игрок: {_display_name(u)} (id {tg_id})",
                reply_markup=kb,
            )
    except Exception:
        pass


@router.callback_query(F.data == "guild:close")
async def cb_guild_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "🏰 Раздел гильдии закрыт.")
    await cb.answer()


@router.callback_query(F.data.startswith("guild:top:"))
async def cb_guild_top(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    user_g = db.get_user_guild(int(u["tg_id"]))
    back_row = []
    if gid > 0 and user_g and int(user_g["id"]) == gid:
        back_row = [InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{gid}")]
    elif user_g:
        back_row = [InlineKeyboardButton(text="◀ К моей гильдии", callback_data=f"guild:panel:{int(user_g['id'])}")]
    else:
        back_row = [InlineKeyboardButton(text="✖ Закрыть", callback_data="guild:close")]
    kb = InlineKeyboardMarkup(inline_keyboard=[back_row])
    await _safe_edit_cb(cb, _guild_top_text(5), reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("guild:panel:"))
async def cb_guild_panel(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    if not g:
        await cb.answer("Гильдия не найдена", show_alert=True)
        return
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Ты не состоишь в этой гильдии", show_alert=True)
        return
    members = db.guild_member_count(gid)
    tg_id = int(u["tg_id"])
    is_owner = _is_guild_owner(g, tg_id)
    is_manager = _is_guild_manager(g, tg_id)
    await _safe_edit_cb(cb, _guild_panel_text(g, members, is_owner), reply_markup=_guild_panel_kb(gid, is_owner, is_manager))
    await cb.answer()


@router.callback_query(F.data.startswith("guild:desc:"))
async def cb_guild_desc(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    if not _is_guild_manager(g, int(u["tg_id"])):
        await cb.answer("Только лидер или заместитель", show_alert=True)
        return
    GUILD_PENDING_DESC[int(u["tg_id"])] = gid
    await cb.message.answer("📝 Введи новое описание гильдии (до 600 символов).")
    await cb.answer("Жду описание")


@router.callback_query(F.data.startswith("guild:rename:"))
async def cb_guild_rename(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    if not _is_guild_manager(g, int(u["tg_id"])):
        await cb.answer("Только лидер или заместитель", show_alert=True)
        return
    GUILD_PENDING_NAME[int(u["tg_id"])] = gid
    await cb.message.answer("✏️ Введи новое название гильдии (3-32 символа).")
    await cb.answer("Жду название")


@router.message(lambda m: bool(m.text) and m.from_user is not None and int(m.from_user.id) in GUILD_PENDING_DESC)
async def guild_desc_input(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    gid = GUILD_PENDING_DESC.pop(tg_id, 0)
    g = db.get_guild(gid)
    if not _is_guild_manager(g, tg_id):
        await message.answer("❌ Нет доступа.")
        return
    db.set_guild_description(gid, message.text.strip())
    g2 = db.get_guild(gid)
    await message.answer("✅ Описание обновлено.")
    is_owner = _is_guild_owner(g2, tg_id)
    is_manager = _is_guild_manager(g2, tg_id)
    await message.answer(_guild_panel_text(g2, db.guild_member_count(gid), is_owner), reply_markup=_guild_panel_kb(gid, is_owner, is_manager))


@router.message(lambda m: bool(m.text) and m.from_user is not None and int(m.from_user.id) in GUILD_PENDING_NAME)
async def guild_name_input(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    gid = GUILD_PENDING_NAME.pop(tg_id, 0)
    g = db.get_guild(gid)
    if not _is_guild_manager(g, tg_id):
        await message.answer("❌ Нет доступа.")
        return
    try:
        db.set_guild_name(gid, message.text.strip())
    except Exception as e:
        await message.answer(f"❌ {e}")
        return
    g2 = db.get_guild(gid)
    await message.answer(f"✅ Название обновлено: {g2['name']}")
    is_owner = _is_guild_owner(g2, tg_id)
    is_manager = _is_guild_manager(g2, tg_id)
    await message.answer(_guild_panel_text(g2, db.guild_member_count(gid), is_owner), reply_markup=_guild_panel_kb(gid, is_owner, is_manager))


@router.callback_query(F.data.startswith("guild:settings:"))
async def cb_guild_settings(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    tg_id = int(u["tg_id"])
    if not _is_guild_manager(g, tg_id):
        await cb.answer("Только лидер или заместитель", show_alert=True)
        return
    can_manage_open = _is_guild_owner(g, tg_id)
    await _safe_edit_cb(
        cb,
        f"⚙️ Настройки гильдии\n"
        f"Название: {g['name']}\n"
        f"Режим набора: {'Открытый' if int(g['open_join']) else 'Закрытый'}\n"
        f"Доступ: {'полный' if can_manage_open else 'изменение названия/описания'}",
        reply_markup=_guild_settings_kb(gid, int(g["open_join"] or 0), can_manage_open=can_manage_open),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("guild:setopen:"))
async def cb_guild_setopen(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, val_s = cb.data.split(":")
    gid = int(gid_s)
    val = int(val_s)
    g = db.get_guild(gid)
    if not _is_guild_owner(g, int(u["tg_id"])):
        await cb.answer("Только владелец", show_alert=True)
        return
    db.set_guild_open_join(gid, val)
    await cb.answer("Режим обновлён")
    await cb_guild_panel(cb)


@router.callback_query(F.data.startswith("guild:delask:"))
async def cb_guild_delete_ask(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    if not g or int(g["owner_id"] or 0) != int(u["tg_id"]):
        await cb.answer("Только владелец может удалить гильдию.", show_alert=True)
        return
    txt = (
        "⚠️ Подтверждение удаления гильдии\n"
        f"Название: {g['name']}\n"
        f"ID: {gid}\n\n"
        "Будут удалены: состав, заявки, активный рейд и история гильдии.\n"
        "Действие необратимо."
    )
    await _safe_edit_cb(cb, txt, reply_markup=_guild_delete_confirm_kb(gid))
    await cb.answer()


@router.callback_query(F.data.startswith("guild:delok:"))
async def cb_guild_delete_confirm(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    if not g or int(g["owner_id"] or 0) != int(u["tg_id"]):
        await cb.answer("Только владелец может удалить гильдию.", show_alert=True)
        return
    db.delete_guild(gid)
    await _safe_edit_cb(cb, f"🗑 Гильдия «{g['name']}» удалена.")
    await cb.answer("Гильдия удалена")


@router.callback_query(F.data.startswith("guild:members:"))
async def cb_guild_members(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, page_s = cb.data.split(":")
    gid = int(gid_s)
    page = max(0, int(page_s))
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Нет доступа", show_alert=True)
        return
    total = db.guild_member_count(gid)
    members = db.list_guild_members(gid, limit=10, offset=page * 10)
    await _safe_edit_cb(cb, f"👥 Участники гильдии\nСтраница: {page + 1}\nВсего: {total}", reply_markup=_guild_members_kb(gid, members, page, total))
    await cb.answer()


@router.callback_query(F.data.startswith("guild:member:"))
async def cb_guild_member_info(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, uid_s, page_s = cb.data.split(":")
    gid = int(gid_s)
    uid = int(uid_s)
    page = int(page_s)
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Нет доступа", show_alert=True)
        return
    tu = db.get_user(uid)
    if not tu:
        await cb.answer("Игрок не найден", show_alert=True)
        return
    nick = _display_name(tu)
    uname = str(tu["username"] or "")
    g = db.get_guild(gid)
    is_owner_viewer = bool(g and int(g["owner_id"] or 0) == int(u["tg_id"]))
    is_self = int(u["tg_id"]) == uid
    target_member = db.get_guild_member(gid, uid)
    role_name = "Участник"
    if target_member:
        rk = str(target_member["role"])
        role_name = "Лидер" if rk == "owner" else ("Заместитель" if rk == "deputy" else "Участник")
    text = (
        f"👤 Участник\n"
        f"Ник: {nick}\n"
        f"Юз: @{uname if uname else 'нет'}\n"
        f"ID: {uid}\n"
        f"Роль: {role_name}\n"
        f"VIP: {VIP_NAMES.get(int(tu['vip_lvl'] or 0), 'Нет')}\n"
        f"Арена: {tu['arena']}"
    )
    rows = []
    if is_owner_viewer and target_member and str(target_member["role"]) != "owner":
        if str(target_member["role"]) == "deputy":
            rows.append([InlineKeyboardButton(text="⬇️ Снять заместителя", callback_data=f"guild:deputy:off:{gid}:{uid}:{page}")])
        else:
            rows.append([InlineKeyboardButton(text="🛡 Назначить заместителем", callback_data=f"guild:deputy:on:{gid}:{uid}:{page}")])
        rows.append([InlineKeyboardButton(text="👑 Передать владельца", callback_data=f"guild:owner:{gid}:{uid}:{page}")])
        rows.append([InlineKeyboardButton(text="🚫 Изгнать", callback_data=f"guild:kick:{gid}:{uid}:{page}")])
    if is_self and target_member and str(target_member["role"]) != "owner":
        rows.append([InlineKeyboardButton(text="🚪 Выйти", callback_data=f"guild:leave:{gid}:{page}")])
    rows.append([InlineKeyboardButton(text="◀ К участникам", callback_data=f"guild:members:{gid}:{page}")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await _safe_edit_cb(cb, text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("guild:kick:"))
async def cb_guild_kick_member(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, uid_s, page_s = cb.data.split(":")
    gid = int(gid_s)
    target_uid = int(uid_s)
    page = int(page_s)

    g = db.get_guild(gid)
    if not g or int(g["owner_id"] or 0) != int(u["tg_id"]):
        await cb.answer("Только владелец может изгонять.", show_alert=True)
        return
    if target_uid == int(u["tg_id"]):
        await cb.answer("Нельзя изгнать себя. Используй «Выйти».", show_alert=True)
        return

    member = db.get_guild_member(gid, target_uid)
    if not member:
        await cb.answer("Игрок уже не в гильдии.", show_alert=True)
        return
    if str(member["role"]) == "owner":
        await cb.answer("Нельзя изгнать владельца.", show_alert=True)
        return

    if not db.remove_guild_member(gid, target_uid):
        await cb.answer("Не удалось изгнать игрока.", show_alert=True)
        return

    try:
        await bot_instance.send_message(target_uid, f"🚫 Ты изгнан из гильдии «{g['name']}».")
    except Exception:
        pass

    total = db.guild_member_count(gid)
    members = db.list_guild_members(gid, limit=10, offset=page * 10)
    if not members and page > 0:
        page = max(0, page - 1)
        members = db.list_guild_members(gid, limit=10, offset=page * 10)
    await _safe_edit_cb(
        cb,
        f"👥 Участники гильдии\nСтраница: {page + 1}\nВсего: {total}",
        reply_markup=_guild_members_kb(gid, members, page, total),
    )
    await cb.answer("Игрок изгнан")


@router.callback_query(F.data.startswith("guild:deputy:"))
async def cb_guild_toggle_deputy(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, mode, gid_s, uid_s, page_s = cb.data.split(":")
    gid = int(gid_s)
    target_uid = int(uid_s)
    page = int(page_s)

    g = db.get_guild(gid)
    if not _is_guild_owner(g, int(u["tg_id"])):
        await cb.answer("Только лидер может назначать заместителя.", show_alert=True)
        return
    if target_uid == int(u["tg_id"]):
        await cb.answer("Себе роль менять нельзя.", show_alert=True)
        return
    target = db.get_guild_member(gid, target_uid)
    if not target:
        await cb.answer("Игрок уже не в гильдии.", show_alert=True)
        return
    if str(target["role"]) == "owner":
        await cb.answer("Нельзя менять роль владельца.", show_alert=True)
        return

    new_role = "deputy" if mode == "on" else "member"
    if not db.set_guild_member_role(gid, target_uid, new_role):
        await cb.answer("Не удалось обновить роль.", show_alert=True)
        return

    total = db.guild_member_count(gid)
    members = db.list_guild_members(gid, limit=10, offset=page * 10)
    await _safe_edit_cb(
        cb,
        f"👥 Участники гильдии\nСтраница: {page + 1}\nВсего: {total}",
        reply_markup=_guild_members_kb(gid, members, page, total),
    )
    await cb.answer("Роль обновлена")


@router.callback_query(F.data.startswith("guild:leave:"))
async def cb_guild_leave_self(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, page_s = cb.data.split(":")
    gid = int(gid_s)
    page = int(page_s)
    tg_id = int(u["tg_id"])

    member = db.get_guild_member(gid, tg_id)
    if not member:
        await cb.answer("Ты уже не в гильдии.", show_alert=True)
        return
    if str(member["role"]) == "owner":
        await cb.answer("Владелец не может выйти из своей гильдии.", show_alert=True)
        return

    if not db.remove_guild_member(gid, tg_id):
        await cb.answer("Не удалось выйти из гильдии.", show_alert=True)
        return

    g = db.get_guild(gid)
    total = db.guild_member_count(gid)
    members = db.list_guild_members(gid, limit=10, offset=page * 10)
    if not members and page > 0:
        page = max(0, page - 1)
        members = db.list_guild_members(gid, limit=10, offset=page * 10)
    await _safe_edit_cb(
        cb,
        f"✅ Ты вышел из гильдии «{g['name'] if g else gid}».\n"
        f"\n👥 Участники гильдии\nСтраница: {page + 1}\nВсего: {total}",
        reply_markup=_guild_members_kb(gid, members, page, total),
    )
    await cb.answer("Выход выполнен")


@router.callback_query(F.data.startswith("guild:owner:"))
async def cb_guild_transfer_owner(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, uid_s, page_s = cb.data.split(":")
    gid = int(gid_s)
    target_uid = int(uid_s)
    page = int(page_s)

    g = db.get_guild(gid)
    if not g or int(g["owner_id"] or 0) != int(u["tg_id"]):
        await cb.answer("Только текущий владелец может передать гильдию.", show_alert=True)
        return
    if target_uid == int(u["tg_id"]):
        await cb.answer("Ты уже владелец.", show_alert=True)
        return

    member = db.get_guild_member(gid, target_uid)
    if not member:
        await cb.answer("Игрок уже не состоит в гильдии.", show_alert=True)
        return

    done, reason = db.transfer_guild_owner(gid, target_uid)
    if not done:
        msg = {
            "guild_not_found": "Гильдия не найдена.",
            "already_owner": "Этот игрок уже владелец.",
            "target_not_member": "Игрок не состоит в гильдии.",
            "owner_not_member": "Текущий владелец не найден в составе.",
        }.get(reason, "Не удалось передать владение.")
        await cb.answer(msg, show_alert=True)
        return

    g2 = db.get_guild(gid)
    total = db.guild_member_count(gid)
    members = db.list_guild_members(gid, limit=10, offset=page * 10)
    if not members and page > 0:
        page = max(0, page - 1)
        members = db.list_guild_members(gid, limit=10, offset=page * 10)
    await _safe_edit_cb(
        cb,
        f"✅ Владелец передан игроку {target_uid}.\n"
        f"\n👥 Участники гильдии «{g2['name'] if g2 else gid}»\nСтраница: {page + 1}\nВсего: {total}",
        reply_markup=_guild_members_kb(gid, members, page, total),
    )
    await cb.answer("Владелец передан")

    if bot_instance is not None:
        try:
            await bot_instance.send_message(target_uid, f"👑 Ты стал владельцем гильдии «{g2['name'] if g2 else gid}».")
        except Exception:
            pass


@router.callback_query(F.data.startswith("guild:reqs:"))
async def cb_guild_reqs(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    if not g or int(g["owner_id"]) != int(u["tg_id"]):
        await cb.answer("Только владелец", show_alert=True)
        return
    reqs = db.list_join_requests(gid, limit=30)
    if not reqs:
        await _safe_edit_cb(cb, "📥 Нет активных заявок.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{gid}")]]))
        await cb.answer()
        return
    await _safe_edit_cb(cb, "📥 Заявки в гильдию:", reply_markup=_guild_requests_kb(gid, reqs))
    await cb.answer()


@router.callback_query(F.data.startswith("guild:req:"))
async def cb_guild_req_resolve(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, act, req_id_s, gid_s = cb.data.split(":")
    req_id = int(req_id_s)
    gid = int(gid_s)
    g = db.get_guild(gid)
    if not g or int(g["owner_id"]) != int(u["tg_id"]):
        await cb.answer("Только владелец", show_alert=True)
        return
    cur_members = db.guild_member_count(gid)
    cap = _guild_member_limit(int(g["level"] or 1))
    if act == "ok" and cur_members >= cap:
        await cb.answer(f"Лимит участников: {cur_members}/{cap}", show_alert=True)
        return
    success, _gid, user_id = db.resolve_join_request(req_id, approve=(act == "ok"))
    if not success:
        await cb.answer("Не удалось обработать заявку", show_alert=True)
        return
    try:
        await bot_instance.send_message(int(user_id), "✅ Твоя заявка в гильдию одобрена!" if act == "ok" else "❌ Твоя заявка в гильдию отклонена.")
    except Exception:
        pass
    await cb.answer("Готово")
    reqs = db.list_join_requests(gid, limit=30)
    if not reqs:
        await _safe_edit_cb(cb, "📥 Нет активных заявок.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀ Назад", callback_data=f"guild:panel:{gid}")]]))
    else:
        await _safe_edit_cb(cb, "📥 Заявки в гильдию:", reply_markup=_guild_requests_kb(gid, reqs))


@router.callback_query(F.data.startswith("guild:upgrade:"))
async def cb_guild_upgrade(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    g = db.get_guild(gid)
    if not g:
        await cb.answer("Гильдия не найдена", show_alert=True)
        return
    if int(g["owner_id"]) != int(u["tg_id"]):
        await cb.answer("Только владелец может качать гильдию", show_alert=True)
        return
    lvl = int(g["level"] or 1)
    if lvl >= 5:
        await cb.answer("Уже максимальный уровень", show_alert=True)
        return
    need = _guild_level_cost(lvl + 1)
    have = int(g["unity_shards"] or 0)
    ok_up, reason = db.guild_upgrade(gid, need, lvl + 1)
    if not ok_up:
        if reason == "not_enough":
            await cb.answer(f"Недостаточно 🛡: нужно {need}, есть {have}", show_alert=True)
        else:
            await cb.answer("Не удалось улучшить", show_alert=True)
        return
    g2 = db.get_guild(gid)
    await _safe_edit_cb(
        cb,
        f"⬆️ Гильдия улучшена до уровня {g2['level']}!\n"
        f"Новые баффы: {_guild_level_buff_text(int(g2['level']))}",
        reply_markup=_guild_panel_kb(gid, True, True),
    )
    await cb.answer("Улучшение выполнено")


def _guild_boss_unity_reward(arena: int, reward: int) -> int:
    # Базовый дроп с множителем по мирам.
    # 1-10 арены: x1.7, 11-15 арены: x2.5.
    base = max(8, int((reward ** 0.45) * (1 + arena * 0.08)))
    old_drop = max(1, base // 21)
    mult = 2.5 if int(arena) >= 11 else 1.7
    return max(1, int(round(old_drop * mult)))


def _guild_boss_coin_pool(arena: int, reward: int) -> int:
    # Общий пул монет, который делится по вкладу в урон.
    # Нерф дропа рейда: итоговый пул в 3 раза ниже текущего.
    base = max(1000, int(reward * (2.4 + arena * 0.22)))
    return max(150, int(base * 0.5 / 3))


async def _guild_boss_fail(gb: GuildBattleState, reason: str) -> tuple[int, int]:
    """Завершает рейд поражением и возвращает (loss, loss_pct)."""
    loss = 0
    loss_pct = 0
    g = db.get_guild(gb.guild_id)
    if g:
        have = int(g["unity_shards"] or 0)
        loss_pct = random.randint(10, 15)
        loss = int(have * loss_pct / 100)
        db.guild_add_unity(gb.guild_id, -loss)
    _drop_guild_battle_state(gb.guild_id)
    db.guild_clear_boss_hits(gb.guild_id)
    GUILD_ACTIVE_BATTLES.pop(gb.guild_id, None)
    GUILD_BOSS_LOCKS.pop(gb.guild_id, None)
    return int(loss), int(loss_pct)


@router.callback_query(F.data.startswith("guild:boss:"))
async def cb_guild_boss_open(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Ты не в этой гильдии", show_alert=True)
        return
    gb = GUILD_ACTIVE_BATTLES.get(gid)
    if gb:
        await _safe_edit_cb(cb, _guild_boss_view(gb), reply_markup=_guild_boss_kb(gid))
        await cb.answer()
        return
    await _safe_edit_cb(cb, "⚔️ Выбери арену гильд-босса:", reply_markup=_guild_boss_arena_kb(gid))
    await cb.answer()


@router.callback_query(F.data.startswith("guild:bossarena:"))
async def cb_guild_boss_arena(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, arena_s = cb.data.split(":")
    gid = int(gid_s)
    arena = int(arena_s)
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Ты не в этой гильдии", show_alert=True)
        return
    if arena < 1 or arena > gd.max_arena():
        await cb.answer("Арена не найдена.", show_alert=True)
        return
    await _safe_edit_cb(cb, f"⚔️ Арена {arena}: {gd.arena_title(arena)}\nВыбери босса:", reply_markup=_guild_boss_pick_kb(gid, arena))
    await cb.answer()


@router.callback_query(F.data.startswith("guild:bosspick:"))
async def cb_guild_boss_pick(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, _, gid_s, arena_s, idx_s = cb.data.split(":")
    gid = int(gid_s)
    arena = int(arena_s)
    idx = int(idx_s)
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Ты не в этой гильдии", show_alert=True)
        return
    actor_id = int(u["tg_id"])
    can_start = _is_guild_owner(user_g, actor_id) or _is_guild_deputy(gid, actor_id)
    if not can_start:
        await cb.answer("Только лидер или заместитель может запускать рейд", show_alert=True)
        return
    if gid in GUILD_ACTIVE_BATTLES:
        gb = GUILD_ACTIVE_BATTLES[gid]
        await _safe_edit_cb(cb, _guild_boss_view(gb), reply_markup=_guild_boss_kb(gid))
        await cb.answer("Бой уже запущен")
        return

    last_day = db.get_guild_boss_cooldown(gid, arena, idx)
    if _guild_boss_on_cooldown(last_day):
        left = _guild_boss_cooldown_left_text(last_day)
        msg = "Этот гильд-босс на КД."
        if left:
            msg += f" Осталось: {left}."
        await cb.answer(msg, show_alert=True)
        return

    boss = gd.ARENAS[arena][idx]
    hp = int(boss.hp * _enemy_hp_mult(arena) * GUILD_BOSS_HP_MULT)
    reward_unity = _guild_boss_unity_reward(arena, int(boss.reward))
    gb = GuildBattleState(gid, arena, idx, boss.name, hp, hp, reward_unity, cb.message.message_id, cb.message.chat.id)
    GUILD_ACTIVE_BATTLES[gid] = gb
    db.guild_clear_boss_hits(gid)
    _persist_guild_battle_state(gb)
    await _safe_edit_cb(cb, _guild_boss_view(gb), reply_markup=_guild_boss_kb(gid))

    # Оповещаем сокланов о старте рейда.
    try:
        guild = db.get_guild(gid)
        member_ids = db.list_guild_member_ids(gid)
        text = (
            f"🏰 Гильдия «{guild['name']}» начала рейд!\n"
            f"👾 Босс: {boss.name}\n"
            f"⚔️ Зайди в «Гильдия -> Гильд-босс» и помоги ударом."
        )
        for mid in member_ids:
            if int(mid) == int(u["tg_id"]):
                continue
            if not _notify_enabled(int(mid), NOTIFY_GUILD_BOSS_KEY):
                continue
            try:
                await bot_instance.send_message(int(mid), text)
            except Exception:
                pass
    except Exception:
        pass

    await cb.answer("Гильд-босс начат")


@router.callback_query(F.data.startswith("guild:bosspick_locked"))
async def cb_guild_boss_pick_locked(cb: CallbackQuery):
    parts = str(cb.data or "").split(":")
    if len(parts) >= 5 and parts[2].isdigit() and parts[3].isdigit() and parts[4].isdigit():
        gid = int(parts[2])
        arena = int(parts[3])
        idx = int(parts[4])
        cd_key = db.get_guild_boss_cooldown(gid, arena, idx)
        left = _guild_boss_cooldown_left_text(cd_key)
        if left:
            await cb.answer(f"Этот гильд-босс на КД. Осталось: {left}", show_alert=True)
            return
    await cb.answer("Этот гильд-босс сейчас на КД.", show_alert=True)


@router.callback_query(F.data.startswith("guild:bossatk:"))
async def cb_guild_boss_attack(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Ты не в этой гильдии", show_alert=True)
        return
    lock = GUILD_BOSS_LOCKS.get(gid)
    if lock is None:
        lock = asyncio.Lock()
        GUILD_BOSS_LOCKS[gid] = lock

    async with lock:
        gb = GUILD_ACTIVE_BATTLES.get(gid)
        if not gb:
            row = db.get_active_guild_battle(gid)
            if not row:
                await cb.answer("Нет активного рейда", show_alert=True)
                return
            gb = GuildBattleState(
                guild_id=gid,
                arena=int(row["arena"]),
                boss_idx=int(row["boss_idx"]),
                boss_name=str(row["boss_name"]),
                boss_hp=int(row["boss_hp"]),
                boss_max_hp=int(row["boss_max_hp"]),
                reward_base=int(row["reward_base"]),
                msg_id=int(row["msg_id"]),
                chat_id=int(row["chat_id"]),
            )
            gb.started_at = int(row["started_at"])
            gb.updated_at = int(row["updated_at"])
            GUILD_ACTIVE_BATTLES[gid] = gb

        if int(time.time()) - int(gb.started_at) >= GUILD_BOSS_TIMEOUT_SEC:
            loss, loss_pct = await _guild_boss_fail(gb, "timeout")
            await _safe_edit_cb(
                cb,
                f"☠️ Гильдия проиграла рейд (время вышло).\n"
                f"Из хранилища списано: {fmt_num(loss)} 🛡 ({loss_pct}%).",
            )
            await cb.answer("Рейд завершён поражением")
            return

        dmg = _calc_player_damage(u)
        if int(gb.arena) >= 11:
            dmg = max(1, int(dmg * 0.95))
        crit = random.random() < 0.02
        if crit:
            dmg = int(dmg * 1.5)
        gb.boss_hp = max(0, int(gb.boss_hp) - int(dmg))
        db.guild_add_boss_hit(gid, int(u["tg_id"]), int(dmg))
        gb.updated_at = int(time.time())

        if gb.boss_hp <= 0:
            unlock_at = (_now_msk() + timedelta(days=GUILD_BOSS_CD_DAYS)).replace(second=0, microsecond=0)
            db.set_guild_boss_cooldown(gid, gb.arena, gb.boss_idx, unlock_at.isoformat(timespec="minutes"))
            db.guild_add_unity(gid, gb.reward_base)

            # Делим монеты между участниками по доле нанесенного урона.
            hit_rows = db.guild_list_boss_hits(gid)
            total_dmg = sum(max(0, int(r["damage"] or 0)) for r in hit_rows)
            boss_reward = int(gd.ARENAS[gb.arena][gb.boss_idx].reward)
            coin_pool = _guild_boss_coin_pool(gb.arena, boss_reward)
            payouts: list[str] = []
            if total_dmg > 0:
                for r in hit_rows:
                    uid = int(r["tg_id"])
                    dmg_part = max(0, int(r["damage"] or 0))
                    if dmg_part <= 0:
                        continue
                    gain = max(1, int(coin_pool * (dmg_part / total_dmg)))
                    tu = db.get_user(uid)
                    if not tu:
                        continue
                    db.update_user(uid, coins=int(tu["coins"] or 0) + gain)
                    nick = str(r["nickname"] or "").strip() or str(r["username"] or f"id{uid}")
                    pct = int((dmg_part / total_dmg) * 100)
                    payouts.append(f"• {nick}: +{fmt_num(gain)} 🪙 ({pct}% урона)")
                    try:
                        await bot_instance.send_message(uid, f"🏰 Награда за рейд: +{fmt_num(gain)} 🪙")
                    except Exception:
                        pass

            db.guild_clear_boss_hits(gid)
            _drop_guild_battle_state(gid)
            GUILD_ACTIVE_BATTLES.pop(gid, None)
            GUILD_BOSS_LOCKS.pop(gid, None)
            payout_text = "\n".join(payouts[:12]) if payouts else "• Нет зарегистрированных ударов"
            await _safe_edit_cb(
                cb,
                f"🏆 Победа гильдии над {gb.boss_name}!\n"
                f"🛡 В хранилище: +{fmt_num(gb.reward_base)} Осколков единства\n"
                f"🪙 Пул монет рейда: {fmt_num(coin_pool)}\n"
                f"{payout_text}",
            )
            await cb.answer("Победа")
            return

        db.update_active_guild_battle_hp(gid, gb.boss_hp)
        log_line = f"\n\n⚔️ Удар: -{fmt_num(dmg)}{' (крит)' if crit else ''}"
        await _safe_edit_cb(cb, _guild_boss_view(gb) + log_line, reply_markup=_guild_boss_kb(gid))
        await cb.answer()


@router.callback_query(F.data.startswith("guild:bossref:"))
async def cb_guild_boss_refresh(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    gid = int(cb.data.split(":")[-1])
    user_g = db.get_user_guild(int(u["tg_id"]))
    if not user_g or int(user_g["id"]) != gid:
        await cb.answer("Ты не в этой гильдии", show_alert=True)
        return
    gb = GUILD_ACTIVE_BATTLES.get(gid)
    if not gb:
        row = db.get_active_guild_battle(gid)
        if not row:
            await cb.answer("Нет активного рейда", show_alert=True)
            return
        gb = GuildBattleState(gid, int(row["arena"]), int(row["boss_idx"]), str(row["boss_name"]), int(row["boss_hp"]), int(row["boss_max_hp"]), int(row["reward_base"]), int(row["msg_id"]), int(row["chat_id"]))
        gb.started_at = int(row["started_at"])
        gb.updated_at = int(row["updated_at"])
        GUILD_ACTIVE_BATTLES[gid] = gb
    if int(time.time()) - int(gb.started_at) >= GUILD_BOSS_TIMEOUT_SEC:
        loss, loss_pct = await _guild_boss_fail(gb, "timeout")
        await _safe_edit_cb(
            cb,
            f"☠️ Гильдия проиграла рейд (время вышло).\n"
            f"Из хранилища списано: {fmt_num(loss)} 🛡 ({loss_pct}%).",
        )
        await cb.answer("Рейд завершён")
        return
    await _safe_edit_cb(cb, _guild_boss_view(gb), reply_markup=_guild_boss_kb(gid))
    await cb.answer()



# ─────────────────────────────────────────────
#  ПРОФИЛЬ
# ─────────────────────────────────────────────
@router.message(Command("profile"))
@router.message(F.text.lower().in_({"проф", "профиль"}))
async def cmd_profile(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    invoker_id = int(u["tg_id"])
    target_id = invoker_id

    # Просмотр другого профиля в чате доступен с VIP S+ только по reply.
    if message.reply_to_message and message.reply_to_message.from_user:
        reply_id = int(message.reply_to_message.from_user.id)
        if reply_id != invoker_id:
            can_view_other = _is_admin(u) or int(u["vip_lvl"] or 0) >= 4
            if not can_view_other:
                await message.answer("❌ Просмотр чужого профиля доступен с привилегии S и выше (только reply в чате).")
                return
            target_id = reply_id

    # По ID через /profile может смотреть только администратор/создатель.
    parts = (message.text or "").strip().split()
    if len(parts) >= 2:
        if not _is_admin(u):
            await message.answer("❌ По ID профиль может смотреть только администратор. Используй reply на сообщение игрока.")
            return
        try:
            target_id = int(parts[1])
        except Exception:
            await message.answer("❌ Неверный ID игрока.")
            return

    tu = db.get_user(target_id)
    if not tu:
        await message.answer("Игрок не найден.")
        return

    # Проверяем скрытый профиль — только при просмотре чужого
    if target_id != invoker_id and not _is_admin(u):
        if _profile_hidden(target_id):
            nick = _display_name(tu)
            arena = int(tu["arena"] or 1)
            await _reply(message,
                f"🔒 Профиль скрыт\n"
                f"👤 {escape(nick)}\n"
                f"🏟 Арена: {arena}",
                parse_mode=ParseMode.HTML,
            )
            return

    text = _profile_text(tu, is_admin_view=False)
    await _reply(message, text, parse_mode=ParseMode.HTML)


@router.message(Command("check"))
@router.message(F.text.lower() == "чек")
@router.message(F.text.lower().startswith("чек "))
async def cmd_profile_check(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        await message.answer("❌ Команда доступна только администрации.")
        return

    raw = (message.text or "").strip()
    parts = raw.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("Использование: /check [id|ник]")
        return
    needle = parts[1].strip()

    if needle.isdigit():
        tu = db.get_user(int(needle))
        if not tu:
            await message.answer("❌ Игрок не найден.")
            return
        text = _profile_text(tu, is_admin_view=False)
        await message.answer(text, parse_mode=ParseMode.HTML)
        return

    rows = db.find_users_by_nickname_or_username(needle, limit=10)
    if not rows:
        await message.answer("❌ Совпадений не найдено.")
        return
    if len(rows) == 1:
        text = _profile_text(rows[0], is_admin_view=False)
        await message.answer(text, parse_mode=ParseMode.HTML)
        return

    lines = [f"🔎 Найдено совпадений: {len(rows)}", SEP]
    for r in rows:
        nick = str(_row_get(r, "nickname", "") or "").strip()
        uname = str(_row_get(r, "username", "") or "").strip()
        shown = nick if nick else (f"@{uname}" if uname else f"id{int(r['tg_id'])}")
        lines.append(f"• {shown} | id {int(r['tg_id'])}")
    lines.append(SEP)
    lines.append("Открой профиль: /profile ID")
    await message.answer("\n".join(lines))


@router.message(Command("balance"))
@router.message(F.text.lower().in_({"б", "баланс"}))
async def cmd_balance(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return

    target = u
    if message.reply_to_message and message.reply_to_message.from_user:
        ru = db.get_user(int(message.reply_to_message.from_user.id))
        if ru:
            target = ru

    nick = _display_name(target)
    text = (
        f"💰 Баланс игрока {nick}\n"
        f"🪙: {fmt_num(int(target['coins'] or 0))}\n"
        f"🔯: {fmt_num(int(target['magic_coins'] or 0))}\n"
        f"💠: {fmt_num(int(target['essence'] or 0))}"
    )
    await _reply(message, text)


# /prof [id|reply]
@router.message(Command("prof"))
async def cmd_prof_admin(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except Exception:
                pass
    if not target_id:
        await message.answer("Использование: /prof [id] или ответ на сообщение.")
        return
    tu = db.get_user(target_id)
    if not tu:
        await message.answer("Игрок не найден.")
        return
    text = _profile_text(tu, is_admin_view=True)
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⚙️ Админ действия", callback_data=f"prof_adm:{target_id}"),
    ]])
    await message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)


@router.callback_query(F.data.startswith("prof_adm:"))
async def cb_prof_adm(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        await cb.answer("Нет доступа.", show_alert=True)
        return
    target_id = int(cb.data.split(":")[1])
    ADMIN_CTX[cb.from_user.id] = {"action": "-", "value": "-", "target_id": target_id}
    await _safe_edit_cb(cb, _admin_view_text(ADMIN_CTX[cb.from_user.id]), reply_markup=_admin_kb())
    await cb.answer()


# ─────────────────────────────────────────────
#  СМЕНА НИКА
# ─────────────────────────────────────────────

_NICK_RE = re.compile(r"^[A-Za-zА-ЯЁа-яё0-9_]{3,16}$")


def _nick_valid(nick: str) -> tuple[bool, str]:
    """
    Возвращает (True, '') если ник валиден, иначе (False, причина).
    Правила: 3–16 символов, только рус/англ буквы, цифры и _, без пробелов.
    """
    if not nick:
        return False, "Ник не может быть пустым."
    if " " in nick:
        return False, "Ник не должен содержать пробелы."
    if not _NICK_RE.match(nick):
        return False, "Только рус/англ буквы, цифры и _ (3–16 символов)."
    return True, ""


@router.message(F.text.lower().regexp(r"^\s*сменить\s+ник\s*$"))
async def cmd_nick_prompt(message: Message):
    ok, _u = await _check_access(message)
    if not ok:
        return
    NICK_PENDING.add(message.from_user.id)
    await message.answer(
        "✏️ Введи новый ник (3–16 символов)\n"
        "Допустимы: рус/англ буквы, цифры, _ (без пробелов)\n"
        "Для отмены напиши: отмена"
    )


@router.message(lambda m: bool(m.text) and m.from_user is not None and m.from_user.id in NICK_PENDING)
async def handle_nick_input(message: Message):
    tg_id = message.from_user.id
    if tg_id not in NICK_PENDING:
        return
    nick = (message.text or "").strip()
    if nick.lower() in {"отмена", "cancel"}:
        NICK_PENDING.discard(tg_id)
        await message.answer("❎ Смена ника отменена.")
        return

    valid, reason = _nick_valid(nick)
    if not valid:
        await message.answer(
            f"❌ {reason}\n"
            "Только рус/англ буквы, цифры и _ (3–16 символов), без пробелов.\n"
            "Попробуй снова или напиши «отмена»."
        )
        return

    if db.is_nickname_taken(nick, exclude_tg_id=tg_id):
        await message.answer(
            f"❌ Ник «{escape(nick)}» уже занят. Выбери другой или напиши «отмена»."
        )
        return

    NICK_PENDING.discard(tg_id)
    db.update_user(tg_id, nickname=nick)
    await message.answer(f"✅ Ник изменён на: <code>{escape(nick)}</code>", parse_mode=ParseMode.HTML)


# ─────────────────────────────────────────────
#  ТРЕНИРОВКА
# ─────────────────────────────────────────────
@router.message(Command("train"))
@router.message(F.text.lower().in_({"трен", "тренировка", "🥊 тренировка"}))
async def cmd_train(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    now = int(time.time())
    tg_id = int(u["tg_id"])

    if int(u["training_active"] or 0):
        started = int(u["last_train_time"] or now)
        dur_cap = _train_duration_secs(u)
        elapsed = (now - started) if dur_cap <= 0 else min(now - started, dur_cap)
        tick_sec = _train_tick_seconds(u)
        ticks = elapsed // tick_sec
        pet_mult = 1 + (_player_pet_bonus(u) // 20)
        power_gain = int(
            ticks
            * pet_mult
            * _aura_power_mult(u)
            * _train_power_mult(u)
            * _true_train_power_mult(u)
            * _bio_train_power_mult(u)
            * TRAIN_POWER_GAIN_MULT
        )
        hunter = _aura_hunter(u)
        cases_got = {"common": 0, "rare": 0, "epic": 0, "legendary": 0, "mythic": 0}
        chance_map = {
            "common": 0.04,
            "rare": 0.018,
            "epic": 0.003,
            "legendary": 0.0003,
            "mythic": 0.00012,
        }
        if int(u["vip_lvl"] or 0) >= 4:
            chance_map["mythic"] = max(chance_map["mythic"], 0.0012)
        if hunter:
            chance_map = {k: v * 2 for k, v in chance_map.items()}
        guild_case_mult = _guild_case_mult(u)
        chance_map = {k: v * guild_case_mult for k, v in chance_map.items()}
        chance_map = {k: v * _train_case_mult(u) for k, v in chance_map.items()}
        chance_map = {k: v * _true_train_case_mult(u) for k, v in chance_map.items()}
        chance_map = {k: v * _bio_train_case_mult(u) for k, v in chance_map.items()}
        chance_map = {k: v * (1.0 + float(_artifact_effects(u).get("afk_case_chance", 0.0))) for k, v in chance_map.items()}
        for _ in range(ticks):
            for key, chance in chance_map.items():
                if random.random() < chance:
                    cases_got[key] += 1
        db.update_user(tg_id,
                       power=int(u["power"] or 0) + power_gain,
                       training_active=0,
                       training_until=0,
                       )
        for key, cnt in cases_got.items():
            if cnt > 0:
                db.add_case_count(tg_id, key, cnt)
        mins = elapsed // 60
        secs = elapsed % 60
        case_lines = [f"  {gd.AFK_CASES[k]}: +{v}" for k, v in cases_got.items() if v > 0]
        text = (
            f"⚙️ Тренировка завершена\n"
            f"⏱ Время: {mins}м {secs}с\n"
            f"⚙️ Получено мощности: +{fmt_num(power_gain)}\n"
            f"🕒 Тик тренировки: {tick_sec}с\n"
        )
        if case_lines:
            text += "📦 Кейсы:\n" + "\n".join(case_lines)
        await _reply(message, text)
        return

    dur_cap = _train_duration_secs(u)
    training_until = 0 if dur_cap <= 0 else now + dur_cap
    db.update_user(
        tg_id,
        training_active=1,
        last_train_time=now,
        training_until=training_until,
    )
    db.set_stat_value(tg_id, VIP_TRAIN_NOTIFY_MARK_KEY, 0)
    if dur_cap <= 0:
        dur_text = "∞ (до ручного завершения)"
    else:
        dur_text = f"до {dur_cap // 3600}ч {(dur_cap % 3600) // 60}м"
    await _reply(
        message,
        "🏋️ Тренировка начата!\n"
        f"Длительность: {dur_text}.\n"
        "Чтобы завершить и забрать награды — снова напиши «трен»."
    )


# ─────────────────────────────────────────────
#  КЕЙСЫ — ПОКАЗ
# ─────────────────────────────────────────────
@router.message(Command("cases"))
@router.message(F.text.lower().in_({"к", "кейсы", "📦 кейсы"}))
async def cmd_cases(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    text = _cases_text(u)
    kb = None
    if message.chat.type == ChatType.PRIVATE:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🛒 Купить кейсы", callback_data="shop_open"),
        ]])
    await _reply(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)


@router.callback_query(F.data == "shop_open")
async def cb_shop_open(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    arena = int(u["arena"] or 1)
    await _safe_edit_cb(cb, "🛒 Магазин кейсов\nВыбери арену:", reply_markup=_shop_arena_kb(arena))
    await cb.answer()


@router.callback_query(F.data == "shop_back")
async def cb_shop_back(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    arena = int(u["arena"] or 1)
    await _safe_edit_cb(cb, "🛒 Магазин кейсов\nВыбери арену:", reply_markup=_shop_arena_kb(arena))
    await cb.answer()


@router.callback_query(F.data == "shop_close")
async def cb_shop_close(cb: CallbackQuery):
    SHOP_CUSTOM_CTX.pop(int(cb.from_user.id), None)
    await _safe_edit_cb(cb, "🛒 Магазин закрыт.")
    await cb.answer()


@router.callback_query(F.data.startswith("shop_arena:"))
async def cb_shop_arena(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    arena = int(cb.data.split(":")[1])
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        await cb.answer("🔒 Эта арена ещё не открыта.", show_alert=True)
        return
    price = _case_price(arena)
    text = (
        f"🛒 Арена {arena}: {gd.arena_title(arena)}\n"
        f"Цена: {fmt_num(price)} 🪙 за кейс\n"
        f"Твои монеты: {fmt_num(int(u['coins'] or 0))} 🪙"
    )
    await _safe_edit_cb(cb, text, reply_markup=_shop_buy_kb(arena))
    await cb.answer()


@router.callback_query(F.data.startswith("shop_buy:"))
async def cb_shop_buy(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, item_type, arena_s, qty_s = cb.data.split(":")
    arena = int(arena_s)
    qty = int(qty_s)
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        await cb.answer("🔒 Арена не открыта.", show_alert=True)
        return
    if qty >= SHOP_CONFIRM_QTY:
        await _safe_edit_cb(cb, _shop_confirm_text(u, item_type, arena, qty), reply_markup=_shop_confirm_kb(item_type, arena, qty))
        await cb.answer()
        return

    ok_buy, msg = _shop_apply_purchase(u, item_type, arena, qty)
    if not ok_buy:
        await cb.answer(msg, show_alert=True)
        return
    await cb.answer(msg, show_alert=True)
    tg_id = int(u["tg_id"])
    u2 = db.get_user(tg_id) or u
    await _safe_edit_cb(
        cb,
        f"🛒 Арена {arena}: {gd.arena_title(arena)}\nЦена: {fmt_num(_case_price(arena))} 🪙 за кейс\nТвои монеты: {fmt_num(int(u2['coins'] or 0))} 🪙",
        reply_markup=_shop_buy_kb(arena),
    )


@router.callback_query(F.data.startswith("shop_custom:"))
async def cb_shop_custom_start(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    _, item_type, arena_s = cb.data.split(":")
    if item_type not in ("weapon", "pet"):
        await cb.answer("Неизвестный тип", show_alert=True)
        return
    arena = int(arena_s)
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        await cb.answer("🔒 Арена не открыта.", show_alert=True)
        return
    icon = "🗡" if item_type == "weapon" else "🐾"
    # Для ввода количества принимаем только reply на это сообщение,
    # чтобы контекст не "преследовал" пользователя в чате.
    prompt = await cb.message.answer(
        f"{icon} Введи количество для покупки (1-10000)\n"
        f"Ответом на это сообщение.\n"
        f"Формат: число, например 25"
    )
    SHOP_CUSTOM_CTX[int(u["tg_id"])] = {
        "item_type": item_type,
        "arena": arena,
        "chat_id": int(prompt.chat.id),
        "prompt_msg_id": int(prompt.message_id),
        "warned_no_reply": 0,
    }
    await cb.answer("Ожидаю количество")


@router.message(_is_shop_custom_input_message)
async def cmd_shop_custom_qty(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    ctx = SHOP_CUSTOM_CTX.get(tg_id)
    if not ctx:
        return

    # В другом чате просто игнорируем ввод, не сбрасывая сценарий.
    if int(message.chat.id) != int(ctx.get("chat_id", 0) or 0):
        return

    # Количество принимаем только reply на сообщение-запрос.
    if not message.reply_to_message or int(message.reply_to_message.message_id) != int(ctx.get("prompt_msg_id", 0) or 0):
        return

    raw = str(message.text or "").strip()
    if not raw.isdigit():
        await message.answer("❌ Введи целое число от 1 до 10000.")
        return
    qty = max(1, min(10000, int(raw)))
    item_type = str(ctx.get("item_type") or "weapon")
    arena = int(ctx.get("arena") or 1)
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        SHOP_CUSTOM_CTX.pop(tg_id, None)
        await message.answer("🔒 Арена не открыта.")
        return

    total_price = _case_price(arena) * qty
    coins = int(u["coins"] or 0)
    if coins < total_price:
        await message.answer(f"💸 Недостаточно монет. Нужно: {fmt_num(total_price)}")
        return

    SHOP_CUSTOM_CTX.pop(tg_id, None)

    if qty >= SHOP_CONFIRM_QTY:
        msg = await message.answer(
            _shop_confirm_text(u, item_type, arena, qty),
            reply_markup=_shop_confirm_kb(item_type, arena, qty),
        )
        _set_cb_owner(msg.chat.id, msg.message_id, tg_id)
        return

    ok_buy, buy_msg = _shop_apply_purchase(u, item_type, arena, qty)
    if not ok_buy:
        await message.answer(buy_msg)
        return
    icon = "🗡" if item_type == "weapon" else "🐾"
    await message.answer(
        f"✅ Куплено {icon} кейсов: {qty}\n"
        f"Арена: {arena}\n"
        f"Списано: {fmt_num(total_price)} 🪙"
    )


@router.callback_query(F.data.startswith("shop_confirm:"))
async def cb_shop_confirm(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    parts = str(cb.data or "").split(":")
    if len(parts) != 4:
        await cb.answer("Кнопка устарела", show_alert=True)
        return
    _tag, item_type, arena_s, qty_s = parts
    try:
        arena = int(arena_s)
        qty = int(qty_s)
    except Exception:
        await cb.answer("Кнопка устарела", show_alert=True)
        return

    ok_buy, msg = _shop_apply_purchase(u, item_type, arena, qty)
    if not ok_buy:
        await cb.answer(msg, show_alert=True)
        return
    u2 = db.get_user(int(u["tg_id"])) or u
    await _safe_edit_cb(
        cb,
        f"🛒 Арена {arena}: {gd.arena_title(arena)}\nЦена: {fmt_num(_case_price(arena))} 🪙 за кейс\nТвои монеты: {fmt_num(int(u2['coins'] or 0))} 🪙",
        reply_markup=_shop_buy_kb(arena),
    )
    await cb.answer(msg, show_alert=True)


@router.callback_query(F.data.startswith("shop_confirm_cancel:"))
async def cb_shop_confirm_cancel(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    parts = str(cb.data or "").split(":")
    if len(parts) != 2 or not parts[1].isdigit():
        await cb.answer("Кнопка устарела", show_alert=True)
        return
    arena = int(parts[1])
    await _safe_edit_cb(
        cb,
        f"🛒 Арена {arena}: {gd.arena_title(arena)}\nЦена: {fmt_num(_case_price(arena))} 🪙 за кейс\nТвои монеты: {fmt_num(int(_row_get(u, 'coins', 0) or 0))} 🪙",
        reply_markup=_shop_buy_kb(arena),
    )
    await cb.answer("Отменено")


def _open_afk_cases(u, case_key: str, count: int, chat_id: int | None = None) -> str:
    tg_id = int(u["tg_id"])
    if not db.consume_case_count(tg_id, case_key, count):
        return f"❌ Недостаточно кейсов ({gd.AFK_CASES.get(case_key, case_key)})."
    rank_idx = int(u["rank_idx"] or 0)
    ring_mult = _ring_bonus_mult(u)
    aura_mult = _aura_gold_mult(u)
    afk_loot_mult = 1.0 + float(_artifact_effects(u).get("afk_loot", 0.0))
    case_double_chance = _artifact_case_double_chance(u)
    total_coins = 0
    for _ in range(count):
        coins = gd.afk_case_coins(case_key, rank_idx)
        if case_double_chance > 0 and random.random() < case_double_chance:
            coins *= 2
        total_coins += int(coins * ring_mult * aura_mult * _guild_coin_mult(u) * _true_coin_mult(u) * _artifact_coin_mult(u) * afk_loot_mult)
    bonus_chat_active = _is_afk_bonus_chat(chat_id)
    if bonus_chat_active:
        total_coins = int(total_coins * AFK_CASE_BONUS_CHAT_MULT)
    db.update_user(tg_id, coins=int(u["coins"] or 0) + total_coins)
    icon = {"common": "📦", "rare": "🔮", "epic": "💎", "legendary": "🔱", "mythic": "🌌"}.get(case_key,
                                                                                                             "📦")
    out = (
        f"{icon} Открыто {count} кейс(ов): {gd.AFK_CASES.get(case_key, case_key)}\n"
        f"🪙 Получено: +{fmt_num(total_coins)}"
    )
    if bonus_chat_active:
        out += "\n+20% ✨"
    return out


def _open_item_cases(u, item_type: str, arena: int, count: int) -> str:
    tg_id = int(u["tg_id"])
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        return "❌ Эта арена ещё не открыта."
    col = f"weapon_cases_a{arena}" if item_type == "weapon" else f"pet_cases_a{arena}"
    cur = int(u[col] or 0)
    if cur < count:
        return f"❌ Недостаточно кейсов (есть {cur}, нужно {count})."
    db.update_user(tg_id, **{col: cur - count})
    db.add_stat(tg_id, _stat_open_key(item_type, arena), count)
    items_got = []
    sold_count = 0
    sold_coins = 0
    bonus_scale = _item_case_bonus_scale(arena)
    case_double_chance = _artifact_case_double_chance(u)
    vip_lvl = int(_row_get(u, "vip_lvl", 0) or 0)
    autosell_threshold = int(db.get_stat(tg_id, VIP_AUTOSELL_THRESHOLD_KEY, 0) or 0) if vip_lvl >= 3 else 0
    for _ in range(count):
        raw_bonus = gd.get_weapon_roll(arena) if item_type == "weapon" else gd.get_pet_roll(arena)
        bonus = max(1, int(raw_bonus * bonus_scale))
        name = gd.get_weapon_name(bonus) if item_type == "weapon" else gd.get_pet_name(bonus, arena)
        qty_gain = 2 if (case_double_chance > 0 and random.random() < case_double_chance) else 1
        if autosell_threshold > 0 and bonus < autosell_threshold and not _autosell_should_keep_item(tg_id, name):
            sold_count += int(qty_gain)
            sold_coins += int(_sell_price(bonus, 1, qty_gain))
            continue
        db.add_inventory_item(tg_id, item_type, name, 1, bonus, qty_gain)
        icon = "🗡" if item_type == "weapon" else "🐾"
        xmark = f" x{qty_gain}" if qty_gain > 1 else ""
        items_got.append(f"  {icon} {name} +{bonus}{xmark}")
    if sold_coins > 0:
        uu = db.get_user(tg_id)
        if uu:
            db.update_user(tg_id, coins=int(_row_get(uu, "coins", 0) or 0) + int(sold_coins))
    header = f"{'🗡 Оружие' if item_type == 'weapon' else '🐾 Питомец'} — Арена {arena} — {count} кейс(ов):\n"
    lines = [header + "\n".join(items_got)]
    if sold_coins > 0:
        lines.append(f"\n♻️ Авто-продажа: {sold_count} шт. | +{fmt_num(sold_coins)} 🪙")
    return "\n".join(lines)


@router.message(F.text.lower().regexp(r'^отк\s+'))
async def cmd_open_case(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    text = message.text.strip().lower()
    parts = text.split()
    if parts[0] != "отк" or len(parts) < 2:
        return
    sub = parts[1]

    count = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
    count = max(1, min(count, _max_case_open_count(u)))

    afk_map = {
        "об": "common", "ред": "rare", "эп": "epic", "лег": "legendary", "мифч": "mythic",
    }
    if sub in afk_map:
        result = _open_afk_cases(u, afk_map[sub], count, chat_id=message.chat.id)
        await _send_text_as_quote_or_file(
            message,
            result,
            file_prefix="case_loot",
            file_caption="Лут большой, отправляю полную выдачу файлом.",
        )
        return

    if sub == "са":
        result = _artifact_open_bags(u, count)
        await _send_text_as_quote_or_file(
            message,
            result,
            file_prefix="artifact_bags",
            file_caption="Результат открытия сумок артефактов.",
        )
        return

    import re
    m = re.match(r'^ко([1-9]|1[0-5])$', sub)
    if m:
        arena = int(m.group(1))
        result = _open_item_cases(u, "weapon", arena, count)
        await _send_text_as_quote_or_file(
            message,
            result,
            file_prefix="case_loot",
            file_caption="Лут большой, отправляю полную выдачу файлом.",
        )
        return
    m = re.match(r'^кп([1-9]|1[0-5])$', sub)
    if m:
        arena = int(m.group(1))
        result = _open_item_cases(u, "pet", arena, count)
        await _send_text_as_quote_or_file(
            message,
            result,
            file_prefix="case_loot",
            file_caption="Лут большой, отправляю полную выдачу файлом.",
        )
        return



@router.message(F.text.lower().regexp(r'^дать\s+(об|ред|эп|лег|мифч|са|эсс|эссенция|essence)\s+\d+$'))
async def cmd_give_afk_cases(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer("❌ Используй ответом на сообщение игрока: дать мифч 10")
        return

    parts = (message.text or "").strip().lower().split()
    if len(parts) != 3:
        await message.answer("❌ Формат: дать [об|ред|эп|лег|мифч|са|эсс] [кол-во]")
        return

    token = parts[1]
    if not parts[2].isdigit():
        await message.answer("❌ Количество должно быть числом.")
        return
    qty = int(parts[2])
    if qty <= 0:
        await message.answer("❌ Количество должно быть больше 0.")
        return

    from_id = int(u["tg_id"])
    target_id = int(message.reply_to_message.from_user.id)
    if target_id == from_id:
        await message.answer("❌ Нельзя передавать самому себе.")
        return

    target_username = message.reply_to_message.from_user.username or str(target_id)
    db.create_user(target_id, target_username)
    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Не удалось найти получателя.")
        return

    afk_map = {
        "об": ("common", "📦 Обычный"),
        "ред": ("rare", "🔮 Редкий"),
        "эп": ("epic", "💎 Эпический"),
        "лег": ("legendary", "🔱 Легендарный"),
        "мифч": ("mythic", "🌌 Мифический"),
    }

    if token == "са":
        have = _artifact_bag_count(from_id)
        if have < qty:
            await message.answer(f"❌ Недостаточно сумок артефактов. Есть: {have}, нужно: {qty}.")
            return
        _artifact_add_bags(from_id, -qty)
        _artifact_add_bags(target_id, qty)
        await message.answer(f"✅ Передано: 👜 Сумка артефактов x{qty}")
        if bot_instance is not None:
            try:
                await bot_instance.send_message(target_id, f"🎁 Тебе передали: 👜 Сумка артефактов x{qty}")
            except Exception:
                pass
        return

    if token in {"эсс", "эссенция", "essence"}:
        have_ess = int(_row_get(u, "essence", 0) or 0)
        if have_ess < qty:
            await message.answer(f"❌ Недостаточно эссенции. Есть: {fmt_num(have_ess)}, нужно: {fmt_num(qty)}.")
            return
        db.update_user(from_id, essence=have_ess - qty)
        target_ess = int(_row_get(tu, "essence", 0) or 0)
        db.update_user(target_id, essence=target_ess + qty)
        await message.answer(f"✅ Передано: 💠 Эссенция x{fmt_num(qty)}")
        if bot_instance is not None:
            try:
                await bot_instance.send_message(target_id, f"🎁 Тебе передали: 💠 Эссенция x{fmt_num(qty)}")
            except Exception:
                pass
        return

    case_info = afk_map.get(token)
    if not case_info:
        await message.answer("❌ Можно передавать только: об, ред, эп, лег, мифч, са, эсс")
        return

    case_key, title = case_info
    if not db.consume_case_count(from_id, case_key, qty):
        own = int(_row_get(u, f"afk_{case_key}", 0) or 0)
        await message.answer(f"❌ Недостаточно кейсов {title}. Есть: {own}, нужно: {qty}.")
        return
    db.add_case_count(target_id, case_key, qty)
    await message.answer(f"✅ Передано: {title} x{qty}")
    if bot_instance is not None:
        try:
            await bot_instance.send_message(target_id, f"🎁 Тебе передали: {title} x{qty}")
        except Exception:
            pass


@router.message(F.text.lower().regexp(r'^(продать|прод)\s+(ко|кп)([1-9]|1[0-5])\s+\d+$'))
async def cmd_sell_item_cases(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    parts = message.text.strip().lower().split()
    if len(parts) < 3:
        await message.answer("❌ Использование: продать коN [кол] | продать кпN [кол]")
        return

    token = parts[1]
    qty_raw = parts[2]
    if not qty_raw.isdigit():
        await message.answer("❌ Количество должно быть положительным числом.")
        return

    qty_req = int(qty_raw)
    if qty_req <= 0:
        await message.answer("❌ Количество должно быть больше 0.")
        return

    m = re.match(r'^(ко|кп)([1-9]|1[0-5])$', token)
    if not m:
        await message.answer("❌ Формат: продать коN [кол] | продать кпN [кол]")
        return

    kind = m.group(1)
    arena = int(m.group(2))
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        await message.answer(f"❌ Нельзя продавать кейсы арены {arena}, пока у тебя не открыта эта арена.")
        return

    tg_id = int(u["tg_id"])
    col = f"weapon_cases_a{arena}" if kind == "ко" else f"pet_cases_a{arena}"
    have = int(u[col] or 0)
    if have <= 0:
        await message.answer("❌ У тебя нет таких кейсов для продажи.")
        return

    qty = min(qty_req, have)
    one_price = _case_price(arena)
    # Продажа с комиссией 10% от цены покупки: игрок получает 90%.
    gain = int(one_price * qty * 0.90)

    db.update_user(tg_id, **{
        col: have - qty,
        "coins": int(u["coins"] or 0) + gain,
    })

    icon = "🎫" if kind == "ко" else "🐾"
    await message.answer(
        f"✅ Продано {icon} {token.upper()} x{qty}\n"
        f"🪙 Начислено: +{fmt_num(gain)} (комиссия 10%)"
        + (f"\nℹ️ Запрошено {qty_req}, продано {qty} (столько было в наличии)." if qty_req > qty else "")
    )


@router.message(Command("inventory"))
@router.message(F.text.lower().in_({"инв", "инвентарь", "🎒 инвентарь"}))
async def cmd_inventory(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    text, kb = _inv_main_text_kb(u)
    sent = await _reply(message, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    if sent:
        _set_cb_owner(sent.chat.id, sent.message_id, tg_id)


def _inv_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗡 Оружие", callback_data="inv:weapon"),
            InlineKeyboardButton(text="🐾 Питомцы", callback_data="inv:pet"),
        ],
        [
            InlineKeyboardButton(text="💍 Кольца", callback_data="inv:rings"),
            InlineKeyboardButton(text="📖 Зачарования", callback_data="inv:enchants"),
        ],
        [
            InlineKeyboardButton(text="✨ Стол зачарований", callback_data="inv:enchant_table"),
        ],
    ])


def _inv_section_kb() -> InlineKeyboardMarkup:
    """Кнопка возврата к главному инвентарю."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗡 Оружие", callback_data="inv:weapon"),
            InlineKeyboardButton(text="🐾 Питомцы", callback_data="inv:pet"),
        ],
        [
            InlineKeyboardButton(text="💍 Кольца", callback_data="inv:rings"),
            InlineKeyboardButton(text="📖 Зачарования", callback_data="inv:enchants"),
        ],
        [
            InlineKeyboardButton(text="✨ Стол зачарований", callback_data="inv:enchant_table"),
            InlineKeyboardButton(text="🔄 Все предметы", callback_data="inv:main"),
        ],
    ])


def _inv_main_text_kb(u) -> tuple[str, InlineKeyboardMarkup]:
    tg_id = int(u["tg_id"])
    text = _inventory_text(u)
    # Если слишком длинный — обрезаем в цитату
    if len(text) > 3800:
        text = text[:3800] + "\n…(список обрезан, используй кнопки разделов)"
    return f"<blockquote expandable>{escape(text)}</blockquote>", _inv_kb()


@router.callback_query(F.data.startswith("inv:"))
async def cb_inventory_section(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    section = cb.data.split(":", 1)[1]
    tg_id = int(u["tg_id"])

    if section == "main":
        text, kb = _inv_main_text_kb(u)
        await _safe_edit_cb(cb, text, reply_markup=kb, parse_mode=ParseMode.HTML)
        await cb.answer()
        return

    if section == "weapon":
        items = [it for it in db.inventory_list(tg_id, limit=5000)
                 if str(_row_get(it, "type", "")) == "weapon"]
        if not items:
            await cb.answer("У тебя нет оружия.", show_alert=True)
            return
        eq_w = int(u["equipped_weapon_id"] or 0)
        all_enchants = db.get_enchants_for_items([int(it["id"]) for it in items])
        lines = ["🗡 Оружие:"]
        for it in items:
            marker = " [НАДЕТО]" if it["id"] == eq_w else ""
            marker += " [В БАНКЕ]" if it["in_bank"] else ""
            is_vip = _is_vip_donate_item_name(str(it["name"] or ""))
            bonus_txt = f"+{it['bonus']} 👑 VIP" if is_vip else f"+{it['bonus']}"
            enc = all_enchants.get(int(it["id"]), {})
            enc_label = ""
            if enc:
                ekey, elvl = list(enc.items())[0]
                edata = gd.ENCHANT_CATALOG.get(ekey)
                if edata:
                    enc_label = f" | {edata['emoji']} {edata['name']} Ур.{elvl}"
            lines.append(f"  ID {it['id']} | {it['name']} | L{it['level']} | {bonus_txt} | x{it['count']}{marker}{enc_label}")
        body = "\n".join(lines)
        await _safe_edit_cb(cb, f"<blockquote expandable>{escape(body)}</blockquote>",
                            reply_markup=_inv_section_kb(), parse_mode=ParseMode.HTML)
        await cb.answer()

    elif section == "pet":
        items = [it for it in db.inventory_list(tg_id, limit=5000)
                 if str(_row_get(it, "type", "")) == "pet"]
        if not items:
            await cb.answer("У тебя нет питомцев.", show_alert=True)
            return
        eq_p = int(u["equipped_pet_id"] or 0)
        all_enchants = db.get_enchants_for_items([int(it["id"]) for it in items])
        lines = ["🐾 Питомцы:"]
        for it in items:
            marker = " [НАДЕТО]" if it["id"] == eq_p else ""
            marker += " [В БАНКЕ]" if it["in_bank"] else ""
            is_vip = _is_vip_donate_item_name(str(it["name"] or ""))
            bonus_txt = f"+{it['bonus']} 👑 VIP" if is_vip else f"+{it['bonus']}"
            enc = all_enchants.get(int(it["id"]), {})
            enc_mark = " ✨" if enc else ""
            enc_label = ""
            if enc:
                ekey, elvl = list(enc.items())[0]
                edata = gd.ENCHANT_CATALOG.get(ekey)
                if edata:
                    enc_label = f" | {edata['emoji']} {edata['name']} Ур.{elvl}"
            lines.append(f"  ID {it['id']} | {it['name']}{enc_mark} | L{it['level']} | {bonus_txt} | x{it['count']}{marker}{enc_label}")
        body = "\n".join(lines)
        await _safe_edit_cb(cb, f"<blockquote expandable>{escape(body)}</blockquote>",
                            reply_markup=_inv_section_kb(), parse_mode=ParseMode.HTML)
        await cb.answer()

    elif section == "rings":
        ring_lvl = int(u["ring_level"] or 0)
        active_ring = int(u["active_ring_level"] or 0)
        lines = ["💍 Кольца:"]
        if ring_lvl == 0:
            lines.append("  Колец нет. Крафти из осколков (/craft).")
        else:
            for i in range(1, ring_lvl + 1):
                active_mark = " [НАДЕТО]" if i == active_ring else ""
                lines.append(f"  {gd.RING_NAMES.get(i, f'Кольцо {i}')}{active_mark}")
        shard_lines = []
        for i in range(1, 6):
            cnt = int(u[f"shard_{i}"] or 0)
            if cnt > 0:
                shard_lines.append(f"  {gd.SHARD_NAMES[i]}: x{cnt}")
        if shard_lines:
            lines += ["", "🧩 Осколки:"] + shard_lines
        body = "\n".join(lines)
        await _safe_edit_cb(cb, f"<blockquote expandable>{escape(body)}</blockquote>",
                            reply_markup=_inv_section_kb(), parse_mode=ParseMode.HTML)
        await cb.answer()

    elif section == "enchants":
        body = _owned_enchants_text(tg_id)
        await _safe_edit_cb(cb, f"<blockquote expandable>{escape(body)}</blockquote>",
                            reply_markup=_inv_section_kb(), parse_mode=ParseMode.HTML)
        await cb.answer()

    elif section == "enchant_table":
        await cb.answer()
        await _start_enchant_table(cb.message, int(u["tg_id"]), edit=False)


# ─────────────────────────────────────────────
#  СТОЛ ЗАЧАРОВАНИЙ
# ─────────────────────────────────────────────

def _enchant_table_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Мои зачарования", callback_data="etable:my_enchants")],
        [InlineKeyboardButton(text="✖ Отмена", callback_data="etable:cancel")],
    ])


def _enchant_table_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="etable:confirm")],
        [InlineKeyboardButton(text="📖 Мои зачарования", callback_data="etable:my_enchants")],
        [InlineKeyboardButton(text="✖ Отмена", callback_data="etable:cancel")],
    ])


def _enchant_name_list() -> str:
    """Список читаемых названий зачарований для подсказки."""
    parts = []
    for key, data in gd.ENCHANT_CATALOG.items():
        target_ru = "🗡 оружие" if data["target"] == "weapon" else "🐾 питомец"
        # Берём первый alias для отображения
        alias = next(a for a, k in ENCHANT_NAME_ALIASES.items() if k == key and not a == key)
        parts.append(f"  • {data['emoji']} {alias} 1-3 — {data['name']} ({target_ru})")
    return "\n".join(parts)


async def _start_enchant_table(message: Message, tg_id: int, edit: bool = False):
    """Запускает первый шаг стола зачарований — запрос ID предмета."""
    ENCHANT_TABLE_CTX[tg_id] = {"step": "item_id", "msg_id": None, "chat_id": message.chat.id}
    text = (
        "✨ Стол зачарований\n"
        "───────────────────\n"
        "Шаг 1 из 3: введи ID предмета\n"
        "(оружие или питомец из инвентаря)\n\n"
        "Пример: 37262"
    )
    if edit:
        await message.edit_text(text, reply_markup=_enchant_table_cancel_kb())
        ENCHANT_TABLE_CTX[tg_id]["msg_id"] = message.message_id
    else:
        sent = await message.answer(text, reply_markup=_enchant_table_cancel_kb())
        ENCHANT_TABLE_CTX[tg_id]["msg_id"] = sent.message_id
        ENCHANT_TABLE_CTX[tg_id]["chat_id"] = sent.chat.id


@router.message(Command("стол"))
@router.message(F.text.lower().in_({"стол", "стол зачарований", "✨ стол зачарований"}))
async def cmd_enchant_table(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await _start_enchant_table(message, int(u["tg_id"]), edit=False)


@router.callback_query(F.data == "inv:enchant_table")
async def cb_inv_enchant_table(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await cb.answer()
    await _start_enchant_table(cb.message, int(u["tg_id"]), edit=False)


@router.callback_query(F.data.startswith("etable:"))
async def cb_enchant_table(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    action = cb.data.split(":", 1)[1]

    if action == "cancel":
        ENCHANT_TABLE_CTX.pop(tg_id, None)
        await _safe_edit_cb(cb, "✨ Стол зачарований закрыт.")
        await cb.answer()
        return

    if action == "my_enchants":
        await cb.answer()
        await cb.message.answer(_owned_enchants_text(tg_id))
        return

    if action == "confirm":
        ctx = ENCHANT_TABLE_CTX.get(tg_id)
        if not ctx or ctx.get("step") != "confirm":
            await cb.answer("Сессия устарела. Начни заново: стол", show_alert=True)
            return

        item_id = int(ctx["item_id"])
        enchant_key = ctx["enchant_key"]
        enchant_lvl = int(ctx["enchant_lvl"])
        enchant_data = gd.ENCHANT_CATALOG.get(enchant_key)

        # Финальные проверки
        it = db.get_inventory_item(tg_id, item_id)
        if not it:
            ENCHANT_TABLE_CTX.pop(tg_id, None)
            await _safe_edit_cb(cb, "❌ Предмет не найден. Начни заново: стол")
            await cb.answer()
            return
        if _is_vip_donate_item_name(str(_row_get(it, "name", "") or "")):
            ENCHANT_TABLE_CTX.pop(tg_id, None)
            await _safe_edit_cb(cb, "❌ На VIP-экипировку нельзя накладывать зачарования.")
            await cb.answer()
            return
        existing = db.get_item_enchants(item_id)
        if existing:
            ENCHANT_TABLE_CTX.pop(tg_id, None)
            await _safe_edit_cb(cb, "❌ На этом предмете уже есть зачарование. Снять нельзя.")
            await cb.answer()
            return
        stat_key = f"enchant:owned:{enchant_key}:{enchant_lvl}"
        owned = int(db.get_stat(tg_id, stat_key, 0) or 0)
        if owned <= 0:
            ENCHANT_TABLE_CTX.pop(tg_id, None)
            await _safe_edit_cb(cb, f"❌ У тебя нет {enchant_data['emoji']} {enchant_data['name']} Ур.{enchant_lvl}.")
            await cb.answer()
            return

        # Применяем
        db.set_stat_value(tg_id, stat_key, owned - 1)
        db.set_item_enchant(tg_id, item_id, enchant_key, enchant_lvl)
        ENCHANT_TABLE_CTX.pop(tg_id, None)
        pct = enchant_data["levels"][enchant_lvl - 1]["bonus_pct"]
        item_name = str(_row_get(it, "name", "") or "")
        await _safe_edit_cb(
            cb,
            f"✅ Зачарование применено!\n"
            f"{enchant_data['emoji']} {enchant_data['name']} Ур.{enchant_lvl} (+{pct}%)\n"
            f"Предмет: {item_name} (ID {item_id})",
        )
        await cb.answer("✅ Готово!")
        return

    await cb.answer()


@router.message(lambda m: bool(m.text) and m.from_user is not None and m.from_user.id in ENCHANT_TABLE_CTX)
async def handle_enchant_table_input(message: Message):
    tg_id = message.from_user.id
    ctx = ENCHANT_TABLE_CTX.get(tg_id)
    if not ctx:
        return

    ok, u = await _check_access(message)
    if not ok:
        ENCHANT_TABLE_CTX.pop(tg_id, None)
        return

    text_raw = (message.text or "").strip()
    bot = message.bot
    chat_id = ctx.get("chat_id", message.chat.id)
    msg_id = ctx.get("msg_id")

    async def _edit_table(new_text: str, kb=None):
        if msg_id and bot:
            try:
                await bot.edit_message_text(
                    new_text,
                    chat_id=chat_id,
                    message_id=msg_id,
                    reply_markup=kb or _enchant_table_cancel_kb(),
                )
            except Exception:
                pass

    # ── Шаг 1: ввод ID предмета ──
    if ctx.get("step") == "item_id":
        if not text_raw.isdigit():
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                "❌ ID должен быть числом. Попробуй снова:\n\nВведи ID предмета:"
            )
            return
        item_id = int(text_raw)
        it = db.get_inventory_item(tg_id, item_id)
        if not it:
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                f"❌ Предмет ID {item_id} не найден в твоём инвентаре.\n\nВведи другой ID:"
            )
            return
        item_type = str(_row_get(it, "type", "") or "")
        if item_type not in ("weapon", "pet"):
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                "❌ Зачарования только на оружие и питомцев.\n\nВведи ID оружия или питомца:"
            )
            return
        if _is_vip_donate_item_name(str(_row_get(it, "name", "") or "")):
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                "❌ На VIP-экипировку нельзя накладывать зачарования.\n\nВведи другой ID:"
            )
            return
        existing = db.get_item_enchants(item_id)
        if existing:
            ekey, elvl = list(existing.items())[0]
            edata = gd.ENCHANT_CATALOG.get(ekey, {})
            ename = edata.get("name", ekey) if edata else ekey
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                f"❌ На предмете уже стоит {ename} Ур.{elvl}.\n"
                "Снять зачарование нельзя.\n\nВведи другой ID:"
            )
            return

        item_name = str(_row_get(it, "name", "") or "")
        item_type_ru = "🗡 Оружие" if item_type == "weapon" else "🐾 Питомец"
        ctx["item_id"] = item_id
        ctx["item_type"] = item_type
        ctx["step"] = "enchant"

        # Подходящие зачарования по типу предмета
        valid_enchants = [(k, d) for k, d in gd.ENCHANT_CATALOG.items() if d["target"] == item_type]
        enc_hint_lines = []
        for ekey, edata in valid_enchants:
            alias = next((a for a, k in ENCHANT_NAME_ALIASES.items() if k == ekey and a != ekey), ekey)
            enc_hint_lines.append(f"  {edata['emoji']} {alias} 1-3 — {edata['name']}")

        await _edit_table(
            f"✨ Стол зачарований\n───────────────────\n"
            f"Предмет: {item_name} (ID {item_id})\n"
            f"Тип: {item_type_ru}\n\n"
            f"Шаг 2 из 3: введи зачарование и уровень\n"
            f"Формат: название уровень\n"
            f"Например: сила 2\n\n"
            f"Доступные для этого предмета:\n" + "\n".join(enc_hint_lines)
        )
        return

    # ── Шаг 2: ввод зачарования ──
    if ctx.get("step") == "enchant":
        parts_input = text_raw.lower().split()
        if len(parts_input) < 2 or not parts_input[-1].isdigit():
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                "❌ Формат: название уровень (например: сила 2)\n\n"
                "Введи зачарование и уровень:"
            )
            return

        lvl_str = parts_input[-1]
        name_str = " ".join(parts_input[:-1])
        enchant_key = ENCHANT_NAME_ALIASES.get(name_str)
        if not enchant_key:
            valid_enchants = [(k, d) for k, d in gd.ENCHANT_CATALOG.items() if d["target"] == ctx.get("item_type")]
            enc_hint_lines = []
            for ekey, edata in valid_enchants:
                alias = next((a for a, k in ENCHANT_NAME_ALIASES.items() if k == ekey and a != ekey), ekey)
                enc_hint_lines.append(f"  {edata['emoji']} {alias} — {edata['name']}")
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                f"❌ Неизвестное зачарование «{name_str}».\n\n"
                "Доступные:\n" + "\n".join(enc_hint_lines) + "\n\nВведи снова:"
            )
            return

        enchant_lvl = int(lvl_str)
        if enchant_lvl < 1 or enchant_lvl > 3:
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                "❌ Уровень от 1 до 3.\n\nВведи снова:"
            )
            return

        enchant_data = gd.ENCHANT_CATALOG[enchant_key]
        if enchant_data["target"] != ctx.get("item_type"):
            target_ru = "оружие" if enchant_data["target"] == "weapon" else "питомца"
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                f"❌ {enchant_data['emoji']} {enchant_data['name']} — только на {target_ru}.\n\nВведи другое зачарование:"
            )
            return

        # Проверяем наличие свитка
        stat_key = f"enchant:owned:{enchant_key}:{enchant_lvl}"
        owned = int(db.get_stat(tg_id, stat_key, 0) or 0)
        if owned <= 0:
            await _edit_table(
                "✨ Стол зачарований\n───────────────────\n"
                f"❌ У тебя нет {enchant_data['emoji']} {enchant_data['name']} Ур.{enchant_lvl}.\n"
                "Купи в Лавке Джинна → 📖 Зачарования.\n\nВведи другое зачарование:"
            )
            return

        ctx["enchant_key"] = enchant_key
        ctx["enchant_lvl"] = enchant_lvl
        ctx["step"] = "confirm"

        pct = enchant_data["levels"][enchant_lvl - 1]["bonus_pct"]
        item_id = int(ctx["item_id"])
        it = db.get_inventory_item(tg_id, item_id)
        item_name = str(_row_get(it, "name", "") or "") if it else f"ID {item_id}"
        item_type_ru = "🗡 Оружие" if ctx.get("item_type") == "weapon" else "🐾 Питомец"

        await _edit_table(
            f"✨ Стол зачарований\n───────────────────\n"
            f"Шаг 3 из 3: подтверди применение\n\n"
            f"Предмет: {item_name} (ID {item_id})\n"
            f"Тип: {item_type_ru}\n"
            f"Зачарование: {enchant_data['emoji']} {enchant_data['name']} Ур.{enchant_lvl} (+{pct}%)\n"
            f"В наличии свитков: {owned}\n\n"
            f"После применения зачарование снять нельзя.",
            kb=_enchant_table_confirm_kb(),
        )


@router.message(Command("art"))
@router.message(Command("artifacts"))
@router.message(F.text.lower().in_({"арт", "артефакт", "артефакты"}))
async def cmd_artifacts(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    msg = await message.answer(_artifact_menu_text(u, 0), reply_markup=_artifact_menu_kb(u, 0))
    _set_cb_owner(msg.chat.id, msg.message_id, int(u["tg_id"]))


@router.callback_query(F.data.startswith("art:"))
async def cb_artifacts(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    parts = str(cb.data or "").split(":")
    # Новый формат: art:{owner_id}:{action}:...
    # Старый формат: art:{action}:...
    owner_from_cb = 0
    action_idx = 1
    if len(parts) >= 3 and parts[1].isdigit():
        owner_from_cb = int(parts[1])
        action_idx = 2
    action = parts[action_idx] if len(parts) > action_idx else ""
    tg_id = int(u["tg_id"])

    if owner_from_cb > 0 and owner_from_cb != tg_id and tg_id not in SUPER_ADMINS:
        await cb.answer("Это кнопки другого игрока.", show_alert=True)
        return

    if action == "close":
        await _safe_edit_cb(cb, "🧿 Раздел артефактов закрыт.")
        await cb.answer()
        return

    if action == "page":
        page_idx = action_idx + 1
        page = int(parts[page_idx]) if len(parts) > page_idx and parts[page_idx].isdigit() else 0
        u2 = db.get_user(tg_id) or u
        await _safe_edit_cb(cb, _artifact_menu_text(u2, page), reply_markup=_artifact_menu_kb(u2, page))
        await cb.answer()
        return

    if action == "item":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        page = int(parts[page_idx])
        txt = _artifact_detail_text(u, item_id)
        kb = _artifact_detail_kb(u, item_id, page)
        await _safe_edit_cb(cb, txt, reply_markup=kb)
        await cb.answer()
        return

    if action == "equip":
        item_idx = action_idx + 1
        slot_idx_pos = action_idx + 2
        page_idx = action_idx + 3
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        slot_idx = int(parts[slot_idx_pos])
        page = int(parts[page_idx])
        if slot_idx < 1 or slot_idx > _artifact_slot_count(u):
            await cb.answer("Слот недоступен.", show_alert=True)
            return
        it = db.get_inventory_item(tg_id, item_id)
        if not it or str(_row_get(it, "type", "")) != "artifact" or int(_row_get(it, "in_bank", 0) or 0) == 1:
            await cb.answer("Артефакт не найден.", show_alert=True)
            return
        slot_keys = list(ARTIFACT_SLOT_KEYS[:_artifact_slot_count(u)])
        slot_stats = db.get_stats(tg_id, slot_keys)
        current_slot_key = ARTIFACT_SLOT_KEYS[slot_idx - 1]
        same_in_other_slots = 0
        for sk in slot_keys:
            if sk == current_slot_key:
                continue
            if int(slot_stats.get(sk, 0) or 0) == int(item_id):
                same_in_other_slots += 1
        item_count = int(_row_get(it, "count", 0) or 0)
        if item_count <= same_in_other_slots:
            await cb.answer("❌ Недостаточно копий артефакта для нескольких слотов.", show_alert=True)
            return
        db.set_stat_value(tg_id, ARTIFACT_SLOT_KEYS[slot_idx - 1], int(item_id))
        u2 = db.get_user(tg_id) or u
        await _safe_edit_cb(cb, _artifact_detail_text(u2, item_id), reply_markup=_artifact_detail_kb(u2, item_id, page))
        await cb.answer(f"Слот {slot_idx} экипирован")
        return

    if action == "trust":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        page = int(parts[page_idx])
        it = db.get_inventory_item(tg_id, item_id)
        if not it or str(_row_get(it, "type", "")) != "artifact":
            await cb.answer("Артефакт не найден.", show_alert=True)
            return
        if _artifact_trust_info(item_id, tg_id):
            await cb.answer("❌ Доверенный артефакт нельзя передоверять. Используй «Вернуть».", show_alert=True)
            return
        ARTIFACT_TRUST_PENDING[tg_id] = {
            "phase": "input",
            "item_id": int(item_id),
            "page": int(page),
            "artifact_msg_id": int(cb.message.message_id),
            "chat_id": int(cb.message.chat.id),
            "set_at": int(time.time()),
        }
        await cb.message.answer(
            "🤝 Доверить артефакт\n"
            "Ответь на сообщение с артефактом в формате:\n"
            "<ник> <время>\n"
            "Примеры: Admin 1м | Admin 1ч | Admin 1д\n"
            "Диапазон: от 1м до 7д",
            reply_markup=_artifact_trust_confirm_kb(tg_id, item_id, page),
        )
        await cb.answer("Формат: ник время")
        return

    if action == "trustcancel":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        page = int(parts[page_idx])
        ARTIFACT_TRUST_PENDING.pop(tg_id, None)
        u2 = db.get_user(tg_id) or u
        await _safe_edit_cb(cb, _artifact_detail_text(u2, item_id), reply_markup=_artifact_detail_kb(u2, item_id, page))
        await cb.answer("Отменено")
        return

    if action == "trustok":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        ctx = ARTIFACT_TRUST_PENDING.get(tg_id) or {}
        if int(time.time()) - int(ctx.get("set_at", 0) or 0) > 15 * 60:
            ARTIFACT_TRUST_PENDING.pop(tg_id, None)
            await cb.answer("Сессия доверия устарела. Начни заново.", show_alert=True)
            return
        if str(ctx.get("phase", "")) != "confirm" or int(ctx.get("item_id", 0) or 0) != int(item_id):
            await cb.answer("Сначала укажи ник и время в ответ на артефакт.", show_alert=True)
            return

        target_id = int(ctx.get("target_id", 0) or 0)
        nick = str(ctx.get("nick", "") or "")
        secs = int(ctx.get("secs", 0) or 0)
        if target_id <= 0 or not nick or secs <= 0:
            ARTIFACT_TRUST_PENDING.pop(tg_id, None)
            await cb.answer("Данные доверия устарели. Повтори заново.", show_alert=True)
            return

        expires_at = int(time.time()) + int(secs)
        move_ok, move_msg, trusted_item_id = db.transfer_artifact_to_trust(
            owner_id=int(tg_id),
            holder_id=int(target_id),
            item_id=int(item_id),
            expires_at=int(expires_at),
        )
        if not move_ok:
            ARTIFACT_TRUST_PENDING.pop(tg_id, None)
            reason_map = {
                "bad_id": "❌ Некорректный ID игрока.",
                "self": "❌ Нельзя доверить артефакт самому себе.",
                "item_not_found": "❌ Артефакт не найден у владельца.",
                "in_bank": "❌ Артефакт в банке. Сними его из банка.",
                "bad_expire": "❌ Некорректное время доверия.",
            }
            await cb.answer(reason_map.get(str(move_msg), "❌ Не удалось выдать доверие."), show_alert=True)
            return

        trusted_item = db.get_inventory_item(int(target_id), int(trusted_item_id))
        trusted_name = str(_row_get(trusted_item, "name", "") or "")
        trusted_level = int(_row_get(trusted_item, "level", 1) or 1)
        trusted_bonus = int(_row_get(trusted_item, "bonus", 0) or 0)
        ARTIFACT_TRUST_PENDING.pop(tg_id, None)

        duration_human = _fmt_uptime(int(secs))
        await _safe_edit_cb(
            cb,
            f"✅ Доверено\n"
            f"👤 Игроку: {nick}\n"
            f"⏳ На: {duration_human}",
        )
        await cb.answer("Готово")
        if bot_instance is not None:
            try:
                await bot_instance.send_message(
                    int(target_id),
                    f"🧿 Тебе доверили артефакт ID {int(trusted_item_id)} на {duration_human}.",
                )
            except Exception:
                pass
        return

    if action == "return":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        page = int(parts[page_idx])
        trust_info = _artifact_trust_info(item_id, tg_id)
        if not trust_info:
            await cb.answer("❌ Этот артефакт не является доверенным.", show_alert=True)
            return
        await _safe_edit_cb(
            cb,
            "↩️ Вернуть артефакт досрочно владельцу?",
            reply_markup=_artifact_return_confirm_kb(tg_id, item_id, page),
        )
        await cb.answer()
        return

    if action == "returncancel":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        page = int(parts[page_idx])
        u2 = db.get_user(tg_id) or u
        await _safe_edit_cb(cb, _artifact_detail_text(u2, item_id), reply_markup=_artifact_detail_kb(u2, item_id, page))
        await cb.answer("Отменено")
        return

    if action == "returnok":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        page = int(parts[page_idx])
        trust_info = _artifact_trust_info(item_id, tg_id)
        if not trust_info:
            await cb.answer("❌ Этот артефакт не является доверенным.", show_alert=True)
            return
        owner_id = int(trust_info.get("owner_id", 0) or 0)
        ok_ret, owner_ret, _holder_ret, reason = db.return_artifact_trust(int(item_id))
        if not ok_ret:
            if reason == "not_found":
                await cb.answer("❌ Доверие не найдено.", show_alert=True)
            else:
                await cb.answer("❌ Не удалось вернуть артефакт.", show_alert=True)
            return
        owner_id = int(owner_ret or owner_id)
        u2 = db.get_user(tg_id) or u
        await _safe_edit_cb(cb, _artifact_menu_text(u2, page), reply_markup=_artifact_menu_kb(u2, page))
        await cb.answer("↩️ Артефакт возвращен владельцу")
        if bot_instance is not None and owner_id > 0:
            try:
                await bot_instance.send_message(owner_id, "↩️ Доверенный артефакт возвращен досрочно.")
            except Exception:
                pass
        return

    if action == "merge":
        item_idx = action_idx + 1
        page_idx = action_idx + 2
        if len(parts) <= page_idx:
            await cb.answer("Кнопка устарела.", show_alert=True)
            return
        item_id = int(parts[item_idx])
        page = int(parts[page_idx])
        ok_merge, msg = _artifact_merge_one(tg_id, item_id)
        u2 = db.get_user(tg_id) or u
        await _safe_edit_cb(cb, _artifact_menu_text(u2, page), reply_markup=_artifact_menu_kb(u2, page))
        await cb.answer(msg, show_alert=not ok_merge)
        return


@router.message(_is_artifact_trust_input_message)
async def handle_artifact_trust_input(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    ctx = ARTIFACT_TRUST_PENDING.get(tg_id)
    if not ctx:
        return
    if int(time.time()) - int(ctx.get("set_at", 0) or 0) > 15 * 60:
        ARTIFACT_TRUST_PENDING.pop(tg_id, None)
        return
    if str(ctx.get("phase", "input")) != "input":
        return

    raw = str(message.text or "").strip()
    if raw.lower() in {"отмена", "cancel", "стоп"}:
        ARTIFACT_TRUST_PENDING.pop(tg_id, None)
        await message.answer("❌ Доверие отменено.")
        return

    # В другом чате просто игнорируем ввод, не сбрасывая сценарий доверия.
    if int(message.chat.id) != int(ctx.get("chat_id", 0)):
        return

    # Доверие задается только ответом на карточку артефакта.
    if not message.reply_to_message or int(message.reply_to_message.message_id) != int(ctx.get("artifact_msg_id", 0)):
        return
    parts = raw.split()
    if len(parts) < 2:
        await message.answer("❌ Формат: <ник> <время>. Пример: Admin 1ч")
        return

    nick = _artifact_trust_nick(parts[0])
    if not nick:
        await message.answer("❌ Некорректный ник. Допустимо 2-32 символа: буквы, цифры, _, -, .")
        return
    duration_token = parts[1]
    secs = _artifact_trust_duration_secs(duration_token)
    if secs <= 0:
        await message.answer("❌ Время должно быть от 1м до 7д. Примеры: 1м, 1ч, 1д")
        return

    item_id = int(ctx.get("item_id", 0) or 0)
    it = db.get_inventory_item(tg_id, item_id)
    if not it or str(_row_get(it, "type", "")) != "artifact":
        ARTIFACT_TRUST_PENDING.pop(tg_id, None)
        await message.answer("❌ Артефакт не найден. Открой раздел артефактов снова.")
        return
    if _artifact_trust_info(item_id, tg_id):
        ARTIFACT_TRUST_PENDING.pop(tg_id, None)
        await message.answer("❌ Доверенный артефакт нельзя передоверять. Используй «Вернуть».")
        return

    target_id, find_reason = _artifact_find_target_by_nick(nick)
    if target_id <= 0:
        reason_map = {
            "not_found": "❌ Игрок с таким ником не найден.",
            "ambiguous_nickname": "❌ Найдено несколько игроков с таким ником. Укажи уникальный ник.",
            "ambiguous_username": "❌ Найдено несколько игроков с таким username. Укажи ник в боте.",
            "bad_nick": "❌ Некорректный ник.",
        }
        await message.answer(reason_map.get(find_reason, "❌ Не удалось найти игрока."))
        return

    page = int(ctx.get("page", 0) or 0)
    duration_human = _fmt_uptime(int(secs))
    ARTIFACT_TRUST_PENDING[tg_id] = {
        "phase": "confirm",
        "item_id": int(item_id),
        "page": int(page),
        "target_id": int(target_id),
        "nick": str(nick),
        "secs": int(secs),
        "set_at": int(time.time()),
    }
    await message.answer(
        f"🤝 Подтвердить доверие?\n"
        f"👤 Игрок: {nick}\n"
        f"⏳ На: {duration_human}",
        reply_markup=_artifact_trust_confirm_kb(tg_id, int(item_id), int(page)),
    )


@router.message(F.text.lower() == "банк")
async def cmd_bank(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    # Артефакты не используем через банк: они управляются отдельным меню.
    items = [it for it in db.inventory_banked_list(tg_id) if str(_row_get(it, "type", "")) != "artifact"]
    if not items:
        await message.answer("🏦 Банк пуст.\nКоманды: банк положить [id] | банк снять [id]")
        return
    lines = ["🏦 Банк предметов:"]
    wp_ids_bank = [int(it["id"]) for it in items if it["type"] in ("weapon", "pet")]
    bank_enchants = db.get_enchants_for_items(wp_ids_bank) if wp_ids_bank else {}
    for it in items:
        icon = "🗡" if it["type"] == "weapon" else "🐾"
        item_enchants = bank_enchants.get(int(it["id"]), {})
        enchant_mark = " ✨" if it["type"] == "pet" and item_enchants else ""
        enchant_label = ""
        if item_enchants:
            parts_e = []
            for ekey, elvl in item_enchants.items():
                edata = gd.ENCHANT_CATALOG.get(ekey)
                if edata:
                    parts_e.append(f"{edata['emoji']} {edata['name']} Ур.{elvl}")
            if parts_e:
                enchant_label = "  📖 " + " | ".join(parts_e)
        lines.append(f"{icon} ID {it['id']} | {it['name']}{enchant_mark} | L{it['level']} | +{it['bonus']} | x{it['count']}{enchant_label}")
    lines.append("\nУправление: банк положить [id] | банк снять [id]")
    text = "\n".join(lines)
    if len(text) > 3500:
        bio = BytesIO(text.encode("utf-8"))
        await message.answer_document(
            BufferedInputFile(bio.read(), filename=f"bank_{tg_id}.txt"),
            caption="Инвентарь большой, отправляю файлом."
        )
    else:
        await message.answer(
            f"\n{text}\n",
            parse_mode=ParseMode.HTML,
        )


@router.message(F.text.lower().regexp(r'^банк\s+(положить|снять)\s+\d+$'))
async def cmd_bank_action(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    parts = message.text.strip().lower().split()
    action = parts[1]
    item_id = int(parts[2])
    tg_id = int(u["tg_id"])
    to_bank = action == "положить"

    it = db.get_inventory_item(tg_id, item_id)
    if it and str(_row_get(it, "type", "")) == "artifact":
        await message.answer("❌ Артефакты не перемещаются через банк. Используй раздел «Артефакты».")
        return

    ok2 = db.set_inventory_bank(tg_id, item_id, to_bank)
    if ok2:
        await message.answer(f"✅ Предмет {item_id} {'перемещён в банк' if to_bank else 'извлечён из банка'}.")
    else:
        await message.answer("❌ Предмет не найден.")


@router.message(F.text.lower().regexp(r'^прод\s+\d+(\s+\d+)?$'))
async def cmd_sell(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    parts = message.text.strip().split()
    item_id = int(parts[1])
    qty = int(parts[2]) if len(parts) >= 3 else 1
    tg_id = int(u["tg_id"])
    it = db.get_inventory_item(tg_id, item_id)
    if not it:
        await message.answer("❌ Предмет не найден.")
        return
    if int(it["in_bank"]):
        await message.answer("❌ Предмет в банке — снача извлеки.")
        return
    if str(_row_get(it, "type", "")) == "artifact":
        await message.answer("❌ Артефакты не продаются.")
        return
    eq_w = int(u["equipped_weapon_id"] or 0)
    eq_p = int(u["equipped_pet_id"] or 0)
    if it["id"] in (eq_w, eq_p):
        await message.answer("❌ Нельзя продать надетый предмет.")
        return
    if _is_vip_donate_item_name(str(_row_get(it, "name", "") or "")):
        await message.answer("❌ VIP-предмет нельзя продать.")
        return
    if it["type"] in ("weapon", "pet") and db.get_item_enchants(int(it["id"])):
        await message.answer("❌ Нельзя продать зачарованный предмет (✨).")
        return
    qty = min(qty, int(it["count"]))
    gain = _sell_price(int(it["bonus"]), int(it["level"]), qty)
    db.consume_inventory_item(tg_id, item_id, qty)
    db.update_user(tg_id, coins=int(u["coins"] or 0) + gain)
    await message.answer(f"✅ Продано x{qty} «{it['name']}» за {fmt_num(gain)} 🪙")


@router.message(F.text.lower().in_({"сел о", "сел п"}))
async def cmd_sell_all(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    text_tokens = message.text.strip().lower().split()
    if len(text_tokens) < 2:
        await message.answer("❌ Использование: сел о | сел п")
        return
    if text_tokens[1] not in ("о", "п"):
        await message.answer("❌ Использование: сел о | сел п")
        return
    item_type = "weapon" if text_tokens[1] == "о" else "pet"
    eq_w = int(u["equipped_weapon_id"] or 0)
    eq_p = int(u["equipped_pet_id"] or 0)
    total_gain = 0
    total_count = 0

    # Продаем партиями: если предметов > 500, дочищаем все за один вызов команды.
    while True:
        items = db.inventory_list(tg_id, limit=500)
        sold_in_pass = 0
        # Batch-загрузка зачарований для всей партии
        wp_ids_sell = [int(it["id"]) for it in items if it["type"] in ("weapon", "pet")]
        batch_enchants = db.get_enchants_for_items(wp_ids_sell) if wp_ids_sell else {}
        for it in items:
            if it["type"] != item_type:
                continue
            if it["in_bank"]:
                continue
            if str(_row_get(it, "type", "")) == "artifact":
                continue
            if it["id"] in (eq_w, eq_p):
                continue
            if _is_vip_donate_item_name(str(_row_get(it, "name", "") or "")):
                continue
            if batch_enchants.get(int(it["id"])):
                continue  # зачарованные предметы не продаются массово
            count = int(it["count"])
            gain = _sell_price(int(it["bonus"]), int(it["level"]), count)
            total_gain += gain
            total_count += count
            sold_in_pass += 1
            db.consume_inventory_item(tg_id, it["id"], count)
        if sold_in_pass == 0:
            break

    db.update_user(tg_id, coins=int(u["coins"] or 0) + total_gain)
    icon = "🗡" if item_type == "weapon" else "🐾"
    await message.answer(f"✅ Продано {total_count} {icon} предметов за {fmt_num(total_gain)} 🪙")


@router.message(Command("upgrade"))
@router.message(F.text.lower().regexp(r'^(син|синтез)(\s+\d+)?(\s+\d+)?$'))
@router.message(F.text.lower() == "🌀 синтез")
async def cmd_synth(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    parts = message.text.strip().split()
    if len(parts) == 1 or (len(parts) == 2 and not parts[1].isdigit()):
        tg_id = int(u["tg_id"])
        results = db.upgrade_all_three_to_one(tg_id)
        if not results:
            await message.answer("❌ Нет 3 одинаковых предметов для синтеза (L1 или L2).")
        else:
            # Короткая сводка по всем результатам автосинтеза.
            total_ops = len(results)
            total_created = 0
            by_key: dict[tuple[str, int, int], int] = {}
            for r in results:
                _track_synth_stats(tg_id, r)
                cnt = int(r.get("created_count", 0) or 0)
                total_created += cnt
                key = (str(r.get("name", "?")), int(r.get("new_level", 0) or 0), int(r.get("new_bonus", 0) or 0))
                by_key[key] = by_key.get(key, 0) + cnt

            lines = [
                "🔄 Массовый синтез завершен",
                f"Операций: {fmt_num(total_ops)} | Создано предметов: {fmt_num(total_created)}",
                "",
                "📦 Результаты:",
            ]
            for (name, lvl, bonus), cnt in sorted(by_key.items(), key=lambda x: (x[0][1], x[0][2], x[0][0])):
                lines.append(f"• {name} L{lvl} +{bonus} x{cnt}")

            await _send_text_as_quote_or_file(
                message,
                "\n".join(lines),
                file_prefix="synth_result",
                file_caption="Синтез большой, отправляю файлом.",
            )
        return
    try:
        item_id = int(parts[1]) if len(parts) >= 2 else 0
        qty = int(parts[2]) if len(parts) >= 3 else 3
    except Exception:
        await message.answer("❌ Использование: син [id] [кол-во]")
        return
    result = db.synth_by_item_id(int(u["tg_id"]), item_id, qty)
    if not result:
        await message.answer("❌ Синтез невозможен (нет предмета, в банке, или L3).")
    else:
        _track_synth_stats(int(u["tg_id"]), result)
        await message.answer(
            f"🔄 Синтез: использовано {result['used_count']} шт.\n"
            f"✨ Создано {result['created_count']} шт.: {result['name']} L{result['new_level']} +{result['new_bonus']}\n"
            f"📦 Осталось: {result['left_count']} шт."
        )


@router.message(Command("craft"))
@router.message(F.text.lower().in_({"крафт", "🛠️ крафт", "🛠 крафт"}))
async def cmd_craft(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    updates = {}
    crafted_lines = []
    crafted_total = 0
    best_ring = int(u["ring_level"] or 0)
    active_ring = int(u["active_ring_level"] or 0)

    for i in range(1, 6):
        cnt = int(u[f"shard_{i}"] or 0)
        crafts = cnt // 10
        if crafts <= 0:
            continue
        left = cnt - crafts * 10
        updates[f"shard_{i}"] = left
        crafted_total += crafts
        best_ring = max(best_ring, i)
        crafted_lines.append(f"• {gd.RING_NAMES.get(i, f'Кольцо {i}')} x{crafts} (остаток: {left})")

    if crafted_total <= 0:
        await message.answer("❌ Нужно 10 одинаковых осколков для крафта кольца.")
        return

    updates["ring_level"] = best_ring
    if active_ring == 0:
        updates["active_ring_level"] = best_ring
    db.update_user(tg_id, **updates)

    await message.answer(
        "🔨 Крафт завершен!\n"
        f"💍 Всего создано колец: {crafted_total}\n"
        + "\n".join(crafted_lines)
    )


@router.message(Command("loadout"))
@router.message(F.text.lower().in_({"экип", "экипировка", "👔 экипировка"}))
async def cmd_loadout(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer(_loadout_text(u))
        return
    await message.answer(_loadout_text(u), reply_markup=_loadout_kb(u))


@router.message(Command("hidestats"))
async def cmd_hide_admin_stats(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    _set_admin_stats_enabled(int(u["tg_id"]), False)
    await message.answer(
        "✅ Режим скрыт.\n"
        "admin-предметы работают как обычные топовые предметы."
    )


@router.message(Command("openstats"))
async def cmd_open_admin_stats(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    _set_admin_stats_enabled(int(u["tg_id"]), True)
    await message.answer(
        "✅ Режим admin-статов включен.\n"
        "При надетых admin-предметах активируются кнопки «Испепелить/Пощадить»."
    )


@router.message(F.text.lower().regexp(r'^(одеть|надеть)\s+(оружие|пета|питомца|питомец)\s+\d+(\s+\d+)?$'))
async def cmd_equip_item(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    parts = message.text.strip().lower().split()
    token = parts[1]
    item_type = "weapon" if token == "оружие" else "pet"

    # Поддержка: надеть оружие 2 [id] — второй слот
    slot2 = False
    if len(parts) >= 4 and parts[2] == "2" and parts[3].isdigit():
        slot2 = True
        item_id = int(parts[3])
        target_id = int(u["tg_id"])
    elif len(parts) >= 4 and parts[3].isdigit() and parts[2].isdigit():
        item_id = int(parts[2])
        target_id = int(parts[3])
    else:
        item_id = int(parts[2])
        target_id = int(u["tg_id"])

    if target_id != int(u["tg_id"]) and not _is_creator(u):
        await message.answer("❌ Нет прав надевать предметы другим.")
        return
    it = db.get_inventory_item(target_id, item_id)
    if not it:
        await message.answer("❌ Предмет с таким ID не найден.")
        return
    if int(it["in_bank"] or 0):
        await message.answer("❌ Этот предмет в банке. Сначала сними его из банка.")
        return

    actual_type = str(it["type"] or "")
    if actual_type not in ("weapon", "pet"):
        await message.answer("❌ Неверный тип предмета.")
        return

    if slot2:
        has_slot = _has_slot2_weapon(target_id) if actual_type == "weapon" else _has_slot2_pet(target_id)
        if not has_slot:
            await message.answer(f"❌ Второй слот {'оружия' if actual_type == 'weapon' else 'питомца'} не куплен.\nКупить: донат → ⚔️ Слоты экипировки")
            return
        field = "equipped_weapon_id_2" if actual_type == "weapon" else "equipped_pet_id_2"
        db.update_user(target_id, **{field: item_id})
        await message.answer(f"✅ Надет во второй слот {'оружия' if actual_type == 'weapon' else 'питомца'}.")
        return

    if actual_type != item_type:
        db.set_equipped_item(target_id, actual_type, item_id)
        await message.answer(
            f"✅ Надет предмет {item_id} как {'оружие' if actual_type == 'weapon' else 'питомец'}.")
        return

    if db.set_equipped_item(target_id, item_type, item_id):
        await message.answer(f"✅ Предмет {item_id} надет.")
    else:
        await message.answer("❌ Не удалось надеть предмет.")


@router.callback_query(F.data.startswith("ring:"))
async def cb_ring(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    lvl = int(cb.data.split(":")[1])
    ring_level = int(u["ring_level"] or 0)
    if lvl > ring_level:
        await cb.answer("❌ Это кольцо ещё не доступно.", show_alert=True)
        return
    db.update_user(int(u["tg_id"]), active_ring_level=lvl)
    u2 = db.get_user(int(u["tg_id"]))
    await _safe_edit_cb(cb, _loadout_text(u2), reply_markup=_loadout_kb(u2))
    await cb.answer(f"💍 Кольцо выбрано: {gd.RING_NAMES.get(lvl, 'Нет')}")


@router.callback_query(F.data.startswith("aura:"))
async def cb_aura(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    aura_key = cb.data.split(":")[1]
    tg_id = int(u["tg_id"])
    if aura_key and not int(_row_get(u, f"aura_{aura_key}", 0) or 0):
        await cb.answer("❌ Эта аура не куплена.", show_alert=True)
        return
    db.update_user(tg_id, active_aura=aura_key)
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _loadout_text(u2), reply_markup=_loadout_kb(u2))
    name = gd.AURA_CATALOG[aura_key]["name"] if aura_key in gd.AURA_CATALOG else "Нет"
    await cb.answer(f"✨ Аура: {name}")


@router.callback_query(F.data == "loadout:best")
async def cb_loadout_best(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    import sqlite3 as _sq
    con = _sq.connect("bot.db", timeout=30)
    con.row_factory = _sq.Row
    weapons = con.execute(
        "SELECT id FROM inventory WHERE tg_id=? AND type='weapon' AND in_bank=0 ORDER BY bonus DESC LIMIT 2",
        (tg_id,)
    ).fetchall()
    pets = con.execute(
        "SELECT id FROM inventory WHERE tg_id=? AND type='pet' AND in_bank=0 ORDER BY bonus DESC LIMIT 2",
        (tg_id,)
    ).fetchall()
    con.close()
    updates = {}
    if weapons:
        updates["equipped_weapon_id"] = int(weapons[0]["id"])
    if len(weapons) >= 2 and _has_slot2_weapon(tg_id):
        updates["equipped_weapon_id_2"] = int(weapons[1]["id"])
    if pets:
        updates["equipped_pet_id"] = int(pets[0]["id"])
    if len(pets) >= 2 and _has_slot2_pet(tg_id):
        updates["equipped_pet_id_2"] = int(pets[1]["id"])
    if updates:
        db.update_user(tg_id, **updates)
    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _loadout_text(u2), reply_markup=_loadout_kb(u2))
    await cb.answer("⚡ Лучшее надето!")


@router.callback_query(F.data.in_({"loadout:w2:equip", "loadout:w2:unequip",
                                    "loadout:p2:equip", "loadout:p2:unequip"}))
async def cb_loadout_slot2(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    action = cb.data  # loadout:w2:equip etc

    if action == "loadout:w2:unequip":
        if not _has_slot2_weapon(tg_id):
            await cb.answer("❌ Второй слот оружия не куплен.", show_alert=True)
            return
        db.update_user(tg_id, equipped_weapon_id_2=0)
        await cb.answer("🗡 Оружие 2 снято.")

    elif action == "loadout:p2:unequip":
        if not _has_slot2_pet(tg_id):
            await cb.answer("❌ Второй слот питомца не куплен.", show_alert=True)
            return
        db.update_user(tg_id, equipped_pet_id_2=0)
        await cb.answer("🐾 Питомец 2 снят.")

    elif action == "loadout:w2:equip":
        if not _has_slot2_weapon(tg_id):
            await cb.answer("❌ Второй слот оружия не куплен.", show_alert=True)
            return
        # Найти лучшее оружие не занятое первым слотом
        eq_w1 = int(_row_get(u, "equipped_weapon_id", 0) or 0)
        import sqlite3 as _sq
        con = _sq.connect("bot.db", timeout=30)
        con.row_factory = _sq.Row
        row = con.execute(
            "SELECT id FROM inventory WHERE tg_id=? AND type='weapon' AND in_bank=0 AND id!=? ORDER BY bonus DESC LIMIT 1",
            (tg_id, eq_w1)
        ).fetchone()
        con.close()
        if not row:
            await cb.answer("❌ Нет свободного оружия для второго слота.", show_alert=True)
            return
        db.update_user(tg_id, equipped_weapon_id_2=int(row["id"]))
        await cb.answer("🗡 Оружие 2 надето!")

    elif action == "loadout:p2:equip":
        if not _has_slot2_pet(tg_id):
            await cb.answer("❌ Второй слот питомца не куплен.", show_alert=True)
            return
        eq_p1 = int(_row_get(u, "equipped_pet_id", 0) or 0)
        import sqlite3 as _sq
        con = _sq.connect("bot.db", timeout=30)
        con.row_factory = _sq.Row
        row = con.execute(
            "SELECT id FROM inventory WHERE tg_id=? AND type='pet' AND in_bank=0 AND id!=? ORDER BY bonus DESC LIMIT 1",
            (tg_id, eq_p1)
        ).fetchone()
        con.close()
        if not row:
            await cb.answer("❌ Нет свободного питомца для второго слота.", show_alert=True)
            return
        db.update_user(tg_id, equipped_pet_id_2=int(row["id"]))
        await cb.answer("🐾 Питомец 2 надето!")

    u2 = db.get_user(tg_id)
    await _safe_edit_cb(cb, _loadout_text(u2), reply_markup=_loadout_kb(u2))


@router.callback_query(F.data == "loadout:refresh")
async def cb_loadout_refresh(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    u2 = db.get_user(int(u["tg_id"]))
    await _safe_edit_cb(cb, _loadout_text(u2), reply_markup=_loadout_kb(u2))
    await cb.answer()


@router.callback_query(F.data == "loadout:noop")
async def cb_loadout_noop(cb: CallbackQuery):
    await cb.answer()


@router.callback_query(F.data == "loadout:close")
async def cb_loadout_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "🧩 Экипировка закрыта.")
    await cb.answer()


@router.message(F.text.lower().in_({"боссы", "boss", "🏛️ боссы"}))
async def cmd_bosses(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("⚔️ Боссы доступны только в ЛС.")
        return
    tg_id = int(u["tg_id"])
    bs = ACTIVE_BATTLES.get(tg_id)
    if bs:
        # Если старое сообщение боя потеряно (удалена история), даем новую точку входа.
        # Не спамим длинными служебными фразами.
        if time.time() - float(bs.last_action or 0) > BATTLE_STALE_SEC:
            ACTIVE_BATTLES.pop(tg_id, None)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            _drop_battle_state(tg_id)
            user_arena = int(u["arena"] or 1)
            await message.answer(_boss_arenas_text(user_arena), reply_markup=_boss_arena_kb(user_arena))
            return
        resume_msg = await message.answer("⚔️")
        ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
        bs.chat_id = resume_msg.chat.id
        bs.msg_id = resume_msg.message_id
        bs.last_action = time.time()
        ACTIVE_BATTLES_BY_MSG[(bs.chat_id, bs.msg_id)] = tg_id
        _persist_battle_state(bs)
        admin_mode = _admin_mode_active(u)
        await _safe_edit(resume_msg, _battle_view(bs, admin_mode=admin_mode), reply_markup=_battle_kb(admin_mode=admin_mode))
        return
    user_arena = int(u["arena"] or 1)
    await message.answer(_boss_arenas_text(user_arena), reply_markup=_boss_arena_kb(user_arena))


@router.callback_query(F.data == "boss_arenas")
async def cb_boss_arenas(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    user_arena = int(u["arena"] or 1)
    await _safe_edit_cb(cb, _boss_arenas_text(user_arena), reply_markup=_boss_arena_kb(user_arena))
    await cb.answer()


@router.callback_query(F.data.startswith("boss_arena:"))
async def cb_boss_arena(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    parts = str(cb.data or "").split(":")
    if len(parts) == 2 and parts[1].isdigit():
        arena = int(parts[1])
    elif len(parts) >= 3 and parts[-1].isdigit():
        # Совместимость со старыми кнопками boss_arena:world:arena
        arena = int(parts[-1])
    else:
        await cb.answer("Кнопка устарела. Нажми 'Боссы' заново.", show_alert=True)
        return
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        await cb.answer("🔒 Арена не открыта.", show_alert=True)
        return
    await _safe_edit_cb(cb, f"⚔️ Арена {arena}: {gd.arena_title(arena)}", reply_markup=_boss_select_kb(arena))
    await cb.answer()


@router.callback_query(F.data == "boss_arena_locked")
async def cb_boss_arena_locked(cb: CallbackQuery):
    await cb.answer("🔒 Эта арена ещё не открыта.", show_alert=True)


@router.callback_query(F.data == "boss_close")
async def cb_boss_close(cb: CallbackQuery):
    await _safe_edit_cb(cb, "⚔️ Боссы закрыты.")
    await cb.answer()


@router.callback_query(F.data.startswith("boss_pick:"))
async def cb_boss_pick(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    _, arena_s, idx_s = cb.data.split(":")
    arena = int(arena_s)
    boss_idx = int(idx_s)
    if tg_id in ACTIVE_BATTLES:
        # Вместо глухой ошибки показываем текущий активный бой.
        bs = ACTIVE_BATTLES[tg_id]
        ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
        bs.chat_id = cb.message.chat.id
        bs.msg_id = cb.message.message_id
        ACTIVE_BATTLES_BY_MSG[(bs.chat_id, bs.msg_id)] = tg_id
        _persist_battle_state(bs)
        admin_mode = _admin_mode_active(u)
        await _render_battle_cb(cb, bs, _battle_view(bs, admin_mode=admin_mode), reply_markup=_battle_kb(admin_mode=admin_mode))
        await cb.answer("⚔️ У тебя уже есть активный бой")
        return

    # Проверяем что арена босса не превышает текущую арену игрока
    user_arena = int(u["arena"] or 1)
    if arena > user_arena:
        await cb.answer("🔒 Эта арена ещё не открыта.", show_alert=True)
        return
    boss = gd.ARENAS[arena][boss_idx]
    boss_hp = int(boss.hp * _enemy_hp_mult(arena))
    boss_atk = int(boss.atk * _enemy_atk_mult(arena))
    player_max_hp = _calc_player_max_hp(u)
    player_dmg = _calc_player_damage(u)
    regen_per_tick = max(1, int(_calc_regen(u) * BATTLE_REGEN_MULT))
    bs = BattleState(
        user_id=tg_id,
        arena=arena,
        boss_idx=boss_idx,
        boss_hp=boss_hp,
        boss_max_hp=boss_hp,
        player_hp=player_max_hp,
        player_max_hp=player_max_hp,
        player_dmg=player_dmg,
        regen_per_tick=regen_per_tick,
        boss_atk=boss_atk,
        msg_id=cb.message.message_id,
        chat_id=cb.message.chat.id,
    )
    ACTIVE_BATTLES[tg_id] = bs
    ACTIVE_BATTLES_BY_MSG[(cb.message.chat.id, cb.message.message_id)] = tg_id
    _persist_battle_state(bs)
    admin_mode = _admin_mode_active(u)
    await _render_battle_cb(cb, bs, _battle_view(bs, admin_mode=admin_mode), reply_markup=_battle_kb(admin_mode=admin_mode))
    await cb.answer()


@router.callback_query(F.data.startswith("battle:"))
async def cb_battle(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    action = cb.data.split(":")[1]

    bs = ACTIVE_BATTLES.get(tg_id)
    if not bs:
        # Если контекст по user_id потерялся, пробуем найти бой по сообщению.
        msg_key = (cb.message.chat.id, cb.message.message_id)
        owner_id = ACTIVE_BATTLES_BY_MSG.get(msg_key)
        if owner_id == tg_id:
            bs = ACTIVE_BATTLES.get(owner_id)
    if not bs:
        # Если процесс/память временно потеряли бой, восстанавливаем из БД.
        bs = _restore_battle_from_db_row(db.get_active_battle(tg_id))
    if not bs:
        await _safe_edit_cb(
            cb,
            "⚠️ Бой не найден или уже завершён.\nВыбери босса заново:",
            reply_markup=_boss_arena_kb(int(u["arena"] or 1)),
        )
        await cb.answer("Бой завершён, выбери босса снова.", show_alert=True)
        return

    bs.last_action = time.time()
    admin_mode = _admin_mode_active(u)

    user_arena = int(u["arena"] or 1)

    # Если после ребёрта арена игрока стала меньше арены активного боя — закрываем бой
    if bs.arena > user_arena and not admin_mode:
        ACTIVE_BATTLES.pop(tg_id, None)
        ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
        _drop_battle_state(tg_id)
        await _safe_edit_cb(
            cb,
            f"🔒 Бой отменён: арена {bs.arena} недоступна после ребёрта.\nВыбери босса заново:",
            reply_markup=_boss_arena_kb(user_arena),
        )
        await cb.answer("🔒 Арена недоступна после ребёрта.", show_alert=True)
        return

    if action == "close":
        ACTIVE_BATTLES.pop(tg_id, None)
        ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
        _drop_battle_state(tg_id)
        await _safe_edit_cb(cb, "⚔️ Бой закрыт.")
        await cb.answer()
        return

    if action == "back":
        ACTIVE_BATTLES.pop(tg_id, None)
        ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
        _drop_battle_state(tg_id)
        await _safe_edit_cb(cb, _boss_arenas_text(user_arena), reply_markup=_boss_arena_kb(user_arena))
        await cb.answer()
        return

    if action == "refresh":
        # Применяем реген
        now = time.time()
        elapsed = now - bs.last_regen
        if elapsed >= BATTLE_REGEN_INTERVAL_SEC:
            ticks = int(elapsed // BATTLE_REGEN_INTERVAL_SEC)
            regen = bs.regen_per_tick * ticks
            bs.player_hp = min(bs.player_max_hp, bs.player_hp + regen)
            bs.last_regen = now
        _persist_battle_state(bs)
        await _render_battle_cb(cb, bs, _battle_view(bs, admin_mode=admin_mode), reply_markup=_battle_kb(admin_mode=admin_mode))
        await cb.answer()
        return

    if action in ("attack", "burn", "mercy"):
        if action in ("burn", "mercy") and not admin_mode:
            await cb.answer("❌ Режим admin-статов отключен или не надет admin-предмет.", show_alert=True)
            return
        now = time.time()
        if action == "attack":
            hit_cd_left = 1.0 - (now - float(bs.last_hit or 0.0))
            if hit_cd_left > 0:
                await cb.answer(f"⏳ КД удара: {max(1, int(math.ceil(hit_cd_left)))}с", show_alert=False)
                return
            bs.last_hit = now
        log_lines = []
        regen_heal = 0
        boss = gd.ARENAS[bs.arena][bs.boss_idx]
        # Реген
        elapsed = now - bs.last_regen
        if elapsed >= BATTLE_REGEN_INTERVAL_SEC:
            ticks = int(elapsed // BATTLE_REGEN_INTERVAL_SEC)
            heal = int(bs.regen_per_tick * ticks * (1.0 + float(_artifact_effects(u).get("heal", 0.0))))
            prev_hp = bs.player_hp
            bs.player_hp = min(bs.player_max_hp, bs.player_hp + heal)
            regen_heal = max(0, bs.player_hp - prev_hp)
            bs.last_regen = now

        # Урон игрока
        crit = False
        if action == "burn":
            player_dmg = max(1, int(bs.boss_hp))
            log_lines.append("🔥 Испепеление: цель уничтожена с одного удара")
        elif action == "mercy":
            player_dmg = 1_000_000_000
            log_lines.append("🤲 Пощада: фиксированный урон 1 000 000 000")
        else:
            player_dmg = bs.player_dmg
            if int(bs.arena) >= 11:
                player_dmg = max(1, int(player_dmg * 0.95))
            crit = random.random() < _artifact_crit_chance(u)
            if crit:
                player_dmg = int(player_dmg * 1.5)
        bs.boss_hp -= player_dmg
        if not admin_mode:
            lifesteal_pct = _artifact_lifesteal_pct(u)
            if lifesteal_pct > 0:
                heal = int(player_dmg * lifesteal_pct)
                if heal > 0:
                    old_hp = bs.player_hp
                    bs.player_hp = min(bs.player_max_hp, bs.player_hp + heal)
                    real_heal = max(0, bs.player_hp - old_hp)
                    if real_heal > 0:
                        log_lines.append(f"🩸 Вампиризм: +{fmt_num(real_heal)} HP")
        log_lines.append(f"⚔️ Ты нанес: {fmt_num(player_dmg)} урона{' (крит!)' if crit else ''}")

        if bs.boss_hp <= 0:
            bs.boss_hp = 0
            # Победа
            boss = gd.ARENAS[bs.arena][bs.boss_idx]
            ring_mult = _ring_bonus_mult(u)
            aura_mult = _aura_gold_mult(u)
            vip_mult = _vip_gold_mult(u)
            guild_mult = _guild_coin_mult(u)
            true_coin_mult = _true_coin_mult(u)
            wd = _today_msk().weekday()
            day_mult = 2 if wd == 2 else 1  # среда x2
            # Среда должна давать честный x2 к базовой награде, без скрытого понижения.
            final_boss_mult = 1.0
            if bs.arena == gd.max_arena() and bs.boss_idx == len(gd.ARENAS[bs.arena]) - 1:
                final_boss_mult = FINAL_BOSS_REWARD_MULT
            reward = int(
                boss.reward
                * BOSS_REWARD_MULT
                * _boss_reward_arena_mult(bs.arena)
                * final_boss_mult
                * ring_mult
                * aura_mult
                * vip_mult
                * guild_mult
                * true_coin_mult
                * _artifact_coin_mult(u)
                * day_mult
            )
            cur_mask = int(_row_get(u, "boss_kill_mask", 0) or 0)
            cur_arena = int(u["arena"] or 1)
            # Прогресс перехода обновляем только если босс убит на текущей арене игрока.
            # Фарм старых арен не должен закрывать "убить всех 3 боссов" на новой арене.
            new_mask = cur_mask
            next_progress = min(3, cur_mask.bit_count())
            updates = {
                "coins": int(u["coins"] or 0) + reward,
                "total_boss_kills": int(u["total_boss_kills"] or 0) + 1,
            }
            if bs.arena == cur_arena:
                new_mask = cur_mask | (1 << bs.boss_idx)
                next_progress = min(3, new_mask.bit_count())
                updates["boss_progress"] = next_progress
                updates["boss_kill_mask"] = new_mask
            db.update_user(tg_id, **updates)
            db.add_stat(tg_id, _stat_boss_key(bs.arena, bs.boss_idx), 1)
            if bs.arena >= 6:
                db.add_stat(tg_id, "boss_kill:world23", 1)
            ACTIVE_BATTLES.pop(tg_id, None)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            _drop_battle_state(tg_id)
            txt = (
                    f"🏆 {boss.name} повержен\n"
                    f"+{fmt_num(reward)} 🪙 | Прогресс {next_progress}/3"
                    + (" (x2)" if wd == 2 else "")
            )
            await _safe_edit_cb(
                cb,
                txt
                + f"\n\n⚔️ Арена {bs.arena}: {gd.arena_title(bs.arena)}\nВыбери босса:",
                reply_markup=_boss_select_kb(bs.arena),
            )
            await cb.answer("🏆 Победа!")
            return

        # Урон босса
        boss_dmg = 0
        boss_crit = False
        if not admin_mode:
            _total_dodge = min(0.85, _artifact_dodge_chance(u) + _enchant_dodge_chance(u))
            if random.random() < _total_dodge:
                boss_dmg = 0
                boss_crit = False
                log_lines.append("⚡ Уклонение!")
            else:
                boss_dmg = bs.boss_atk
                boss_crit = random.random() < 0.01
                if boss_crit:
                    boss_dmg = int(boss_dmg * 1.5)
                bs.player_hp -= boss_dmg
                reflect_pct = _artifact_reflect_pct(u)
                if reflect_pct > 0 and boss_dmg > 0:
                    reflected = int(boss_dmg * reflect_pct)
                    if reflected > 0:
                        bs.boss_hp = max(0, bs.boss_hp - reflected)
                        log_lines.append(f"🛡 Отражение: -{fmt_num(reflected)}")
        else:
            bs.player_hp = bs.player_max_hp

        if bs.boss_hp <= 0:
            bs.boss_hp = 0
            reward = int(
                boss.reward
                * BOSS_REWARD_MULT
                * _boss_reward_arena_mult(bs.arena)
                * (FINAL_BOSS_REWARD_MULT if (bs.arena == gd.max_arena() and bs.boss_idx == len(gd.ARENAS[bs.arena]) - 1) else 1.0)
                * _ring_bonus_mult(u)
                * _aura_gold_mult(u)
                * _vip_gold_mult(u)
                * _guild_coin_mult(u)
                * _true_coin_mult(u)
                * _artifact_coin_mult(u)
                * (2 if _today_msk().weekday() == 2 else 1)
            )
            cur_mask = int(_row_get(u, "boss_kill_mask", 0) or 0)
            cur_arena = int(u["arena"] or 1)
            new_mask = cur_mask
            next_progress = min(3, cur_mask.bit_count())
            updates = {
                "coins": int(u["coins"] or 0) + reward,
                "total_boss_kills": int(u["total_boss_kills"] or 0) + 1,
            }
            if bs.arena == cur_arena:
                new_mask = cur_mask | (1 << bs.boss_idx)
                next_progress = min(3, new_mask.bit_count())
                updates["boss_progress"] = next_progress
                updates["boss_kill_mask"] = new_mask
            db.update_user(tg_id, **updates)
            db.add_stat(tg_id, _stat_boss_key(bs.arena, bs.boss_idx), 1)
            if bs.arena >= 6:
                db.add_stat(tg_id, "boss_kill:world23", 1)
            ACTIVE_BATTLES.pop(tg_id, None)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            _drop_battle_state(tg_id)
            txt = f"🏆 {boss.name} повержен\n+{fmt_num(reward)} 🪙 | Прогресс {next_progress}/3"
            await _safe_edit_cb(
                cb,
                txt
                + f"\n\n⚔️ Арена {bs.arena}: {gd.arena_title(bs.arena)}\nВыбери босса:",
                reply_markup=_boss_select_kb(bs.arena),
            )
            await cb.answer("🏆 Победа!")
            return

        if bs.player_hp <= 0:
            # 🕯 Разовый шанс выжить в бою с 1 HP.
            if not bool(getattr(bs, "survive_used", False)) and random.random() < _artifact_survive_chance(u):
                bs.player_hp = 1
                bs.survive_used = True
                log_lines.append("🕯 Свеча Жизни спасла тебя от смертельного удара")
            else:
                bs.player_hp = 0
                cur_coins = int(u["coins"] or 0)
                loss_pct = random.randint(7, 30)
                loss = int(cur_coins * loss_pct / 100)
                db.update_user(tg_id, coins=max(0, cur_coins - loss))
                ACTIVE_BATTLES.pop(tg_id, None)
                ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
                _drop_battle_state(tg_id)
                await _safe_edit_cb(
                    cb,
                    f"☠ Поражение: -{fmt_num(loss)} 🪙 ({loss_pct}%)",
                    reply_markup=_boss_arena_kb(user_arena),
                )
                await cb.answer("☠ Поражение!")
                return

        log_lines.append(f"👾 Босс ударил: {fmt_num(boss_dmg)} урона{' (крит!)' if boss_crit else ''}")

        _persist_battle_state(bs)
        await _render_battle_cb(
            cb,
            bs,
            _battle_view(bs, log_lines, admin_mode=admin_mode, regen_heal=regen_heal),
            reply_markup=_battle_kb(admin_mode=admin_mode),
        )
        await cb.answer()




# ─────────────────────────────────────────────
#  ДАНЖ
# ─────────────────────────────────────────────
async def _start_dungeon_for_user(send_func, u, chat_id: int, diff: str):
    """Стартует данж выбранной сложности (или возвращает в текущий активный данж)."""
    tg_id = int(u["tg_id"])
    now = time.time()
    last = DUNGEON_LOCKS.get(tg_id, 0)
    if now - last < 3:
        await send_func("⏳ Подожди немного перед новым данжем.", None)
        return

    if tg_id in ACTIVE_DUNGEONS:
        ds = ACTIVE_DUNGEONS[tg_id]
        if int(time.time() - ds.started_at) >= 600:
            updates = {
                "coins": int(u["coins"] or 0) + ds.gold,
                "magic_coins": int(u["magic_coins"] or 0) + ds.magic,
            }
            for k, v in ds.shards.items():
                updates[f"shard_{k}"] = int(_row_get(u, f"shard_{k}", 0) or 0) + v
            db.update_user(tg_id, **updates)
            ACTIVE_DUNGEONS.pop(tg_id, None)
            ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)
            _drop_dungeon_state(tg_id)
            await send_func("⏱ Предыдущий данж завершен по таймауту. Награды начислены.", None)
        else:
            msg = await send_func(_dungeon_view(ds), _dungeon_kb())
            ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)
            ds.chat_id = msg.chat.id
            ds.msg_id = msg.message_id
            ACTIVE_DUNGEONS_BY_MSG[(ds.chat_id, ds.msg_id)] = tg_id
            _persist_dungeon_state(ds)
            return

    mode = _today_dungeon_mode()
    arena = int(u["arena"] or 1)
    ds = DungeonState(tg_id, mode, arena, 0, int(chat_id), difficulty=str(diff or "easy"))
    ds.player_dmg = _calc_player_damage(u)
    ds.wave = 1
    hp_mult = _dungeon_hp_mult_by_difficulty(ds.difficulty, ds.wave, arena)
    ds.enemy_max_hp = int((1800 + (ds.wave ** 2.25) * 180 + arena * 2200) * _enemy_hp_mult(arena) * hp_mult)
    ds.enemy_hp = ds.enemy_max_hp
    ds.enemy_atk = int((40 + ds.wave * 14 + arena * 26) * _enemy_atk_mult(arena) * (1.0 if ds.difficulty == "easy" else (1.4 if ds.difficulty == "medium" else 1.9)))
    msg = await send_func(_dungeon_view(ds), _dungeon_kb())
    ds.chat_id = msg.chat.id
    ds.msg_id = msg.message_id
    ACTIVE_DUNGEONS[tg_id] = ds
    ACTIVE_DUNGEONS_BY_MSG[(msg.chat.id, msg.message_id)] = tg_id
    DUNGEON_LOCKS[tg_id] = now
    _persist_dungeon_state(ds)


@router.message(Command("dungeon"))
@router.message(F.text.lower().in_({"данж", "данжен", "⛩️ данж"}))
@router.message(F.text.lower().regexp(r"^(данж|данжен|⛩️ данж)\s+\S+"))
async def cmd_dungeon(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if message.chat.type != ChatType.PRIVATE:
        await message.answer("🗺 Данж доступен только в ЛС.")
        return
    arena = int(u["arena"] or 1)
    raw = str(message.text or "").strip().lower()

    # Кнопка нижней панели приходит как "⛩️ Данж" (2 токена из-за emoji),
    # но это не аргумент сложности. Извлекаем только явный хвост после команды.
    diff_tail = ""
    m = re.match(r"^(?:/dungeon(?:@\w+)?|данж|данжен|⛩️?\s*данж)(?:\s+(.*))?$", raw, re.IGNORECASE)
    if m and m.group(1):
        diff_tail = str(m.group(1)).strip()

    if not diff_tail:
        msg = await message.answer(_dungeon_diff_select_text(arena), reply_markup=_dungeon_diff_kb(arena))
        _set_cb_owner(msg.chat.id, msg.message_id, int(u["tg_id"]))
        return

    diff = _parse_dungeon_difficulty(f"данж {diff_tail}")
    if not _dungeon_diff_unlocked(arena, diff):
        min_arena = _dungeon_diff_min_arena(diff)
        await message.answer(f"🔒 Сложность {DUNGEON_DIFFICULTY_NAMES.get(diff, diff)} откроется с арены {min_arena}.")
        return

    async def _send(text: str, reply_markup=None):
        return await message.answer(text, reply_markup=reply_markup)

    await _start_dungeon_for_user(_send, u, int(message.chat.id), diff)


def _dungeon_apply_rewards(ds: DungeonState, u: dict) -> tuple[int, int, dict[int, int]]:
    """Считает награды за волну. Возвращает (gold, magic, shards_dict)."""
    wave = ds.wave
    arena = ds.arena
    diff_key = str(ds.difficulty or "easy")
    reward_mult = _dungeon_reward_mult_by_difficulty(diff_key)

    ring_mult = _ring_bonus_mult(u)
    aura_mult = _aura_gold_mult(u)
    vip_mult = _vip_gold_mult(u)
    guild_mult = _guild_coin_mult(u)
    true_coin_mult = _true_coin_mult(u)
    wave_gold = int((32 + wave * 8 + arena * 22) * (1 + wave * 0.025))
    if ds.mode == "greed":
        wave_gold *= 2
    wave_gold = int(
        wave_gold
        * ring_mult
        * aura_mult
        * vip_mult
        * guild_mult
        * true_coin_mult
        * _artifact_coin_mult(u)
        * 0.62
        * reward_mult
    )
    magic = 0
    shards: dict[int, int] = {}
    shard_mult = 1.2 if int(u["vip_lvl"] or 0) >= 2 else 1.0

    wd = _today_msk().weekday()
    if wd == 2 and ds.mode == "greed":
        pass  # x2 уже учтено в формуле

    shard_chance_bonus = 0.0
    magic_mult = 1.0
    if diff_key == "medium":
        shard_chance_bonus = 0.02
        magic_mult = 1.10
    elif diff_key == "hard":
        shard_chance_bonus = 0.04
        magic_mult = 1.20

    if ds.mode == "tomb":
        # Шанс осколков в данже снижен в 10 раз.
        if random.random() < (0.022 + shard_chance_bonus):
            s = random.choices(range(1, 6), weights=SHARD_WEIGHTS)[0]
            shards[s] = shards.get(s, 0) + 1
            if shard_mult > 1 and random.random() < 0.2:
                shards[s] = shards.get(s, 0) + 1
    elif ds.mode == "grot":
        magic = int(
            round(
                random.randint(1, 3)
                * magic_mult
                * (1.0 + float(_artifact_effects(u).get("dungeon_magic", 0.0)))
                * reward_mult
            )
        )
    elif ds.mode == "chaos":
        if random.random() < (0.028 + shard_chance_bonus):
            s = random.choices(range(1, 6), weights=SHARD_WEIGHTS)[0]
            shards[s] = shards.get(s, 0) + 1
            if shard_mult > 1 and random.random() < 0.2:
                shards[s] = shards.get(s, 0) + 1
        if random.random() < 0.55:
            magic = int(
                round(
                    random.randint(1, 2)
                    * magic_mult
                    * (1.0 + float(_artifact_effects(u).get("dungeon_magic", 0.0))
                    )
                    * reward_mult
                )
            )
    return wave_gold, magic, shards


@router.callback_query(F.data.startswith("dungeon:"))
async def cb_dungeon(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    action = cb.data.split(":")[1]

    if action == "close":
        await _safe_edit_cb(cb, "⛩ Выбор сложности закрыт.")
        await cb.answer()
        return

    if action == "diff_locked":
        diff = str((cb.data or "").split(":")[-1] or "easy")
        min_arena = _dungeon_diff_min_arena(diff)
        await cb.answer(f"🔒 Откроется с арены {min_arena}", show_alert=True)
        return

    if action == "diff":
        diff = str((cb.data or "").split(":")[-1] or "easy")
        arena = int(u["arena"] or 1)
        if not _dungeon_diff_unlocked(arena, diff):
            await cb.answer(f"🔒 Откроется с арены {_dungeon_diff_min_arena(diff)}", show_alert=True)
            return

        async def _send(text: str, reply_markup=None):
            return await cb.message.answer(text, reply_markup=reply_markup)

        await _start_dungeon_for_user(_send, u, int(cb.message.chat.id), diff)
        await cb.answer(f"⛩ Сложность: {DUNGEON_DIFFICULTY_NAMES.get(diff, diff)}")
        return

    ds = ACTIVE_DUNGEONS.get(tg_id)
    if not ds:
        # Если данж не в памяти (рестарт/потеря контекста), пробуем поднять из БД.
        db_ds = db.get_active_dungeon(tg_id)
        ds = _restore_dungeon_from_db_row(db_ds)
        if not ds:
            if db_ds:
                # Истекший сохраненный данж закрываем и начисляем награды один раз.
                started_at = float(db_ds["started_at"] or 0)
                if time.time() - started_at > 660:
                    updates = {
                        "coins": int(u["coins"] or 0) + int(db_ds["gold"] or 0),
                        "magic_coins": int(u["magic_coins"] or 0) + int(db_ds["magic"] or 0),
                    }
                    try:
                        shards = json.loads(str(db_ds["shards_json"] or "{}"))
                    except Exception:
                        shards = {}
                    for k, v in dict(shards).items():
                        try:
                            shard_idx = int(k)
                            shard_val = int(v)
                        except Exception:
                            continue
                        updates[f"shard_{shard_idx}"] = int(_row_get(u, f"shard_{shard_idx}", 0) or 0) + shard_val
                    db.update_user(tg_id, **updates)
                    _drop_dungeon_state(tg_id)
                    await _safe_edit_cb(cb, "⏱ Предыдущий данж завершился по времени. Награды начислены.\nНапиши «данж», чтобы начать новый.")
                    await cb.answer("Данж завершен по времени", show_alert=True)
                    return
            await _safe_edit_cb(cb, "🗺 Данж не найден. Напиши «данж», чтобы начать заново.")
            await cb.answer("Данж не найден.", show_alert=True)
            return

    if action == "exit":
        # Выход — сохраняем награды
        await _dungeon_end(cb, ds, u, tg_id)
        return

    if action == "refresh":
        elapsed = int(time.time() - ds.started_at)
        if elapsed >= 600:
            await _dungeon_end(cb, ds, u, tg_id)
            return
        _persist_dungeon_state(ds)
        await _safe_edit_cb(cb, _dungeon_view(ds), reply_markup=_dungeon_kb())
        await cb.answer()
        return

    if action == "attack":
        elapsed = int(time.time() - ds.started_at)
        if elapsed >= 600:
            await _dungeon_end(cb, ds, u, tg_id)
            return

        # Атака волны
        player_dmg = ds.player_dmg
        ds.enemy_hp -= player_dmg

        if ds.enemy_hp <= 0:
            # Волна пройдена
            cleared_wave = int(ds.wave)
            gold, magic, shards = _dungeon_apply_rewards(ds, u)
            ds.gold += gold
            ds.magic += magic
            for k, v in shards.items():
                ds.shards[k] = ds.shards.get(k, 0) + v
            ds.note = f"✅ Волна {ds.wave} | +{fmt_num(gold)}🪙"
            ds.wave += 1
            if ds.wave > ds.max_waves:
                await _dungeon_end(cb, ds, u, tg_id)
                return
            # Следующая волна
            hp_mult = _dungeon_hp_mult_by_difficulty(ds.difficulty, ds.wave, ds.arena)
            ds.enemy_max_hp = int((1800 + (ds.wave ** 2.25) * 180 + ds.arena * 2200) * _enemy_hp_mult(ds.arena) * hp_mult)
            ds.enemy_hp = ds.enemy_max_hp
            ds.enemy_atk = int((40 + ds.wave * 14 + ds.arena * 26) * _enemy_atk_mult(ds.arena) * (1.0 if ds.difficulty == "easy" else (1.4 if ds.difficulty == "medium" else 1.9)))
        else:
            ds.note = f"⚔️ -{fmt_num(player_dmg)}"

        _persist_dungeon_state(ds)
        await _safe_edit_cb(cb, _dungeon_view(ds), reply_markup=_dungeon_kb())
        await cb.answer()


async def _dungeon_end(cb: CallbackQuery, ds: DungeonState, u, tg_id: int):
    ACTIVE_DUNGEONS.pop(tg_id, None)
    ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)
    _drop_dungeon_state(tg_id)
    # Начисляем награды
    coins = int(u["coins"] or 0) + ds.gold
    magic = int(u["magic_coins"] or 0) + ds.magic
    updates = {"coins": coins, "magic_coins": magic}
    # Осколки
    for k, v in ds.shards.items():
        col = f"shard_{k}"
        updates[col] = int(u[col] or 0) + v
    db.update_user(tg_id, **updates)
    text = _dungeon_finish_text(ds)
    await _safe_edit_cb(cb, text)
    await cb.answer("🗺 Данж завершён!")


async def _dungeon_end_msg(message: Message, ds: DungeonState, u, tg_id: int):
    ACTIVE_DUNGEONS.pop(tg_id, None)
    ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)
    _drop_dungeon_state(tg_id)
    updates = {
        "coins": int(u["coins"] or 0) + ds.gold,
        "magic_coins": int(u["magic_coins"] or 0) + ds.magic,
    }
    for k, v in ds.shards.items():
        updates[f"shard_{k}"] = int(_row_get(u, f"shard_{k}", 0) or 0) + v
    db.update_user(tg_id, **updates)
    await message.answer(_dungeon_finish_text(ds), reply_markup=fw_dungeon_kb())


@router.message(F.text.lower().startswith("fw "))
async def cmd_floodwait_actions(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    text = str(message.text or "").strip().lower()

    if text == "fw ◀ в приключения":
        await message.answer("⚔️ Возврат в меню приключений.", reply_markup=adventures_kb())
        return

    bs = ACTIVE_BATTLES.get(tg_id) or _restore_battle_from_db_row(db.get_active_battle(tg_id))
    ds = ACTIVE_DUNGEONS.get(tg_id) or _restore_dungeon_from_db_row(db.get_active_dungeon(tg_id))

    if text == "fw 🔄 обновить":
        if bs:
            now = time.time()
            elapsed = now - bs.last_regen
            if elapsed >= BATTLE_REGEN_INTERVAL_SEC:
                ticks = int(elapsed // BATTLE_REGEN_INTERVAL_SEC)
                regen = bs.regen_per_tick * ticks
                bs.player_hp = min(bs.player_max_hp, bs.player_hp + regen)
                bs.last_regen = now
            _persist_battle_state(bs)
            await message.answer(_battle_view(bs, admin_mode=_admin_mode_active(u)), reply_markup=fw_battle_kb())
            return
        if ds:
            elapsed = int(time.time() - ds.started_at)
            if elapsed >= 600:
                await _dungeon_end_msg(message, ds, u, tg_id)
                return
            _persist_dungeon_state(ds)
            await message.answer(_dungeon_view(ds), reply_markup=fw_dungeon_kb())
            return
        await message.answer("⚠️ Нет активного боя или данжа.", reply_markup=adventures_kb())
        return

    if text in {"fw ⚔️ атаковать", "fw ◀ к боссам", "fw ✖ закрыть"}:
        if not bs:
            await message.answer("⚠️ Активный бой не найден.", reply_markup=adventures_kb())
            return

        user_arena = int(u["arena"] or 1)
        if text == "fw ✖ закрыть":
            ACTIVE_BATTLES.pop(tg_id, None)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            _drop_battle_state(tg_id)
            await message.answer("⚔️ Бой закрыт.", reply_markup=fw_battle_kb())
            return

        if text == "fw ◀ к боссам":
            ACTIVE_BATTLES.pop(tg_id, None)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            _drop_battle_state(tg_id)
            await message.answer(_boss_arenas_text(user_arena), reply_markup=_boss_arena_kb(user_arena))
            return

        # FW атака
        now = time.time()
        hit_cd_left = 1.0 - (now - float(bs.last_hit or 0.0))
        if hit_cd_left > 0:
            await message.answer(f"⏳ КД удара: {max(1, int(math.ceil(hit_cd_left)))}с", reply_markup=fw_battle_kb())
            return
        bs.last_hit = now
        bs.last_action = now
        log_lines = []
        regen_heal = 0

        elapsed = now - bs.last_regen
        if elapsed >= BATTLE_REGEN_INTERVAL_SEC:
            ticks = int(elapsed // BATTLE_REGEN_INTERVAL_SEC)
            heal = int(bs.regen_per_tick * ticks * (1.0 + float(_artifact_effects(u).get("heal", 0.0))))
            prev_hp = bs.player_hp
            bs.player_hp = min(bs.player_max_hp, bs.player_hp + heal)
            regen_heal = max(0, bs.player_hp - prev_hp)
            bs.last_regen = now

        player_dmg = bs.player_dmg
        if int(bs.arena) >= 11:
            player_dmg = max(1, int(player_dmg * 0.95))
        crit = random.random() < _artifact_crit_chance(u)
        if crit:
            player_dmg = int(player_dmg * 1.5)
        bs.boss_hp -= player_dmg

        lifesteal_pct = _artifact_lifesteal_pct(u)
        if lifesteal_pct > 0:
            heal = int(player_dmg * lifesteal_pct)
            if heal > 0:
                old_hp = bs.player_hp
                bs.player_hp = min(bs.player_max_hp, bs.player_hp + heal)
                real_heal = max(0, bs.player_hp - old_hp)
                if real_heal > 0:
                    log_lines.append(f"🩸 Вампиризм: +{fmt_num(real_heal)} HP")
        log_lines.append(f"⚔️ Ты нанес: {fmt_num(player_dmg)} урона{' (крит!)' if crit else ''}")

        boss = gd.ARENAS[bs.arena][bs.boss_idx]
        if bs.boss_hp <= 0:
            bs.boss_hp = 0
            wd = date.today().weekday()
            final_boss_mult = FINAL_BOSS_REWARD_MULT if (bs.arena == gd.max_arena() and bs.boss_idx == len(gd.ARENAS[bs.arena]) - 1) else 1.0
            reward = int(
                boss.reward
                * BOSS_REWARD_MULT
                * _boss_reward_arena_mult(bs.arena)
                * final_boss_mult
                * _ring_bonus_mult(u)
                * _aura_gold_mult(u)
                * _vip_gold_mult(u)
                * _guild_coin_mult(u)
                * _true_coin_mult(u)
                * _artifact_coin_mult(u)
                * (2 if wd == 2 else 1)
            )
            cur_mask = int(_row_get(u, "boss_kill_mask", 0) or 0)
            cur_arena = int(u["arena"] or 1)
            new_mask = cur_mask
            next_progress = min(3, cur_mask.bit_count())
            updates = {"coins": int(u["coins"] or 0) + reward, "total_boss_kills": int(u["total_boss_kills"] or 0) + 1}
            if bs.arena == cur_arena:
                new_mask = cur_mask | (1 << bs.boss_idx)
                next_progress = min(3, new_mask.bit_count())
                updates["boss_progress"] = next_progress
                updates["boss_kill_mask"] = new_mask
            db.update_user(tg_id, **updates)
            db.add_stat(tg_id, _stat_boss_key(bs.arena, bs.boss_idx), 1)
            if bs.arena >= 6:
                db.add_stat(tg_id, "boss_kill:world23", 1)
            ACTIVE_BATTLES.pop(tg_id, None)
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
            _drop_battle_state(tg_id)
            await message.answer(
                f"🏆 {boss.name} повержен\n+{fmt_num(reward)} 🪙 | Прогресс {next_progress}/3\n\n⚔️ Арена {bs.arena}: {gd.arena_title(bs.arena)}\nВыбери босса:",
                reply_markup=_boss_select_kb(bs.arena),
            )
            return

        _total_dodge2 = min(0.85, _artifact_dodge_chance(u) + _enchant_dodge_chance(u))
        if random.random() < _total_dodge2:
            boss_dmg = 0
            boss_crit = False
            log_lines.append("⚡ Уклонение!")
        else:
            boss_dmg = bs.boss_atk
            boss_crit = random.random() < 0.01
            if boss_crit:
                boss_dmg = int(boss_dmg * 1.5)
            bs.player_hp -= boss_dmg
            reflect_pct = _artifact_reflect_pct(u)
            if reflect_pct > 0 and boss_dmg > 0:
                reflected = int(boss_dmg * reflect_pct)
                if reflected > 0:
                    bs.boss_hp = max(0, bs.boss_hp - reflected)
                    log_lines.append(f"🛡 Отражение: -{fmt_num(reflected)}")

        if bs.player_hp <= 0:
            if not bool(getattr(bs, "survive_used", False)) and random.random() < _artifact_survive_chance(u):
                bs.player_hp = 1
                bs.survive_used = True
                log_lines.append("🕯 Свеча Жизни спасла тебя от смертельного удара")
            else:
                bs.player_hp = 0
                cur_coins = int(u["coins"] or 0)
                loss_pct = random.randint(7, 30)
                loss = int(cur_coins * loss_pct / 100)
                db.update_user(tg_id, coins=max(0, cur_coins - loss))
                ACTIVE_BATTLES.pop(tg_id, None)
                ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
                _drop_battle_state(tg_id)
                await message.answer(f"☠ Поражение: -{fmt_num(loss)} 🪙 ({loss_pct}%)", reply_markup=fw_battle_kb())
                return

        log_lines.append(f"👾 Босс ударил: {fmt_num(boss_dmg)} урона{' (крит!)' if boss_crit else ''}")
        _persist_battle_state(bs)
        await message.answer(_battle_view(bs, log_lines, admin_mode=False, regen_heal=regen_heal), reply_markup=fw_battle_kb())
        return

    if text in {"fw ⚔️ волна", "fw 🚪 выйти"}:
        if not ds:
            await message.answer("⚠️ Активный данж не найден.", reply_markup=adventures_kb())
            return

        if text == "fw 🚪 выйти":
            await _dungeon_end_msg(message, ds, u, tg_id)
            return

        elapsed = int(time.time() - ds.started_at)
        if elapsed >= 600:
            await _dungeon_end_msg(message, ds, u, tg_id)
            return

        # FW волна
        player_dmg = ds.player_dmg
        ds.enemy_hp -= player_dmg
        if ds.enemy_hp <= 0:
            cleared_wave = int(ds.wave)
            gold, magic, shards = _dungeon_apply_rewards(ds, u)
            ds.gold += gold
            ds.magic += magic
            for k, v in shards.items():
                ds.shards[k] = ds.shards.get(k, 0) + v
            ds.note = f"✅ Волна {ds.wave} | +{fmt_num(gold)}🪙"
            ds.wave += 1
            if ds.wave > ds.max_waves:
                await _dungeon_end_msg(message, ds, u, tg_id)
                return
            hp_mult = _dungeon_hp_mult_by_difficulty(ds.difficulty, ds.wave, ds.arena)
            ds.enemy_max_hp = int((1800 + (ds.wave ** 2.25) * 180 + ds.arena * 2200) * _enemy_hp_mult(ds.arena) * hp_mult)
            ds.enemy_hp = ds.enemy_max_hp
            ds.enemy_atk = int((40 + ds.wave * 14 + ds.arena * 26) * _enemy_atk_mult(ds.arena) * (1.0 if ds.difficulty == "easy" else (1.4 if ds.difficulty == "medium" else 1.9)))
        else:
            ds.note = f"⚔️ -{fmt_num(player_dmg)}"

        _persist_dungeon_state(ds)
        await message.answer(_dungeon_view(ds), reply_markup=fw_dungeon_kb())
        return


# ─────────────────────────────────────────────
#  АРЕНА (ПЕРЕХОД)
# ─────────────────────────────────────────────
@router.message(Command("arena"))
@router.message(F.text.lower().in_({"арена", "🏟️ арена"}))
async def cmd_arena(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    arena = int(u["arena"] or 1)
    if arena >= gd.max_arena():
        await message.answer(f"🏟 Ты уже на максимальной арене ({arena})!")
        return

    boss_progress = _boss_unique_progress(u)
    if boss_progress != int(_row_get(u, "boss_progress", 0) or 0):
        db.update_user(tg_id, boss_progress=boss_progress)

    ready, status_lines = _arena_requirements_status(u)
    if not ready:
        await message.answer("\n".join(status_lines))
        return

    new_arena = arena + 1
    db.update_user(tg_id, arena=new_arena, boss_progress=0, boss_kill_mask=0)
    await _maybe_complete_referral(tg_id)
    await message.answer(
        f"🏟 Переход на арену {new_arena}: {gd.arena_title(new_arena)}!\n"
        f"🌫 {gd.arena_mood(new_arena)}"
    )


# ─────────────────────────────────────────────
#  РЕБЁРТ
# ─────────────────────────────────────────────
@router.message(Command("rebirth"))
@router.message(F.text.lower().in_({"реберт", "ребёрт", "⚡ ребёрт", "ребёрты", "♻️ ребёрты"}))
async def cmd_rebirth(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await message.answer(_rebirths_hub_text(u), reply_markup=_rebirths_hub_kb())


@router.callback_query(F.data == "rebirth:hub")
async def cb_rebirth_hub(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    await _safe_edit_cb(cb, _rebirths_hub_text(u), reply_markup=_rebirths_hub_kb())
    await cb.answer()


@router.callback_query(F.data == "rebirth:open_normal")
async def cb_rebirth_open_normal(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    text, kb = _normal_rebirth_offer_text(u)
    await _safe_edit_cb(cb, text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "rebirth:open_true")
async def cb_rebirth_open_true(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    text, kb = _true_rebirth_offer_text(u)
    await _safe_edit_cb(cb, text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data == "rebirth:confirm_normal")
async def cb_rebirth_confirm(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    tg_id = int(u["tg_id"])
    rebirth_count = int(u["rebirth_count"] or 0)
    cost = int(220000 * (3.0 ** rebirth_count) * ECONOMY_COST_MULT)
    power_required = _rebirth_power_required(rebirth_count)
    coins = int(u["coins"] or 0)
    power_now = int(u["power"] or 0)
    if coins < cost or power_now < power_required:
        await cb.answer(
            f"❌ Нужно {fmt_num(cost)} 🪙 и {fmt_num(power_required)} ⚙️",
            show_alert=True,
        )
        return

    # После ребёрта игрок не должен оставаться в старом бою/данже.
    _clear_user_combat_states(tg_id)

    new_mult = _rebirth_mult_expected(rebirth_count + 1)
    new_rank = min(int(u["rank_idx"] or 0) + 1, len(gd.RANKS) - 1)
    db.update_user(tg_id,
                   coins=0, arena=1, boss_progress=0, boss_kill_mask=0, power=0,
                   deposit_amount=0, deposit_started_at=0,
                   rebirth_count=rebirth_count + 1,
                   rebirth_mult=new_mult,
                   rank_idx=new_rank,
                   last_rebirth_at=int(time.time()),
                   )
    db.clear_stats(tg_id)
    rank_name = gd.RANKS[new_rank]
    await _safe_edit_cb(cb,
                        f"♻️ Ребёрт #{rebirth_count + 1} выполнен!\n"
                        f"🏅 Новый ранг: {rank_name}\n"
                        f"💢 Множитель урона: x{new_mult:.2f}"
                        )
    await cb.answer("♻️ Ребёрт!")


@router.callback_query(F.data.startswith("rebirth_confirm:"))
async def cb_rebirth_confirm_legacy(cb: CallbackQuery):
    # Поддержка старых кнопок подтверждения после обновления.
    await cb_rebirth_confirm(cb)


@router.callback_query(F.data.startswith("rebirth:confirm_true:"))
async def cb_true_rebirth_confirm(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    stage = int(cb.data.split(":")[-1] or 0)
    expected_stage = _next_true_rebirth_stage(u)
    if stage != expected_stage or stage <= 0:
        await cb.answer("⚠️ Данные устарели. Обнови меню ребёртов.", show_alert=True)
        return

    ready, _lines = _true_rebirth_status(u, stage)
    if not ready:
        await cb.answer("❌ Требования не выполнены.", show_alert=True)
        return

    tg_id = int(u["tg_id"])

    # Жесткий сброс должен очищать и активные бои/данжи.
    _clear_user_combat_states(tg_id)

    # В true rebirth сохраняются только выбранные артефакты по слотам.
    _artifact_prepare_true_rebirth_keep(tg_id, int(_row_get(u, "vip_lvl", 0) or 0))

    if not db.true_rebirth_reset_user(tg_id, stage):
        await cb.answer("❌ Не удалось выполнить истинное перерождение.", show_alert=True)
        return

    db.update_user(tg_id, last_rebirth_at=int(time.time()))

    if stage == 1:
        bonus_text = "x2 урон | x3 монеты | x1.5 HP | x5 мощность трен | x1.5 шанс кейсов трен"
    else:
        bonus_text = "x3 урон | x5 монеты | x2 HP | x10 мощность трен | x3 шанс кейсов трен"
    await _safe_edit_cb(
        cb,
        f"⚡ Истинное перерождение #{stage} выполнено!\n"
        f"Бонусы активированы:\n{bonus_text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀ К ребёртам", callback_data="rebirth:hub")]
        ])
    )
    await cb.answer("⚡ Готово!")


@router.callback_query(F.data == "rebirth:close")
async def cb_rebirth_cancel(cb: CallbackQuery):
    await _safe_edit_cb(cb, "♻️ Меню ребёртов закрыто.")
    await cb.answer()


# ─────────────────────────────────────────────
#  ЕЖЕДНЕВНЫЙ БОНУС
# ─────────────────────────────────────────────
def _daily_claim_text(u) -> str:
    tg_id = int(u["tg_id"])
    now = int(time.time())
    last = int(u["last_daily_claim"] or 0)
    if now - last < 86400:
        remain = 86400 - (now - last)
        h = remain // 3600
        m = (remain % 3600) // 60
        return f"🎁 Следующий бонус через {h}ч {m}м."
    rank_idx = int(u["rank_idx"] or 0)
    reward = int(500 * (1 + rank_idx * 0.5) * _true_coin_mult(u) * _artifact_coin_mult(u))
    db.update_user(
        tg_id,
        coins=int(u["coins"] or 0) + reward,
        last_daily_claim=now,
    )
    return f"🎁 Ежедневный бонус: +{fmt_num(reward)} 🪙"


@router.message(F.text.lower().in_({"еб", "еж", "ежедневка", "ежедневный бонус", "бонус", "🎁 ежедневный бонус"}))
@router.message(Command("daily"))
async def cmd_daily(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await message.answer(_daily_claim_text(u))


# ─────────────────────────────────────────────
#  ДЕПОЗИТ
# ─────────────────────────────────────────────
def _deposit_preview(amount: int, u=None) -> tuple[int, int]:
    gross = int(max(0, amount) * 1.5)
    fee = int(gross * (_deposit_fee_pct(u) / 100.0)) if u is not None else int(gross * 0.05)
    return max(0, gross - fee), fee


def _deposit_text(u) -> str:
    amount = int(_row_get(u, "deposit_amount", 0) or 0)
    started_at = int(_row_get(u, "deposit_started_at", 0) or 0)
    if amount <= 0 or started_at <= 0:
        return (
            "🏦 Депозит\n"
            f"{SEP}\n"
            "Активного депозита нет.\n"
            "Команды:\n"
            "• деп положить все\n"
            "• деп положить [сумма]\n"
            "• депозит снять\n"
            "Через 3 дня можно снять: +50%, затем комиссия 5%."
        )
    unlock_ts = started_at + DEPOSIT_LOCK_DAYS * 86400
    now = int(time.time())
    payout, fee = _deposit_preview(amount, u)
    if now >= unlock_ts:
        status = "✅ Доступно к снятию"
        timer = ""
    else:
        remain = max(0, unlock_ts - now)
        d = remain // 86400
        h = (remain % 86400) // 3600
        m = (remain % 3600) // 60
        status = f"⏳ До снятия: {d}д {h}ч {m}м"
        timer = "\nСнять можно через 3 дня с момента первого пополнения."
    started = _fmt_ts_msk(started_at, "%d.%m.%Y %H:%M")
    return (
        "🏦 Депозит\n"
        f"{SEP}\n"
        f"💰 Вложено: {fmt_num(amount)} 🪙\n"
        f"📈 Начисление (+50%): {fmt_num(int(amount * 1.5))} 🪙\n"
        f"🧾 Комиссия 5%: -{fmt_num(fee)} 🪙\n"
        f"✅ К выдаче: {fmt_num(payout)} 🪙\n"
        f"🕓 Старт: {started}\n"
        f"{status}"
        f"{timer}\n"
        f"{SEP}\n"
        "Пополнение: деп положить [сумма|все]\n"
        "Снятие: деп снять"
    )


@router.message(Command("dep"))
@router.message(Command("deposit"))
@router.message(F.text.lower().in_({"деп", "депозит"}))
async def cmd_deposit_info(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    await message.answer(_deposit_text(u))


@router.message(F.text.lower().regexp(r"^(деп|депозит)\s+положить\s+(все|\d+)$"))
async def cmd_deposit_put(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    parts = message.text.strip().lower().split()
    token = parts[-1]
    coins = int(u["coins"] or 0)
    if token == "все":
        amount = coins
    else:
        amount = int(token)
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше 0.")
        return
    if amount > coins:
        await message.answer(f"❌ Недостаточно монет. У тебя: {fmt_num(coins)}")
        return
    prev_amount = int(_row_get(u, "deposit_amount", 0) or 0)
    prev_started = int(_row_get(u, "deposit_started_at", 0) or 0)
    now = int(time.time())
    new_amount = prev_amount + amount
    started = prev_started if prev_started > 0 and prev_amount > 0 else now
    db.update_user(int(u["tg_id"]), coins=coins - amount, deposit_amount=new_amount, deposit_started_at=started)
    u2 = db.get_user(int(u["tg_id"]))
    await message.answer(
        f"✅ На депозит зачислено: {fmt_num(amount)} 🪙\n"
        f"💰 Итого в депозите: {fmt_num(new_amount)} 🪙\n\n"
        f"{_deposit_text(u2)}"
    )


@router.message(F.text.lower().regexp(r"^(деп|депозит)\s+снять$"))
async def cmd_deposit_withdraw(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    amount = int(_row_get(u, "deposit_amount", 0) or 0)
    started_at = int(_row_get(u, "deposit_started_at", 0) or 0)
    if amount <= 0 or started_at <= 0:
        await message.answer("❌ У тебя нет активного депозита.")
        return
    unlock_ts = started_at + DEPOSIT_LOCK_DAYS * 86400
    now = int(time.time())
    if now < unlock_ts:
        remain = unlock_ts - now
        d = remain // 86400
        h = (remain % 86400) // 3600
        m = (remain % 3600) // 60
        await message.answer(f"⏳ Снять депозит можно через {d}д {h}ч {m}м.")
        return
    payout, fee = _deposit_preview(amount, u)
    db.update_user(
        int(u["tg_id"]),
        coins=int(u["coins"] or 0) + payout,
        deposit_amount=0,
        deposit_started_at=0,
    )
    await message.answer(
        "🏦 Депозит снят\n"
        f"Вложено: {fmt_num(amount)} 🪙\n"
        f"Начислено (+50%): {fmt_num(int(amount * 1.5))} 🪙\n"
        f"Комиссия 5%: -{fmt_num(fee)} 🪙\n"
        f"К получению: +{fmt_num(payout)} 🪙"
    )


# ─────────────────────────────────────────────
#  КАЗИНО
# ─────────────────────────────────────────────
@router.message(Command("casino"))
@router.message(F.text.lower().regexp(r'^(казино|каз)\b'))
@router.message(F.text.lower() == "🎰 казино")
async def cmd_casino(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return

    parts = message.text.strip().split()
    if len(parts) < 4:
        await message.answer(
            "🎰 Казино\n"
            "Формат: каз [валюта] [ставка] [цвет]\n"
            "Пример: каз монеты 10000 🔴\n"
            "Цвета: ⚪ 🟢 🔵 🟣 🔴"
        )
        return

    color_token = parts[-1]
    bet_token = parts[-2]
    currency_token = " ".join(parts[1:-2]).strip().lower()

    bet = _parse_amount_token(bet_token)
    if bet <= 0:
        await message.answer("❌ Ставка должна быть числом > 0. Можно: 3m/3м, 3b/3б, 3t/3т, 3q")
        return

    chosen_color = _normalize_color_token(color_token)
    if not chosen_color:
        await message.answer("❌ Неизвестный цвет. Доступно: ⚪ 🟢 🔵 🟣 🔴")
        return

    currency_map = {
        "монеты": ("coins", "🪙 Монеты"),
        "монета": ("coins", "🪙 Монеты"),
        "coins": ("coins", "🪙 Монеты"),
        "маг": ("magic_coins", "🔯 Маг. монеты"),
        "маг монеты": ("magic_coins", "🔯 Маг. монеты"),
        "магические монеты": ("magic_coins", "🔯 Маг. монеты"),
        "magic": ("magic_coins", "🔯 Маг. монеты"),
        "magic_coins": ("magic_coins", "🔯 Маг. монеты"),
        "эссенция": ("essence", "💠 Эссенция"),
        "эсс": ("essence", "💠 Эссенция"),
        "essence": ("essence", "💠 Эссенция"),
    }
    if currency_token not in currency_map:
        await message.answer("❌ Валюта: монеты | маг монеты | эссенция")
        return

    field, label = currency_map[currency_token]
    balance = int(u[field] or 0)
    if balance < bet:
        await message.answer(
            f"❌ Недостаточно средств.\n{label}: {fmt_num(balance)}\nНужно: {fmt_num(bet)}"
        )
        return

    rolled_color = random.choice(CASINO_COLORS)
    win = chosen_color == rolled_color

    new_balance = balance - bet
    payout = 0
    if win:
        payout = bet * 5
        new_balance += payout

    db.update_user(int(u["tg_id"]), **{field: new_balance})

    if win:
        await message.answer(
            f"🎰 Казино\n"
            f"Твой цвет: {chosen_color} ({CASINO_COLOR_NAMES[chosen_color]})\n"
            f"Выпал цвет: {rolled_color} ({CASINO_COLOR_NAMES[rolled_color]})\n"
            f"🏆 Победа! Выплата: +{fmt_num(payout)}\n"
            f"{label}: {fmt_num(new_balance)}"
        )
    else:
        await message.answer(
            f"🎰 Казино\n"
            f"Твой цвет: {chosen_color} ({CASINO_COLOR_NAMES[chosen_color]})\n"
            f"Выпал цвет: {rolled_color} ({CASINO_COLOR_NAMES[rolled_color]})\n"
            f"💸 Проигрыш. Потеряно: -{fmt_num(bet)}\n"
            f"{label}: {fmt_num(new_balance)}"
        )


@router.message(Command("coin"))
async def cmd_mini_coin(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _mini_allowed_for_user(u, int(message.chat.id)):
        await message.answer("❌ Мини-игры доступны только в игровом чате или с надетым 🎮 Ключом Аркады.")
        return
    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Формат: /coin [валюта] [ставка] [орел|решка]")
        return
    field, icon = _mini_currency_from_token(parts[1])
    side_raw = parts[3].strip().lower()
    side = "орел" if side_raw in ("орел", "орёл", "heads", "h") else "решка" if side_raw in ("решка", "tails", "t") else ""
    bet = _parse_amount_token(parts[2])
    if not field or bet <= 0 or not side:
        await message.answer("Формат: /coin [валюта] [ставка] [орел|решка]")
        return
    if not _mini_take_balance(u, field, bet):
        await message.answer("❌ Недостаточно средств.")
        return
    roll = random.choice(["орел", "решка"])
    if roll == side:
        payout = int(bet * 1.9)
        _mini_add_balance(int(u["tg_id"]), field, payout)
        await message.answer(f"🪙 {roll} | +{fmt_num(payout)} {icon}")
    else:
        await message.answer(f"🪙 {roll} | -{fmt_num(bet)} {icon}")


@router.message(Command("dice"))
async def cmd_mini_dice(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _mini_allowed_for_user(u, int(message.chat.id)):
        await message.answer("❌ Мини-игры доступны только в игровом чате или с надетым 🎮 Ключом Аркады.")
        return
    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Формат: /dice [валюта] [ставка] [1-6]")
        return
    field, icon = _mini_currency_from_token(parts[1])
    bet = _parse_amount_token(parts[2])
    if not field or bet <= 0 or not parts[3].isdigit():
        await message.answer("Формат: /dice [валюта] [ставка] [1-6]")
        return
    pick = int(parts[3])
    if pick < 1 or pick > 6:
        await message.answer("❌ Число должно быть от 1 до 6.")
        return
    if not _mini_take_balance(u, field, bet):
        await message.answer("❌ Недостаточно средств.")
        return
    roll = random.randint(1, 6)
    if roll == pick:
        payout = int(bet * 5.5)
        _mini_add_balance(int(u["tg_id"]), field, payout)
        await message.answer(f"🎲 Выпало {roll} | +{fmt_num(payout)} {icon}")
    else:
        await message.answer(f"🎲 Выпало {roll} | -{fmt_num(bet)} {icon}")


@router.message(Command("ladder"))
async def cmd_mini_ladder(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _mini_allowed_for_user(u, int(message.chat.id)):
        await message.answer("❌ Мини-игры доступны только в игровом чате или с надетым 🎮 Ключом Аркады.")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Формат: /ladder [валюта] [ставка]")
        return
    field, icon = _mini_currency_from_token(parts[1])
    bet = _parse_amount_token(parts[2])
    if not field or bet <= 0:
        await message.answer("Формат: /ladder [валюта] [ставка]")
        return
    uid = int(u["tg_id"])
    if not _mini_take_balance(u, field, bet):
        await message.answer("❌ Недостаточно средств.")
        return
    st = {
        "uid": uid,
        "field": field,
        "icon": icon,
        "bet": bet,
        "stage": 0,
        "mult": 1.0,
    }
    MINI_LADDER[uid] = st
    text = f"🪜 Ladder\nСтавка: {fmt_num(bet)} {icon}\nШаг: 0/{len(LADDER_MULTS)} | x1.00"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬆️ Дальше", callback_data=f"mini:lad:go:{uid}")],
        [InlineKeyboardButton(text="💰 Забрать", callback_data=f"mini:lad:take:{uid}")],
    ])
    msg = await message.answer(text, reply_markup=kb)
    _set_cb_owner(msg.chat.id, msg.message_id, uid)


@router.message(Command("safe"))
async def cmd_mini_safe(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _mini_allowed_for_user(u, int(message.chat.id)):
        await message.answer("❌ Мини-игры доступны только в игровом чате или с надетым 🎮 Ключом Аркады.")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Формат: /safe [валюта] [ставка]")
        return
    field, icon = _mini_currency_from_token(parts[1])
    bet = _parse_amount_token(parts[2])
    if not field or bet <= 0:
        await message.answer("Формат: /safe [валюта] [ставка]")
        return
    uid = int(u["tg_id"])
    if not _mini_take_balance(u, field, bet):
        await message.answer("❌ Недостаточно средств.")
        return
    MINI_SAFE[uid] = {
        "uid": uid,
        "field": field,
        "icon": icon,
        "bet": bet,
        "win": random.randint(1, 3),
    }
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="1", callback_data=f"mini:safe:pick:{uid}:1"),
        InlineKeyboardButton(text="2", callback_data=f"mini:safe:pick:{uid}:2"),
        InlineKeyboardButton(text="3", callback_data=f"mini:safe:pick:{uid}:3"),
    ]])
    msg = await message.answer(f"🧰 Safe\nСтавка: {fmt_num(bet)} {icon}\nВыбери сундук 1-3", reply_markup=kb)
    _set_cb_owner(msg.chat.id, msg.message_id, uid)


@router.message(Command("race"))
async def cmd_mini_race(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _mini_allowed_for_user(u, int(message.chat.id)):
        await message.answer("❌ Мини-игры доступны только в игровом чате или с надетым 🎮 Ключом Аркады.")
        return
    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Формат: /race [валюта] [ставка] [1-4]")
        return
    field, icon = _mini_currency_from_token(parts[1])
    bet = _parse_amount_token(parts[2])
    if not field or bet <= 0 or not parts[3].isdigit():
        await message.answer("Формат: /race [валюта] [ставка] [1-4]")
        return
    pick = int(parts[3])
    if pick < 1 or pick > 4:
        await message.answer("❌ Номер должен быть от 1 до 4.")
        return
    if not _mini_take_balance(u, field, bet):
        await message.answer("❌ Недостаточно средств.")
        return
    winner = random.randint(1, 4)
    if winner == pick:
        payout = int(bet * RACE_MULT)
        _mini_add_balance(int(u["tg_id"]), field, payout)
        await message.answer(f"🏁 Победил #{winner} | +{fmt_num(payout)} {icon}")
    else:
        await message.answer(f"🏁 Победил #{winner} | -{fmt_num(bet)} {icon}")


@router.message(Command("rr"))
@router.message(Command("roulette"))
async def cmd_mini_rr(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _mini_allowed_for_user(u, int(message.chat.id)):
        await message.answer("❌ Мини-игры доступны только в игровом чате или с надетым 🎮 Ключом Аркады.")
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer("Формат: ответом на игрока -> /rr [валюта] [ставка]")
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Формат: /rr [валюта] [ставка]")
        return
    field, icon = _mini_currency_from_token(parts[1])
    bet = _parse_amount_token(parts[2])
    if not field or bet <= 0:
        await message.answer("Формат: /rr [валюта] [ставка]")
        return
    p1 = int(u["tg_id"])
    p2 = int(message.reply_to_message.from_user.id)
    if p1 == p2:
        await message.answer("❌ Нельзя вызвать самого себя.")
        return
    u2 = db.get_user(p2)
    if not u2:
        await message.answer("❌ У соперника нет профиля в боте.")
        return
    if int(_row_get(u, field, 0) or 0) < bet:
        await message.answer("❌ У тебя недостаточно средств для ставки.")
        return

    global MINI_RR_COUNTER
    MINI_RR_COUNTER += 1
    rr_id = MINI_RR_COUNTER
    st = {
        "id": rr_id,
        "chat_id": int(message.chat.id),
        "p1": p1,
        "p2": p2,
        "field": field,
        "icon": icon,
        "bet": bet,
        "status": "pending",
        "turn": p1,
        "idx": 0,
        "chambers": [],
        "last_log": "",
    }
    MINI_RR[rr_id] = st
    msg = await message.answer(_mini_rr_view(st), reply_markup=_mini_rr_kb_pending(rr_id))
    st["msg_id"] = int(msg.message_id)


@router.message(Command("mine"))
async def cmd_mini_mine(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _mini_allowed_for_user(u, int(message.chat.id)):
        await message.answer("❌ Мини-игры доступны только в игровом чате или с надетым 🎮 Ключом Аркады.")
        return
    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer("Формат: /mine [валюта] [ставка] [мин 1-8]")
        return
    field, icon = _mini_currency_from_token(parts[1])
    bet = _parse_amount_token(parts[2])
    if not field or bet <= 0 or not parts[3].isdigit():
        await message.answer("Формат: /mine [валюта] [ставка] [мин 1-8]")
        return
    if field == "magic_coins":
        await message.answer("❌ В Минах нельзя играть на маг. монеты. Доступны: монеты, эссенция.")
        return
    mines = int(parts[3])
    if mines < MINE_MINES_MIN or mines > MINE_MINES_MAX:
        await message.answer("❌ Количество мин: 2..6")
        return
    uid = int(u["tg_id"])
    if not _mini_take_balance(u, field, bet):
        await message.answer("❌ Недостаточно средств.")
        return
    mines_set = set(random.sample(range(MINE_CELLS), mines))
    st = {
        "uid": uid,
        "field": field,
        "icon": icon,
        "bet": bet,
        "mines": mines,
        "mines_set": mines_set,
        "opened": set(),
        "mult": 1.0,
    }
    MINI_MINE[uid] = st
    msg = await message.answer(_mini_mine_view(st), reply_markup=_mini_mine_kb(st))
    _set_cb_owner(msg.chat.id, msg.message_id, uid)


@router.callback_query(F.data.startswith("mini:"))
async def cb_mini_games(cb: CallbackQuery):
    ok, _u = await _check_cb_access(cb)
    if not ok:
        return
    parts = str(cb.data or "").split(":")
    if len(parts) < 3:
        await cb.answer()
        return
    kind = parts[1]
    action = parts[2]

    # Блокируем начисление выигрышей из игр открытых до ребёрта
    _payout_actions = {"take", "go", "pick", "fire", "accept"}
    if action in _payout_actions:
        uid = int(cb.from_user.id)
        _u2 = db.get_user(uid)
        if False and _u2 and _cb_stale_after_rebirth(cb, _u2):
            # ОТКЛЮЧЕНО: убрана проверка
            if kind == "lad":
                _st = MINI_LADDER.pop(uid, None)
                if _st:
                    _mini_add_balance(uid, _st["field"], int(_st["bet"]))
            elif kind == "mine":
                _st = MINI_MINE.pop(uid, None)
                if _st:
                    _mini_add_balance(uid, _st["field"], int(_st["bet"]))
            elif kind == "safe":
                MINI_SAFE.pop(uid, None)
                # safe ставка уже списана, но т.к. ребёрт — монеты и так 0
            elif kind == "rr" and len(parts) >= 4:
                _rr_id = int(parts[3])
                MINI_RR.pop(_rr_id, None)
            await _safe_edit_cb(cb, "♻️ После ребёрта активные игры аннулированы.")
            await cb.answer("♻️ Игра аннулирована после ребёрта.", show_alert=True)
            return

    if kind == "rr" and len(parts) >= 4:
        rr_id = int(parts[3])
        st = MINI_RR.get(rr_id)
        if not st:
            await cb.answer("Игра завершена.", show_alert=True)
            return
        uid = int(cb.from_user.id)
        p1 = int(st["p1"])
        p2 = int(st["p2"])
        if uid not in (p1, p2):
            await cb.answer("Это не твоя игра.", show_alert=True)
            return

        if action == "decline":
            if str(st.get("status", "pending")) != "pending":
                await cb.answer("Уже нельзя отказаться.", show_alert=True)
                return
            if uid != p2:
                await cb.answer("Только соперник может отказаться.", show_alert=True)
                return
            MINI_RR.pop(rr_id, None)
            await _safe_edit_cb(cb, "❌ Русская рулетка отклонена.")
            await cb.answer()
            return

        if action == "accept":
            if str(st.get("status", "pending")) != "pending":
                await cb.answer("Игра уже подтверждена.", show_alert=True)
                return
            if uid != p2:
                await cb.answer("Только соперник может принять.", show_alert=True)
                return
            u1 = db.get_user(p1)
            u2 = db.get_user(p2)
            if not u1 or not u2:
                MINI_RR.pop(rr_id, None)
                await _safe_edit_cb(cb, "❌ Профиль игрока не найден.")
                await cb.answer()
                return
            field = str(st["field"])
            bet = int(st["bet"])
            if int(_row_get(u1, field, 0) or 0) < bet:
                MINI_RR.pop(rr_id, None)
                await _safe_edit_cb(cb, "❌ У инициатора недостаточно средств.")
                await cb.answer()
                return
            if int(_row_get(u2, field, 0) or 0) < bet:
                MINI_RR.pop(rr_id, None)
                await _safe_edit_cb(cb, "❌ У соперника недостаточно средств.")
                await cb.answer()
                return
            if not _mini_take_balance(u1, field, bet):
                MINI_RR.pop(rr_id, None)
                await _safe_edit_cb(cb, "❌ Не удалось списать ставку у инициатора.")
                await cb.answer()
                return
            if not _mini_take_balance(u2, field, bet):
                _mini_add_balance(p1, field, bet)
                MINI_RR.pop(rr_id, None)
                await _safe_edit_cb(cb, "❌ Не удалось списать ставку у соперника.")
                await cb.answer()
                return
            chambers = [1] + [0] * (RR_CHAMBERS - 1)
            random.shuffle(chambers)
            st["chambers"] = chambers
            st["idx"] = 0
            st["status"] = "active"
            st["turn"] = random.choice([p1, p2])
            st["last_log"] = f"✅ Игра началась. Первый ход: {_duel_user_label(int(st['turn']))}"
            await _safe_edit_cb(cb, _mini_rr_view(st), reply_markup=_mini_rr_kb_active(rr_id))
            await cb.answer("Старт!")
            return

        if action == "fire":
            if str(st.get("status", "pending")) != "active":
                await cb.answer("Игра не активна.", show_alert=True)
                return
            if uid != int(st.get("turn", 0) or 0):
                await cb.answer("Сейчас ход соперника.", show_alert=True)
                return
            idx = int(st.get("idx", 0) or 0)
            chambers = list(st.get("chambers", []))
            if idx >= len(chambers):
                chambers = [1] + [0] * (RR_CHAMBERS - 1)
                random.shuffle(chambers)
                idx = 0
            shot = int(chambers[idx])
            st["idx"] = idx + 1
            enemy = p2 if uid == p1 else p1
            if shot == 1:
                payout = int(st["bet"]) * 2
                _mini_add_balance(enemy, str(st["field"]), payout)
                st["status"] = "finished"
                st["last_log"] = (
                    f"💥 Выстрел! {_duel_user_label(uid)} проиграл.\n"
                    f"🏆 Победитель: {_duel_user_label(enemy)} (+{fmt_num(payout)} {st['icon']})"
                )
                MINI_RR.pop(rr_id, None)
                await _safe_edit_cb(cb, _mini_rr_view(st))
                await cb.answer("Выстрел")
                return
            st["turn"] = enemy
            left = max(0, RR_CHAMBERS - int(st["idx"]))
            st["last_log"] = (
                f"🔫 Щелчок... патрона нет.\n"
                f"Осталось выстрелов до перезарядки: {left}"
            )
            await _safe_edit_cb(cb, _mini_rr_view(st), reply_markup=_mini_rr_kb_active(rr_id))
            await cb.answer("Щелчок")
            return

    if kind == "lad" and len(parts) >= 4:
        uid = int(parts[3])
        if int(cb.from_user.id) != uid:
            await cb.answer("Это не твоя игра.", show_alert=True)
            return
        st = MINI_LADDER.get(uid)
        if not st:
            await cb.answer("Игра завершена.", show_alert=True)
            return
        if action == "take":
            payout = int(st["bet"] * st["mult"])
            _mini_add_balance(uid, st["field"], payout)
            MINI_LADDER.pop(uid, None)
            await _safe_edit_cb(cb, f"🪜 Забрал: +{fmt_num(payout)} {st['icon']}")
            await cb.answer()
            return
        if action == "go":
            stage = int(st["stage"])
            if stage >= len(LADDER_CHANCES):
                payout = int(st["bet"] * st["mult"])
                _mini_add_balance(uid, st["field"], payout)
                MINI_LADDER.pop(uid, None)
                await _safe_edit_cb(cb, f"🪜 Макс шаг! +{fmt_num(payout)} {st['icon']}")
                await cb.answer()
                return
            if random.random() > float(LADDER_CHANCES[stage]):
                MINI_LADDER.pop(uid, None)
                await _safe_edit_cb(cb, f"🪜 Падение на шаге {stage + 1}. Ставка сгорела.")
                await cb.answer("Проигрыш")
                return
            st["stage"] = stage + 1
            st["mult"] = float(LADDER_MULTS[stage])
            text = f"🪜 Ladder\nСтавка: {fmt_num(st['bet'])} {st['icon']}\nШаг: {st['stage']}/{len(LADDER_MULTS)} | x{st['mult']:.2f}"
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬆️ Дальше", callback_data=f"mini:lad:go:{uid}")],
                [InlineKeyboardButton(text="💰 Забрать", callback_data=f"mini:lad:take:{uid}")],
            ])
            await _safe_edit_cb(cb, text, reply_markup=kb)
            await cb.answer()
            return

    if kind == "safe" and action == "pick" and len(parts) >= 5:
        uid = int(parts[3])
        pick = int(parts[4])
        if int(cb.from_user.id) != uid:
            await cb.answer("Это не твоя игра.", show_alert=True)
            return
        st = MINI_SAFE.pop(uid, None)
        if not st:
            await cb.answer("Игра завершена.", show_alert=True)
            return
        win = int(st["win"])
        if pick == win:
            payout = int(st["bet"] * SAFE_MULT)
            _mini_add_balance(uid, st["field"], payout)
            await _safe_edit_cb(cb, f"🧰 Сундук {pick} | +{fmt_num(payout)} {st['icon']}")
        else:
            await _safe_edit_cb(cb, f"🧰 Мимо. Выигрышный сундук: {win}")
        await cb.answer()
        return

    if kind == "mine" and len(parts) >= 4:
        uid = int(parts[3])
        if int(cb.from_user.id) != uid:
            await cb.answer("Это не твоя игра.", show_alert=True)
            return
        st = MINI_MINE.get(uid)
        if not st:
            await cb.answer("Игра завершена.", show_alert=True)
            return
        if action == "noop":
            await cb.answer()
            return
        if action == "take":
            payout = int(st["bet"] * st["mult"])
            _mini_add_balance(uid, st["field"], payout)
            MINI_MINE.pop(uid, None)
            await _safe_edit_cb(cb, f"💣 Забрал: +{fmt_num(payout)} {st['icon']}")
            await cb.answer()
            return
        if action == "open" and len(parts) >= 5:
            idx = int(parts[4])
            if idx < 0 or idx >= MINE_CELLS or idx in st["opened"]:
                await cb.answer()
                return
            if idx in st["mines_set"]:
                MINI_MINE.pop(uid, None)
                await _safe_edit_cb(cb, "💣 Мина! Ставка сгорела.", reply_markup=_mini_mine_kb(st, reveal_all=True))
                await cb.answer("Мина")
                return
            st["opened"].add(idx)
            st["mult"] = _mini_mine_mult(len(st["opened"]), int(st["mines"]))
            safe_total = MINE_CELLS - int(st["mines"])
            if len(st["opened"]) >= safe_total:
                payout = int(st["bet"] * st["mult"])
                _mini_add_balance(uid, st["field"], payout)
                MINI_MINE.pop(uid, None)
                await _safe_edit_cb(cb, f"💣 Поле очищено! +{fmt_num(payout)} {st['icon']}", reply_markup=_mini_mine_kb(st, reveal_all=True))
                await cb.answer("Победа")
                return
            await _safe_edit_cb(cb, _mini_mine_view(st), reply_markup=_mini_mine_kb(st))
            await cb.answer()
            return

    await cb.answer()


# ─────────────────────────────────────────────
#  ДРУЖЕСКИЙ БОЙ (только чаты)
# ─────────────────────────────────────────────
@router.message(F.text.lower().regexp(r"^(дружеский\s+бой|бой)\b"))
async def cmd_friendly_battle(message: Message):
    ok, _ = await _check_access(message)
    if not ok:
        return
    if message.chat.type == ChatType.PRIVATE:
        await message.answer("🤝 Дружеские бои доступны только в чатах.")
        return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.answer(
            "Использование (ответом на сообщение игрока):\n"
            "бой монеты 1000\n"
            "бой маг монеты 100\n"
            "бой эссенция 10\n"
            "бой ко6 5\n"
            "бой кп10 3"
        )
        return

    raw = (message.text or "").strip()
    m = re.match(r"^(дружеский\s+бой|бой)\s+(.+)$", raw, flags=re.IGNORECASE)
    if not m:
        await message.answer("❌ Укажи ставку: бой [тип] [кол-во]")
        return

    payload = m.group(2).strip()
    parts = payload.split()
    if len(parts) < 2 or not parts[-1].isdigit():
        await message.answer("❌ Формат: бой [тип] [кол-во]")
        return
    amount = int(parts[-1])
    if amount <= 0:
        await message.answer("❌ Ставка должна быть больше 0.")
        return
    token = " ".join(parts[:-1]).strip().lower()
    asset_kind, asset_key, asset_label = _duel_asset_from_token(token)
    if not asset_kind:
        await message.answer("❌ Тип ставки: монеты | маг монеты | эссенция | коN | кпN")
        return

    inviter_id = int(message.from_user.id)
    target_id = int(message.reply_to_message.from_user.id)
    if inviter_id == target_id:
        await message.answer("❌ Нельзя вызвать себя на бой.")
        return

    inv_u = db.get_user(inviter_id)
    tgt_u = db.get_user(target_id)
    if not inv_u or not tgt_u:
        await message.answer("❌ Профиль одного из игроков не найден.")
        return

    if not _duel_has_balance(inv_u, asset_key, amount):
        await message.answer(f"❌ У тебя не хватает для ставки: {fmt_num(amount)} {asset_label}")
        return

    global FRIENDLY_DUEL_COUNTER
    FRIENDLY_DUEL_COUNTER += 1
    duel_id = FRIENDLY_DUEL_COUNTER

    msg = await message.answer(
        f"🤝 Дружеский бой создан!\n"
        f"Ставка: {fmt_num(amount)} {asset_label}\n"
        f"Вызывающий: {_duel_user_label(inviter_id)}\n"
        f"Соперник: {_duel_user_label(target_id)}\n\n"
        "Первый ход определяется случайно.\n"
        f"Урон каждого удара: {FRIENDLY_DUEL_MIN_DMG}..{FRIENDLY_DUEL_MAX_DMG}",
        reply_markup=_friendly_duel_kb_pending(duel_id),
    )

    FRIENDLY_DUELS[duel_id] = FriendlyDuelState(
        duel_id=duel_id,
        chat_id=msg.chat.id,
        msg_id=msg.message_id,
        inviter_id=inviter_id,
        target_id=target_id,
        asset_kind=asset_kind,
        asset_key=asset_key,
        asset_label=asset_label,
        amount=amount,
    )


@router.callback_query(F.data.startswith("duel:"))
async def cb_friendly_duel(cb: CallbackQuery):
    data = (cb.data or "").split(":")
    if len(data) < 3:
        await cb.answer()
        return
    action = data[1]
    try:
        duel_id = int(data[2])
    except Exception:
        await cb.answer("Бой не найден.", show_alert=True)
        return

    ds = FRIENDLY_DUELS.get(duel_id)
    if not ds:
        await cb.answer("Бой не найден или завершен.", show_alert=True)
        return

    uid = int(cb.from_user.id)
    if uid not in (ds.inviter_id, ds.target_id):
        await cb.answer("Это не твой бой.", show_alert=True)
        return

    if action == "decline":
        if ds.status != "pending":
            await cb.answer("Уже нельзя отказаться.", show_alert=True)
            return
        if uid != ds.target_id:
            await cb.answer("Только соперник может отказаться.", show_alert=True)
            return
        ds.status = "cancelled"
        await _safe_edit_cb(cb, "❌ Дружеский бой отклонен.")
        FRIENDLY_DUELS.pop(duel_id, None)
        await cb.answer()
        return

    if action == "accept":
        if ds.status != "pending":
            await cb.answer("Бой уже подтвержден.", show_alert=True)
            return
        if uid != ds.target_id:
            await cb.answer("Только соперник может принять бой.", show_alert=True)
            return

        inv_u = db.get_user(ds.inviter_id)
        tgt_u = db.get_user(ds.target_id)
        if not inv_u or not tgt_u:
            ds.status = "cancelled"
            FRIENDLY_DUELS.pop(duel_id, None)
            await _safe_edit_cb(cb, "❌ Профиль одного из игроков не найден.")
            await cb.answer()
            return

        if not _duel_has_balance(inv_u, ds.asset_key, ds.amount):
            ds.status = "cancelled"
            FRIENDLY_DUELS.pop(duel_id, None)
            await _safe_edit_cb(cb, f"❌ У вызывающего не хватает ставки: {fmt_num(ds.amount)} {ds.asset_label}")
            await cb.answer()
            return
        if not _duel_has_balance(tgt_u, ds.asset_key, ds.amount):
            ds.status = "cancelled"
            FRIENDLY_DUELS.pop(duel_id, None)
            await _safe_edit_cb(cb, f"❌ У соперника нету {fmt_num(ds.amount)} {ds.asset_label}")
            await cb.answer()
            return

        if not _duel_take_stake(ds.inviter_id, ds.asset_key, ds.amount):
            ds.status = "cancelled"
            FRIENDLY_DUELS.pop(duel_id, None)
            await _safe_edit_cb(cb, "❌ Не удалось списать ставку у вызывающего.")
            await cb.answer()
            return
        if not _duel_take_stake(ds.target_id, ds.asset_key, ds.amount):
            _duel_add_reward(ds.inviter_id, ds.asset_key, ds.amount)
            ds.status = "cancelled"
            FRIENDLY_DUELS.pop(duel_id, None)
            await _safe_edit_cb(cb, f"❌ У соперника нету {fmt_num(ds.amount)} {ds.asset_label}")
            await cb.answer()
            return

        ds.status = "active"
        ds.turn_id = random.choice([ds.inviter_id, ds.target_id])
        ds.last_action = int(time.time())
        ds.last_log = f"✅ Бой начался! Первый ход у {_duel_user_label(ds.turn_id)}."
        await _safe_edit_cb(cb, _friendly_duel_view(ds), reply_markup=_friendly_duel_kb_active(duel_id))
        await cb.answer("Бой начался!")
        return

    if action == "close":
        if ds.status == "pending":
            ds.status = "cancelled"
            FRIENDLY_DUELS.pop(duel_id, None)
            await _safe_edit_cb(cb, "✖ Бой закрыт.")
            await cb.answer()
            return
        if ds.status != "active":
            await cb.answer("Бой уже завершен.", show_alert=True)
            return
        await cb.answer("Закрыть нельзя: бой уже идет до победы.", show_alert=True)
        return

    if action == "hit":
        if ds.status != "active":
            await cb.answer("Бой не активен.", show_alert=True)
            return
        if uid != ds.turn_id:
            await cb.answer("Сейчас ход соперника.", show_alert=True)
            return

        enemy_id = ds.target_id if uid == ds.inviter_id else ds.inviter_id
        dmg = random.randint(FRIENDLY_DUEL_MIN_DMG, FRIENDLY_DUEL_MAX_DMG)
        ds.hp[enemy_id] = max(0, int(ds.hp[enemy_id]) - dmg)
        ds.last_action = int(time.time())
        ds.last_log = f"⚔️ {_duel_user_label(uid)} нанес {fmt_num(dmg)} урона"

        if ds.hp[enemy_id] <= 0:
            winner_id = uid
            total = ds.amount * 2
            _duel_add_reward(winner_id, ds.asset_key, total)
            ds.status = "finished"
            ds.last_log = (
                f"🏆 Победитель: {_duel_user_label(winner_id)}\n"
                f"Награда: +{fmt_num(total)} {ds.asset_label}"
            )
            await _safe_edit_cb(cb, _friendly_duel_view(ds))
            FRIENDLY_DUELS.pop(duel_id, None)
            await cb.answer("Победа!")
            return

        ds.turn_id = enemy_id
        await _safe_edit_cb(cb, _friendly_duel_view(ds), reply_markup=_friendly_duel_kb_active(duel_id))
        await cb.answer()
# ─────────────────────────────────────────────
#  FAST-КОНКУРС: /fk
# ─────────────────────────────────────────────
def _resolve_reward_field(reward_token: str) -> str:
    token = (reward_token or "").strip().lower()
    field_alias = {
        "coins": "coins", "монеты": "coins", "монета": "coins", "money": "coins",
        "magic": "magic_coins", "маг": "magic_coins", "magic_coins": "magic_coins", "магмонеты": "magic_coins",
        "essence": "essence", "эссенция": "essence", "эсс": "essence",
        "power": "power", "мощность": "power", "сила": "power",
        "vip": "vip_lvl", "vip_lvl": "vip_lvl", "вип": "vip_lvl",
        "admin": "admin_role", "админ": "admin_role", "admin_role": "admin_role",
        "arena": "arena", "арена": "arena",
        "rank": "rank_idx", "ранг": "rank_idx",
        "rebirth": "rebirth_count", "rebirth_count": "rebirth_count", "реберт": "rebirth_count", "ребёрт": "rebirth_count",
        "hp": "hp_boost", "хп": "hp_boost", "hp_boost": "hp_boost",
        "ring": "ring_level", "кольцо": "ring_level", "ring_level": "ring_level",
    }
    field = field_alias.get(token, "")
    if field:
        return field
    return _case_field_from_token(token)


def _grant_reward_by_token(target_id: int, reward_token: str, amount: int) -> tuple[bool, str, int, int, str]:
    if amount <= 0:
        return False, "", 0, 0, "Кол-во должно быть больше 0."
    tu = db.get_user(int(target_id))
    if not tu:
        return False, "", 0, 0, "Игрок не найден."

    token = str(reward_token or "").strip().lower()
    if token in {"са", "sa", "сумка", "сумки", "artifact_bag", "bag"}:
        cur = _artifact_bag_count(int(target_id))
        _artifact_add_bags(int(target_id), int(amount))
        return True, "artifact_bag", cur, cur + int(amount), ""

    field = _resolve_reward_field(reward_token)
    if not field:
        return False, "", 0, 0, "Неизвестный тип награды."

    cur = int(_row_get(tu, field, 0) or 0)
    new_val = cur + int(amount)
    if field == "vip_lvl":
        new_val = min(max(new_val, 0), 5)
    elif field == "admin_role":
        new_val = min(max(new_val, 0), 5)
    elif field == "arena":
        new_val = min(max(new_val, 1), gd.max_arena())
    elif field == "rank_idx":
        new_val = min(max(new_val, 0), len(gd.RANKS) - 1)
    elif field == "ring_level":
        new_val = min(max(new_val, 0), 5)

    updates = {field: new_val}
    if field == "ring_level":
        cur_active = int(_row_get(tu, "active_ring_level", 0) or 0)
        updates["active_ring_level"] = min(max(cur_active, new_val), 5)

    db.update_user(int(target_id), **updates)
    return True, field, cur, new_val, ""


@router.message(Command("fk"))
async def cmd_fast_contest(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return

    if not _is_admin(u):
        await message.answer("❌ Команда /fk доступна только администрации.")
        return

    raw = (message.text or "").strip()
    payload = ""
    parts = raw.split(maxsplit=1)
    if len(parts) > 1:
        payload = parts[1].strip()

    low = payload.lower()
    if low in {"stop", "стоп", "cancel", "отмена"}:
        async with FAST_CONTEST_LOCK:
            if not FAST_CONTEST_STATE.get("active"):
                await message.answer("❌ Сейчас нет активного fast-конкурса.")
                return
            FAST_CONTEST_STATE.clear()
            FAST_CONTEST_ANSWERED.clear()
        await message.answer("🛑 Fast-конкурс остановлен.")
        return

    start_parts = payload.split(maxsplit=2)
    if len(start_parts) >= 3 and start_parts[1].isdigit():
        reward_token = start_parts[0].strip().lower()
        amount = int(start_parts[1])
        question = start_parts[2].strip()
        if amount <= 0 or len(question) < 3:
            await message.answer("❌ Проверь формат: /fk money 1000 ваш вопрос")
            return
        if not _resolve_reward_field(reward_token):
            await message.answer("❌ Неизвестный тип награды. Пример: money, coins, essence, ко3, кп7")
            return

        contest_id = int(time.time() * 1000)
        async with FAST_CONTEST_LOCK:
            FAST_CONTEST_STATE.clear()
            FAST_CONTEST_ANSWERED.clear()
            FAST_CONTEST_STATE.update({
                "id": contest_id,
                "active": True,
                "owner_id": int(u["tg_id"]),
                "reward_token": reward_token,
                "amount": amount,
                "question": question,
                "started_at": int(time.time()),
            })

        users = db.list_users_for_notify()
        sent = 0
        failed = 0
        text = (
            "Конкурс начался🎉\n"
            f"Вопрос: {question}\n"
            "Победит первый ответ.\n"
            "Ответ: /fc ваш ответ"
        )
        for row in users:
            uid = int(row["tg_id"])
            if int(row["banned"] or 0):
                continue
            if not _notify_enabled(uid, NOTIFY_FAST_CONTEST_KEY):
                continue
            try:
                await message.bot.send_message(uid, text)
                sent += 1
            except Exception:
                failed += 1

        await message.answer(
            "✅ Fast-конкурс запущен.\n"
            f"🎁 Награда: {reward_token} +{fmt_num(amount)}\n"
            f"👥 Уведомлено: {fmt_num(sent)}\n"
            f"⚠️ Ошибок доставки: {fmt_num(failed)}"
        )
        return

    await message.answer(
        "Использование:\n"
        "/fk money 1000 ваш вопрос\n"
        "Остановить: /fk стоп"
    )


@router.message(Command("fc"))
async def cmd_fast_contest_answer(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return

    raw = (message.text or "").strip()
    payload = ""
    parts = raw.split(maxsplit=1)
    if len(parts) > 1:
        payload = parts[1].strip()

    async with FAST_CONTEST_LOCK:
        if not FAST_CONTEST_STATE.get("active"):
            await message.answer("❌ Сейчас нет активного fast-конкурса.")
            return

        tg_id = int(u["tg_id"])
        if tg_id in FAST_CONTEST_ANSWERED:
            await message.answer("❌ Ответ можно отправить только один раз.")
            return

        answer = payload.strip()
        if answer.lower().startswith("ответ "):
            answer = answer[6:].strip()
        if not answer:
            await message.answer("❌ Формат ответа: /fc ваш ответ")
            return

        FAST_CONTEST_ANSWERED.add(tg_id)
        if not FAST_CONTEST_STATE.get("winner_id"):
            FAST_CONTEST_STATE["winner_id"] = tg_id
            FAST_CONTEST_STATE["winner_answer"] = answer[:300]
            FAST_CONTEST_STATE["active"] = False

        winner_id = int(FAST_CONTEST_STATE.get("winner_id", 0) or 0)
        winner_answer = str(FAST_CONTEST_STATE.get("winner_answer", "") or "")
        owner_id = int(FAST_CONTEST_STATE.get("owner_id", 0) or 0)
        reward_token = str(FAST_CONTEST_STATE.get("reward_token", "coins") or "coins")
        amount = int(FAST_CONTEST_STATE.get("amount", 0) or 0)
        question = str(FAST_CONTEST_STATE.get("question", "") or "")

        if winner_id != tg_id:
            await message.answer("❌ Уже есть победитель этого fast-конкурса.")
            return

    ok_grant, field, old_val, new_val, err = _grant_reward_by_token(winner_id, reward_token, amount)
    if not ok_grant:
        await message.answer(f"⚠️ Победа засчитана, но награду выдать не удалось: {err}")
    else:
        await message.answer(f"🏆 Ты первый! Награда выдана: {field} +{fmt_num(amount)}")

    if bot_instance is not None:
        try:
            if ok_grant:
                await bot_instance.send_message(
                    winner_id,
                    f"🎉 Победа в fast-конкурсе!\n"
                    f"❓ Вопрос: {question}\n"
                    f"🎁 Награда: {field} +{fmt_num(amount)}"
                )
        except Exception:
            pass
        try:
            if owner_id > 0:
                uname = (message.from_user.username or "").strip() if message.from_user else ""
                user_link = f"<a href=\"tg://user?id={winner_id}\">{winner_id}</a>"
                owner_text = (
                    "⚡ Fast-конкурс завершен\n"
                    f"❓ Вопрос: {escape(question)}\n"
                    f"🏆 Победитель: {user_link}\n"
                    f"ID: <code>{winner_id}</code>\n"
                    f"Юз: {'@' + escape(uname) if uname else 'нет username'}\n"
                    f"Ответ: {escape(winner_answer)}\n"
                    f"Награда: {escape(field)} +{fmt_num(amount)}"
                )
                await bot_instance.send_message(owner_id, owner_text, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    async with FAST_CONTEST_LOCK:
        FAST_CONTEST_STATE.clear()
        FAST_CONTEST_ANSWERED.clear()


# Выдача награды за конкурс (только создатель)
@router.message(Command("congive"))
async def cmd_congive(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return

    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer(
            "Использование:\n"
            "/congive [id] [тип] [кол-во]\n"
            "или /congive [тип] [кол-во] [id]\n"
            "Примеры: /congive 123456789 coins 10000 | /congive ко3 5 123456789\n"
            "Типы: coins, magic, essence, power, vip, admin, arena, rank, rebirth, hp, ring,\n"
            "об/ред/эп/лег/мифч, ко1..ко15, кп1..кп15, са"
        )
        return

    target_id = None
    reward_token = ""
    amount = 0
    try:
        if parts[1].isdigit():
            target_id = int(parts[1])
            reward_token = parts[2].strip().lower()
            amount = int(parts[3])
        else:
            reward_token = parts[1].strip().lower()
            amount = int(parts[2])
            target_id = int(parts[3])
    except Exception:
        await message.answer("❌ Неверный формат. Пример: /congive 123456789 coins 10000")
        return

    if amount <= 0:
        await message.answer("❌ Кол-во должно быть больше 0.")
        return

    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Игрок не найден (он должен хотя бы раз запустить бота).")
        return

    ok_grant, field, cur, new_val, err = _grant_reward_by_token(target_id, reward_token, amount)
    if not ok_grant:
        await message.answer(f"❌ {err}")
        return

    await message.answer(
        f"🏆 Награда за конкурс выдана.\n"
        f"👤 ID: {target_id}\n"
        f"🎁 {field}: +{fmt_num(amount)}\n"
        f"📊 Было: {fmt_num(cur)} -> Стало: {fmt_num(new_val)}"
    )
    try:
        await message.bot.send_message(
            target_id,
            f"🎉 Ты получил награду за конкурс!\n{field}: +{fmt_num(amount)}"
        )
    except Exception:
        pass


# ─────────────────────────────────────────────
#  МОДЕРАЦИЯ: /bb /bub /mute /unmute
# ─────────────────────────────────────────────
def _parse_duration(s: str) -> int:
    s = s.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return int(s[:-1]) * 86400
    return int(s) * 60


def _parse_rollback_window(token: str) -> int:
    t = str(token or "").strip().lower()
    mapping = {
        "5m": 5 * 60,
        "5м": 5 * 60,
        "30m": 30 * 60,
        "30м": 30 * 60,
        "1h": 3600,
        "1ч": 3600,
        "10h": 10 * 3600,
        "10ч": 10 * 3600,
        "24h": 24 * 3600,
        "24ч": 24 * 3600,
    }
    return int(mapping.get(t, 0) or 0)


def _case_field_from_token(token: str) -> str:
    t = (token or "").strip().lower()
    afk_alias = {
        "об": "afk_common",
        "обыч": "afk_common",
        "обычный": "afk_common",
        "ред": "afk_rare",
        "редкий": "afk_rare",
        "эп": "afk_epic",
        "эпик": "afk_epic",
        "эпический": "afk_epic",
        "лег": "afk_legendary",
        "легенд": "afk_legendary",
        "легендарный": "afk_legendary",
        "мифч": "afk_mythic",
        "миф": "afk_mythic",
        "мифический": "afk_mythic",
    }
    if t in afk_alias:
        return afk_alias[t]
    m = re.match(r"^ко([1-9]|1[0-5])$", t)
    if m:
        return f"weapon_cases_a{int(m.group(1))}"
    m = re.match(r"^кп([1-9]|1[0-5])$", t)
    if m:
        return f"pet_cases_a{int(m.group(1))}"
    return ""


@router.message(Command("bb"))
async def cmd_ban(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _can_moderate(u):
        return
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except Exception:
                pass
    if not target_id:
        await message.answer("Использование: /bb [id] или ответ на сообщение.")
        return
    if target_id in SUPER_ADMINS:
        await message.answer("🛡 Нельзя банить суперадминов.")
        return
    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Игрок не найден.")
        return
    db.update_user(target_id, banned=1)
    await message.answer(f"🚫 Игрок {target_id} забанен.")


@router.message(Command("bub"))
async def cmd_unban(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _can_moderate(u):
        return
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except Exception:
                pass
    if not target_id:
        await message.answer("Использование: /bub [id]")
        return
    db.update_user(target_id, banned=0)
    await message.answer(f"✅ Игрок {target_id} разбанен.")


@router.message(Command("mute"))
async def cmd_mute(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _can_moderate(u):
        return
    target_id = None
    duration_str = "10m"
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        parts = message.text.split()
        if len(parts) >= 2:
            duration_str = parts[1]
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except Exception:
                pass
        if len(parts) >= 3:
            duration_str = parts[2]
    if not target_id:
        await message.answer("Использование: /mute [id] 10m")
        return
    if target_id in SUPER_ADMINS:
        await message.answer("🛡 Нельзя мутить суперадминов.")
        return
    try:
        secs = _parse_duration(duration_str)
    except Exception:
        secs = 600
    muted_until = int(time.time()) + secs
    db.update_user(target_id, muted_until=muted_until)
    await message.answer(f"🔇 Игрок {target_id} замучен на {secs // 60} мин.")


@router.message(Command("unmute"))
async def cmd_unmute(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _can_moderate(u):
        return
    target_id = None
    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        parts = message.text.split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except Exception:
                pass
    if not target_id:
        await message.answer("Использование: /unmute [id]")
        return
    db.update_user(target_id, muted_until=0)
    await message.answer(f"🔊 Игрок {target_id} размучен.")


@router.message(Command("notifyoff"))
async def cmd_notify_off(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /notifyoff [id]")
        return
    try:
        target_id = int(parts[1])
    except Exception:
        await message.answer("❌ Неверный ID.")
        return
    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Игрок не найден.")
        return
    if not db.set_notify_off(target_id, True):
        await message.answer("❌ Не удалось обновить настройки уведомлений.")
        return
    await message.answer(
        f"🔕 Рассылки отключены для {target_id}.\n"
        "Игрок больше не получает /soo, промо-рассылки, /fk, /con и другие массовые сообщения."
    )


@router.message(Command("notifyon"))
async def cmd_notify_on(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /notifyon [id]")
        return
    try:
        target_id = int(parts[1])
    except Exception:
        await message.answer("❌ Неверный ID.")
        return
    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Игрок не найден.")
        return
    if not db.set_notify_off(target_id, False):
        await message.answer("❌ Не удалось обновить настройки уведомлений.")
        return
    await message.answer(f"🔔 Рассылки снова включены для {target_id}.")


@router.message(Command("stop"))
async def cmd_stop_user_combat(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    target_id = None
    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = int(message.reply_to_message.from_user.id)
    else:
        parts = (message.text or "").split()
        if len(parts) >= 2:
            try:
                target_id = int(parts[1])
            except Exception:
                pass

    if not target_id:
        await message.answer("Использование: /stop [id] или ответом на сообщение игрока.")
        return

    tu = db.get_user(int(target_id))
    if not tu:
        await message.answer("❌ Игрок не найден.")
        return

    had_battle = bool(ACTIVE_BATTLES.get(int(target_id))) or bool(db.get_active_battle(int(target_id)))
    had_dungeon = bool(ACTIVE_DUNGEONS.get(int(target_id))) or bool(db.get_active_dungeon(int(target_id)))

    _clear_user_combat_states(int(target_id))

    if not had_battle and not had_dungeon:
        await message.answer(f"ℹ️ У игрока {target_id} нет активных боёв/данжей.")
        return

    status = []
    if had_battle:
        status.append("босс-боев")
    if had_dungeon:
        status.append("данжей")
    await message.answer(f"🛑 Остановлено у {target_id}: {', '.join(status)}.")

    if bot_instance is not None:
        try:
            await bot_instance.send_message(
                int(target_id),
                "🛑 Администратор остановил твой активный бой/данж.\n"
                "Запусти заново через команды: боссы / данж.",
            )
        except Exception:
            pass


@router.message(Command("reset"))
async def cmd_global_profiles_reset(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return

    users_count = db.reset_all_users_preserve_core()

    ACTIVE_BATTLES.clear()
    ACTIVE_BATTLES_BY_MSG.clear()
    ACTIVE_DUNGEONS.clear()
    ACTIVE_DUNGEONS_BY_MSG.clear()
    DUNGEON_LOCKS.clear()
    FRIENDLY_DUELS.clear()
    CONTEST_STATE.clear()
    CONTEST_ANSWERED.clear()
    FAST_CONTEST_STATE.clear()
    FAST_CONTEST_ANSWERED.clear()
    ACTIVITY_MONITOR.clear()

    await message.answer(
        "🔄 Глобальный сброс профилей выполнен.\n"
        f"Сброшено игроков: {fmt_num(users_count)}\n"
        "Сохранено: админ-роли, ники, дата регистрации/лейбл, профильный текст, топ-донат."
    )


@router.message(Command("rollback"))
async def cmd_rollback(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return

    parts = (message.text or "").strip().split()
    target_id = 0
    all_mode = False
    window_secs = 0

    if message.reply_to_message and message.reply_to_message.from_user:
        if len(parts) < 2:
            await message.answer("Использование: /rollback [5m|30m|1h|10h|24h] (reply) или /rollback [id|all] [время]")
            return
        target_id = int(message.reply_to_message.from_user.id)
        window_secs = _parse_rollback_window(parts[1])
    else:
        if len(parts) < 3:
            await message.answer("Использование: /rollback [id|all] [5m|30m|1h|10h|24h]")
            return
        token = str(parts[1]).strip().lower()
        all_mode = token in {"all", "все", "всех"}
        if not all_mode and not token.isdigit():
            await message.answer("❌ Укажи ID игрока или all.")
            return
        if not all_mode:
            target_id = int(token)
        window_secs = _parse_rollback_window(parts[2])

    if window_secs <= 0:
        await message.answer("❌ Поддерживаются интервалы: 5m, 30m, 1h, 10h, 24h")
        return

    if all_mode:
        restored = db.restore_all_users_rollback(window_secs)
        if restored <= 0:
            await message.answer("❌ Не найдено подходящих снапшотов для отката.")
            return

        ACTIVE_BATTLES.clear()
        ACTIVE_BATTLES_BY_MSG.clear()
        ACTIVE_DUNGEONS.clear()
        ACTIVE_DUNGEONS_BY_MSG.clear()
        _restore_battles_from_db()
        _restore_dungeons_from_db()
        await message.answer(
            f"⏪ Выполнен откат всех игроков на {_fmt_uptime(window_secs)}.\n"
            f"Восстановлено профилей: {fmt_num(restored)}"
        )
        return

    tu = db.get_user(int(target_id))
    if not tu:
        await message.answer("❌ Игрок не найден.")
        return
    restored_ok, snap_ts = db.restore_user_rollback(int(target_id), window_secs)
    if not restored_ok:
        await message.answer("❌ Не найден подходящий снапшот для этого игрока.")
        return

    bs = ACTIVE_BATTLES.pop(int(target_id), None)
    if bs:
        ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
    ds = ACTIVE_DUNGEONS.pop(int(target_id), None)
    if ds:
        ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)
    _restore_battle_from_db_row(db.get_active_battle(int(target_id)))
    _restore_dungeon_from_db_row(db.get_active_dungeon(int(target_id)))

    await message.answer(
        f"⏪ Игрок {target_id} откатан на {_fmt_uptime(window_secs)}.\n"
        f"Снапшот: {_fmt_ts_msk(snap_ts)} МСК"
    )


@router.message(Command("transferacc"))
async def cmd_transfer_account(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return

    parts = (message.text or "").strip().split()
    if len(parts) < 3:
        await message.answer("Использование: /transferacc [old_id] [new_id]")
        return
    if not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("❌ ID должны быть числами.")
        return

    old_id = int(parts[1])
    new_id = int(parts[2])
    if old_id == new_id:
        await message.answer("❌ old_id и new_id должны отличаться.")
        return

    new_username = ""
    if message.reply_to_message and message.reply_to_message.from_user and int(message.reply_to_message.from_user.id) == new_id:
        new_username = str(message.reply_to_message.from_user.username or "").strip()

    try:
        ok_move, reason = db.transfer_account_progress(old_id, new_id, new_username=new_username)
    except Exception as e:
        log.error(f"transferacc error: {e}")
        await message.answer(f"❌ Ошибка переноса: {e}")
        return

    if not ok_move:
        msg = {
            "bad_id": "❌ Неверные ID.",
            "same_id": "❌ old_id и new_id совпадают.",
            "old_not_found": "❌ Старый аккаунт не найден.",
        }.get(reason, f"❌ Перенос не выполнен: {reason}")
        await message.answer(msg)
        return

    # Сбрасываем RAM-состояние для обоих ID и поднимаем его из БД заново.
    for uid in (old_id, new_id):
        bs = ACTIVE_BATTLES.pop(uid, None)
        if bs:
            ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
        ds = ACTIVE_DUNGEONS.pop(uid, None)
        if ds:
            ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)

    _restore_battle_from_db_row(db.get_active_battle(new_id))
    _restore_dungeon_from_db_row(db.get_active_dungeon(new_id))

    await message.answer(
        "✅ Перенос аккаунта выполнен.\n"
        f"Старый ID: {old_id}\n"
        f"Новый ID: {new_id}\n"
        "Перенесены профиль, инвентарь, статы и боевой прогресс."
    )


def _target_from_cmd_or_reply(message: Message) -> int:
    if message.reply_to_message and message.reply_to_message.from_user:
        return int(message.reply_to_message.from_user.id)
    parts = (message.text or "").split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return 0


@router.message(Command("title"))
async def cmd_set_title(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    parts = (message.text or "").split()
    target_id = 0
    title_text = ""

    if message.reply_to_message and message.reply_to_message.from_user:
        target_id = int(message.reply_to_message.from_user.id)
        title_text = (message.text or "").replace(parts[0], "", 1).strip() if parts else ""
    else:
        if len(parts) < 3:
            await message.answer("Использование: /title [id] [текст] или reply: /title [текст]")
            return
        if not parts[1].isdigit():
            await message.answer("❌ Неверный ID игрока.")
            return
        target_id = int(parts[1])
        title_text = " ".join(parts[2:]).strip()

    if target_id <= 0:
        await message.answer("❌ Укажи корректный ID игрока.")
        return
    if not title_text:
        await message.answer("❌ Текст титула не может быть пустым.")
        return

    title_text = title_text[:120]
    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Профиль не найден.")
        return

    db.update_user(target_id, profile_title=title_text)
    await message.answer(f"✅ Титул выдан игроку {target_id}: {title_text}")


@router.message(Command("cleartitle"))
@router.message(Command("untitle"))
async def cmd_clear_title(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    target_id = _target_from_cmd_or_reply(message)
    if target_id <= 0:
        await message.answer("Использование: /cleartitle [id] или reply на игрока")
        return

    tu = db.get_user(target_id)
    if not tu:
        await message.answer("❌ Профиль не найден.")
        return

    db.update_user(target_id, profile_title="")
    await message.answer(f"✅ Титул удален у игрока {target_id}.")


@router.message(Command("donpet"))
async def cmd_donpet(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    target_id = _target_from_cmd_or_reply(message)
    if target_id <= 0:
        await message.answer("Использование: /donpet [id] или reply на игрока")
        return
    success, info = _grant_vip_donate_item(target_id, "pet")
    if not success:
        await message.answer(f"❌ {info}")
        return
    await message.answer(f"✅ {info}")
    if bot_instance is not None:
        try:
            await bot_instance.send_message(
                int(target_id),
                "🎉 Спасибо за покупку VIP-питомца!\n"
                "👑 Предмет зачислен и надет автоматически.",
            )
        except Exception:
            pass


@router.message(Command("donekip"))
@router.message(Command("donequip"))
@router.message(Command("donwep"))
async def cmd_donekip(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    target_id = _target_from_cmd_or_reply(message)
    if target_id <= 0:
        await message.answer("Использование: /donekip [id] или reply на игрока")
        return
    success, info = _grant_vip_donate_item(target_id, "weapon")
    if not success:
        await message.answer(f"❌ {info}")
        return
    await message.answer(f"✅ {info}")
    if bot_instance is not None:
        try:
            await bot_instance.send_message(
                int(target_id),
                "🎉 Спасибо за покупку VIP-оружия!\n"
                "👑 Предмет зачислен и надет автоматически.",
            )
        except Exception:
            pass


@router.message(Command("donfull"))
@router.message(Command("donset"))
async def cmd_donfull(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    target_id = _target_from_cmd_or_reply(message)
    if target_id <= 0:
        await message.answer("Использование: /donfull [id] или reply на игрока")
        return

    p_ok, p_info = _grant_vip_donate_item(target_id, "pet")
    w_ok, w_info = _grant_vip_donate_item(target_id, "weapon")
    if not p_ok and not w_ok:
        await message.answer(f"❌ Не удалось выдать набор:\n• Питомец: {p_info}\n• Оружие: {w_info}")
        return

    lines = ["✅ Выдан полный VIP-набор:"]
    lines.append(f"• Питомец: {'OK' if p_ok else 'ошибка'}")
    lines.append(f"• Оружие: {'OK' if w_ok else 'ошибка'}")
    await message.answer("\n".join(lines))

    if bot_instance is not None:
        try:
            await bot_instance.send_message(
                int(target_id),
                "🎉 Спасибо за покупку полного VIP-набора!\n"
                "👑 VIP-питомец и VIP-оружие зачислены и надеты автоматически.",
            )
        except Exception:
            pass


def _parse_item_ids_payload(raw: str) -> list[int]:
    cleaned = str(raw or "").replace(",", " ")
    out: list[int] = []
    for token in cleaned.split():
        if token.isdigit():
            out.append(int(token))
    uniq: list[int] = []
    seen: set[int] = set()
    for v in out:
        if v in seen:
            continue
        seen.add(v)
        uniq.append(v)
    return uniq


def _artifact_cfg_from_token(token: str) -> tuple[str, dict] | tuple[None, None]:
    raw = str(token or "").strip()
    if not raw:
        return None, None

    # 1) Прямой выбор по emoji (например, 🎮, 🥋, 🗝)
    if raw in ARTIFACT_TYPES:
        return raw, ARTIFACT_TYPES[raw]

    low = raw.lower()

    # 2) По ключу эффекта (например, crit, regen, mini_any)
    for emo, cfg in ARTIFACT_TYPES.items():
        if str(cfg.get("effect", "")).lower() == low:
            return emo, cfg

    # 3) По части названия артефакта
    matches = []
    for emo, cfg in ARTIFACT_TYPES.items():
        name_low = str(cfg.get("name", "")).lower()
        if low in name_low:
            matches.append((emo, cfg))
    if len(matches) == 1:
        return matches[0]

    return None, None


@router.message(Command("save"))
async def cmd_save_items(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /save id[, id2, id3]")
        return

    item_ids = _parse_item_ids_payload(parts[1])
    if not item_ids:
        await message.answer("❌ Укажи хотя бы один числовой item_id.")
        return

    saved = 0
    misses: list[str] = []
    owners: set[int] = set()
    for item_id in item_ids[:100]:
        ok_save, reason, owner_id = db.save_item_by_id(int(item_id))
        if ok_save:
            saved += 1
            owners.add(int(owner_id))
        else:
            misses.append(f"{item_id} ({reason})")

    lines = [
        "✅ Сохранение предметов для истинного реберта обновлено.",
        f"Сохранено: {saved}",
    ]
    if owners:
        lines.append(f"Владельцы: {', '.join(str(x) for x in sorted(owners))}")
    if misses:
        lines.append("Не найдено: " + ", ".join(misses[:20]))
    await message.answer("\n".join(lines))


@router.message(Command("unsave"))
async def cmd_unsave_items(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /unsave id[, id2, id3]")
        return

    item_ids = _parse_item_ids_payload(parts[1])
    if not item_ids:
        await message.answer("❌ Укажи хотя бы один числовой item_id.")
        return

    removed = 0
    not_saved: list[str] = []
    for item_id in item_ids[:100]:
        if db.unsave_item_by_id(int(item_id)):
            removed += 1
        else:
            not_saved.append(str(item_id))

    lines = [
        "✅ Список сохранения обновлен.",
        f"Убрано из сохранения: {removed}",
    ]
    if not_saved:
        lines.append("Не были сохранены: " + ", ".join(not_saved[:20]))
    await message.answer("\n".join(lines))


@router.message(Command("delart"))
async def cmd_delete_artifact_by_id(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /delart [item_id]")
        return

    item_id = int(parts[1])
    import sqlite3 as _sq

    with _sq.connect("bot.db", timeout=30) as con:
        con.row_factory = _sq.Row
        row = con.execute(
            "SELECT id, tg_id, type, name, count FROM inventory WHERE id = ?",
            (item_id,),
        ).fetchone()
        if not row:
            await message.answer("❌ Артефакт не найден.")
            return
        if str(row["type"] or "") != "artifact":
            await message.answer("❌ Этот item_id не является артефактом.")
            return

        holder_id = int(row["tg_id"] or 0)

        # Если предмет доверенный, очищаем связь доверия у владельца.
        trust = _artifact_trust_info(item_id, holder_id)
        if trust:
            con.execute("DELETE FROM artifact_trust WHERE item_id = ?", (int(item_id),))

        con.execute("DELETE FROM inventory WHERE id = ?", (item_id,))
        con.execute("DELETE FROM saved_items WHERE item_id = ?", (item_id,))

    # Чистим слоты держателя, если там стоял удаленный артефакт.
    for slot_key in ARTIFACT_SLOT_KEYS:
        if int(db.get_stat(holder_id, slot_key, 0) or 0) == item_id:
            db.set_stat_value(holder_id, slot_key, 0)

    await message.answer(
        f"✅ Артефакт удален.\n"
        f"ID: {item_id}\n"
        f"Держатель: {holder_id}"
    )


@router.message(Command("giveart"))
async def cmd_give_artifact(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return

    # Формат:
    # /giveart [тип] [уровень] [кол-во] [id]
    # /giveart [тип] [уровень] [кол-во]   (reply на игрока)
    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer(
            "Использование:\n"
            "/giveart [тип] [уровень 1-10] [кол-во] [id]\n"
            "или reply на игрока без id\n"
            "Примеры: /giveart 🎮 1 1 123456789 | /giveart crit 5 3"
        )
        return

    art_token = str(parts[1]).strip()
    if not str(parts[2]).isdigit() or not str(parts[3]).isdigit():
        await message.answer("❌ Уровень и количество должны быть числами.")
        return

    level = max(1, min(10, int(parts[2])))
    count = max(1, min(10000, int(parts[3])))

    target_id = 0
    if len(parts) >= 5 and str(parts[4]).isdigit():
        target_id = int(parts[4])
    elif message.reply_to_message and message.reply_to_message.from_user:
        target_id = int(message.reply_to_message.from_user.id)

    if target_id <= 0:
        await message.answer("❌ Укажи ID игрока или используй reply на сообщение игрока.")
        return

    tu = db.get_user(int(target_id))
    if not tu:
        await message.answer("❌ Игрок не найден (он должен хотя бы раз запустить бота).")
        return

    emo, cfg = _artifact_cfg_from_token(art_token)
    if not emo or not cfg:
        await message.answer("❌ Неизвестный тип артефакта. Используй emoji/эффект/название.")
        return

    item_name = f"{emo} {cfg['name']}"
    db.add_inventory_item(int(target_id), "artifact", item_name, int(level), 0, int(count))

    await message.answer(
        "✅ Артефакт выдан.\n"
        f"👤 Игрок: {target_id}\n"
        f"🧿 Тип: {emo} {cfg['name']} ({cfg['effect']})\n"
        f"⭐ Уровень: {level}\n"
        f"📦 Кол-во: {count}"
    )


# ─────────────────────────────────────────────
#  /set — только создатель
# ─────────────────────────────────────────────
@router.message(Command("set"))
async def cmd_set(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    parts = message.text.split()
    if len(parts) < 4:
        await message.answer(
            "Использование:\n"
            "/set монеты [число] [id]\n"
            "/set маг [число] [id]\n"
            "/set арена [число] [id]\n"
            "/set мощность [число] [id]\n"
            "/set хп [число] [id]\n"
            "/set ранг [число] [id]\n"
            "/set vip [число] [id]\n"
            "/set эссенция [число] [id]\n"
            "/set кейсы [тип] [число] [id] (тип: ко1..ко15, кп1..кп15, об, ред, эп, лег, мифч)"
        )
        return
    field_raw = parts[1].lower()

    if field_raw in {"кейсы", "кейс", "cases"}:
        if len(parts) < 4:
            await message.answer("❌ Использование: /set кейсы [тип] [число] [id] (или ответом на сообщение)")
            return
        case_type = parts[2].lower()
        try:
            value = int(parts[3])
        except Exception:
            await message.answer("❌ Неверное число кейсов.")
            return

        target_id = None
        if len(parts) >= 5:
            try:
                target_id = int(parts[4])
            except Exception:
                await message.answer("❌ Неверный ID игрока.")
                return
        elif message.reply_to_message:
            target_id = int(message.reply_to_message.from_user.id)

        if not target_id:
            await message.answer("❌ Укажи ID: /set кейсы [тип] [число] [id] или используй reply.")
            return

        col = _case_field_from_token(case_type)
        if not col:
            await message.answer("❌ Неизвестный тип кейса. Пример: ко1, кп10, об, ред, эп, лег, мифч")
            return
        if not db.get_user(target_id):
            await message.answer("❌ Игрок не найден (он должен хотя бы раз запустить бота).")
            return
        db.update_user(target_id, **{col: value})
        await message.answer(f"✅ {col} → {value} для {target_id}")
        return

    try:
        value = int(parts[2])
        target_id = int(parts[3])
    except Exception:
        await message.answer("❌ Неверный формат.")
        return
    field_map = {
        "монеты": "coins", "маг": "magic_coins", "арена": "arena",
        "хп": "hp_boost", "ранг": "rank_idx", "vip": "vip_lvl",
        "эссенция": "essence", "мощность": "power", "power": "power",
    }
    if field_raw in field_map:
        db.update_user(target_id, **{field_map[field_raw]: value})
        await message.answer(f"✅ {field_raw} → {value} для {target_id}")
    else:
        await message.answer("❌ Неизвестное поле.")


@router.message(Command("hide_clan"))
async def cmd_hide_clan(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /hide_clan [id_клана]")
        return
    try:
        guild_id = int(parts[1])
    except Exception:
        await message.answer("❌ Неверный ID клана.")
        return
    g = db.get_guild(guild_id)
    if not g:
        await message.answer("❌ Клан не найден.")
        return
    if not db.set_guild_hidden(guild_id, True):
        await message.answer("❌ Не удалось скрыть клан.")
        return
    await message.answer(f"✅ Клан «{g['name']}» (ID {guild_id}) скрыт из топа.")


@router.message(Command("show_clan"))
@router.message(Command("open_clan"))
async def cmd_show_clan(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Использование: /open_clan [id_клана]")
        return
    try:
        guild_id = int(parts[1])
    except Exception:
        await message.answer("❌ Неверный ID клана.")
        return
    g = db.get_guild(guild_id)
    if not g:
        await message.answer("❌ Клан не найден.")
        return
    if not db.set_guild_hidden(guild_id, False):
        await message.answer("❌ Не удалось вернуть клан в топ.")
        return
    await message.answer(f"✅ Клан «{g['name']}» (ID {guild_id}) снова виден в топе.")


@router.message(Command("clan_down"))
async def cmd_clan_down(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_admin(u):
        return
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer("Использование: /clan_down [id_клана] [уровень 1-5]")
        return
    try:
        guild_id = int(parts[1])
        level = int(parts[2])
    except Exception:
        await message.answer("❌ Неверный формат. Пример: /clan_down 14 2")
        return
    g = db.get_guild(guild_id)
    if not g:
        await message.answer("❌ Клан не найден.")
        return
    ok_set, prev, cur = db.set_guild_level(guild_id, level)
    if not ok_set:
        await message.answer("❌ Не удалось изменить уровень (допустимо 1..5).")
        return
    await message.answer(
        f"✅ Уровень клана «{g['name']}» обновлен: {prev} -> {cur}\n"
        f"Новые бонусы: {_guild_level_buff_text(cur)}"
    )


# ─────────────────────────────────────────────
#  /admin — ПАНЕЛЬ
# ─────────────────────────────────────────────
@router.message(Command("item_ids"))
async def cmd_item_ids(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    _write_admin_id_file()
    text = _admin_item_ids_text()
    if len(text) > 3500:
        bio = BytesIO(text.encode("utf-8"))
        await message.answer_document(
            BufferedInputFile(bio.read(), filename=ADMIN_ID_FILE),
            caption="Список ID предметов.",
        )
    else:
        await message.answer(text)


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    if not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    if tg_id not in ADMIN_CTX:
        ADMIN_CTX[tg_id] = {"action": "-", "value": "-", "target_id": "-"}
    await message.answer(
        _admin_view_text(ADMIN_CTX[tg_id]),
        reply_markup=_admin_kb(),
    )


@router.callback_query(F.data.startswith("adm_act:"))
async def cb_adm_act(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        await cb.answer("Нет доступа.", show_alert=True)
        return
    action = cb.data.split(":")[1]
    tg_id = int(u["tg_id"])
    ctx = ADMIN_CTX.get(tg_id, {"action": "-", "value": "-", "target_id": "-"})
    ctx["action"] = action
    ctx["value"] = "-"
    ADMIN_CTX[tg_id] = ctx

    if action in ("jinn_on", "jinn_off", "update_all"):
        await _safe_edit_cb(cb, _admin_view_text(ctx), reply_markup=_admin_kb())
        await cb.answer()
        return

    await _safe_edit_cb(cb, _admin_view_text(ctx), reply_markup=_admin_kb())
    await cb.answer(f"Выбрано: {action}. Вводи в формате: значение id")
    

@router.message(Command("tech"))
async def cmd_tech(message: Message):
    ok, u = await _check_access(message)
    if not ok:
        return
    
    if not _is_creator(u):
        await message.answer("❌ Команда доступна только создателям.")
        return
    
    parts = (message.text or "").strip().split()
    if len(parts) < 2:
        lines = ["🔧 Статус техработ:", SEP]
        for cmd, status in TECH_COMMANDS.items():
            icon = "🔴" if status else "🟢"
            lines.append(f"{icon} {cmd}: {'ВЫКЛ' if status else 'ВКЛ'}")
        lines.append("")
        lines.append("Использование: /tech [команда] [on|off]")
        lines.append("Пример: /tech профиль off  (выключить команду)")
        lines.append("       /tech профиль on   (включить команду)")
        await message.answer("\n".join(lines))
        return
    
    cmd_name = parts[1].lower()
    action = parts[2].lower() if len(parts) >= 3 else ""
    
    if cmd_name not in TECH_COMMANDS:
        await message.answer(f"❌ Команда «{cmd_name}» не найдена в списке.")
        return
    
    if action == "on":
        TECH_COMMANDS[cmd_name] = False   # ← on = РАБОТАЕТ
        await message.answer(f"🟢 Команда «{cmd_name}» теперь РАБОТАЕТ.")
    elif action == "off":
        TECH_COMMANDS[cmd_name] = True    # ← off = НА ТЕХРАБОТЕ
        await message.answer(f"🔴 Команда «{cmd_name}» теперь НА ТЕХРАБОТЕ.")
    else:
        status = TECH_COMMANDS[cmd_name]
        await message.answer(f"📊 Команда «{cmd_name}»: {'🔴 НА ТЕХРАБОТЕ' if status else '🟢 РАБОТАЕТ'}")


@router.callback_query(F.data == "adm_reset")
async def cb_adm_reset(cb: CallbackQuery):
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    ADMIN_CTX[tg_id] = {"action": "-", "value": "-", "target_id": "-"}
    await _safe_edit_cb(cb, _admin_view_text(ADMIN_CTX[tg_id]), reply_markup=_admin_kb())
    await cb.answer("Сброшено.")


@router.callback_query(F.data == "adm_confirm")
async def cb_adm_confirm(cb: CallbackQuery):
    global JINN_FORCED_UNTIL
    ok, u = await _check_cb_access(cb)
    if not ok:
        return
    if not _is_creator(u):
        return
    tg_id = int(u["tg_id"])
    ctx = ADMIN_CTX.get(tg_id, {})
    action = ctx.get("action", "-")
    value = ctx.get("value", "-")
    target_id = ctx.get("target_id", "-")

    if action == "jinn_on":
        JINN_FORCED_UNTIL = int(time.time()) + 3600
        ADMIN_CTX[tg_id] = {"action": "-", "value": "-", "target_id": "-"}
        await _safe_edit_cb(cb, "🧞 Джинн принудительно открыт на 1 час!", reply_markup=_admin_kb())
        await cb.answer("Джинн ВКЛ")
        return

    if action == "jinn_off":
        JINN_FORCED_UNTIL = 0
        ADMIN_CTX[tg_id] = {"action": "-", "value": "-", "target_id": "-"}
        await _safe_edit_cb(cb, "🌑 Джинн принудительно закрыт.", reply_markup=_admin_kb())
        await cb.answer("Джинн ВЫКЛ")
        return

    if action == "update_all":
        users_count = db.update_all_for_update_reset()
        ADMIN_CTX[tg_id] = {"action": "-", "value": "-", "target_id": "-"}
        await _safe_edit_cb(
            cb,
            f"🆕 Обновление применено.\nСброшено игроков: {fmt_num(users_count)}\n"
            f"• Арена -> 1\n• Монеты -> 0\n• Прогресс боссов/квестов очищен",
            reply_markup=_admin_kb(),
        )
        await cb.answer("Обновление применено")
        return

    if action == "delete_guild":
        try:
            guild_id = int(str(value).strip())
        except Exception:
            await cb.answer("❌ Введи id гильдии (число).", show_alert=True)
            return
        g = db.get_guild(guild_id)
        if not g:
            await cb.answer("❌ Гильдия не найдена.", show_alert=True)
            return
        db.delete_guild(guild_id)
        ADMIN_CTX[tg_id] = {"action": "-", "value": "-", "target_id": "-"}
        await _safe_edit_cb(
            cb,
            f"🗑 Гильдия удалена:\nID {guild_id}\nНазвание: {g['name']}",
            reply_markup=_admin_kb(),
        )
        await cb.answer("Гильдия удалена")
        return

    if target_id == "-" or value == "-":
        await cb.answer("❌ Не задан target_id или значение.", show_alert=True)
        return

    try:
        target_id = int(target_id)
    except Exception:
        await cb.answer("❌ Неверный ID.", show_alert=True)
        return

    if target_id in SUPER_ADMINS and action in ("ban", "mute"):
        await cb.answer("🛡 Нельзя.", show_alert=True)
        return

    tu = db.get_user(target_id)
    if not tu and action not in ("unban", "unmute", "guild_unity"):
        await cb.answer("❌ Игрок не найден.", show_alert=True)
        return

    result_msg = "✅ Готово."
    try:
        if action == "coins":
            db.update_user(target_id, coins=int(value))
        elif action == "essence":
            db.update_user(target_id, essence=int(value))
        elif action == "guild_unity":
            delta = int(value)
            g = db.get_guild(target_id)
            if not g:
                raise ValueError("Клан не найден")
            before = int(g["unity_shards"] or 0)
            db.guild_add_unity(target_id, delta)
            g2 = db.get_guild(target_id)
            after = int(g2["unity_shards"] or 0) if g2 else max(0, before + delta)
            sign = "+" if delta >= 0 else ""
            result_msg = (
                f"✅ Клан «{g['name']}» (ID {target_id})\n"
                f"🛡 Осколки единства: {fmt_num(before)} -> {fmt_num(after)} ({sign}{fmt_num(delta)})"
            )
        elif action == "cases":
            raw = str(value).strip()
            if "=" not in raw:
                raise ValueError("Для кейсов формат: коN=число или кпN=число или об=число")
            token, amount_s = raw.split("=", 1)
            col = _case_field_from_token(token)
            if not col:
                raise ValueError("Неизвестный тип кейса")
            db.update_user(target_id, **{col: int(amount_s)})
        elif action == "rank_idx":
            db.update_user(target_id, rank_idx=min(int(value), len(gd.RANKS) - 1))
        elif action == "vip_lvl":
            db.update_user(target_id, vip_lvl=min(max(int(value), 0), 6))
        elif action == "rebirth_count":
            db.update_user(target_id, rebirth_count=int(value))
        elif action == "arena":
            db.update_user(target_id, arena=min(max(int(value), 1), gd.max_arena()))
        elif action == "power":
            db.update_user(target_id, power=max(0, int(value)))
        elif action == "hp_boost":
            db.update_user(target_id, hp_boost=int(value))
        elif action == "admin_role":
            db.update_user(target_id, admin_role=min(int(value), 5))
        elif action == "ring_level":
            lvl = min(int(value), 5)
            db.update_user(target_id, ring_level=lvl, active_ring_level=lvl)
        elif action == "aura":
            aura_key = str(value).strip()
            if aura_key in gd.AURA_CATALOG:
                db.update_user(target_id, **{f"aura_{aura_key}": 1, "active_aura": aura_key})
            elif aura_key == "clear":
                db.update_user(target_id, active_aura="")
        elif action == "reg_label":
            raw = str(value).strip()
            low = raw.lower()
            if low in ("clear", "date", "дата", "reset", "none", "-"):
                db.update_user(target_id, reg_label="")
            elif low in ("old", "олд"):
                db.update_user(target_id, reg_label="олд")
            else:
                db.update_user(target_id, reg_label=raw)
        elif action in ("grant_weapon", "grant_pet"):
            item_id, custom_id, qty = _parse_admin_item_value(str(value))
            item = ADMIN_ITEM_CATALOG.get(item_id)
            if not item:
                raise ValueError("Неизвестный item_id")
            need_type = "weapon" if action == "grant_weapon" else "pet"
            if item["type"] != need_type:
                want = "оружие" if need_type == "weapon" else "питомец"
                raise ValueError(f"Этот admin_id не подходит для действия «{want}»")
            db.add_inventory_item(
                target_id,
                item["type"],
                item["name"],
                int(item.get("level", 1)),
                int(item["bonus"]),
                qty,
            )
            kind = "оружие" if item["type"] == "weapon" else "питомец"
            mark = f", custom_id={custom_id}" if custom_id else ""
            result_msg = f"✅ Выдано: {kind} «{item['name']}» x{qty} (admin_id={item_id}{mark}) игроку {target_id}"
        elif action == "profile_title":
            db.update_user(target_id, profile_title=str(value))
        elif action == "profile_note":
            db.update_user(target_id, profile_note=str(value))
        elif action == "ban":
            db.update_user(target_id, banned=1)
        elif action == "unban":
            db.update_user(target_id, banned=0)
        elif action == "mute":
            secs = _parse_duration(str(value))
            db.update_user(target_id, muted_until=int(time.time()) + secs)
        elif action == "unmute":
            db.update_user(target_id, muted_until=0)
        elif action == "reset":
            nick = str(tu["nickname"] or "") if tu else ""
            db.update_user(
                target_id,
                coins=1000,
                arena=1,
                boss_progress=0,
                boss_kill_mask=0,
                power=0,
                rebirth_count=0,
                rebirth_mult=1.0,
                rank_idx=0,
                true_rebirth_count=0,
                afk_common=0,
                afk_rare=0,
                afk_epic=0,
                afk_legendary=0,
                afk_mythic=0,
                magic_coins=0,
                essence=0,
                ring_level=0,
                active_ring_level=0,
                aura_regen=0,
                aura_fortune=0,
                aura_master=0,
                aura_hunter=0,
                aura_wrath=0,
                active_aura="",
                vip_lvl=0,
                hp_boost=0,
                total_boss_kills=0,
                equipped_weapon_id=0,
                equipped_pet_id=0,
                banned=0,
                muted_until=0,
                admin_role=0,
                profile_title="",
                profile_note="",
                last_daily_claim=0,
                reg_label="",
                training_active=0,
                training_until=0,
                nickname=nick,
            )
            result_msg = f"🔄 Полный сброс игрока {target_id} выполнен."
    except Exception as e:
        log.error(f"adm_confirm error: {e}")
        await cb.answer(f"❌ Ошибка: {e}", show_alert=True)
        return

    ADMIN_CTX[tg_id] = {"action": "-", "value": "-", "target_id": target_id}
    await _safe_edit_cb(cb, result_msg, reply_markup=_admin_kb())
    await cb.answer("✅ Выполнено.")


@router.message(lambda m: bool(m.text) and m.from_user is not None and ADMIN_CTX.get(m.from_user.id, {}).get("action", "-") != "-")
async def handle_admin_input(message: Message):
    tg_id = message.from_user.id
    ctx = ADMIN_CTX.get(tg_id)
    if not ctx or ctx.get("action", "-") == "-":
        return
    u = db.get_user(tg_id)
    if not u or not _is_creator(u):
        return
    text = message.text.strip()
    parts = text.split()
    action = ctx.get("action", "-")

    if action in ("jinn_on", "jinn_off", "update_all"):
        return

    if action in ("profile_title", "profile_note", "reg_label"):
        if len(parts) >= 2 and parts[-1].isdigit():
            ctx["value"] = " ".join(parts[:-1]).strip()
            ctx["target_id"] = int(parts[-1])
        else:
            ctx["value"] = text
    elif len(parts) >= 2:
        ctx["value"] = " ".join(parts[:-1]).strip()
        try:
            ctx["target_id"] = int(parts[-1])
        except Exception:
            pass
    elif len(parts) == 1:
        ctx["value"] = parts[0]

    ADMIN_CTX[tg_id] = ctx
    await message.answer(_admin_view_text(ctx) + "\n\nНажми ✅ Подтвердить в панели /admin")


# ─────────────────────────────────────────────
#  ГРУППЫ — АЛИАСЫ
# ─────────────────────────────────────────────
GROUP_ALIASES = {
    "проф": cmd_profile,
    "профиль": cmd_profile,
    "б": cmd_balance,
    "баланс": cmd_balance,
    "трен": cmd_train,
    "тренировка": cmd_train,
    "к": cmd_cases,
    "кейсы": cmd_cases,
    "инв": cmd_inventory,
    "инвентарь": cmd_inventory,
    "син": cmd_synth,
    "синтез": cmd_synth,
    "арена": cmd_arena,
    "реберт": cmd_rebirth,
    "ребёрт": cmd_rebirth,
    "ребёрты": cmd_rebirth,
    "экип": cmd_loadout,
    "экипировка": cmd_loadout,
    "ивент": cmd_event,
    "событие": cmd_event,
    "гайд": cmd_guide,
    "помощь": cmd_help,
    "хелп": cmd_help,
    "донат": cmd_donate,
    "сп": cmd_donate_special_alias,
    "каз": cmd_casino,
    "казино": cmd_casino,
    "миры": cmd_worlds,
    "гильдия": cmd_guild,
    "клан": cmd_clan,
    "крафт": cmd_craft,
    "еб": cmd_daily,
    "еж": cmd_daily,
    "ежедневка": cmd_daily,
    "бонус": cmd_daily,
    "деп": cmd_deposit_info,
    "депозит": cmd_deposit_info,
    "бонусы": cmd_bonuses,
    "топ донат": cmd_topdon,
    "топ дон": cmd_topdon,
    "улучшения": cmd_train_upgrades,
    "улуч": cmd_train_upgrades,
    "апы": cmd_train_upgrades,
    "реф": cmd_ref,
    "рефка": cmd_ref,
    "реферал": cmd_ref,
    "рефералы": cmd_ref,
}


@router.callback_query(F.data == "__stale__")
async def cb_fallback_stale(cb: CallbackQuery):
    # Заглушка — намеренно пустой обработчик для несуществующих кнопок
    try:
        await cb.answer()
    except Exception:
        pass


async def _jinn_notifier(bot: Bot):
    global JINN_PREALERT_SENT
    while True:
        try:
            day_key = _today_msk().isoformat()
            now_msk = _now_msk()
            cur_hour = now_msk.hour
            cur_minute = now_msk.minute
            users = db.list_users_for_notify()
            for row in users:
                tg_id = int(row["tg_id"])
                if int(row["banned"] or 0):
                    continue
                u = db.get_user(tg_id)
                if not u:
                    continue
                hour = int(u["trader_hour"] or -1)
                today = str(u["trader_day"] or "")
                if today != day_key:
                    hour = db.ensure_trader_hour(tg_id, day_key)
                if hour < 0:
                    continue
                alert_key = (tg_id, day_key, hour)
                if alert_key in JINN_PREALERT_SENT:
                    continue
                opens_in_secs = (hour * 3600) - (cur_hour * 3600 + cur_minute * 60 + now_msk.second)
                if 0 < opens_in_secs <= 180:
                    JINN_PREALERT_SENT.add(alert_key)
                    if not _notify_enabled(tg_id, NOTIFY_JINN_KEY):
                        continue
                    try:
                        await bot.send_message(
                            tg_id,
                            "📣 Джинн Аззар скоро появится в лавке.\nОсталось меньше 3 минут."
                        )
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"jinn_notifier error: {e}")
        await asyncio.sleep(30)


async def _vip_train_notify_worker(bot: Bot):
    """Уведомляет VIP 2+ о завершении тренировки (когда время вышло)."""
    while True:
        try:
            now_ts = int(time.time())
            for row in db.list_users_for_notify():
                uid = int(_row_get(row, "tg_id", 0) or 0)
                if uid <= 0:
                    continue
                u = db.get_user(uid)
                if not u:
                    continue
                if int(_row_get(u, "vip_lvl", 0) or 0) < 2:
                    continue
                if int(_row_get(u, "training_active", 0) or 0) != 1:
                    continue
                until_ts = int(_row_get(u, "training_until", 0) or 0)
                if until_ts <= 0 or now_ts < until_ts:
                    continue
                mark = int(db.get_stat(uid, VIP_TRAIN_NOTIFY_MARK_KEY, 0) or 0)
                if mark == until_ts:
                    continue
                db.set_stat_value(uid, VIP_TRAIN_NOTIFY_MARK_KEY, until_ts)
                try:
                    await bot.send_message(uid, "⏱ Тренировка завершена. Напиши «трен», чтобы забрать награды.")
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"vip_train_notify_worker error: {e}")
        await asyncio.sleep(60)


async def _vip_autosynth_worker(bot: Bot):
    """Авто-синтез для VIP 2+ с интервалами по уровню привилегии."""
    while True:
        try:
            now_ts = int(time.time())
            for row in db.list_users_for_notify():
                uid = int(_row_get(row, "tg_id", 0) or 0)
                if uid <= 0:
                    continue
                u = db.get_user(uid)
                if not u:
                    continue
                interval = _autosynth_interval_secs(u)
                if interval <= 0:
                    continue
                if int(db.get_stat(uid, VIP_AUTOSYNTH_DISABLED_KEY, 0) or 0) == 1:
                    continue
                last_ts = int(db.get_stat(uid, VIP_AUTOSYNTH_LAST_KEY, 0) or 0)
                if now_ts - last_ts < interval:
                    continue
                results = db.upgrade_all_three_to_one(uid, limit=2000)
                db.set_stat_value(uid, VIP_AUTOSYNTH_LAST_KEY, now_ts)
                if not results:
                    continue
                for r in results:
                    _track_synth_stats(uid, r)
                created_total = sum(int(r.get("created_count", 0) or 0) for r in results)
                try:
                    await bot.send_message(
                        uid,
                        f"🌀 Авто-синтез: операций {fmt_num(len(results))}, создано предметов {fmt_num(created_total)}.",
                    )
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"vip_autosynth_worker error: {e}")
        await asyncio.sleep(60)


async def _cleanup_battles():
    while True:
        try:
            now = time.time()
            to_remove = [uid for uid, bs in list(ACTIVE_BATTLES.items()) if now - bs.last_action > BATTLE_STALE_SEC]
            for uid in to_remove:
                bs = ACTIVE_BATTLES.pop(uid, None)
                if bs:
                    ACTIVE_BATTLES_BY_MSG.pop((bs.chat_id, bs.msg_id), None)
                    _drop_battle_state(uid)
        except Exception as e:
            log.warning(f"cleanup_battles error: {e}")
        await asyncio.sleep(60)


async def _cleanup_dungeons():
    while True:
        try:
            now = time.time()
            expired = [uid for uid, ds in list(ACTIVE_DUNGEONS.items()) if now - ds.started_at > 660]
            for uid in expired:
                ds = ACTIVE_DUNGEONS.pop(uid, None)
                if ds:
                    ACTIVE_DUNGEONS_BY_MSG.pop((ds.chat_id, ds.msg_id), None)
                    _drop_dungeon_state(uid)
        except Exception as e:
            log.warning(f"cleanup_dungeons error: {e}")
        await asyncio.sleep(30)


async def _promo_unpin_worker(bot: Bot):
    while True:
        try:
            expired_rows = db.list_expired_active_promo_broadcasts(int(time.time()))
            for row in expired_rows:
                bid = int(row["id"])
                chat_id = int(row["chat_id"])
                msg_id = int(row["message_id"])
                try:
                    await bot.unpin_chat_message(chat_id=chat_id, message_id=msg_id)
                except Exception:
                    pass
                db.deactivate_promo_broadcast(bid)
        except Exception as e:
            log.warning(f"promo_unpin_worker error: {e}")
        await asyncio.sleep(60)


async def _rollback_snapshot_worker():
    """Периодически делает снапшоты профилей для команды /rollback."""
    while True:
        try:
            created = db.create_all_users_rollback_snapshot()
            if created > 0:
                log.info(f"rollback snapshot: {created} users")
        except Exception as e:
            log.warning(f"rollback_snapshot_worker error: {e}")
        await asyncio.sleep(300)


async def _auto_promo_worker(bot: Bot):
    """Ежедневно в 15:00 МСК создает авто-промокод на 6 часов."""
    global AUTO_PROMO_LAST_DAY
    while True:
        try:
            msk_now = _now_msk()
            day_key = msk_now.date().isoformat()
            # Узкое окно, чтобы избежать дублей при перезапуске и дребезге таймера.
            if (
                msk_now.hour == AUTO_PROMO_MSK_HOUR
                and msk_now.minute in (0, 1, 2)
                and AUTO_PROMO_LAST_DAY != day_key
            ):
                await _create_daily_auto_promo(bot)
                AUTO_PROMO_LAST_DAY = day_key
                log.info(f"auto promo created for {day_key}")
        except Exception as e:
            log.warning(f"auto_promo_worker error: {e}")
        await asyncio.sleep(20)



async def _bio_bonus_watcher(bot: Bot):
    while True:
        try:
            users = db.list_users_for_bio_scan()
            for row in users:
                tg_id = int(row["tg_id"])
                if int(row["banned"] or 0):
                    continue
                await _refresh_bio_bonus_for_user(bot, tg_id, notify=True)
                await asyncio.sleep(0.05)
        except Exception as e:
            log.warning(f"bio_bonus_watcher error: {e}")
        await asyncio.sleep(1800)


async def _artifact_trust_worker():
    """Возвращает доверенные артефакты владельцам после окончания таймера."""
    while True:
        try:
            now_ts = int(time.time())
            # Keep runtime typing simple for wider interpreter compatibility.
            notify_owner = set()
            notify_holder = []

            # 1) Возвращаем всё, что истекло по времени.
            for due in db.list_expired_artifact_trust(now_ts):
                item_id = int(_row_get(due, "item_id", 0) or 0)
                if item_id <= 0:
                    continue
                try:
                    ok_return, owner_id, holder_id, _reason = db.return_artifact_trust(item_id)
                    if ok_return:
                        notify_owner.add(int(owner_id))
                        if int(holder_id) > 0 and int(holder_id) != int(owner_id):
                            notify_holder.append((int(holder_id), int(item_id)))
                except Exception as inner_e:
                    log.warning(f"artifact_trust_worker expire error item={item_id}: {inner_e}")

            # 2) Самовосстановление: если доверие существует, но предмет утерян/перемещен,
            # возвращаем владельцу немедленно.
            for row in db.list_all_artifact_trust(limit=5000):
                item_id = int(_row_get(row, "item_id", 0) or 0)
                holder_id = int(_row_get(row, "holder_id", 0) or 0)
                owner_id = int(_row_get(row, "owner_id", 0) or 0)
                if item_id <= 0 or holder_id <= 0 or owner_id <= 0:
                    continue
                if int(_row_get(row, "expires_at", 0) or 0) <= now_ts:
                    continue
                inv = db.get_inventory_item(holder_id, item_id)
                if inv and str(_row_get(inv, "type", "") or "") == "artifact":
                    continue
                try:
                    ok_return, ret_owner, ret_holder, _reason = db.return_artifact_trust(item_id)
                    if ok_return:
                        notify_owner.add(int(ret_owner))
                        if int(ret_holder) > 0 and int(ret_holder) != int(ret_owner):
                            notify_holder.append((int(ret_holder), int(item_id)))
                except Exception as inner_e:
                    log.warning(f"artifact_trust_worker heal error item={item_id}: {inner_e}")

            # Уведомления отправляем после фиксации БД.
            if bot_instance is not None:
                for owner_id in sorted(notify_owner):
                    try:
                        await bot_instance.send_message(int(owner_id), "🧿 Доверенный артефакт автоматически возвращен.")
                    except Exception:
                        pass
                for holder_id, item_id in notify_holder:
                    try:
                        await bot_instance.send_message(
                            int(holder_id),
                            f"⏳ Срок доверия артефакта ID {item_id} истек. Предмет возвращен владельцу.",
                        )
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"artifact_trust_worker error: {e}")
        await asyncio.sleep(20)


def _restore_battles_from_db():
    now = time.time()
    restored = 0
    for row in db.list_active_battles():
        user_id = int(row["tg_id"])
        last_action = float(row["last_action"] or 0)
        if now - last_action > BATTLE_STALE_SEC:
            db.delete_active_battle(user_id)
            continue
        bs = BattleState(
            user_id=user_id,
            arena=int(row["arena"]),
            boss_idx=int(row["boss_idx"]),
            boss_hp=int(row["boss_hp"]),
            boss_max_hp=int(row["boss_max_hp"]),
            player_hp=int(row["player_hp"]),
            player_max_hp=int(row["player_max_hp"]),
            player_dmg=int(row["player_dmg"]),
            regen_per_tick=int(row["regen_per_tick"]),
            boss_atk=int(row["boss_atk"]),
            msg_id=int(row["msg_id"]),
            chat_id=int(row["chat_id"]),
        )
        bs.last_regen = float(row["last_regen"] or now)
        bs.last_action = last_action or now
        ACTIVE_BATTLES[user_id] = bs
        ACTIVE_BATTLES_BY_MSG[(bs.chat_id, bs.msg_id)] = user_id
        restored += 1
    if restored:
        log.info(f"Восстановлено активных боёв: {restored}")


def _restore_dungeons_from_db():
    now = time.time()
    restored = 0
    for row in db.list_active_dungeons():
        user_id = int(row["tg_id"])
        started_at = float(row["started_at"] or now)
        if now - started_at > 660:
            db.delete_active_dungeon(user_id)
            continue
        ds = DungeonState(
            user_id=user_id,
            mode=str(row["mode"] or _today_dungeon_mode()),
            arena=int(row["arena"]),
            msg_id=int(row["msg_id"]),
            chat_id=int(row["chat_id"]),
        )
        ds.wave = int(row["wave"])
        ds.max_waves = int(row["max_waves"])
        ds.gold = int(row["gold"])
        ds.magic = int(row["magic"])
        try:
            raw_shards = json.loads(str(row["shards_json"] or "{}"))
            ds.shards = {int(k): int(v) for k, v in dict(raw_shards).items()}
        except Exception:
            ds.shards = {}
        ds.started_at = started_at
        ds.enemy_hp = int(row["enemy_hp"])
        ds.enemy_max_hp = int(row["enemy_max_hp"])
        ds.enemy_atk = int(row["enemy_atk"])
        ds.player_dmg = int(row["player_dmg"])
        ds.note = str(row["note"] or "")
        ACTIVE_DUNGEONS[user_id] = ds
        ACTIVE_DUNGEONS_BY_MSG[(ds.chat_id, ds.msg_id)] = user_id
        restored += 1
    if restored:
        log.info(f"Восстановлено активных данжей: {restored}")


def _restore_contest_from_db():
    if not _contest_restore_from_db():
        return
    contest_id = int(CONTEST_STATE.get("id", 0) or 0)
    ends_at = int(CONTEST_STATE.get("ends_at", 0) or 0)
    if contest_id <= 0 or ends_at <= 0:
        return
    delay = max(1, ends_at - int(time.time()))
    asyncio.create_task(_contest_end_worker(contest_id, delay))
    log.info(f"Восстановлен конкурс #{contest_id}. До конца: {delay}с")


async def _world_boss_worker():
    while True:
        try:
            state = db.world_boss_apply_regen(int(time.time()), WORLD_BOSS_REGEN_SEC, WORLD_BOSS_REGEN_HP)
            if not state:
                state = _ensure_world_boss_event()
            if state and int(state["is_finished"] or 0) and int(state["rewards_done"] or 0) == 0:
                await _world_boss_distribute_rewards(state)
        except Exception as e:
            log.warning(f"world_boss_worker error: {e}")
        await asyncio.sleep(5)


async def _health_http_worker():
    """Мини-HTTP сервер для платформ, где процесс должен слушать PORT."""
    port_raw = str(os.getenv("PORT", "")).strip()
    port_candidates = []
    if port_raw:
        try:
            port_candidates.append(int(port_raw))
        except Exception:
            log.warning(f"health worker: invalid PORT='{port_raw}'")
    # На некоторых хостингах PORT не пробрасывают в env, но healthcheck
    # все равно ожидает открытый локальный порт.
    for fallback_port in (3000, 8080, 8000):
        if fallback_port not in port_candidates:
            port_candidates.append(fallback_port)

    async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            try:
                await reader.read(1024)
            except Exception:
                pass
            body = b"ok"
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"Content-Length: 2\r\n"
                b"Connection: close\r\n\r\n" + body
            )
            writer.write(resp)
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    server = None
    bound_port = 0
    for port in port_candidates:
        try:
            server = await asyncio.start_server(_handle, host="0.0.0.0", port=int(port))
            bound_port = int(port)
            break
        except Exception as e:
            log.warning(f"health worker start failed on :{port}: {e}")
    if server is None:
        return

    log.info(f"Health HTTP слушает :{bound_port}")
    async with server:
        await server.serve_forever()


# ─────────────────────────────────────────────
#  ЗАПУСК БОТА
# ─────────────────────────────────────────────
bot_instance: Bot = None


def _build_public_commands():
    # Keep this block syntax-simple for strict runtimes and copy/paste-safe deploys.
    raw_commands = []
    raw_commands.append(("start", "Start"))
    raw_commands.append(("id", "My ID"))
    raw_commands.append(("ref", "Referrals"))
    raw_commands.append(("profile", "Profile"))
    raw_commands.append(("balance", "Balance"))
    raw_commands.append(("train", "Training"))
    raw_commands.append(("cases", "Cases"))
    raw_commands.append(("inventory", "Inventory"))
    raw_commands.append(("dungeon", "Dungeon"))
    raw_commands.append(("guide", "Guide"))
    raw_commands.append(("help", "Help"))
    raw_commands.append(("daily", "Daily bonus"))
    raw_commands.append(("bonuses", "Bonuses"))
    raw_commands.append(("casino", "Casino"))
    raw_commands.append(("startivent", "Event"))
    raw_commands.append(("arena", "Arena"))
    raw_commands.append(("rebirth", "Rebirth"))
    raw_commands.append(("upgrades", "Upgrades"))
    raw_commands.append(("craft", "Craft"))
    raw_commands.append(("guild", "Guild"))
    raw_commands.append(("clan", "Clan"))
    raw_commands.append(("topdon", "Top donate"))
    raw_commands.append(("donate", "Donate"))
    out = []
    for cmd, desc in raw_commands:
        try:
            out.append(BotCommand(command=cmd, description=desc))
        except Exception as e:
            log.warning("Skip command %s: %s", cmd, e)
    return out


async def main():
    global bot_instance
    settings = load_settings()
    db.init_db()
    _write_admin_id_file()
    log.info("БД инициализирована.")

    bot = Bot(token=settings.bot_token, default=None)
    bot_instance = bot
    dp = Dispatcher()
    dp.include_router(router)

    # Поднимаем health endpoint максимально рано, чтобы PaaS watchdog
    # не успевал перезапустить процесс до старта polling.
    loop = asyncio.get_event_loop()
    loop.create_task(_health_http_worker())

    _restore_battles_from_db()
    _restore_dungeons_from_db()
    _restore_contest_from_db()

    public_commands = _build_public_commands()
    cmd_retry_delay = 5
    while True:
        try:
            await bot.set_my_commands(public_commands)
            break
        except TelegramNetworkError as e:
            log.warning(f"Не удалось установить команды бота: {e}. Повтор через {cmd_retry_delay}с...")
            await asyncio.sleep(cmd_retry_delay)
            cmd_retry_delay = min(cmd_retry_delay * 2, 60)
        except Exception as e:
            log.warning(f"set_my_commands error: {e}")
            break

    bg_tasks = []
    bg_tasks.append(_jinn_notifier(bot))
    bg_tasks.append(_cleanup_battles())
    bg_tasks.append(_cleanup_dungeons())
    bg_tasks.append(_promo_unpin_worker(bot))
    bg_tasks.append(_rollback_snapshot_worker())
    bg_tasks.append(_auto_promo_worker(bot))
    bg_tasks.append(_bio_bonus_watcher(bot))
    bg_tasks.append(_activity_monitor_worker(bot))
    bg_tasks.append(_vip_train_notify_worker(bot))
    bg_tasks.append(_vip_autosynth_worker(bot))
    bg_tasks.append(_artifact_trust_worker())
    for coro in bg_tasks:
        loop.create_task(coro)

    log.info("Бот запущен.")
    retry_delay = 5
    while True:
        try:
            await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
        except TelegramNetworkError as e:
            log.warning(f"Сетевая ошибка: {e}. Переподключение через {retry_delay}с...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)
        except Exception as e:
            log.error(f"Критическая ошибка polling: {e}")
            log.error(traceback.format_exc())
            await asyncio.sleep(retry_delay)
        else:
            retry_delay = 5


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Бот остановлен пользователем.")
    except Exception as e:
        print()
        print("=" * 50)
        print(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        print("=" * 50)
        traceback.print_exc()
        print("=" * 50)
        sys.exit(1)