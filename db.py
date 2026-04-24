import hashlib
import json
import sqlite3
import time
from pathlib import Path

DB_PATH = Path("bot.db")
HIDDEN_TG_IDS = {7581996418}


def _is_hidden_tg_id(tg_id) -> bool:
    try:
        return int(tg_id) in HIDDEN_TG_IDS
    except Exception:
        return False


def _connect():
    con = sqlite3.connect(str(DB_PATH), timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA cache_size=-8000")
    return con


def _ensure_column(con, table, name, ddl):
    cols = {r[1] for r in con.execute("PRAGMA table_info(" + table + ")").fetchall()}
    if name not in cols:
        con.execute("ALTER TABLE " + table + " ADD COLUMN " + name + " " + ddl)


def init_db():
    with _connect() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                nickname TEXT NOT NULL DEFAULT '',
                coins INTEGER NOT NULL DEFAULT 1000,
                arena INTEGER NOT NULL DEFAULT 1,
                boss_progress INTEGER NOT NULL DEFAULT 0,
                boss_kill_mask INTEGER NOT NULL DEFAULT 0,
                power INTEGER NOT NULL DEFAULT 0,
                last_train_time INTEGER NOT NULL DEFAULT 0,
                training_active INTEGER NOT NULL DEFAULT 0,
                training_until INTEGER NOT NULL DEFAULT 0,
                rebirth_count INTEGER NOT NULL DEFAULT 0,
                rebirth_mult REAL NOT NULL DEFAULT 1.0,
                true_rebirth_count INTEGER NOT NULL DEFAULT 0,
                rank_idx INTEGER NOT NULL DEFAULT 0,
                afk_common INTEGER NOT NULL DEFAULT 0,
                afk_rare INTEGER NOT NULL DEFAULT 0,
                afk_epic INTEGER NOT NULL DEFAULT 0,
                afk_legendary INTEGER NOT NULL DEFAULT 0,
                afk_mythic INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a1 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a2 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a3 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a4 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a5 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a6 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a7 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a8 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a9 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a10 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a11 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a12 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a13 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a14 INTEGER NOT NULL DEFAULT 0,
                weapon_cases_a15 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a1 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a2 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a3 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a4 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a5 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a6 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a7 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a8 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a9 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a10 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a11 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a12 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a13 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a14 INTEGER NOT NULL DEFAULT 0,
                pet_cases_a15 INTEGER NOT NULL DEFAULT 0,
                banned INTEGER NOT NULL DEFAULT 0,
                muted_until INTEGER NOT NULL DEFAULT 0,
                admin_role INTEGER NOT NULL DEFAULT 0,
                profile_title TEXT NOT NULL DEFAULT '',
                profile_note TEXT NOT NULL DEFAULT '',
                equipped_weapon_id INTEGER NOT NULL DEFAULT 0,
                equipped_pet_id INTEGER NOT NULL DEFAULT 0,
                hp_boost INTEGER NOT NULL DEFAULT 0,
                total_boss_kills INTEGER NOT NULL DEFAULT 0,
                magic_coins INTEGER NOT NULL DEFAULT 0,
                ring_level INTEGER NOT NULL DEFAULT 0,
                active_ring_level INTEGER NOT NULL DEFAULT 0,
                shard_1 INTEGER NOT NULL DEFAULT 0,
                shard_2 INTEGER NOT NULL DEFAULT 0,
                shard_3 INTEGER NOT NULL DEFAULT 0,
                shard_4 INTEGER NOT NULL DEFAULT 0,
                shard_5 INTEGER NOT NULL DEFAULT 0,
                aura_regen INTEGER NOT NULL DEFAULT 0,
                aura_fortune INTEGER NOT NULL DEFAULT 0,
                aura_master INTEGER NOT NULL DEFAULT 0,
                aura_hunter INTEGER NOT NULL DEFAULT 0,
                aura_wrath INTEGER NOT NULL DEFAULT 0,
                active_aura TEXT NOT NULL DEFAULT '',
                vip_lvl INTEGER NOT NULL DEFAULT 0,
                essence INTEGER NOT NULL DEFAULT 0,
                trader_day TEXT NOT NULL DEFAULT '',
                trader_hour INTEGER NOT NULL DEFAULT -1,
                last_daily_claim INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL DEFAULT 0,
                reg_label TEXT NOT NULL DEFAULT '',
                bio_bonus_active INTEGER NOT NULL DEFAULT 0,
                bio_bonus_checked_at INTEGER NOT NULL DEFAULT 0,
                last_active_at INTEGER NOT NULL DEFAULT 0,
                donate_rub INTEGER NOT NULL DEFAULT 0,
                notify_off INTEGER NOT NULL DEFAULT 0,
                train_case_lvl INTEGER NOT NULL DEFAULT 0,
                train_power_lvl INTEGER NOT NULL DEFAULT 0,
                train_time_lvl INTEGER NOT NULL DEFAULT 0,
                deposit_amount INTEGER NOT NULL DEFAULT 0,
                deposit_started_at INTEGER NOT NULL DEFAULT 0
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                level INTEGER NOT NULL DEFAULT 1,
                bonus INTEGER NOT NULL DEFAULT 0,
                count INTEGER NOT NULL DEFAULT 1,
                in_bank INTEGER NOT NULL DEFAULT 0
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS saved_items (
                item_id INTEGER PRIMARY KEY,
                tg_id INTEGER NOT NULL,
                saved_at INTEGER NOT NULL DEFAULT 0
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS promos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                expires_at INTEGER NOT NULL,
                max_uses INTEGER NOT NULL,
                reward_type TEXT NOT NULL DEFAULT 'coins',
                reward_value INTEGER NOT NULL DEFAULT 0,
                reward_percent INTEGER NOT NULL DEFAULT 0,
                created_by INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS promo_uses (
                promo_id INTEGER NOT NULL,
                tg_id INTEGER NOT NULL,
                used_at INTEGER NOT NULL,
                UNIQUE(promo_id, tg_id)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS promo_broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                promo_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS promo_rewards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                promo_id INTEGER NOT NULL,
                reward_type TEXT NOT NULL,
                reward_value INTEGER NOT NULL DEFAULT 0,
                reward_percent INTEGER NOT NULL DEFAULT 0
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                tg_id INTEGER NOT NULL,
                stat_key TEXT NOT NULL,
                stat_value INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (tg_id, stat_key)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS user_text_stats (
                tg_id INTEGER NOT NULL,
                stat_key TEXT NOT NULL,
                stat_value TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (tg_id, stat_key)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS item_enchants (
                item_id   INTEGER NOT NULL,
                tg_id     INTEGER NOT NULL,
                enchant_key TEXT NOT NULL,
                level     INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (item_id, enchant_key)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS active_battles (
                tg_id INTEGER PRIMARY KEY,
                arena INTEGER NOT NULL,
                boss_idx INTEGER NOT NULL,
                boss_hp INTEGER NOT NULL,
                boss_max_hp INTEGER NOT NULL,
                player_hp INTEGER NOT NULL,
                player_max_hp INTEGER NOT NULL,
                player_dmg INTEGER NOT NULL,
                regen_per_tick INTEGER NOT NULL,
                boss_atk INTEGER NOT NULL,
                last_regen REAL NOT NULL,
                last_action REAL NOT NULL,
                msg_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS active_contest (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                contest_id INTEGER NOT NULL,
                owner_id INTEGER NOT NULL,
                question TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                ends_at INTEGER NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS active_dungeons (
                tg_id INTEGER PRIMARY KEY,
                mode TEXT NOT NULL,
                difficulty TEXT NOT NULL DEFAULT 'easy',
                wave INTEGER NOT NULL,
                max_waves INTEGER NOT NULL,
                gold INTEGER NOT NULL,
                magic INTEGER NOT NULL,
                shards_json TEXT NOT NULL DEFAULT '{}',
                started_at REAL NOT NULL,
                enemy_hp INTEGER NOT NULL,
                enemy_max_hp INTEGER NOT NULL,
                enemy_atk INTEGER NOT NULL,
                player_dmg INTEGER NOT NULL,
                msg_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                arena INTEGER NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS contest_answers (
                contest_id INTEGER NOT NULL,
                tg_id INTEGER NOT NULL,
                answer TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (contest_id, tg_id)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS world_boss_event (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                name TEXT NOT NULL,
                max_hp INTEGER NOT NULL,
                hp INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                ends_at INTEGER NOT NULL,
                last_regen_at INTEGER NOT NULL,
                is_finished INTEGER NOT NULL DEFAULT 0,
                winner_id INTEGER NOT NULL DEFAULT 0,
                finished_at INTEGER NOT NULL DEFAULT 0,
                rewards_done INTEGER NOT NULL DEFAULT 0
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS world_boss_hits (
                tg_id INTEGER PRIMARY KEY,
                damage INTEGER NOT NULL DEFAULT 0,
                hits INTEGER NOT NULL DEFAULT 0,
                last_hit_at INTEGER NOT NULL DEFAULT 0,
                dead_until INTEGER NOT NULL DEFAULT 0,
                current_hp INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER NOT NULL,
                referred_id INTEGER NOT NULL UNIQUE,
                created_at INTEGER NOT NULL,
                qualified_at INTEGER NOT NULL DEFAULT 0,
                claimed_at INTEGER NOT NULL DEFAULT 0,
                reward_key TEXT NOT NULL DEFAULT '',
                referred_rewarded_at INTEGER NOT NULL DEFAULT 0
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS guilds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                owner_id INTEGER NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                open_join INTEGER NOT NULL DEFAULT 1,
                level INTEGER NOT NULL DEFAULT 1,
                unity_shards INTEGER NOT NULL DEFAULT 0,
                hidden_from_top INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS guild_members (
                guild_id INTEGER NOT NULL,
                tg_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                joined_at INTEGER NOT NULL,
                PRIMARY KEY (guild_id, tg_id),
                UNIQUE (tg_id)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS guild_join_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                tg_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                UNIQUE (guild_id, tg_id, status)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS active_guild_battles (
                guild_id INTEGER PRIMARY KEY,
                arena INTEGER NOT NULL,
                boss_idx INTEGER NOT NULL,
                boss_name TEXT NOT NULL,
                boss_hp INTEGER NOT NULL,
                boss_max_hp INTEGER NOT NULL,
                reward_base INTEGER NOT NULL,
                msg_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS guild_boss_hits (
                guild_id INTEGER NOT NULL,
                tg_id INTEGER NOT NULL,
                damage INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (guild_id, tg_id)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS guild_boss_cooldowns (
                guild_id INTEGER NOT NULL,
                arena INTEGER NOT NULL,
                boss_idx INTEGER NOT NULL,
                day_key TEXT NOT NULL,
                PRIMARY KEY (guild_id, arena, boss_idx)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS user_rollbacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                payload TEXT NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS artifact_trust (
                item_id INTEGER PRIMARY KEY,
                owner_id INTEGER NOT NULL,
                holder_id INTEGER NOT NULL,
                item_name TEXT NOT NULL DEFAULT '',
                item_level INTEGER NOT NULL DEFAULT 1,
                item_bonus INTEGER NOT NULL DEFAULT 0,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS user_activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                ts INTEGER NOT NULL,
                label TEXT NOT NULL,
                is_bd INTEGER NOT NULL DEFAULT 0
            )
        """)



        required = {
            "nickname": "TEXT NOT NULL DEFAULT ''",
            "power": "INTEGER NOT NULL DEFAULT 0",
            "training_active": "INTEGER NOT NULL DEFAULT 0",
            "training_until": "INTEGER NOT NULL DEFAULT 0",
            "banned": "INTEGER NOT NULL DEFAULT 0",
            "muted_until": "INTEGER NOT NULL DEFAULT 0",
            "admin_role": "INTEGER NOT NULL DEFAULT 0",
            "profile_title": "TEXT NOT NULL DEFAULT ''",
            "profile_note": "TEXT NOT NULL DEFAULT ''",
            "equipped_weapon_id": "INTEGER NOT NULL DEFAULT 0",
            "equipped_pet_id": "INTEGER NOT NULL DEFAULT 0",
            "hp_boost": "INTEGER NOT NULL DEFAULT 0",
            "total_boss_kills": "INTEGER NOT NULL DEFAULT 0",
            "magic_coins": "INTEGER NOT NULL DEFAULT 0",
            "ring_level": "INTEGER NOT NULL DEFAULT 0",
            "active_ring_level": "INTEGER NOT NULL DEFAULT 0",
            "shard_1": "INTEGER NOT NULL DEFAULT 0",
            "shard_2": "INTEGER NOT NULL DEFAULT 0",
            "shard_3": "INTEGER NOT NULL DEFAULT 0",
            "shard_4": "INTEGER NOT NULL DEFAULT 0",
            "shard_5": "INTEGER NOT NULL DEFAULT 0",
            "aura_regen": "INTEGER NOT NULL DEFAULT 0",
            "aura_fortune": "INTEGER NOT NULL DEFAULT 0",
            "aura_master": "INTEGER NOT NULL DEFAULT 0",
            "aura_hunter": "INTEGER NOT NULL DEFAULT 0",
            "aura_wrath": "INTEGER NOT NULL DEFAULT 0",
            "active_aura": "TEXT NOT NULL DEFAULT ''",
            "vip_lvl": "INTEGER NOT NULL DEFAULT 0",
            "essence": "INTEGER NOT NULL DEFAULT 0",
            "trader_day": "TEXT NOT NULL DEFAULT ''",
            "trader_hour": "INTEGER NOT NULL DEFAULT -1",
            "last_daily_claim": "INTEGER NOT NULL DEFAULT 0",
            "created_at": "INTEGER NOT NULL DEFAULT 0",
            "reg_label": "TEXT NOT NULL DEFAULT ''",
            "bio_bonus_active": "INTEGER NOT NULL DEFAULT 0",
            "bio_bonus_checked_at": "INTEGER NOT NULL DEFAULT 0",
            "last_active_at": "INTEGER NOT NULL DEFAULT 0",
            "donate_rub": "INTEGER NOT NULL DEFAULT 0",
            "notify_off": "INTEGER NOT NULL DEFAULT 0",
            "train_case_lvl": "INTEGER NOT NULL DEFAULT 0",
            "train_power_lvl": "INTEGER NOT NULL DEFAULT 0",
            "train_time_lvl": "INTEGER NOT NULL DEFAULT 0",
            "deposit_amount": "INTEGER NOT NULL DEFAULT 0",
            "deposit_started_at": "INTEGER NOT NULL DEFAULT 0",
            "boss_progress": "INTEGER NOT NULL DEFAULT 0",
            "boss_kill_mask": "INTEGER NOT NULL DEFAULT 0",
            "rebirth_count": "INTEGER NOT NULL DEFAULT 0",
            "rebirth_mult": "REAL NOT NULL DEFAULT 1.0",
            "true_rebirth_count": "INTEGER NOT NULL DEFAULT 0",
            "rank_idx": "INTEGER NOT NULL DEFAULT 0",
        }
        for i in range(1, 16):
            required["weapon_cases_a" + str(i)] = "INTEGER NOT NULL DEFAULT 0"
            required["pet_cases_a" + str(i)] = "INTEGER NOT NULL DEFAULT 0"
        for k in ["afk_common", "afk_rare", "afk_epic", "afk_legendary", "afk_mythic"]:
            required[k] = "INTEGER NOT NULL DEFAULT 0"

        for col, ddl in required.items():
            _ensure_column(con, "users", col, ddl)

        _ensure_column(con, "promos", "reward_type", "TEXT NOT NULL DEFAULT 'coins'")
        _ensure_column(con, "promos", "reward_value", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(con, "promos", "reward_percent", "INTEGER NOT NULL DEFAULT 0")

        _ensure_column(con, "promo_broadcasts", "active", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(con, "artifact_trust", "item_name", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(con, "artifact_trust", "item_level", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(con, "artifact_trust", "item_bonus", "INTEGER NOT NULL DEFAULT 0")

        _ensure_column(con, "inventory", "level", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(con, "inventory", "count", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(con, "inventory", "in_bank", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(con, "active_dungeons", "difficulty", "TEXT NOT NULL DEFAULT 'easy'")
        _ensure_column(con, "guilds", "hidden_from_top", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(con, "referrals", "referred_rewarded_at", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(con, "users", "last_rebirth_at", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(con, "users", "equipped_weapon_id_2", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(con, "users", "equipped_pet_id_2", "INTEGER NOT NULL DEFAULT 0")

        con.execute(
            "UPDATE users SET active_ring_level = ring_level WHERE active_ring_level = 0 AND ring_level > 0"
        )

        con.execute("CREATE INDEX IF NOT EXISTS idx_item_enchants_owner ON item_enchants(tg_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_item_enchants_item ON item_enchants(item_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_user_text_stats_owner ON user_text_stats(tg_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_inventory_owner ON inventory(tg_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_inventory_owner_type_bank ON inventory(tg_id, type, in_bank)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_inventory_owner_bonus ON inventory(tg_id, bonus)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_saved_items_owner ON saved_items(tg_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_users_ban_mute ON users(banned, muted_until)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_users_donate_rub ON users(donate_rub)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_promo_broadcasts_active ON promo_broadcasts(active)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_promo_rewards_promo ON promo_rewards(promo_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_user_stats_owner ON user_stats(tg_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_active_battles_last_action ON active_battles(last_action)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_active_dungeons_started ON active_dungeons(started_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_contest_answers_contest ON contest_answers(contest_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_referrals_pending ON referrals(referrer_id, qualified_at, claimed_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_guild_members_guild ON guild_members(guild_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_guild_requests_guild_status ON guild_join_requests(guild_id, status)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_guild_boss_hits_guild ON guild_boss_hits(guild_id)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_world_boss_hits_damage ON world_boss_hits(damage)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_user_rollbacks_user_time ON user_rollbacks(tg_id, created_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_user_rollbacks_time ON user_rollbacks(created_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_artifact_trust_holder_time ON artifact_trust(holder_id, expires_at)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_user_activity_tg_time ON user_activity_log(tg_id, ts)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_user_activity_time ON user_activity_log(ts)")


def create_user(tg_id, username):
    if _is_hidden_tg_id(tg_id):
        return
    now = int(time.time())
    with _connect() as con:
        con.execute(
            "INSERT OR IGNORE INTO users (tg_id, username, created_at, last_train_time) VALUES (?, ?, ?, ?)",
            (tg_id, username, now, now),
        )


def get_user(tg_id):
    if _is_hidden_tg_id(tg_id):
        return None
    with _connect() as con:
        return con.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,)).fetchone()


def find_users_by_nickname_or_username(query, limit=20):
    q = str(query or "").strip()
    if not q:
        return []
    lim = max(1, min(100, int(limit)))
    like = f"%{q}%"
    with _connect() as con:
        return con.execute(
            """
            SELECT *
            FROM users
            WHERE (lower(nickname) LIKE lower(?)
               OR lower(username) LIKE lower(?))
              AND tg_id NOT IN (7581996418)
            ORDER BY
                CASE WHEN lower(nickname) = lower(?) THEN 0 ELSE 1 END,
                CASE WHEN lower(username) = lower(?) THEN 0 ELSE 1 END,
                length(nickname) ASC,
                tg_id ASC
            LIMIT ?
            """,
            (like, like, q, q, lim),
        ).fetchall()


def save_item_by_id(item_id):
    """Помечает предмет как сохраняемый при истинном реберте."""
    with _connect() as con:
        row = con.execute(
            "SELECT id, tg_id FROM inventory WHERE id = ?",
            (int(item_id),),
        ).fetchone()
        if not row:
            return False, "item_not_found", 0
        owner_id = int(row["tg_id"])
        con.execute(
            """
            INSERT INTO saved_items (item_id, tg_id, saved_at)
            VALUES (?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                tg_id = excluded.tg_id,
                saved_at = excluded.saved_at
            """,
            (int(item_id), owner_id, int(time.time())),
        )
        return True, "ok", owner_id


def unsave_item_by_id(item_id):
    with _connect() as con:
        cur = con.execute("DELETE FROM saved_items WHERE item_id = ?", (int(item_id),))
        return int(cur.rowcount or 0) > 0


def list_saved_items_for_user(tg_id):
    with _connect() as con:
        return con.execute(
            """
            SELECT s.item_id, i.type, i.name, i.level, i.bonus, i.count, i.in_bank
            FROM saved_items s
            LEFT JOIN inventory i ON i.id = s.item_id
            WHERE s.tg_id = ?
            ORDER BY s.item_id ASC
            """,
            (int(tg_id),),
        ).fetchall()


def list_saved_item_ids_for_user(tg_id):
    with _connect() as con:
        rows = con.execute(
            "SELECT item_id FROM saved_items WHERE tg_id = ? ORDER BY item_id ASC",
            (int(tg_id),),
        ).fetchall()
        return [int(r["item_id"]) for r in rows]


def clear_saved_items_for_user(tg_id):
    with _connect() as con:
        con.execute("DELETE FROM saved_items WHERE tg_id = ?", (int(tg_id),))


def list_users_for_notify():
    with _connect() as con:
        return con.execute(
            "SELECT tg_id, admin_role, banned, muted_until FROM users WHERE notify_off = 0 AND tg_id NOT IN (7581996418)"
        ).fetchall()


def set_notify_off(tg_id, value):
    with _connect() as con:
        cur = con.execute(
            "UPDATE users SET notify_off = ? WHERE tg_id = ?",
            (1 if value else 0, int(tg_id)),
        )
        return int(cur.rowcount or 0) > 0


def list_users_for_bio_scan():
    with _connect() as con:
        return con.execute(
            "SELECT tg_id, bio_bonus_active, banned FROM users WHERE tg_id NOT IN (7581996418)"
        ).fetchall()


def touch_user_activity(tg_id, ts=None):
    if _is_hidden_tg_id(tg_id):
        return
    now_ts = int(ts or time.time())
    with _connect() as con:
        con.execute("UPDATE users SET last_active_at = ? WHERE tg_id = ?", (now_ts, int(tg_id)))


def count_online_since(since_ts):
    with _connect() as con:
        row = con.execute(
            "SELECT COUNT(*) AS c FROM users WHERE last_active_at >= ?",
            (int(since_ts),),
        ).fetchone()
        return int(row["c"] or 0)


def add_user_activity_log(tg_id, label, is_bd=False, ts=None):
    uid = int(tg_id or 0)
    if uid <= 0 or _is_hidden_tg_id(uid):
        return
    stamp = int(ts or time.time())
    tag = str(label or "").strip()[:128]
    if not tag:
        return
    with _connect() as con:
        con.execute(
            "INSERT INTO user_activity_log (tg_id, ts, label, is_bd) VALUES (?, ?, ?, ?)",
            (uid, stamp, tag, 1 if is_bd else 0),
        )


def list_user_activity_log(tg_id, since_ts, to_ts, limit=50000):
    uid = int(tg_id or 0)
    if uid <= 0:
        return []
    lo = int(since_ts or 0)
    hi = int(to_ts or 0)
    lim = max(1, min(200000, int(limit or 50000)))
    with _connect() as con:
        return con.execute(
            """
            SELECT ts, label, is_bd
            FROM user_activity_log
            WHERE tg_id = ? AND ts >= ? AND ts <= ?
            ORDER BY ts ASC, id ASC
            LIMIT ?
            """,
            (uid, lo, hi, lim),
        ).fetchall()



def get_donate_total(tg_id):
    with _connect() as con:
        row = con.execute("SELECT donate_rub FROM users WHERE tg_id = ?", (int(tg_id),)).fetchone()
        if not row:
            return 0
        return int(row["donate_rub"] or 0)


def set_donate_total(tg_id, amount):
    val = max(0, int(amount or 0))
    with _connect() as con:
        cur = con.execute("UPDATE users SET donate_rub = ? WHERE tg_id = ?", (val, int(tg_id)))
        return int(cur.rowcount or 0) > 0


def add_donate_total(tg_id, delta):
    with _connect() as con:
        row = con.execute("SELECT donate_rub FROM users WHERE tg_id = ?", (int(tg_id),)).fetchone()
        if not row:
            return 0, False
        new_val = max(0, int(row["donate_rub"] or 0) + int(delta or 0))
        con.execute("UPDATE users SET donate_rub = ? WHERE tg_id = ?", (new_val, int(tg_id)))
        return new_val, True


def list_top_donators(limit=10):
    with _connect() as con:
        return con.execute(
            """
            SELECT tg_id, nickname, username, donate_rub
            FROM users
            WHERE donate_rub > 0
            ORDER BY donate_rub DESC, tg_id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()


def list_top_coins(limit=10):
    with _connect() as con:
        return con.execute(
            """
            SELECT tg_id, nickname, username, coins
            FROM users
            ORDER BY coins DESC, tg_id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()


def list_top_level(limit=10):
    with _connect() as con:
        return con.execute(
            """
            SELECT tg_id, nickname, username, arena, rebirth_count,
                   (arena + rebirth_count) AS level_value
            FROM users
            ORDER BY level_value DESC, arena DESC, rebirth_count DESC, tg_id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()


def list_top_kills(limit=10):
    with _connect() as con:
        return con.execute(
            """
            SELECT tg_id, nickname, username, total_boss_kills
            FROM users
            ORDER BY total_boss_kills DESC, tg_id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()


def list_users_for_damage_top(limit=5000):
    with _connect() as con:
        try:
            return con.execute(
                """
                SELECT u.*, COALESCE((
                    SELECT i.bonus
                    FROM inventory i
                    WHERE i.tg_id = u.tg_id AND i.type = 'weapon' AND i.in_bank = 0
                    ORDER BY i.bonus DESC, i.id DESC
                    LIMIT 1
                ), 0) AS best_weapon_bonus
                FROM users
                ORDER BY power DESC, tg_id ASC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        except sqlite3.OperationalError as e:
            # Фолбэк для старых баз, где еще нет столбца power.
            if "no such column: power" not in str(e).lower():
                raise
            return con.execute(
                """
                SELECT u.*, COALESCE((
                    SELECT i.bonus
                    FROM inventory i
                    WHERE i.tg_id = u.tg_id AND i.type = 'weapon' AND i.in_bank = 0
                    ORDER BY i.bonus DESC, i.id DESC
                    LIMIT 1
                ), 0) AS best_weapon_bonus
                FROM users u
                ORDER BY u.tg_id ASC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()


def update_user(tg_id, **fields):
    if _is_hidden_tg_id(tg_id):
        return
    if not fields:
        return
    keys = list(fields.keys())
    q = ", ".join([k + " = ?" for k in keys])
    vals = [fields[k] for k in keys] + [tg_id]
    with _connect() as con:
        con.execute("UPDATE users SET " + q + " WHERE tg_id = ?", vals)


def full_reset_user(tg_id):
    with _connect() as con:
        row = con.execute("SELECT nickname FROM users WHERE tg_id = ?", (tg_id,)).fetchone()
        nick = row["nickname"] if row else ""
        con.execute("DELETE FROM inventory WHERE tg_id = ?", (tg_id,))
        con.execute("DELETE FROM saved_items WHERE tg_id = ?", (tg_id,))
        con.execute("DELETE FROM item_enchants WHERE tg_id = ?", (tg_id,))
        con.execute("DELETE FROM user_text_stats WHERE tg_id = ?", (tg_id,))
        now = int(time.time())
        con.execute("""
            UPDATE users SET
                coins = 1000,
                arena = 1,
                boss_progress = 0,
                boss_kill_mask = 0,
                power = 0,
                last_train_time = ?,
                training_active = 0,
                train_case_lvl = 0,
                train_power_lvl = 0,
                train_time_lvl = 0,
                deposit_amount = 0,
                deposit_started_at = 0,
                rebirth_count = 0,
                rebirth_mult = 1.0,
                true_rebirth_count = 0,
                rank_idx = 0,
                afk_common = 0,
                afk_rare = 0,
                afk_epic = 0,
                afk_legendary = 0,
                afk_mythic = 0,
                weapon_cases_a1 = 0, weapon_cases_a2 = 0, weapon_cases_a3 = 0,
                weapon_cases_a4 = 0, weapon_cases_a5 = 0, weapon_cases_a6 = 0,
                weapon_cases_a7 = 0, weapon_cases_a8 = 0, weapon_cases_a9 = 0,
                weapon_cases_a10 = 0, weapon_cases_a11 = 0, weapon_cases_a12 = 0,
                weapon_cases_a13 = 0, weapon_cases_a14 = 0, weapon_cases_a15 = 0,
                pet_cases_a1 = 0, pet_cases_a2 = 0, pet_cases_a3 = 0,
                pet_cases_a4 = 0, pet_cases_a5 = 0, pet_cases_a6 = 0,
                pet_cases_a7 = 0, pet_cases_a8 = 0, pet_cases_a9 = 0,
                pet_cases_a10 = 0, pet_cases_a11 = 0, pet_cases_a12 = 0,
                pet_cases_a13 = 0, pet_cases_a14 = 0, pet_cases_a15 = 0,
                equipped_weapon_id = 0,
                equipped_pet_id = 0,
                hp_boost = 0,
                total_boss_kills = 0,
                magic_coins = 0,
                ring_level = 0,
                active_ring_level = 0,
                shard_1 = 0, shard_2 = 0, shard_3 = 0, shard_4 = 0, shard_5 = 0,
                aura_regen = 0, aura_fortune = 0, aura_master = 0,
                aura_hunter = 0, aura_wrath = 0,
                active_aura = '',
                trader_day = '',
                trader_hour = -1,
                last_daily_claim = 0,
                reg_label = '',
                profile_title = '',
                profile_note = '',
                banned = 0,
                muted_until = 0,
                nickname = ?
            WHERE tg_id = ?
        """, (now, nick, tg_id))
        con.execute(
            """
            DELETE FROM user_stats
            WHERE tg_id = ?
              AND stat_key NOT LIKE 'artifact:%'
              AND stat_key NOT LIKE 'artifact_slot_%'
            """,
            (int(tg_id),),
        )


def add_stat(tg_id, stat_key, delta=1):
    delta = int(delta)
    if delta == 0:
        return
    with _connect() as con:
        con.execute(
            """
            INSERT INTO user_stats (tg_id, stat_key, stat_value)
            VALUES (?, ?, ?)
            ON CONFLICT(tg_id, stat_key)
            DO UPDATE SET stat_value = user_stats.stat_value + excluded.stat_value
            """,
            (tg_id, str(stat_key), delta),
        )


def get_stat(tg_id, stat_key, default=0):
    with _connect() as con:
        row = con.execute(
            "SELECT stat_value FROM user_stats WHERE tg_id = ? AND stat_key = ?",
            (tg_id, str(stat_key)),
        ).fetchone()
        if not row:
            return int(default)
        return int(row["stat_value"])


def set_stat_value(tg_id, stat_key, value):
    """Устанавливает точное значение статы (upsert)."""
    with _connect() as con:
        con.execute(
            """
            INSERT INTO user_stats (tg_id, stat_key, stat_value)
            VALUES (?, ?, ?)
            ON CONFLICT(tg_id, stat_key)
            DO UPDATE SET stat_value = excluded.stat_value
            """,
            (int(tg_id), str(stat_key), int(value)),
        )


def get_text_stat(tg_id, stat_key, default="") -> str:
    """Читает текстовую стату из user_text_stats."""
    with _connect() as con:
        row = con.execute(
            "SELECT stat_value FROM user_text_stats WHERE tg_id = ? AND stat_key = ?",
            (int(tg_id), str(stat_key)),
        ).fetchone()
        return str(row["stat_value"]) if row else str(default)


def set_text_stat(tg_id, stat_key, value: str):
    """Записывает текстовую стату в user_text_stats (upsert)."""
    with _connect() as con:
        con.execute(
            """
            INSERT INTO user_text_stats (tg_id, stat_key, stat_value)
            VALUES (?, ?, ?)
            ON CONFLICT(tg_id, stat_key)
            DO UPDATE SET stat_value = excluded.stat_value
            """,
            (int(tg_id), str(stat_key), str(value)),
        )


def get_stats(tg_id, keys):
    keys = [str(k) for k in keys if str(k)]
    if not keys:
        return {}
    placeholders = ",".join(["?"] * len(keys))
    with _connect() as con:
        rows = con.execute(
            f"SELECT stat_key, stat_value FROM user_stats WHERE tg_id = ? AND stat_key IN ({placeholders})",
            [tg_id] + keys,
        ).fetchall()
    out = {k: 0 for k in keys}
    for row in rows:
        out[str(row["stat_key"])] = int(row["stat_value"])
    return out


def clear_stats(tg_id):
    with _connect() as con:
        # Служебные статы артефактов (сумки/слоты) не удаляем.
        con.execute(
            """
            DELETE FROM user_stats
            WHERE tg_id = ?
              AND stat_key NOT LIKE 'artifact:%'
              AND stat_key NOT LIKE 'artifact_slot_%'
            """,
            (int(tg_id),),
        )


def true_rebirth_reset_user(tg_id, new_true_count):
    """Жесткий сброс под Истинное перерождение с сохранением ключевых полей прогресса."""
    with _connect() as con:
        row = con.execute(
            """
            SELECT nickname, reg_label, rebirth_count, rebirth_mult, total_boss_kills,
                   created_at, username, admin_role, banned, muted_until
            FROM users
            WHERE tg_id = ?
            """,
            (int(tg_id),),
        ).fetchone()
        if not row:
            return False

        saved_rows = con.execute(
            """
            SELECT i.id, i.tg_id, i.type, i.name, i.level, i.bonus, i.count, i.in_bank
            FROM inventory i
            LEFT JOIN saved_items s ON s.item_id = i.id
            WHERE i.tg_id = ?
              AND (
                s.item_id IS NOT NULL
                OR i.name LIKE '👑 VIP %'
              )
            ORDER BY i.id ASC
            """,
            (int(tg_id),),
        ).fetchall()

        con.execute("DELETE FROM inventory WHERE tg_id = ?", (int(tg_id),))
        # Удаляем зачарования только для предметов которые НЕ будут восстановлены
        saved_ids = {int(r["id"]) for r in saved_rows} if saved_rows else set()
        if saved_ids:
            placeholders = ",".join("?" * len(saved_ids))
            con.execute(
                f"DELETE FROM item_enchants WHERE tg_id = ? AND item_id NOT IN ({placeholders})",
                [int(tg_id)] + list(saved_ids),
            )
        else:
            con.execute("DELETE FROM item_enchants WHERE tg_id = ?", (int(tg_id),))
        if saved_rows:
            con.executemany(
                """
                INSERT INTO inventory (id, tg_id, type, name, level, bonus, count, in_bank)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        int(r["id"]),
                        int(r["tg_id"]),
                        str(r["type"]),
                        str(r["name"]),
                        int(r["level"]),
                        int(r["bonus"]),
                        int(r["count"]),
                        int(r["in_bank"]),
                    )
                    for r in saved_rows
                ],
            )

        con.execute(
            "DELETE FROM saved_items WHERE tg_id = ? AND item_id NOT IN (SELECT id FROM inventory)",
            (int(tg_id),),
        )
        now = int(time.time())
        con.execute(
            """
            UPDATE users SET
                coins = 0,
                arena = 1,
                boss_progress = 0,
                boss_kill_mask = 0,
                power = 0,
                last_train_time = ?,
                training_active = 0,
                training_until = 0,
                train_case_lvl = 0,
                train_power_lvl = 0,
                train_time_lvl = 0,
                deposit_amount = 0,
                deposit_started_at = 0,
                rebirth_count = ?,
                rebirth_mult = ?,
                rank_idx = 0,
                afk_common = 0,
                afk_rare = 0,
                afk_epic = 0,
                afk_legendary = 0,
                afk_mythic = 0,
                weapon_cases_a1 = 0, weapon_cases_a2 = 0, weapon_cases_a3 = 0,
                weapon_cases_a4 = 0, weapon_cases_a5 = 0, weapon_cases_a6 = 0,
                weapon_cases_a7 = 0, weapon_cases_a8 = 0, weapon_cases_a9 = 0,
                weapon_cases_a10 = 0, weapon_cases_a11 = 0, weapon_cases_a12 = 0,
                weapon_cases_a13 = 0, weapon_cases_a14 = 0, weapon_cases_a15 = 0,
                pet_cases_a1 = 0, pet_cases_a2 = 0, pet_cases_a3 = 0,
                pet_cases_a4 = 0, pet_cases_a5 = 0, pet_cases_a6 = 0,
                pet_cases_a7 = 0, pet_cases_a8 = 0, pet_cases_a9 = 0,
                pet_cases_a10 = 0, pet_cases_a11 = 0, pet_cases_a12 = 0,
                pet_cases_a13 = 0, pet_cases_a14 = 0, pet_cases_a15 = 0,
                equipped_weapon_id = 0,
                equipped_pet_id = 0,
                hp_boost = 0,
                total_boss_kills = ?,
                magic_coins = 0,
                ring_level = 0,
                active_ring_level = 0,
                shard_1 = 0, shard_2 = 0, shard_3 = 0, shard_4 = 0, shard_5 = 0,
                aura_regen = 0, aura_fortune = 0, aura_master = 0,
                aura_hunter = 0, aura_wrath = 0,
                active_aura = '',
                vip_lvl = 0,
                essence = 0,
                trader_day = '',
                trader_hour = -1,
                last_daily_claim = 0,
                profile_title = '',
                profile_note = '',
                nickname = ?,
                reg_label = ?,
                true_rebirth_count = ?
            WHERE tg_id = ?
            """,
            (
                now,
                int(row["rebirth_count"] or 0),
                float(row["rebirth_mult"] or 1.0),
                int(row["total_boss_kills"] or 0),
                str(row["nickname"] or ""),
                str(row["reg_label"] or ""),
                int(new_true_count),
                int(tg_id),
            ),
        )
        con.execute(
            """
            DELETE FROM user_stats
            WHERE tg_id = ?
              AND stat_key NOT LIKE 'artifact:%'
              AND stat_key NOT LIKE 'artifact_slot_%'
            """,
            (int(tg_id),),
        )
        return True


def update_all_for_update_reset():
    """Мягкий вайп под обновление: арена/монеты/прогресс, без трогания экипа и привилегий."""
    with _connect() as con:
        row = con.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        users_count = int(row["c"] or 0)
        con.execute(
            "UPDATE users SET arena = 1, coins = 0, boss_progress = 0, boss_kill_mask = 0"
        )
        con.execute(
            """
            DELETE FROM user_stats
            WHERE stat_key NOT LIKE 'artifact:%'
              AND stat_key NOT LIKE 'artifact_slot_%'
            """
        )
        return users_count


def reset_all_users_preserve_core():
    """Глобальный сброс профилей.

    Сохраняем: admin_role, nickname, created_at/reg_label, profile_note, donate_rub.
    По пользователям обнуляется/очищается все остальное.
    """
    now = int(time.time())
    with _connect() as con:
        row = con.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        users_count = int(row["c"] or 0)

        con.execute("DELETE FROM inventory")
        con.execute("DELETE FROM saved_items")
        con.execute("DELETE FROM item_enchants")
        con.execute("DELETE FROM user_stats")
        con.execute("DELETE FROM user_text_stats")
        con.execute("DELETE FROM active_battles")
        con.execute("DELETE FROM active_dungeons")
        con.execute("DELETE FROM world_boss_hits")
        con.execute("DELETE FROM referrals")
        con.execute("DELETE FROM promo_uses")

        con.execute(
            """
            UPDATE users SET
                coins = 0,
                arena = 1,
                boss_progress = 0,
                boss_kill_mask = 0,
                power = 0,
                last_train_time = ?,
                training_active = 0,
                training_until = 0,
                rebirth_count = 0,
                rebirth_mult = 1.0,
                true_rebirth_count = 0,
                rank_idx = 0,
                afk_common = 0,
                afk_rare = 0,
                afk_epic = 0,
                afk_legendary = 0,
                afk_mythic = 0,
                weapon_cases_a1 = 0, weapon_cases_a2 = 0, weapon_cases_a3 = 0,
                weapon_cases_a4 = 0, weapon_cases_a5 = 0, weapon_cases_a6 = 0,
                weapon_cases_a7 = 0, weapon_cases_a8 = 0, weapon_cases_a9 = 0,
                weapon_cases_a10 = 0, weapon_cases_a11 = 0, weapon_cases_a12 = 0,
                weapon_cases_a13 = 0, weapon_cases_a14 = 0, weapon_cases_a15 = 0,
                pet_cases_a1 = 0, pet_cases_a2 = 0, pet_cases_a3 = 0,
                pet_cases_a4 = 0, pet_cases_a5 = 0, pet_cases_a6 = 0,
                pet_cases_a7 = 0, pet_cases_a8 = 0, pet_cases_a9 = 0,
                pet_cases_a10 = 0, pet_cases_a11 = 0, pet_cases_a12 = 0,
                pet_cases_a13 = 0, pet_cases_a14 = 0, pet_cases_a15 = 0,
                equipped_weapon_id = 0,
                equipped_pet_id = 0,
                hp_boost = 0,
                total_boss_kills = 0,
                magic_coins = 0,
                ring_level = 0,
                active_ring_level = 0,
                shard_1 = 0, shard_2 = 0, shard_3 = 0, shard_4 = 0, shard_5 = 0,
                aura_regen = 0, aura_fortune = 0, aura_master = 0,
                aura_hunter = 0, aura_wrath = 0,
                active_aura = '',
                vip_lvl = 0,
                essence = 0,
                trader_day = '',
                trader_hour = -1,
                last_daily_claim = 0,
                profile_title = '',
                bio_bonus_active = 0,
                bio_bonus_checked_at = 0,
                last_active_at = 0,
                banned = 0,
                muted_until = 0,
                train_case_lvl = 0,
                train_power_lvl = 0,
                train_time_lvl = 0,
                deposit_amount = 0,
                deposit_started_at = 0
            """,
            (now,),
        )
        return users_count


def save_active_battle(
        tg_id,
        arena,
        boss_idx,
        boss_hp,
        boss_max_hp,
        player_hp,
        player_max_hp,
        player_dmg,
        regen_per_tick,
        boss_atk,
        last_regen,
        last_action,
        msg_id,
        chat_id,
):
    with _connect() as con:
        con.execute(
            """
            INSERT INTO active_battles (
                tg_id, arena, boss_idx, boss_hp, boss_max_hp,
                player_hp, player_max_hp, player_dmg, regen_per_tick, boss_atk,
                last_regen, last_action, msg_id, chat_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                arena = excluded.arena,
                boss_idx = excluded.boss_idx,
                boss_hp = excluded.boss_hp,
                boss_max_hp = excluded.boss_max_hp,
                player_hp = excluded.player_hp,
                player_max_hp = excluded.player_max_hp,
                player_dmg = excluded.player_dmg,
                regen_per_tick = excluded.regen_per_tick,
                boss_atk = excluded.boss_atk,
                last_regen = excluded.last_regen,
                last_action = excluded.last_action,
                msg_id = excluded.msg_id,
                chat_id = excluded.chat_id,
                updated_at = excluded.updated_at
            """,
            (
                int(tg_id), int(arena), int(boss_idx), int(boss_hp), int(boss_max_hp),
                int(player_hp), int(player_max_hp), int(player_dmg), int(regen_per_tick), int(boss_atk),
                float(last_regen), float(last_action), int(msg_id), int(chat_id), int(time.time()),
            ),
        )


def delete_active_battle(tg_id):
    with _connect() as con:
        con.execute("DELETE FROM active_battles WHERE tg_id = ?", (int(tg_id),))


def list_active_battles():
    with _connect() as con:
        return con.execute("SELECT * FROM active_battles").fetchall()


def get_active_battle(tg_id):
    with _connect() as con:
        return con.execute(
            "SELECT * FROM active_battles WHERE tg_id = ?",
            (int(tg_id),),
        ).fetchone()


def save_active_dungeon(
        tg_id,
        mode,
        difficulty,
        wave,
        max_waves,
        gold,
        magic,
        shards,
        started_at,
        enemy_hp,
        enemy_max_hp,
        enemy_atk,
        player_dmg,
        msg_id,
        chat_id,
        arena,
        note,
):
    shards_json = json.dumps(shards or {}, ensure_ascii=False, separators=(",", ":"))
    with _connect() as con:
        con.execute(
            """
            INSERT INTO active_dungeons (
                tg_id, mode, difficulty, wave, max_waves, gold, magic, shards_json,
                started_at, enemy_hp, enemy_max_hp, enemy_atk, player_dmg,
                msg_id, chat_id, arena, note, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                mode = excluded.mode,
                difficulty = excluded.difficulty,
                wave = excluded.wave,
                max_waves = excluded.max_waves,
                gold = excluded.gold,
                magic = excluded.magic,
                shards_json = excluded.shards_json,
                started_at = excluded.started_at,
                enemy_hp = excluded.enemy_hp,
                enemy_max_hp = excluded.enemy_max_hp,
                enemy_atk = excluded.enemy_atk,
                player_dmg = excluded.player_dmg,
                msg_id = excluded.msg_id,
                chat_id = excluded.chat_id,
                arena = excluded.arena,
                note = excluded.note,
                updated_at = excluded.updated_at
            """,
            (
                int(tg_id), str(mode), str(difficulty or "easy"), int(wave), int(max_waves), int(gold), int(magic), shards_json,
                float(started_at), int(enemy_hp), int(enemy_max_hp), int(enemy_atk), int(player_dmg),
                int(msg_id), int(chat_id), int(arena), str(note or ""), int(time.time()),
            ),
        )


def delete_active_dungeon(tg_id):
    with _connect() as con:
        con.execute("DELETE FROM active_dungeons WHERE tg_id = ?", (int(tg_id),))


def list_active_dungeons():
    with _connect() as con:
        return con.execute("SELECT * FROM active_dungeons").fetchall()


def get_active_dungeon(tg_id):
    with _connect() as con:
        return con.execute(
            "SELECT * FROM active_dungeons WHERE tg_id = ?",
            (int(tg_id),),
        ).fetchone()


def set_active_contest(contest_id, owner_id, question, started_at, ends_at):
    with _connect() as con:
        old = con.execute("SELECT contest_id FROM active_contest WHERE id = 1").fetchone()
        if old:
            con.execute("DELETE FROM contest_answers WHERE contest_id = ?", (int(old["contest_id"]),))
        con.execute(
            """
            INSERT INTO active_contest (id, contest_id, owner_id, question, started_at, ends_at)
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                contest_id = excluded.contest_id,
                owner_id = excluded.owner_id,
                question = excluded.question,
                started_at = excluded.started_at,
                ends_at = excluded.ends_at
            """,
            (int(contest_id), int(owner_id), str(question), int(started_at), int(ends_at)),
        )


def get_active_contest():
    with _connect() as con:
        return con.execute("SELECT * FROM active_contest WHERE id = 1").fetchone()


def clear_active_contest():
    with _connect() as con:
        row = con.execute("SELECT contest_id FROM active_contest WHERE id = 1").fetchone()
        if row:
            con.execute("DELETE FROM contest_answers WHERE contest_id = ?", (int(row["contest_id"]),))
        con.execute("DELETE FROM active_contest WHERE id = 1")


def add_contest_answer(contest_id, tg_id, answer):
    with _connect() as con:
        try:
            con.execute(
                "INSERT INTO contest_answers (contest_id, tg_id, answer, created_at) VALUES (?, ?, ?, ?)",
                (int(contest_id), int(tg_id), str(answer), int(time.time())),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def list_contest_answers(contest_id):
    with _connect() as con:
        return con.execute(
            "SELECT tg_id, answer FROM contest_answers WHERE contest_id = ? ORDER BY created_at ASC",
            (int(contest_id),),
        ).fetchall()


def add_case_count(tg_id, case_key, delta=1):
    col = "afk_" + case_key
    with _connect() as con:
        con.execute("UPDATE users SET " + col + " = " + col + " + ? WHERE tg_id = ?", (max(0, delta), tg_id))


def consume_case_count(tg_id, case_key, count):
    count = max(1, count)
    col = "afk_" + case_key
    with _connect() as con:
        row = con.execute("SELECT " + col + " as c FROM users WHERE tg_id = ?", (tg_id,)).fetchone()
        if not row or row["c"] < count:
            return False
        con.execute("UPDATE users SET " + col + " = " + col + " - ? WHERE tg_id = ?", (count, tg_id))
        return True


def add_inventory_item(tg_id, item_type, name, level, bonus, count=1):
    with _connect() as con:
        _upsert_inventory_in_con(con, tg_id, item_type, name, level, bonus, count)


def _upsert_inventory_in_con(con, tg_id, item_type, name, level, bonus, count=1):
    row = con.execute(
        """
        SELECT i.id, i.count
        FROM inventory i
        WHERE i.tg_id = ?
          AND i.type = ?
          AND i.name = ?
          AND i.level = ?
          AND i.bonus = ?
          AND i.in_bank = 0
          AND (? <> 'artifact' OR NOT EXISTS (
                SELECT 1 FROM artifact_trust t WHERE t.item_id = i.id
          ))
        """,
        (tg_id, item_type, name, level, bonus, item_type),
    ).fetchone()
    if row:
        con.execute("UPDATE inventory SET count = ? WHERE id = ?", (int(row["count"]) + int(count), row["id"]))
    else:
        con.execute(
            "INSERT INTO inventory (tg_id, type, name, level, bonus, count) VALUES (?, ?, ?, ?, ?, ?)",
            (tg_id, item_type, name, level, bonus, count),
        )


def get_inventory_item(tg_id, item_id):
    with _connect() as con:
        return con.execute(
            "SELECT * FROM inventory WHERE tg_id = ? AND id = ?",
            (tg_id, item_id),
        ).fetchone()


def consume_inventory_item(tg_id, item_id, count):
    count = max(1, int(count))
    with _connect() as con:
        row = con.execute(
            "SELECT id, count, name FROM inventory WHERE tg_id = ? AND id = ?",
            (tg_id, item_id),
        ).fetchone()
        if not row or int(row["count"]) < count:
            return False
        if str(row["name"] or "").startswith("👑 VIP "):
            return False
        left = int(row["count"]) - count
        if left <= 0:
            con.execute("DELETE FROM inventory WHERE id = ?", (item_id,))
            con.execute("DELETE FROM saved_items WHERE item_id = ?", (item_id,))
            con.execute("DELETE FROM item_enchants WHERE item_id = ?", (item_id,))
            con.execute(
                "UPDATE users SET equipped_weapon_id = CASE WHEN equipped_weapon_id = ? THEN 0 ELSE equipped_weapon_id END, equipped_pet_id = CASE WHEN equipped_pet_id = ? THEN 0 ELSE equipped_pet_id END WHERE tg_id = ?",
                (item_id, item_id, tg_id),
            )
        else:
            con.execute("UPDATE inventory SET count = ? WHERE id = ?", (left, item_id))
        return True


def inventory_list(tg_id, limit=5000):
    with _connect() as con:
        # Показываем в первую очередь последние дропы, чтобы новые предметы
        # не терялись в хвосте при большом инвентаре.
        return con.execute(
            "SELECT * FROM inventory WHERE tg_id = ? ORDER BY id DESC LIMIT ?",
            (tg_id, max(1, int(limit))),
        ).fetchall()


def inventory_banked_list(tg_id, limit=5000):
    with _connect() as con:
        return con.execute(
            "SELECT * FROM inventory WHERE tg_id = ? AND in_bank = 1 ORDER BY id DESC LIMIT ?",
            (tg_id, max(1, int(limit))),
        ).fetchall()


def inventory_equipped_bonus(tg_id, item_type):
    with _connect() as con:
        user = con.execute("SELECT equipped_weapon_id, equipped_pet_id FROM users WHERE tg_id = ?", (tg_id,)).fetchone()
        if not user:
            return 0
        eq_id = int(user["equipped_weapon_id"] if item_type == "weapon" else user["equipped_pet_id"])
        if eq_id > 0:
            row = con.execute(
                "SELECT bonus FROM inventory WHERE id = ? AND tg_id = ? AND type = ?",
                (eq_id, tg_id, item_type),
            ).fetchone()
            if row and row["bonus"] is not None:
                return int(row["bonus"])
        row = con.execute(
            "SELECT id, bonus FROM inventory WHERE tg_id = ? AND type = ? AND in_bank = 0 ORDER BY bonus DESC, id DESC LIMIT 1",
            (tg_id, item_type),
        ).fetchone()
        if not row:
            return 0
        if item_type == "weapon":
            con.execute("UPDATE users SET equipped_weapon_id = ? WHERE tg_id = ?", (int(row["id"]), tg_id))
        else:
            con.execute("UPDATE users SET equipped_pet_id = ? WHERE tg_id = ?", (int(row["id"]), tg_id))
        return int(row["bonus"])


def set_equipped_item(tg_id, item_type, item_id):
    with _connect() as con:
        row = con.execute(
            "SELECT id FROM inventory WHERE tg_id = ? AND id = ? AND type = ? AND in_bank = 0",
            (tg_id, item_id, item_type),
        ).fetchone()
        if not row:
            return False
        field = "equipped_weapon_id" if item_type == "weapon" else "equipped_pet_id"
        con.execute("UPDATE users SET " + field + " = ? WHERE tg_id = ?", (item_id, tg_id))
        return True


def set_inventory_bank(tg_id, item_id, to_bank):
    with _connect() as con:
        row = con.execute("SELECT id FROM inventory WHERE tg_id = ? AND id = ?", (tg_id, item_id)).fetchone()
        if not row:
            return False
        if to_bank:
            con.execute(
                "UPDATE users SET equipped_weapon_id = CASE WHEN equipped_weapon_id = ? THEN 0 ELSE equipped_weapon_id END, equipped_pet_id = CASE WHEN equipped_pet_id = ? THEN 0 ELSE equipped_pet_id END WHERE tg_id = ?",
                (item_id, item_id, tg_id),
            )
        val = 1 if to_bank else 0
        con.execute("UPDATE inventory SET in_bank = ? WHERE id = ? AND tg_id = ?", (val, item_id, tg_id))
        return True


def synth_by_item_id(tg_id, item_id, requested_count):
    requested_count = max(3, int(requested_count or 3))
    with _connect() as con:
        row = con.execute(
            "SELECT * FROM inventory WHERE tg_id = ? AND id = ?",
            (tg_id, item_id),
        ).fetchone()
        if not row:
            return None
        if int(row["in_bank"]) == 1:
            return None
        level = int(row["level"])
        if level >= 3:
            return None

        # Зачарованный предмет нельзя пускать в синтез — он уникален
        enchanted_check = con.execute(
            "SELECT item_id FROM item_enchants WHERE item_id = ? LIMIT 1", (item_id,)
        ).fetchone()
        if enchanted_check:
            return None

        # Разрешаем синтезировать предметы с одинаковыми type/name/level даже при разном bonus.
        # Зачарованные стаки исключаем из кандидатов.
        stacks = con.execute(
            """
            SELECT i.id, i.count, i.bonus
            FROM inventory i
            WHERE i.tg_id = ? AND i.in_bank = 0 AND i.type = ? AND i.name = ? AND i.level = ?
              AND NOT EXISTS (SELECT 1 FROM item_enchants e WHERE e.item_id = i.id)
            ORDER BY i.bonus DESC, i.id DESC
            """,
            (tg_id, row["type"], row["name"], level),
        ).fetchall()
        total_count = sum(int(st["count"] or 0) for st in stacks)
        usable = min(total_count, requested_count)
        merges = usable // 3
        if merges <= 0:
            return None

        used_count = merges * 3
        need = used_count
        consumed_bonus_values = []
        for st in stacks:
            if need <= 0:
                break
            sid = int(st["id"])
            cnt = int(st["count"] or 0)
            take = min(cnt, need)
            if take <= 0:
                continue
            consumed_bonus_values.extend([int(st["bonus"] or 0)] * take)
            left = cnt - take
            if left <= 0:
                con.execute("DELETE FROM inventory WHERE id = ?", (sid,))
                con.execute("DELETE FROM saved_items WHERE item_id = ?", (sid,))
                con.execute("DELETE FROM item_enchants WHERE item_id = ?", (sid,))
                con.execute(
                    "UPDATE users SET equipped_weapon_id = CASE WHEN equipped_weapon_id = ? THEN 0 ELSE equipped_weapon_id END, equipped_pet_id = CASE WHEN equipped_pet_id = ? THEN 0 ELSE equipped_pet_id END WHERE tg_id = ?",
                    (sid, sid, tg_id),
                )
            else:
                con.execute("UPDATE inventory SET count = ? WHERE id = ?", (left, sid))
            need -= take

        if len(consumed_bonus_values) < used_count:
            return None

        mul = 2.5 if level == 1 else (6.0 / 2.5)
        new_level = level + 1
        base_bonus = int(round(sum(consumed_bonus_values) / len(consumed_bonus_values)))
        new_bonus = int(round(base_bonus * mul))
        _upsert_inventory_in_con(con, tg_id, row["type"], row["name"], new_level, new_bonus, merges)
        return {
            "name": row["name"],
            "new_level": new_level,
            "new_bonus": new_bonus,
            "created_count": merges,
            "used_count": used_count,
            "left_count": max(0, total_count - used_count),
        }


def upgrade_three_to_one(tg_id):
    """Автосинтез: ищет любые 3 одинаковых предмета, даже если они в разных строках инвентаря.
    Зачарованные предметы полностью исключаются из автосинтеза."""
    with _connect() as con:
        group_row = con.execute(
            """
            SELECT i.type, i.name, i.level, SUM(i.count) AS total_count
            FROM inventory i
            WHERE i.tg_id = ? AND i.in_bank = 0 AND i.level < 3
              AND NOT EXISTS (SELECT 1 FROM item_enchants e WHERE e.item_id = i.id)
            GROUP BY i.type, i.name, i.level
            HAVING SUM(i.count) >= 3
            ORDER BY i.level DESC, i.name ASC
            LIMIT 1
            """,
            (tg_id,),
        ).fetchone()
        if not group_row:
            return None

        item_type = str(group_row["type"])
        name = str(group_row["name"])
        level = int(group_row["level"])
        total_count = int(group_row["total_count"])

        need = 3
        stacks = con.execute(
            """
            SELECT i.id, i.count, i.bonus
            FROM inventory i
            WHERE i.tg_id = ? AND i.in_bank = 0 AND i.type = ? AND i.name = ? AND i.level = ?
              AND NOT EXISTS (SELECT 1 FROM item_enchants e WHERE e.item_id = i.id)
            ORDER BY i.bonus DESC, i.id DESC
            """,
            (tg_id, item_type, name, level),
        ).fetchall()

        consumed_bonus_values = []
        for st in stacks:
            if need <= 0:
                break
            item_id = int(st["id"])
            cnt = int(st["count"])
            take = min(cnt, need)
            if take > 0:
                consumed_bonus_values.extend([int(st["bonus"] or 0)] * take)
            left = cnt - take
            if left <= 0:
                con.execute("DELETE FROM inventory WHERE id = ?", (item_id,))
                con.execute("DELETE FROM saved_items WHERE item_id = ?", (item_id,))
                con.execute("DELETE FROM item_enchants WHERE item_id = ?", (item_id,))
                con.execute(
                    "UPDATE users SET equipped_weapon_id = CASE WHEN equipped_weapon_id = ? THEN 0 ELSE equipped_weapon_id END, equipped_pet_id = CASE WHEN equipped_pet_id = ? THEN 0 ELSE equipped_pet_id END WHERE tg_id = ?",
                    (item_id, item_id, tg_id),
                )
            else:
                con.execute("UPDATE inventory SET count = ? WHERE id = ?", (left, item_id))
            need -= take

        if need > 0:
            return None

        mul = 2.5 if level == 1 else (6.0 / 2.5)
        new_level = level + 1
        base_bonus = int(round(sum(consumed_bonus_values) / len(consumed_bonus_values))) if consumed_bonus_values else 0
        new_bonus = int(round(int(base_bonus) * mul))
        _upsert_inventory_in_con(con, tg_id, item_type, name, new_level, new_bonus, 1)

        return {
            "name": name,
            "new_level": new_level,
            "new_bonus": new_bonus,
            "created_count": 1,
            "used_count": 3,
            "left_count": max(0, total_count - 3),
        }


def upgrade_all_three_to_one(tg_id, limit=2000):
    """Запускает автосинтез до исчерпания комбинаций (или до limit итераций)."""
    out = []
    for _ in range(max(1, int(limit))):
        r = upgrade_three_to_one(tg_id)
        if not r:
            break
        out.append(r)
    return out


def create_promo(code, expires_at, max_uses, created_by, reward_type="coins", reward_value=0, reward_percent=0):
    with _connect() as con:
        cur = con.execute(
            "INSERT INTO promos (code, expires_at, max_uses, reward_type, reward_value, reward_percent, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (code.upper(), expires_at, max_uses, reward_type, reward_value, reward_percent, created_by, int(time.time())),
        )
        return int(cur.lastrowid)


def add_promo_reward(promo_id, reward_type, reward_value=0, reward_percent=0):
    with _connect() as con:
        con.execute(
            "INSERT INTO promo_rewards (promo_id, reward_type, reward_value, reward_percent) VALUES (?, ?, ?, ?)",
            (promo_id, reward_type, int(reward_value), int(reward_percent)),
        )


def get_promo_rewards(promo_id):
    with _connect() as con:
        rows = con.execute(
            "SELECT reward_type, reward_value, reward_percent FROM promo_rewards WHERE promo_id = ? ORDER BY id",
            (promo_id,),
        ).fetchall()
        if rows:
            return rows
        # Фолбэк для старых промо из одной награды.
        old = con.execute(
            "SELECT reward_type, reward_value, reward_percent FROM promos WHERE id = ?",
            (promo_id,),
        ).fetchone()
        return [old] if old else []


def add_promo_broadcast(promo_id, chat_id, message_id):
    with _connect() as con:
        con.execute(
            "INSERT INTO promo_broadcasts (promo_id, chat_id, message_id, active) VALUES (?, ?, ?, 1)",
            (promo_id, chat_id, message_id),
        )


def list_expired_active_promo_broadcasts(now_ts):
    with _connect() as con:
        return con.execute(
            """
            SELECT pb.id, pb.chat_id, pb.message_id
            FROM promo_broadcasts pb
            JOIN promos p ON p.id = pb.promo_id
            WHERE pb.active = 1 AND p.expires_at <= ?
            """,
            (int(now_ts),),
        ).fetchall()


def deactivate_promo_broadcast(broadcast_id):
    with _connect() as con:
        con.execute(
            "UPDATE promo_broadcasts SET active = 0 WHERE id = ?",
            (broadcast_id,),
        )


def get_promo(code):
    with _connect() as con:
        return con.execute("SELECT * FROM promos WHERE code = ?", (code.upper(),)).fetchone()


def list_active_promos(now_ts, limit=50):
    with _connect() as con:
        return con.execute(
            """
            SELECT
                p.id,
                p.code,
                p.expires_at,
                p.max_uses,
                (
                    SELECT COUNT(*)
                    FROM promo_uses pu
                    WHERE pu.promo_id = p.id
                ) AS used_count
            FROM promos p
            WHERE p.expires_at > ?
            ORDER BY p.expires_at ASC, p.id DESC
            LIMIT ?
            """,
            (int(now_ts), int(limit)),
        ).fetchall()


def expire_all_active_promos(now_ts=None):
    """Принудительно завершает все активные промокоды."""
    ts = int(now_ts or time.time())
    with _connect() as con:
        cur = con.execute(
            "UPDATE promos SET expires_at = ? WHERE expires_at > ?",
            (ts, ts),
        )
        # Сразу помечаем закрепы неактивными, чтобы воркер не трогал их повторно.
        con.execute("UPDATE promo_broadcasts SET active = 0 WHERE active = 1")
        return int(cur.rowcount or 0)


def get_promo_uses_count(promo_id):
    with _connect() as con:
        row = con.execute("SELECT COUNT(*) AS c FROM promo_uses WHERE promo_id = ?", (promo_id,)).fetchone()
        return int(row["c"])


def mark_promo_use(promo_id, tg_id):
    with _connect() as con:
        try:
            con.execute(
                "INSERT INTO promo_uses (promo_id, tg_id, used_at) VALUES (?, ?, ?)",
                (promo_id, tg_id, int(time.time())),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def ensure_trader_hour(tg_id, day_key):
    with _connect() as con:
        row = con.execute("SELECT trader_day, trader_hour FROM users WHERE tg_id = ?", (tg_id,)).fetchone()
        if not row:
            return -1
        if str(row["trader_day"] or "") == day_key and int(row["trader_hour"] or -1) >= 0:
            return int(row["trader_hour"])
        prev_hour = int(row["trader_hour"] or -1)
        digest = hashlib.sha256(f"{int(tg_id)}:{day_key}".encode("utf-8")).hexdigest()
        hour = int(digest[:8], 16) % 24
        # Не повторяем вчерашний час, чтобы окно лавки было реально разным по дням.
        if prev_hour >= 0 and hour == prev_hour:
            step = (int(digest[8:10], 16) % 23) + 1
            hour = (hour + step) % 24
        con.execute("UPDATE users SET trader_day = ?, trader_hour = ? WHERE tg_id = ?", (day_key, hour, tg_id))
        return hour


def bind_referral(referrer_id, referred_id):
    referrer_id = int(referrer_id)
    referred_id = int(referred_id)
    if referrer_id <= 0 or referred_id <= 0:
        return False, "bad_id"
    if referrer_id == referred_id:
        return False, "self"
    with _connect() as con:
        ref_user = con.execute("SELECT tg_id FROM users WHERE tg_id = ?", (referrer_id,)).fetchone()
        if not ref_user:
            return False, "referrer_not_found"
        exists = con.execute("SELECT referrer_id FROM referrals WHERE referred_id = ?", (referred_id,)).fetchone()
        if exists:
            return False, "already_bound"
        con.execute(
            "INSERT INTO referrals (referrer_id, referred_id, created_at) VALUES (?, ?, ?)",
            (referrer_id, referred_id, int(time.time())),
        )
        return True, "ok"


def get_referrer_id(referred_id):
    with _connect() as con:
        row = con.execute(
            "SELECT referrer_id FROM referrals WHERE referred_id = ?",
            (int(referred_id),),
        ).fetchone()
        return int(row["referrer_id"]) if row else 0


def mark_referral_qualified_if_ready(referred_id, min_arena=3):
    referred_id = int(referred_id)
    with _connect() as con:
        row = con.execute(
            """
            SELECT r.referrer_id, r.qualified_at, u.arena
            FROM referrals r
            JOIN users u ON u.tg_id = r.referred_id
            WHERE r.referred_id = ?
            """,
            (referred_id,),
        ).fetchone()
        if not row:
            return 0
        if int(row["qualified_at"] or 0) > 0:
            return 0
        if int(row["arena"] or 1) < int(min_arena):
            return 0
        cur = con.execute(
            "UPDATE referrals SET qualified_at = ? WHERE referred_id = ? AND qualified_at = 0",
            (int(time.time()), referred_id),
        )
        if int(cur.rowcount or 0) <= 0:
            return 0
        return int(row["referrer_id"])


def referral_stats(referrer_id):
    with _connect() as con:
        row = con.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN qualified_at > 0 THEN 1 ELSE 0 END) AS qualified,
                SUM(CASE WHEN claimed_at > 0 THEN 1 ELSE 0 END) AS claimed,
                SUM(CASE WHEN qualified_at > 0 AND claimed_at = 0 THEN 1 ELSE 0 END) AS pending
            FROM referrals
            WHERE referrer_id = ?
            """,
            (int(referrer_id),),
        ).fetchone()
        if not row:
            return {"total": 0, "qualified": 0, "claimed": 0, "pending": 0}
        return {
            "total": int(row["total"] or 0),
            "qualified": int(row["qualified"] or 0),
            "claimed": int(row["claimed"] or 0),
            "pending": int(row["pending"] or 0),
        }


def list_pending_referrals(referrer_id, limit=20):
    with _connect() as con:
        return con.execute(
            """
            SELECT r.referred_id, r.created_at, r.qualified_at, u.username, u.nickname, u.arena
            FROM referrals r
            JOIN users u ON u.tg_id = r.referred_id
            WHERE r.referrer_id = ? AND r.qualified_at > 0 AND r.claimed_at = 0
            ORDER BY r.qualified_at ASC, r.referred_id ASC
            LIMIT ?
            """,
            (int(referrer_id), int(limit)),
        ).fetchall()


def claim_pending_referral(referrer_id, referred_id, reward_key):
    with _connect() as con:
        cur = con.execute(
            """
            UPDATE referrals
            SET claimed_at = ?, reward_key = ?
            WHERE referrer_id = ?
              AND referred_id = ?
              AND qualified_at > 0
              AND claimed_at = 0
            """,
            (int(time.time()), str(reward_key or ""), int(referrer_id), int(referred_id)),
        )
        return int(cur.rowcount or 0) > 0


def claim_referred_reward_if_qualified(referred_id):
    """Помечает одноразовую награду приглашённому и возвращает referrer_id."""
    referred_id = int(referred_id)
    with _connect() as con:
        row = con.execute(
            "SELECT referrer_id, qualified_at, referred_rewarded_at FROM referrals WHERE referred_id = ?",
            (referred_id,),
        ).fetchone()
        if not row:
            return 0
        if int(row["qualified_at"] or 0) <= 0:
            return 0
        if int(row["referred_rewarded_at"] or 0) > 0:
            return 0
        cur = con.execute(
            """
            UPDATE referrals
            SET referred_rewarded_at = ?
            WHERE referred_id = ?
              AND qualified_at > 0
              AND referred_rewarded_at = 0
            """,
            (int(time.time()), referred_id),
        )
        if int(cur.rowcount or 0) <= 0:
            return 0
        return int(row["referrer_id"] or 0)


def get_user_guild(tg_id):
    with _connect() as con:
        return con.execute(
            """
            SELECT g.*
            FROM guild_members gm
            JOIN guilds g ON g.id = gm.guild_id
            WHERE gm.tg_id = ?
            """,
            (int(tg_id),),
        ).fetchone()


def get_user_guild_role(tg_id):
    with _connect() as con:
        row = con.execute(
            "SELECT role FROM guild_members WHERE tg_id = ?",
            (int(tg_id),),
        ).fetchone()
        return str(row["role"]) if row else ""


def get_guild(guild_id):
    with _connect() as con:
        return con.execute("SELECT * FROM guilds WHERE id = ?", (int(guild_id),)).fetchone()


def list_top_guilds(limit=5):
    with _connect() as con:
        return con.execute(
            """
            SELECT
                g.id,
                g.name,
                g.level,
                g.owner_id,
                g.unity_shards,
                COUNT(gm.tg_id) AS members
            FROM guilds g
            LEFT JOIN guild_members gm ON gm.guild_id = g.id
            WHERE g.hidden_from_top = 0
            GROUP BY g.id
            ORDER BY g.level DESC, members DESC, g.unity_shards DESC, g.id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()


def set_guild_hidden(guild_id, hidden):
    with _connect() as con:
        cur = con.execute(
            "UPDATE guilds SET hidden_from_top = ? WHERE id = ?",
            (1 if hidden else 0, int(guild_id)),
        )
        return int(cur.rowcount or 0) > 0


def set_guild_level(guild_id, new_level):
    lvl = int(new_level)
    if lvl < 1 or lvl > 5:
        return False, 0, 0
    with _connect() as con:
        row = con.execute("SELECT level FROM guilds WHERE id = ?", (int(guild_id),)).fetchone()
        if not row:
            return False, 0, 0
        prev = int(row["level"] or 1)
        cur = con.execute(
            "UPDATE guilds SET level = ? WHERE id = ?",
            (lvl, int(guild_id)),
        )
        if int(cur.rowcount or 0) <= 0:
            return False, prev, prev
        return True, prev, lvl


def create_guild(owner_id, name):
    clean = str(name or "").strip()
    if len(clean) < 3 or len(clean) > 32:
        raise ValueError("Название должно быть 3-32 символа")
    with _connect() as con:
        in_guild = con.execute("SELECT 1 FROM guild_members WHERE tg_id = ?", (int(owner_id),)).fetchone()
        if in_guild:
            raise ValueError("Ты уже состоишь в гильдии")
        cur = con.execute(
            "INSERT INTO guilds (name, owner_id, created_at) VALUES (?, ?, ?)",
            (clean, int(owner_id), int(time.time())),
        )
        gid = int(cur.lastrowid)
        con.execute(
            "INSERT INTO guild_members (guild_id, tg_id, role, joined_at) VALUES (?, ?, 'owner', ?)",
            (gid, int(owner_id), int(time.time())),
        )
        return gid


def set_guild_description(guild_id, description):
    with _connect() as con:
        con.execute("UPDATE guilds SET description = ? WHERE id = ?", (str(description or "")[:600], int(guild_id)))


def set_guild_name(guild_id, name):
    clean = str(name or "").strip()
    if len(clean) < 3 or len(clean) > 32:
        raise ValueError("Название должно быть 3-32 символа")
    with _connect() as con:
        try:
            con.execute("UPDATE guilds SET name = ? WHERE id = ?", (clean, int(guild_id)))
        except sqlite3.IntegrityError:
            raise ValueError("Гильдия с таким названием уже существует")


def set_guild_open_join(guild_id, open_join):
    with _connect() as con:
        con.execute("UPDATE guilds SET open_join = ? WHERE id = ?", (1 if open_join else 0, int(guild_id)))


def delete_guild(guild_id):
    gid = int(guild_id)
    with _connect() as con:
        con.execute("DELETE FROM guild_members WHERE guild_id = ?", (gid,))
        con.execute("DELETE FROM guild_join_requests WHERE guild_id = ?", (gid,))
        con.execute("DELETE FROM active_guild_battles WHERE guild_id = ?", (gid,))
        con.execute("DELETE FROM guild_boss_hits WHERE guild_id = ?", (gid,))
        con.execute("DELETE FROM guild_boss_cooldowns WHERE guild_id = ?", (gid,))
        cur = con.execute("DELETE FROM guilds WHERE id = ?", (gid,))
        return int(cur.rowcount or 0) > 0


def guild_member_count(guild_id):
    with _connect() as con:
        row = con.execute("SELECT COUNT(*) AS c FROM guild_members WHERE guild_id = ?", (int(guild_id),)).fetchone()
        return int(row["c"] or 0)


def list_guild_members(guild_id, limit=10, offset=0):
    with _connect() as con:
        return con.execute(
            """
            SELECT gm.guild_id, gm.tg_id, gm.role, gm.joined_at, u.username, u.nickname, u.vip_lvl, u.arena
            FROM guild_members gm
            JOIN users u ON u.tg_id = gm.tg_id
            WHERE gm.guild_id = ?
            ORDER BY CASE gm.role WHEN 'owner' THEN 0 WHEN 'deputy' THEN 1 ELSE 2 END, gm.joined_at ASC
            LIMIT ? OFFSET ?
            """,
            (int(guild_id), int(limit), int(offset)),
        ).fetchall()


def list_guild_member_ids(guild_id):
    with _connect() as con:
        rows = con.execute(
            "SELECT tg_id FROM guild_members WHERE guild_id = ?",
            (int(guild_id),),
        ).fetchall()
        return [int(r["tg_id"]) for r in rows]


def add_guild_member(guild_id, tg_id, role="member"):
    with _connect() as con:
        in_guild = con.execute("SELECT 1 FROM guild_members WHERE tg_id = ?", (int(tg_id),)).fetchone()
        if in_guild:
            return False
        con.execute(
            "INSERT INTO guild_members (guild_id, tg_id, role, joined_at) VALUES (?, ?, ?, ?)",
            (int(guild_id), int(tg_id), str(role), int(time.time())),
        )
        return True


def get_guild_member(guild_id, tg_id):
    with _connect() as con:
        return con.execute(
            "SELECT guild_id, tg_id, role, joined_at FROM guild_members WHERE guild_id = ? AND tg_id = ?",
            (int(guild_id), int(tg_id)),
        ).fetchone()


def set_guild_member_role(guild_id, tg_id, role):
    role = str(role or "member").strip().lower()
    if role not in ("member", "deputy"):
        return False
    with _connect() as con:
        row = con.execute(
            "SELECT role FROM guild_members WHERE guild_id = ? AND tg_id = ?",
            (int(guild_id), int(tg_id)),
        ).fetchone()
        if not row:
            return False
        if str(row["role"]) == "owner":
            return False
        cur = con.execute(
            "UPDATE guild_members SET role = ? WHERE guild_id = ? AND tg_id = ?",
            (role, int(guild_id), int(tg_id)),
        )
        return int(cur.rowcount or 0) > 0


def remove_guild_member(guild_id, tg_id):
    with _connect() as con:
        cur = con.execute(
            "DELETE FROM guild_members WHERE guild_id = ? AND tg_id = ? AND role <> 'owner'",
            (int(guild_id), int(tg_id)),
        )
        return int(cur.rowcount or 0) > 0


def transfer_guild_owner(guild_id, new_owner_id):
    """Передаёт владение гильдией действующему участнику."""
    gid = int(guild_id)
    uid = int(new_owner_id)
    with _connect() as con:
        g = con.execute("SELECT owner_id FROM guilds WHERE id = ?", (gid,)).fetchone()
        if not g:
            return False, "guild_not_found"
        cur_owner = int(g["owner_id"] or 0)
        if cur_owner == uid:
            return False, "already_owner"

        target_member = con.execute(
            "SELECT role FROM guild_members WHERE guild_id = ? AND tg_id = ?",
            (gid, uid),
        ).fetchone()
        if not target_member:
            return False, "target_not_member"

        old_owner_member = con.execute(
            "SELECT role FROM guild_members WHERE guild_id = ? AND tg_id = ?",
            (gid, cur_owner),
        ).fetchone()
        if not old_owner_member:
            return False, "owner_not_member"

        con.execute("UPDATE guilds SET owner_id = ? WHERE id = ?", (uid, gid))
        con.execute(
            "UPDATE guild_members SET role = 'member' WHERE guild_id = ? AND tg_id = ?",
            (gid, cur_owner),
        )
        con.execute(
            "UPDATE guild_members SET role = 'owner' WHERE guild_id = ? AND tg_id = ?",
            (gid, uid),
        )
        return True, "ok"


def create_join_request(guild_id, tg_id):
    with _connect() as con:
        in_guild = con.execute("SELECT 1 FROM guild_members WHERE tg_id = ?", (int(tg_id),)).fetchone()
        if in_guild:
            return False, "already_member"
        exists = con.execute(
            "SELECT id FROM guild_join_requests WHERE guild_id = ? AND tg_id = ? AND status = 'pending'",
            (int(guild_id), int(tg_id)),
        ).fetchone()
        if exists:
            return False, "already_pending"
        con.execute(
            "INSERT INTO guild_join_requests (guild_id, tg_id, status, created_at) VALUES (?, ?, 'pending', ?)",
            (int(guild_id), int(tg_id), int(time.time())),
        )
        return True, "ok"


def list_join_requests(guild_id, limit=50):
    with _connect() as con:
        return con.execute(
            """
            SELECT r.id, r.guild_id, r.tg_id, r.created_at, u.username, u.nickname, u.arena, u.vip_lvl
            FROM guild_join_requests r
            JOIN users u ON u.tg_id = r.tg_id
            WHERE r.guild_id = ? AND r.status = 'pending'
            ORDER BY r.id ASC
            LIMIT ?
            """,
            (int(guild_id), int(limit)),
        ).fetchall()


def resolve_join_request(request_id, approve):
    with _connect() as con:
        req = con.execute(
            "SELECT * FROM guild_join_requests WHERE id = ? AND status = 'pending'",
            (int(request_id),),
        ).fetchone()
        if not req:
            return False, None, None
        guild_id = int(req["guild_id"])
        user_id = int(req["tg_id"])
        target_status = "approved" if approve else "rejected"

        if approve:
            in_guild = con.execute("SELECT 1 FROM guild_members WHERE tg_id = ?", (user_id,)).fetchone()
            if in_guild:
                # Пользователь уже в гильдии: просто закрываем заявку как отклоненную.
                con.execute(
                    "UPDATE guild_join_requests SET status = 'rejected' WHERE id = ?",
                    (int(request_id),),
                )
                return False, guild_id, user_id

        # Чистим старые завершенные записи по этой паре, чтобы не упасть
        # на UNIQUE(guild_id, tg_id, status) при повторной модерации заявок.
        con.execute(
            "DELETE FROM guild_join_requests WHERE guild_id = ? AND tg_id = ? AND status = ? AND id <> ?",
            (guild_id, user_id, target_status, int(request_id)),
        )
        con.execute(
            "UPDATE guild_join_requests SET status = ? WHERE id = ?",
            (target_status, int(request_id)),
        )
        if not approve:
            return True, guild_id, user_id
        con.execute(
            "INSERT INTO guild_members (guild_id, tg_id, role, joined_at) VALUES (?, ?, 'member', ?)",
            (guild_id, user_id, int(time.time())),
        )
        return True, guild_id, user_id


def guild_add_unity(guild_id, delta):
    delta = int(delta)
    if delta == 0:
        return
    with _connect() as con:
        con.execute(
            "UPDATE guilds SET unity_shards = MAX(0, unity_shards + ?) WHERE id = ?",
            (delta, int(guild_id)),
        )


def guild_upgrade(guild_id, cost, new_level):
    with _connect() as con:
        row = con.execute("SELECT level, unity_shards FROM guilds WHERE id = ?", (int(guild_id),)).fetchone()
        if not row:
            return False, "not_found"
        if int(row["level"]) != int(new_level) - 1:
            return False, "wrong_level"
        if int(row["unity_shards"]) < int(cost):
            return False, "not_enough"
        con.execute(
            "UPDATE guilds SET level = ?, unity_shards = unity_shards - ? WHERE id = ?",
            (int(new_level), int(cost), int(guild_id)),
        )
        return True, "ok"


def save_active_guild_battle(guild_id, arena, boss_idx, boss_name, boss_hp, boss_max_hp, reward_base, msg_id, chat_id, started_at):
    with _connect() as con:
        con.execute(
            """
            INSERT INTO active_guild_battles (
                guild_id, arena, boss_idx, boss_name, boss_hp, boss_max_hp,
                reward_base, msg_id, chat_id, started_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                arena = excluded.arena,
                boss_idx = excluded.boss_idx,
                boss_name = excluded.boss_name,
                boss_hp = excluded.boss_hp,
                boss_max_hp = excluded.boss_max_hp,
                reward_base = excluded.reward_base,
                msg_id = excluded.msg_id,
                chat_id = excluded.chat_id,
                started_at = excluded.started_at,
                updated_at = excluded.updated_at
            """,
            (
                int(guild_id), int(arena), int(boss_idx), str(boss_name), int(boss_hp), int(boss_max_hp),
                int(reward_base), int(msg_id), int(chat_id), int(started_at), int(time.time()),
            ),
        )


def update_active_guild_battle_hp(guild_id, boss_hp):
    with _connect() as con:
        con.execute(
            "UPDATE active_guild_battles SET boss_hp = ?, updated_at = ? WHERE guild_id = ?",
            (int(boss_hp), int(time.time()), int(guild_id)),
        )


def get_active_guild_battle(guild_id):
    with _connect() as con:
        return con.execute("SELECT * FROM active_guild_battles WHERE guild_id = ?", (int(guild_id),)).fetchone()


def delete_active_guild_battle(guild_id):
    with _connect() as con:
        con.execute("DELETE FROM active_guild_battles WHERE guild_id = ?", (int(guild_id),))


def guild_add_boss_hit(guild_id, tg_id, damage):
    dmg = max(0, int(damage))
    if dmg <= 0:
        return
    with _connect() as con:
        con.execute(
            """
            INSERT INTO guild_boss_hits (guild_id, tg_id, damage, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, tg_id) DO UPDATE SET
                damage = guild_boss_hits.damage + excluded.damage,
                updated_at = excluded.updated_at
            """,
            (int(guild_id), int(tg_id), dmg, int(time.time())),
        )


def guild_list_boss_hits(guild_id):
    with _connect() as con:
        return con.execute(
            """
            SELECT h.tg_id, h.damage, u.username, u.nickname
            FROM guild_boss_hits h
            JOIN users u ON u.tg_id = h.tg_id
            WHERE h.guild_id = ?
            ORDER BY h.damage DESC, h.tg_id ASC
            """,
            (int(guild_id),),
        ).fetchall()


def guild_clear_boss_hits(guild_id):
    with _connect() as con:
        con.execute("DELETE FROM guild_boss_hits WHERE guild_id = ?", (int(guild_id),))


def get_guild_boss_cooldown(guild_id, arena, boss_idx):
    with _connect() as con:
        row = con.execute(
            "SELECT day_key FROM guild_boss_cooldowns WHERE guild_id = ? AND arena = ? AND boss_idx = ?",
            (int(guild_id), int(arena), int(boss_idx)),
        ).fetchone()
        return str(row["day_key"]) if row else ""


def set_guild_boss_cooldown(guild_id, arena, boss_idx, day_key):
    with _connect() as con:
        con.execute(
            """
            INSERT INTO guild_boss_cooldowns (guild_id, arena, boss_idx, day_key)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, arena, boss_idx) DO UPDATE SET
                day_key = excluded.day_key
            """,
            (int(guild_id), int(arena), int(boss_idx), str(day_key)),
        )


def get_world_boss_event():
    with _connect() as con:
        return con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()


def world_boss_set_max_hp(new_max_hp, refill_current=True):
    """Обновляет max_hp мирового босса. Опционально заполняет текущее HP до нового максимума."""
    new_max_hp = max(1, int(new_max_hp))
    with _connect() as con:
        row = con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()
        if not row:
            return None
        cur_hp = int(row["hp"] or 0)
        if refill_current:
            cur_hp = new_max_hp
        else:
            cur_hp = min(cur_hp, new_max_hp)
        con.execute(
            "UPDATE world_boss_event SET max_hp = ?, hp = ? WHERE id = 1",
            (new_max_hp, cur_hp),
        )
        return con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()


def world_boss_adjust_hp(delta=0, set_hp=None):
    """Меняет текущее HP мирового босса. Если HP выходит за max, max тоже расширяется."""
    with _connect() as con:
        row = con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()
        if not row:
            return None
        max_hp = int(row["max_hp"] or 1)
        hp = int(row["hp"] or 0)
        if set_hp is not None:
            new_hp = int(set_hp)
        else:
            new_hp = hp + int(delta)
        new_hp = max(0, new_hp)
        if new_hp > max_hp:
            max_hp = new_hp
        con.execute(
            "UPDATE world_boss_event SET hp = ?, max_hp = ? WHERE id = 1",
            (int(new_hp), int(max_hp)),
        )
        return con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()


def create_world_boss_event(name, max_hp, started_at, ends_at):
    with _connect() as con:
        con.execute(
            """
            INSERT INTO world_boss_event (
                id, name, max_hp, hp, started_at, ends_at, last_regen_at,
                is_finished, winner_id, finished_at, rewards_done
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0)
            ON CONFLICT(id) DO NOTHING
            """,
            (
                str(name),
                int(max_hp),
                int(max_hp),
                int(started_at),
                int(ends_at),
                int(started_at),
            ),
        )


def world_boss_apply_regen(now_ts, tick_seconds=5, heal_per_tick=5):
    with _connect() as con:
        row = con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()
        if not row:
            return None
        if int(row["is_finished"] or 0):
            return row
        now_ts = int(now_ts)
        if now_ts >= int(row["ends_at"] or 0):
            return row
        hp = int(row["hp"] or 0)
        max_hp = int(row["max_hp"] or 0)
        if hp >= max_hp:
            return row
        last_regen = int(row["last_regen_at"] or now_ts)
        ticks = (now_ts - last_regen) // int(max(1, tick_seconds))
        if ticks <= 0:
            return row
        healed = int(ticks) * int(max(1, heal_per_tick))
        new_hp = min(max_hp, hp + healed)
        new_last_regen = last_regen + int(ticks) * int(max(1, tick_seconds))
        con.execute(
            "UPDATE world_boss_event SET hp = ?, last_regen_at = ? WHERE id = 1",
            (new_hp, new_last_regen),
        )
        return con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()


def get_world_boss_hit(tg_id):
    with _connect() as con:
        return con.execute("SELECT * FROM world_boss_hits WHERE tg_id = ?", (int(tg_id),)).fetchone()


def world_boss_apply_player_regen(tg_id, max_hp, now_ts, tick_seconds=3, heal_per_tick=1):
    """
    Применяет реген игрока в ивент-бое независимо от удара.
    Возвращает (row, healed).
    """
    tg_id = int(tg_id)
    max_hp = max(1, int(max_hp))
    now_ts = int(now_ts)
    tick_seconds = max(1, int(tick_seconds))
    heal_per_tick = max(1, int(heal_per_tick))

    with _connect() as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute("SELECT * FROM world_boss_hits WHERE tg_id = ?", (tg_id,)).fetchone()

        if not row:
            con.execute(
                """
                INSERT INTO world_boss_hits (tg_id, damage, hits, last_hit_at, dead_until, current_hp, updated_at)
                VALUES (?, 0, 0, 0, 0, ?, ?)
                """,
                (tg_id, max_hp, now_ts),
            )
            out = con.execute("SELECT * FROM world_boss_hits WHERE tg_id = ?", (tg_id,)).fetchone()
            con.commit()
            return out, 0

        dead_until = int(row["dead_until"] or 0)
        current_hp = int(row["current_hp"] or 0)
        updated_at = int(row["updated_at"] or 0)

        if current_hp <= 0 or current_hp > max_hp:
            current_hp = max_hp if dead_until <= now_ts else max(0, current_hp)

        healed = 0
        new_updated_at = updated_at if updated_at > 0 else now_ts

        if dead_until <= now_ts:
            elapsed = max(0, now_ts - new_updated_at)
            ticks = elapsed // tick_seconds
            if ticks > 0:
                healed = int(ticks) * heal_per_tick
                current_hp = min(max_hp, current_hp + healed)
                new_updated_at = new_updated_at + int(ticks) * tick_seconds

        con.execute(
            "UPDATE world_boss_hits SET current_hp = ?, updated_at = ? WHERE tg_id = ?",
            (int(current_hp), int(new_updated_at), tg_id),
        )
        out = con.execute("SELECT * FROM world_boss_hits WHERE tg_id = ?", (tg_id,)).fetchone()
        con.commit()
        return out, int(max(0, healed))


def world_boss_apply_hit(tg_id, damage, now_ts, current_hp_after, dead_until):
    """
    Атомарно применяет удар игрока к мировому боссу и обновляет статистику игрока.
    Возвращает row события после обновления.
    """
    dmg = max(0, int(damage))
    now_ts = int(now_ts)
    with _connect() as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()
        if not row:
            con.commit()
            return None
        if int(row["is_finished"] or 0) or now_ts >= int(row["ends_at"] or 0):
            con.commit()
            return row

        hp = int(row["hp"] or 0)
        new_hp = max(0, hp - dmg)
        finished = 1 if new_hp <= 0 else 0
        winner_id = int(tg_id) if finished else int(row["winner_id"] or 0)
        finished_at = now_ts if finished else int(row["finished_at"] or 0)

        con.execute(
            """
            UPDATE world_boss_event
            SET hp = ?,
                is_finished = CASE WHEN is_finished = 0 AND ? = 1 THEN 1 ELSE is_finished END,
                winner_id = CASE WHEN is_finished = 0 AND ? = 1 THEN ? ELSE winner_id END,
                finished_at = CASE WHEN is_finished = 0 AND ? = 1 THEN ? ELSE finished_at END
            WHERE id = 1
            """,
            (new_hp, finished, finished, winner_id, finished, finished_at),
        )

        con.execute(
            """
            INSERT INTO world_boss_hits (
                tg_id, damage, hits, last_hit_at, dead_until, current_hp, updated_at
            ) VALUES (?, ?, 1, ?, ?, ?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                damage = world_boss_hits.damage + excluded.damage,
                hits = world_boss_hits.hits + 1,
                last_hit_at = excluded.last_hit_at,
                dead_until = excluded.dead_until,
                current_hp = excluded.current_hp,
                updated_at = excluded.updated_at
            """,
            (
                int(tg_id),
                int(dmg),
                now_ts,
                int(dead_until),
                max(0, int(current_hp_after)),
                now_ts,
            ),
        )
        out = con.execute("SELECT * FROM world_boss_event WHERE id = 1").fetchone()
        con.commit()
        return out


def list_world_boss_hits(limit=100000):
    with _connect() as con:
        return con.execute(
            """
            SELECT h.tg_id, h.damage, h.hits, h.last_hit_at, h.dead_until, h.current_hp,
                   u.username, u.nickname, u.arena
            FROM world_boss_hits h
            JOIN users u ON u.tg_id = h.tg_id
            ORDER BY h.damage DESC, h.tg_id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()


def mark_world_boss_rewards_done():
    with _connect() as con:
        row = con.execute("SELECT rewards_done FROM world_boss_event WHERE id = 1").fetchone()
        if not row:
            return False
        if int(row["rewards_done"] or 0):
            return False
        con.execute("UPDATE world_boss_event SET rewards_done = 1 WHERE id = 1")
        return True


def _snapshot_user_payload_in_con(con, tg_id):
    user = con.execute("SELECT * FROM users WHERE tg_id = ?", (int(tg_id),)).fetchone()
    if not user:
        return None
    inv = con.execute(
        "SELECT id, tg_id, type, name, level, bonus, count, in_bank FROM inventory WHERE tg_id = ? ORDER BY id ASC",
        (int(tg_id),),
    ).fetchall()
    saved = con.execute(
        "SELECT item_id, tg_id, saved_at FROM saved_items WHERE tg_id = ? ORDER BY item_id ASC",
        (int(tg_id),),
    ).fetchall()
    stats = con.execute(
        "SELECT tg_id, stat_key, stat_value FROM user_stats WHERE tg_id = ? ORDER BY stat_key ASC",
        (int(tg_id),),
    ).fetchall()
    battle = con.execute("SELECT * FROM active_battles WHERE tg_id = ?", (int(tg_id),)).fetchone()
    dungeon = con.execute("SELECT * FROM active_dungeons WHERE tg_id = ?", (int(tg_id),)).fetchone()
    return {
        "user": dict(user),
        "inventory": [dict(r) for r in inv],
        "saved_items": [dict(r) for r in saved],
        "user_stats": [dict(r) for r in stats],
        "active_battle": dict(battle) if battle else None,
        "active_dungeon": dict(dungeon) if dungeon else None,
    }


def create_user_rollback_snapshot(tg_id, ts=None):
    now_ts = int(ts or time.time())
    with _connect() as con:
        payload = _snapshot_user_payload_in_con(con, int(tg_id))
        if not payload:
            return False
        con.execute(
            "INSERT INTO user_rollbacks (tg_id, created_at, payload) VALUES (?, ?, ?)",
            (int(tg_id), now_ts, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))),
        )
        con.execute("DELETE FROM user_rollbacks WHERE created_at < ?", (now_ts - 3 * 86400,))
        return True


def create_all_users_rollback_snapshot(ts=None):
    now_ts = int(ts or time.time())
    created = 0
    with _connect() as con:
        ids = con.execute("SELECT tg_id FROM users ORDER BY tg_id ASC").fetchall()
        for row in ids:
            uid = int(row["tg_id"])
            payload = _snapshot_user_payload_in_con(con, uid)
            if not payload:
                continue
            con.execute(
                "INSERT INTO user_rollbacks (tg_id, created_at, payload) VALUES (?, ?, ?)",
                (uid, now_ts, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))),
            )
            created += 1
        con.execute("DELETE FROM user_rollbacks WHERE created_at < ?", (now_ts - 3 * 86400,))
    return created


def _restore_user_payload_in_con(con, payload):
    user = dict(payload.get("user") or {})
    if not user:
        return False
    uid = int(user.get("tg_id", 0) or 0)
    if uid <= 0:
        return False

    cols = list(user.keys())
    col_sql = ", ".join(cols)
    placeholders = ", ".join(["?"] * len(cols))
    upd_cols = [c for c in cols if c != "tg_id"]
    upd_sql = ", ".join([f"{c} = excluded.{c}" for c in upd_cols])
    con.execute(
        f"INSERT INTO users ({col_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT(tg_id) DO UPDATE SET {upd_sql}",
        [user[c] for c in cols],
    )

    con.execute("DELETE FROM inventory WHERE tg_id = ?", (uid,))
    con.execute("DELETE FROM item_enchants WHERE tg_id = ?", (uid,))
    con.execute("DELETE FROM user_text_stats WHERE tg_id = ?", (uid,))
    inv_rows = list(payload.get("inventory") or [])
    if inv_rows:
        con.executemany(
            """
            INSERT INTO inventory (id, tg_id, type, name, level, bonus, count, in_bank)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    int(r.get("id", 0) or 0),
                    int(r.get("tg_id", uid) or uid),
                    str(r.get("type", "") or ""),
                    str(r.get("name", "") or ""),
                    int(r.get("level", 1) or 1),
                    int(r.get("bonus", 0) or 0),
                    int(r.get("count", 1) or 1),
                    int(r.get("in_bank", 0) or 0),
                )
                for r in inv_rows
            ],
        )

    con.execute("DELETE FROM saved_items WHERE tg_id = ?", (uid,))
    saved_rows = list(payload.get("saved_items") or [])
    if saved_rows:
        con.executemany(
            "INSERT INTO saved_items (item_id, tg_id, saved_at) VALUES (?, ?, ?)",
            [
                (
                    int(r.get("item_id", 0) or 0),
                    int(r.get("tg_id", uid) or uid),
                    int(r.get("saved_at", 0) or 0),
                )
                for r in saved_rows
            ],
        )

    con.execute("DELETE FROM user_stats WHERE tg_id = ?", (uid,))
    stat_rows = list(payload.get("user_stats") or [])
    if stat_rows:
        con.executemany(
            "INSERT INTO user_stats (tg_id, stat_key, stat_value) VALUES (?, ?, ?)",
            [
                (
                    int(r.get("tg_id", uid) or uid),
                    str(r.get("stat_key", "") or ""),
                    int(r.get("stat_value", 0) or 0),
                )
                for r in stat_rows
            ],
        )

    con.execute("DELETE FROM active_battles WHERE tg_id = ?", (uid,))
    battle = payload.get("active_battle")
    if battle:
        cols_b = list(battle.keys())
        con.execute(
            f"INSERT INTO active_battles ({', '.join(cols_b)}) VALUES ({', '.join(['?'] * len(cols_b))})",
            [battle[c] for c in cols_b],
        )

    con.execute("DELETE FROM active_dungeons WHERE tg_id = ?", (uid,))
    dungeon = payload.get("active_dungeon")
    if dungeon:
        cols_d = list(dungeon.keys())
        con.execute(
            f"INSERT INTO active_dungeons ({', '.join(cols_d)}) VALUES ({', '.join(['?'] * len(cols_d))})",
            [dungeon[c] for c in cols_d],
        )
    return True


def restore_user_rollback(tg_id, seconds_ago):
    uid = int(tg_id)
    target_ts = int(time.time()) - max(0, int(seconds_ago or 0))
    with _connect() as con:
        row = con.execute(
            """
            SELECT id, created_at, payload
            FROM user_rollbacks
            WHERE tg_id = ? AND created_at <= ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (uid, target_ts),
        ).fetchone()
        if not row:
            return False, 0
        try:
            payload = json.loads(str(row["payload"] or "{}"))
        except Exception:
            return False, 0
        con.execute("BEGIN IMMEDIATE")
        ok = _restore_user_payload_in_con(con, payload)
        if not ok:
            con.rollback()
            return False, 0
        con.commit()
        return True, int(row["created_at"] or 0)


def restore_all_users_rollback(seconds_ago):
    target_ts = int(time.time()) - max(0, int(seconds_ago or 0))
    restored = 0
    with _connect() as con:
        rows = con.execute(
            """
            SELECT r.tg_id, r.created_at, r.payload
            FROM user_rollbacks r
            JOIN (
                SELECT tg_id, MAX(created_at) AS mx
                FROM user_rollbacks
                WHERE created_at <= ?
                GROUP BY tg_id
            ) x ON x.tg_id = r.tg_id AND x.mx = r.created_at
            ORDER BY r.tg_id ASC
            """,
            (target_ts,),
        ).fetchall()
        if not rows:
            return 0
        con.execute("BEGIN IMMEDIATE")
        try:
            for row in rows:
                payload = json.loads(str(row["payload"] or "{}"))
                if _restore_user_payload_in_con(con, payload):
                    restored += 1
            con.commit()
        except Exception:
            con.rollback()
            raise
    return restored


def transfer_account_progress(old_tg_id, new_tg_id, new_username=""):
    """Переносит полный прогресс с одного tg_id на другой.

    Возвращает (ok, reason).
    """
    old_id = int(old_tg_id)
    new_id = int(new_tg_id)
    if old_id <= 0 or new_id <= 0:
        return False, "bad_id"
    if old_id == new_id:
        return False, "same_id"

    with _connect() as con:
        old_user = con.execute("SELECT * FROM users WHERE tg_id = ?", (old_id,)).fetchone()
        if not old_user:
            return False, "old_not_found"
        new_user = con.execute("SELECT * FROM users WHERE tg_id = ?", (new_id,)).fetchone()

        con.execute("BEGIN IMMEDIATE")
        try:
            # Чистим существующие данные нового ID, чтобы не ловить UNIQUE-конфликты.
            con.execute("DELETE FROM inventory WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM saved_items WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM user_stats WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM active_battles WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM active_dungeons WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM world_boss_hits WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM promo_uses WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM contest_answers WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM guild_members WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM guild_join_requests WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM guild_boss_hits WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM referrals WHERE referred_id = ? OR referrer_id = ?", (new_id, new_id))
            con.execute("DELETE FROM item_enchants WHERE tg_id = ?", (new_id,))
            con.execute("DELETE FROM user_text_stats WHERE tg_id = ?", (new_id,))

            # Переносим строки, где tg_id является владельцем.
            con.execute("UPDATE inventory SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE saved_items SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE user_stats SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE item_enchants SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE user_text_stats SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE active_battles SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE active_dungeons SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE world_boss_hits SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE promo_uses SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE contest_answers SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE guild_members SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE guild_join_requests SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE guild_boss_hits SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))
            con.execute("UPDATE user_rollbacks SET tg_id = ? WHERE tg_id = ?", (new_id, old_id))

            # Перенос зависимостей, где ID участвует как ссылка.
            con.execute("UPDATE referrals SET referrer_id = ? WHERE referrer_id = ?", (new_id, old_id))
            con.execute("UPDATE referrals SET referred_id = ? WHERE referred_id = ?", (new_id, old_id))
            con.execute("UPDATE guilds SET owner_id = ? WHERE owner_id = ?", (new_id, old_id))
            con.execute("UPDATE active_contest SET owner_id = ? WHERE owner_id = ?", (new_id, old_id))
            con.execute("UPDATE promos SET created_by = ? WHERE created_by = ?", (new_id, old_id))
            con.execute("UPDATE world_boss_event SET winner_id = ? WHERE winner_id = ?", (new_id, old_id))
            con.execute("UPDATE artifact_trust SET owner_id = ? WHERE owner_id = ?", (new_id, old_id))
            con.execute("UPDATE artifact_trust SET holder_id = ? WHERE holder_id = ?", (new_id, old_id))

            # Обновляем/создаем профиль нового ID значениями старого.
            old_data = dict(old_user)
            old_data["tg_id"] = new_id
            target_username = str(new_username or "").strip()
            if not target_username and new_user:
                target_username = str(new_user["username"] or "").strip()
            if not target_username:
                target_username = str(old_data.get("username", "") or "").strip() or str(new_id)
            old_data["username"] = target_username

            cols = list(old_data.keys())
            col_sql = ", ".join(cols)
            placeholders = ", ".join(["?"] * len(cols))
            upd_cols = [c for c in cols if c != "tg_id"]
            upd_sql = ", ".join([f"{c} = excluded.{c}" for c in upd_cols])
            con.execute(
                f"INSERT INTO users ({col_sql}) VALUES ({placeholders}) "
                f"ON CONFLICT(tg_id) DO UPDATE SET {upd_sql}",
                [old_data[c] for c in cols],
            )
            con.execute("DELETE FROM users WHERE tg_id = ?", (old_id,))

            con.commit()
            return True, "ok"
        except Exception:
            con.rollback()
            raise


def upsert_artifact_trust(item_id, owner_id, holder_id, expires_at, item_name="", item_level=1, item_bonus=0):
    now_ts = int(time.time())
    with _connect() as con:
        con.execute(
            """
            INSERT INTO artifact_trust (
                item_id, owner_id, holder_id,
                item_name, item_level, item_bonus,
                expires_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                owner_id = excluded.owner_id,
                holder_id = excluded.holder_id,
                item_name = excluded.item_name,
                item_level = excluded.item_level,
                item_bonus = excluded.item_bonus,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at
            """,
            (
                int(item_id), int(owner_id), int(holder_id),
                str(item_name or ""), int(item_level or 1), int(item_bonus or 0),
                int(expires_at), now_ts,
            ),
        )


def get_artifact_trust(item_id):
    with _connect() as con:
        row = con.execute(
            """
            SELECT item_id, owner_id, holder_id,
                   item_name, item_level, item_bonus,
                   expires_at, created_at
            FROM artifact_trust
            WHERE item_id = ?
            """,
            (int(item_id),),
        ).fetchone()
        return row


def delete_artifact_trust(item_id):
    with _connect() as con:
        cur = con.execute("DELETE FROM artifact_trust WHERE item_id = ?", (int(item_id),))
        return int(cur.rowcount or 0) > 0


def list_expired_artifact_trust(now_ts):
    with _connect() as con:
        return con.execute(
            """
            SELECT item_id, owner_id, holder_id,
                   item_name, item_level, item_bonus,
                   expires_at, created_at
            FROM artifact_trust
            WHERE expires_at <= ?
            ORDER BY expires_at ASC, item_id ASC
            """,
            (int(now_ts),),
        ).fetchall()


def list_all_artifact_trust(limit=5000):
    with _connect() as con:
        return con.execute(
            """
            SELECT item_id, owner_id, holder_id,
                   item_name, item_level, item_bonus,
                   expires_at, created_at
            FROM artifact_trust
            ORDER BY created_at ASC, item_id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()


def _clear_artifact_slots_in_con(con, tg_id, item_id):
    for slot_key in ("artifact_slot_1", "artifact_slot_2", "artifact_slot_3"):
        con.execute(
            """
            UPDATE user_stats
            SET stat_value = 0
            WHERE tg_id = ? AND stat_key = ? AND stat_value = ?
            """,
            (int(tg_id), slot_key, int(item_id)),
        )


def transfer_artifact_to_trust(owner_id, holder_id, item_id, expires_at):
    """Передает ровно 1 копию артефакта в доверие и создает запись trust атомарно."""
    owner_id = int(owner_id)
    holder_id = int(holder_id)
    item_id = int(item_id)
    expires_at = int(expires_at)
    if owner_id <= 0 or holder_id <= 0:
        return False, "bad_id", 0
    if owner_id == holder_id:
        return False, "self", 0
    if item_id <= 0:
        return False, "item_not_found", 0
    if expires_at <= int(time.time()):
        return False, "bad_expire", 0

    with _connect() as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute(
                """
                SELECT id, tg_id, type, name, level, bonus, count, in_bank
                FROM inventory
                WHERE id = ? AND tg_id = ?
                """,
                (item_id, owner_id),
            ).fetchone()
            if not row or str(row["type"] or "") != "artifact":
                con.rollback()
                return False, "item_not_found", 0
            if int(row["in_bank"] or 0) == 1:
                con.rollback()
                return False, "in_bank", 0

            src_count = max(0, int(row["count"] or 0))
            if src_count <= 0:
                con.rollback()
                return False, "item_not_found", 0

            item_name = str(row["name"] or "")
            item_level = max(1, int(row["level"] or 1))
            item_bonus = int(row["bonus"] or 0)

            if src_count > 1:
                con.execute("UPDATE inventory SET count = ? WHERE id = ?", (src_count - 1, item_id))
                cur = con.execute(
                    """
                    INSERT INTO inventory (tg_id, type, name, level, bonus, count, in_bank)
                    VALUES (?, 'artifact', ?, ?, ?, 1, 0)
                    """,
                    (holder_id, item_name, item_level, item_bonus),
                )
                trusted_item_id = int(cur.lastrowid)
            else:
                con.execute(
                    "UPDATE inventory SET tg_id = ?, in_bank = 0, count = 1 WHERE id = ?",
                    (holder_id, item_id),
                )
                con.execute("DELETE FROM saved_items WHERE item_id = ?", (item_id,))
                trusted_item_id = item_id
                _clear_artifact_slots_in_con(con, owner_id, item_id)

            now_ts = int(time.time())
            con.execute(
                """
                INSERT INTO artifact_trust (
                    item_id, owner_id, holder_id,
                    item_name, item_level, item_bonus,
                    expires_at, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    holder_id = excluded.holder_id,
                    item_name = excluded.item_name,
                    item_level = excluded.item_level,
                    item_bonus = excluded.item_bonus,
                    expires_at = excluded.expires_at,
                    created_at = excluded.created_at
                """,
                (
                    int(trusted_item_id), owner_id, holder_id,
                    item_name, item_level, item_bonus,
                    expires_at, now_ts,
                ),
            )
            con.commit()
            return True, "ok", int(trusted_item_id)
        except Exception:
            con.rollback()
            raise


def _return_artifact_trust_in_con(con, row):
    item_id = int(row["item_id"] or 0)
    owner_id = int(row["owner_id"] or 0)
    holder_id = int(row["holder_id"] or 0)
    item_name = str(row["item_name"] or "")
    item_level = max(1, int(row["item_level"] or 1))
    item_bonus = int(row["item_bonus"] or 0)
    if item_id <= 0 or owner_id <= 0:
        return False, owner_id, holder_id

    inv = con.execute(
        "SELECT id, tg_id, type, name, level, bonus, count FROM inventory WHERE id = ?",
        (item_id,),
    ).fetchone()

    if inv and str(inv["type"] or "") == "artifact":
        cur_owner = int(inv["tg_id"] or 0)
        cur_count = max(1, int(inv["count"] or 1))
        name = str(inv["name"] or item_name)
        lvl = max(1, int(inv["level"] or item_level))
        bonus = int(inv["bonus"] or item_bonus)

        if cur_count > 1 and cur_owner != owner_id:
            con.execute("UPDATE inventory SET count = ? WHERE id = ?", (cur_count - 1, item_id))
            _upsert_inventory_in_con(con, owner_id, "artifact", name, lvl, bonus, 1)
        elif cur_owner != owner_id:
            # Возвращаем единицу артефакта владельцу через upsert, чтобы
            # не плодить дубли x1/x1 вместо одного стака x2.
            con.execute("DELETE FROM inventory WHERE id = ?", (item_id,))
            _upsert_inventory_in_con(con, owner_id, "artifact", name, lvl, bonus, 1)
    else:
        _upsert_inventory_in_con(con, owner_id, "artifact", item_name, item_level, item_bonus, 1)

    _clear_artifact_slots_in_con(con, holder_id, item_id)
    con.execute("DELETE FROM artifact_trust WHERE item_id = ?", (item_id,))
    return True, owner_id, holder_id


def return_artifact_trust(item_id):
    """Принудительно возвращает доверенный артефакт владельцу (атомарно)."""
    item_id = int(item_id)
    if item_id <= 0:
        return False, 0, 0, "bad_item"
    with _connect() as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute(
                """
                SELECT item_id, owner_id, holder_id, item_name, item_level, item_bonus, expires_at, created_at
                FROM artifact_trust
                WHERE item_id = ?
                """,
                (item_id,),
            ).fetchone()
            if not row:
                con.rollback()
                return False, 0, 0, "not_found"
            ok, owner_id, holder_id = _return_artifact_trust_in_con(con, row)
            if not ok:
                con.rollback()
                return False, owner_id, holder_id, "bad_row"
            con.commit()
            return True, owner_id, holder_id, "ok"
        except Exception:
            con.rollback()
            raise


# ─────────────────────────────────────────────
#  ЗАЧАРОВАНИЯ ПРЕДМЕТОВ
# ─────────────────────────────────────────────

def get_item_enchant(item_id: int, enchant_key: str) -> int:
    """Возвращает уровень зачарования (0 если нет)."""
    with _connect() as con:
        row = con.execute(
            "SELECT level FROM item_enchants WHERE item_id = ? AND enchant_key = ?",
            (int(item_id), enchant_key),
        ).fetchone()
        return int(row["level"]) if row else 0


def get_item_enchants(item_id: int) -> dict:
    """Возвращает словарь {enchant_key: level} для предмета."""
    with _connect() as con:
        rows = con.execute(
            "SELECT enchant_key, level FROM item_enchants WHERE item_id = ?",
            (int(item_id),),
        ).fetchall()
        return {r["enchant_key"]: int(r["level"]) for r in rows}


def get_enchants_for_items(item_ids: list) -> dict:
    """Batch-загрузка зачарований для списка item_id.
    Возвращает {item_id: {enchant_key: level}}."""
    if not item_ids:
        return {}
    with _connect() as con:
        placeholders = ",".join("?" * len(item_ids))
        rows = con.execute(
            f"SELECT item_id, enchant_key, level FROM item_enchants WHERE item_id IN ({placeholders})",
            [int(i) for i in item_ids],
        ).fetchall()
    result: dict = {}
    for r in rows:
        iid = int(r["item_id"])
        result.setdefault(iid, {})[r["enchant_key"]] = int(r["level"])
    return result


def get_user_enchants(tg_id: int) -> list:
    """Все зачарования пользователя (для отображения)."""
    with _connect() as con:
        return con.execute(
            "SELECT item_id, enchant_key, level FROM item_enchants WHERE tg_id = ? ORDER BY item_id",
            (int(tg_id),),
        ).fetchall()


def set_item_enchant(tg_id: int, item_id: int, enchant_key: str, level: int) -> bool:
    """Ставит/обновляет зачарование на предмет. level=0 — удалить."""
    tg_id = int(tg_id)
    item_id = int(item_id)
    level = int(level)
    with _connect() as con:
        if level <= 0:
            con.execute(
                "DELETE FROM item_enchants WHERE item_id = ? AND enchant_key = ?",
                (item_id, enchant_key),
            )
        else:
            con.execute(
                """
                INSERT INTO item_enchants (item_id, tg_id, enchant_key, level)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(item_id, enchant_key) DO UPDATE SET level = excluded.level, tg_id = excluded.tg_id
                """,
                (item_id, tg_id, enchant_key, level),
            )
        return True


def delete_item_enchants(item_id: int):
    """Удаляет все зачарования предмета (при продаже/удалении)."""
    with _connect() as con:
        con.execute("DELETE FROM item_enchants WHERE item_id = ?", (int(item_id),))


def is_nickname_taken(nickname: str, exclude_tg_id: int = 0) -> bool:
    """Проверяет, занят ли ник другим игроком (регистронезависимо)."""
    with _connect() as con:
        row = con.execute(
            "SELECT tg_id FROM users WHERE LOWER(nickname) = LOWER(?) AND tg_id != ?",
            (str(nickname).strip(), int(exclude_tg_id)),
        ).fetchone()
        return row is not None