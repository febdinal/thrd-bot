#!/usr/bin/env python3
"""
Bot Threads + AI untuk dijalankan di VPS (via cron).

Perintah:
  python bot.py cek              -> tes koneksi Threads & AI
  python bot.py posting          -> AI membuat 1 post dari topik.txt lalu memposting
  python bot.py balas            -> AI membalas komentar di post kamu + mention dari orang lain
  python bot.py cari             -> cari post orang lain berdasar kata kunci -> DRAF ke antrian
  python bot.py antrian          -> lihat draf balasan yang menunggu persetujuan
  python bot.py setujui <id>     -> kirim draf nomor <id>
  python bot.py tolak <id>       -> buang draf nomor <id>
  python bot.py refresh-token    -> paksa perpanjang token Threads

Semua pengaturan ada di file .env (lihat env.example).
"""

import json
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import re

import httpx
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent
load_dotenv(BASE / ".env")

API = "https://graph.threads.net/v1.0"
TOKEN_FILE = BASE / "token.json"
DB_FILE = BASE / "bot.db"


def env(name, default=None, cast=str):
    v = os.environ.get(name, default)
    if v is None:
        return None
    if cast is bool:
        return str(v).strip().lower() in ("1", "true", "ya", "yes")
    return cast(v)


DRY_RUN = env("DRY_RUN", "true", bool)          # true = tidak benar-benar memposting
# Gateway AI kompatibel OpenAI (sama seperti wabot-app), mis. https://bankai.web.id/api
# Chat dikirim ke {AI_BASE_URL}/v1/chat/completions. BASE_URL/API_KEY dipakai bila AI_* kosong.
AI_BASE_URL = (env("AI_BASE_URL", "") or env("BASE_URL", "") or "").strip().rstrip("/")
AI_API_KEY = (env("AI_API_KEY", "") or env("API_KEY", "") or "").strip()
MODEL = env("AI_MODEL", "deepseek-v4.1-flash")
AI_TIMEOUT = 45
MAX_BALAS_PER_JALAN = env("MAX_BALAS_PER_JALAN", "8", int)
MAX_BALAS_PER_ORANG = env("MAX_BALAS_PER_ORANG", "3", int)  # per utas, cegah debat tanpa akhir
UMUR_MAKS_JAM = env("UMUR_KOMENTAR_MAKS_JAM", "48", int)
POST_DIPANTAU = env("JUMLAH_POST_DIPANTAU", "5", int)
BALAS_MENTION = env("BALAS_MENTION", "true", bool)
KATA_KUNCI = [k.strip() for k in env("KATA_KUNCI", "").split(",") if k.strip()]
MAX_DRAF_CARI = env("MAX_DRAF_CARI", "5", int)


def log(*a):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]", *a, flush=True)


# ---------------------------------------------------------------- database
def db():
    con = sqlite3.connect(DB_FILE)
    con.execute("""CREATE TABLE IF NOT EXISTS dibalas (
        target_id TEXT PRIMARY KEY, root_id TEXT, username TEXT,
        balasan TEXT, waktu TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS antrian (
        id INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT UNIQUE,
        username TEXT, teks_asli TEXT, permalink TEXT, draf TEXT,
        status TEXT DEFAULT 'menunggu', waktu TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS post_saya (
        id TEXT PRIMARY KEY, teks TEXT, waktu TEXT)""")
    return con


def sudah_dibalas(con, target_id):
    return con.execute("SELECT 1 FROM dibalas WHERE target_id=?",
                       (target_id,)).fetchone() is not None


def jumlah_balasan_ke(con, root_id, username):
    return con.execute("SELECT COUNT(*) FROM dibalas WHERE root_id=? AND username=?",
                       (root_id, username)).fetchone()[0]


def catat_balasan(con, target_id, root_id, username, teks):
    if DRY_RUN:  # simulasi tidak dicatat, supaya nanti tetap dibalas sungguhan
        return
    con.execute("INSERT OR REPLACE INTO dibalas VALUES (?,?,?,?,?)",
                (target_id, root_id, username, teks, datetime.now().isoformat()))
    con.commit()


# ---------------------------------------------------------------- token
def load_token():
    tok = (env("THREADS_ACCESS_TOKEN", "") or "").strip()
    if TOKEN_FILE.exists():
        data = json.loads(TOKEN_FILE.read_text())
        # token di .env diganti (mis. setelah menambah izin) -> pakai token baru itu
        if not tok or data.get("env_token") in (None, tok):
            if "env_token" not in data and tok:
                data["env_token"] = tok
                save_token(data)
            return data
        log("Token baru terdeteksi di .env, token.json diperbarui.")
    if not tok:
        sys.exit("THREADS_ACCESS_TOKEN belum diisi di .env")
    data = {"access_token": tok, "env_token": tok, "refreshed_at": datetime.now().isoformat()}
    save_token(data)
    return data


def save_token(data):
    TOKEN_FILE.write_text(json.dumps(data, indent=2))
    os.chmod(TOKEN_FILE, 0o600)


def token():
    return load_token()["access_token"]


def refresh_token(force=False):
    """Token berlaku 60 hari. Diperpanjang otomatis tiap 7 hari."""
    data = load_token()
    last = datetime.fromisoformat(data["refreshed_at"])
    if not force and datetime.now() - last < timedelta(days=7):
        return
    r = httpx.get("https://graph.threads.net/refresh_access_token",
                  params={"grant_type": "th_refresh_token",
                          "access_token": data["access_token"]}, timeout=30)
    j = r.json()
    if "access_token" in j:
        save_token({"access_token": j["access_token"], "env_token": data.get("env_token"),
                    "refreshed_at": datetime.now().isoformat()})
        log(f"Token diperpanjang, berlaku {int(j.get('expires_in', 0)) // 86400} hari.")
    else:
        log("GAGAL perpanjang token:", j)


# ---------------------------------------------------------------- Threads API
def th(method, path, **params):
    params = {k: v for k, v in params.items() if v is not None}
    params["access_token"] = token()
    url = path if path.startswith("http") else f"{API}{path}"
    with httpx.Client(timeout=30) as c:
        r = c.get(url, params=params) if method == "GET" else c.post(url, data=params)
    try:
        j = r.json()
    except ValueError:
        j = {"raw": r.text}
    if r.status_code >= 400 or "error" in j:
        raise RuntimeError(f"Threads API {r.status_code}: {j.get('error', j)}")
    return j


_ME = None


def me():
    global _ME
    if _ME is None:
        _ME = th("GET", "/me", fields="id,username")
    return _ME


def publish(text, reply_to_id=None):
    text = text.strip()
    if len(text) > 500:
        text = text[:497].rsplit(" ", 1)[0] + "..."
    if DRY_RUN:
        log(f"[DRY_RUN] akan memposting{' balasan ke ' + reply_to_id if reply_to_id else ''}:\n    {text}")
        return {"id": "dry-run"}
    cont = th("POST", "/me/threads", media_type="TEXT", text=text, reply_to_id=reply_to_id)
    err = None
    for _ in range(5):
        time.sleep(3)
        try:
            return th("POST", "/me/threads_publish", creation_id=cont["id"])
        except RuntimeError as e:
            err = e
    raise RuntimeError(f"Gagal publish: {err}")


def parse_time(ts):
    # format Threads: 2026-11-19T03:59:21+0000
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")


def masih_baru(item):
    ts = item.get("timestamp")
    if not ts:
        return True
    return datetime.now(timezone.utc) - parse_time(ts) < timedelta(hours=UMUR_MAKS_JAM)


# ---------------------------------------------------------------- AI
def ai_endpoint(path):
    return f"{AI_BASE_URL}{path}" if AI_BASE_URL.endswith("/v1") else f"{AI_BASE_URL}/v1{path}"


def rapikan(teks):
    """Threads tidak menampilkan markdown: buang <think>, **tebal**, judul #, dan blok kode."""
    teks = re.sub(r"<think>.*?</think>", "", teks, flags=re.S | re.I)
    teks = re.sub(r"```[a-zA-Z0-9]*\n?", "", teks)
    teks = re.sub(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$", r"\1", teks, flags=re.M)
    teks = re.sub(r"^\s*[*•]\s+", "- ", teks, flags=re.M)
    teks = re.sub(r"\*\*(.+?)\*\*", r"\1", teks)
    teks = re.sub(r"__(.+?)__", r"\1", teks)
    teks = re.sub(r"[ \t]+\n", "\n", teks)
    teks = re.sub(r"\n{3,}", "\n\n", teks)
    return teks.strip()


def _alasan_error(status, detail):
    alasan = ("API key AI ditolak" if status in (401, 403) else
              "Saldo AI tidak cukup" if status == 402 else
              "Terlalu banyak permintaan ke AI" if status == 429 else
              "Layanan AI sedang bermasalah" if status >= 500 else
              f"AI menolak permintaan ({status})")
    return f"{alasan}: {detail}"[:255] if detail else alasan


def ai(system, prompt, max_tokens=500):
    """Satu tanya-jawab ke gateway AI (bankAI), cara pemanggilannya sama dengan wabot-app."""
    if not AI_BASE_URL or not AI_API_KEY:
        sys.exit("AI belum dikonfigurasi (isi AI_BASE_URL dan AI_API_KEY di .env)")
    body = {
        "model": MODEL,
        "max_tokens": max(max_tokens, 500),
        "temperature": 0.6,
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": prompt}],
    }
    err = None
    for coba in range(3):
        try:
            r = httpx.post(ai_endpoint("/chat/completions"), json=body, timeout=AI_TIMEOUT,
                           headers={"Content-Type": "application/json",
                                    "Authorization": f"Bearer {AI_API_KEY}"})
        except httpx.TimeoutException:
            err = "AI tidak merespons (timeout)"
        except httpx.HTTPError as e:
            err = f"Gagal menghubungi layanan AI: {e}"[:255]
        else:
            try:
                j = r.json()
            except ValueError:
                j = {}
            if r.is_success:
                content = ((j.get("choices") or [{}])[0].get("message") or {}).get("content")
                teks = rapikan(content) if isinstance(content, str) else ""
                if not teks:
                    raise RuntimeError("AI mengembalikan balasan kosong")
                return teks
            e = j.get("error")
            detail = (e if isinstance(e, str) else (e or {}).get("message")) or j.get("message") or ""
            err = _alasan_error(r.status_code, detail)
            if r.status_code in (401, 402, 403):
                sys.exit(err)          # key salah / saldo habis: percuma diulang
            if r.status_code != 429 and r.status_code < 500:
                raise RuntimeError(err)
        time.sleep(5 * (coba + 1))     # timeout, 429, 5xx: coba lagi
    raise RuntimeError(err)


def persona():
    f = BASE / "persona.txt"
    return f.read_text(encoding="utf-8") if f.exists() else "Kamu pengelola akun Threads."


ATURAN_BALAS = """
Aturan saat membalas:
- Balas dalam bahasa yang sama dengan komentar. Singkat (maks 2-3 kalimat, < 400 karakter).
- Nyambung dengan isi komentar, jangan generik, jangan promosi, jangan menyebar link.
- Jangan mengarang fakta, angka, atau janji atas nama pemilik akun.
- Kalau komentar berisi keluhan serius, pertanyaan pribadi/sensitif, ujaran kebencian,
  spam, provokasi, atau kamu ragu -> jawab persis dengan satu kata: SKIP
- Tulis HANYA teks balasannya, tanpa tanda kutip atau penjelasan.
"""


def buat_balasan(konteks, komentar, username):
    out = ai(persona() + "\n" + ATURAN_BALAS,
             f"Konteks utas:\n{konteks}\n\nKomentar dari @{username}:\n{komentar}\n\nTulis balasan:")
    out = out.strip().strip('"')
    return None if out.upper().startswith("SKIP") or not out else out


# ---------------------------------------------------------------- perintah
def cmd_cek():
    m = me()
    log(f"Threads OK: @{m['username']} (id {m['id']})")
    lim = th("GET", "/me/threads_publishing_limit",
             fields="quota_usage,config,reply_quota_usage,reply_config")["data"][0]
    log(f"Kuota post: {lim['quota_usage']}/{lim['config']['quota_total']}, "
        f"balasan: {lim.get('reply_quota_usage')}/{lim.get('reply_config', {}).get('quota_total')}")
    log("AI OK:", ai("Jawab singkat.", "Balas dengan kata: siap", 20))
    log(f"DRY_RUN = {DRY_RUN}")


def cmd_posting():
    con = db()
    topik_f = BASE / "topik.txt"
    topik = [t.strip() for t in topik_f.read_text(encoding="utf-8").splitlines()
             if t.strip() and not t.startswith("#")] if topik_f.exists() else []
    if not topik:
        sys.exit("Isi dulu topik.txt (satu topik per baris).")
    t = random.choice(topik)
    terakhir = [r[0] for r in con.execute(
        "SELECT teks FROM post_saya ORDER BY waktu DESC LIMIT 10")]
    prompt = (f"Topik: {t}\n\nPost terakhir (jangan diulang idenya):\n- " +
              "\n- ".join(terakhir or ["(belum ada)"]) +
              "\n\nTulis SATU post Threads baru, maks 450 karakter, natural seperti manusia, "
              "boleh diakhiri pertanyaan untuk memancing diskusi. Tanpa hashtag berlebihan. "
              "Tulis HANYA isi post-nya.")
    teks = ai(persona(), prompt, 500).strip().strip('"')
    if not teks:
        sys.exit("AI tidak menghasilkan teks, posting dibatalkan.")
    res = publish(teks)
    if DRY_RUN:
        return
    con.execute("INSERT OR REPLACE INTO post_saya VALUES (?,?,?)",
                (res["id"], teks, datetime.now().isoformat()))
    con.commit()
    log(f"Post selesai (topik: {t}) id={res['id']}")


def cmd_balas():
    con = db()
    m = me()
    sisa = MAX_BALAS_PER_JALAN

    # 1) komentar di post milik sendiri
    posts = th("GET", "/me/threads", fields="id,text,timestamp,media_type",
               limit=POST_DIPANTAU).get("data", [])
    for p in posts:
        if sisa <= 0:
            break
        try:
            conv = th("GET", f"/{p['id']}/conversation",
                      fields="id,text,username,timestamp,replied_to", reverse="false").get("data", [])
        except RuntimeError as e:
            log("Lewati post", p["id"], e)
            continue
        by_id = {c["id"]: c for c in conv}
        for c in conv:
            if sisa <= 0:
                break
            if c.get("username") == m["username"] or not c.get("text"):
                continue
            if sudah_dibalas(con, c["id"]) or not masih_baru(c):
                continue
            if jumlah_balasan_ke(con, p["id"], c["username"]) >= MAX_BALAS_PER_ORANG:
                continue
            # jangan balas kalau komentar ini sudah kita balas manual
            if any(x.get("username") == m["username"] and
                   (x.get("replied_to") or {}).get("id") == c["id"] for x in conv):
                catat_balasan(con, c["id"], p["id"], c["username"], "(sudah dibalas manual)")
                continue
            parent = by_id.get((c.get("replied_to") or {}).get("id"))
            konteks = f"Post saya: {p.get('text', '')}"
            if parent and parent["id"] != p["id"]:
                konteks += f"\nKomentar sebelumnya dari @{parent.get('username')}: {parent.get('text', '')}"
            balasan = buat_balasan(konteks, c["text"], c["username"])
            if not balasan:
                log(f"SKIP komentar @{c['username']}: {c['text'][:60]!r}")
                catat_balasan(con, c["id"], p["id"], c["username"], "(skip)")
                continue
            publish(balasan, reply_to_id=c["id"])
            catat_balasan(con, c["id"], p["id"], c["username"], balasan)
            sisa -= 1
            log(f"Balas @{c['username']}: {balasan[:80]}")
            time.sleep(random.uniform(20, 60) if not DRY_RUN else 0)

    # 2) mention: orang lain menandai akun kamu di utas mereka
    if BALAS_MENTION and sisa > 0:
        try:
            mentions = th("GET", "/me/mentions",
                          fields="id,text,username,timestamp,permalink").get("data", [])
        except RuntimeError as e:
            log("Mention tidak bisa dibaca (cek izin threads_manage_mentions):", e)
            mentions = []
        for mt in mentions:
            if sisa <= 0:
                break
            if mt.get("username") == m["username"] or sudah_dibalas(con, mt["id"]):
                continue
            if not masih_baru(mt) or not mt.get("text"):
                continue
            balasan = buat_balasan(f"@{mt['username']} menandai saya di utas mereka.",
                                   mt["text"], mt["username"])
            if not balasan:
                catat_balasan(con, mt["id"], mt["id"], mt["username"], "(skip)")
                continue
            try:
                publish(balasan, reply_to_id=mt["id"])
            except RuntimeError as e:
                log("Gagal balas mention:", e)
                continue
            catat_balasan(con, mt["id"], mt["id"], mt["username"], balasan)
            sisa -= 1
            log(f"Balas mention @{mt['username']}: {balasan[:80]}")
            time.sleep(random.uniform(20, 60) if not DRY_RUN else 0)

    log(f"Selesai. {MAX_BALAS_PER_JALAN - sisa} balasan dibuat.")


def cmd_cari():
    """Cari post orang lain -> buat DRAF, tidak langsung dikirim."""
    if not KATA_KUNCI:
        sys.exit("Isi KATA_KUNCI di .env, contoh: KATA_KUNCI=analisis data,python")
    con = db()
    m = me()
    dibuat = 0
    for kw in KATA_KUNCI:
        try:
            hasil = th("GET", "/keyword_search", q=kw, search_type="RECENT",
                       fields="id,text,username,timestamp,permalink").get("data", [])
        except RuntimeError as e:
            log(f"Pencarian '{kw}' gagal (butuh izin threads_keyword_search):", e)
            continue
        if hasil and all(h.get("username") == m["username"] for h in hasil):
            log(f"'{kw}': hanya post milik sendiri yang muncul -> izin keyword search "
                "belum lolos App Review, jadi post publik orang lain belum bisa dicari.")
        for h in hasil:
            if dibuat >= MAX_DRAF_CARI:
                break
            if h.get("username") == m["username"] or not h.get("text") or not masih_baru(h):
                continue
            if con.execute("SELECT 1 FROM antrian WHERE target_id=?", (h["id"],)).fetchone():
                continue
            draf = buat_balasan(f"Post publik dari @{h['username']} (topik: {kw})",
                                h["text"], h["username"])
            if not draf:
                continue
            con.execute("INSERT INTO antrian (target_id, username, teks_asli, permalink, draf, waktu)"
                        " VALUES (?,?,?,?,?,?)",
                        (h["id"], h["username"], h["text"], h.get("permalink", ""), draf,
                         datetime.now().isoformat()))
            con.commit()
            dibuat += 1
    log(f"{dibuat} draf baru masuk antrian. Lihat dengan: python bot.py antrian")


def cmd_antrian():
    rows = db().execute("SELECT id, username, teks_asli, permalink, draf FROM antrian "
                        "WHERE status='menunggu' ORDER BY id").fetchall()
    if not rows:
        print("Antrian kosong.")
    for i, u, asli, link, draf in rows:
        print(f"\n#{i}  @{u}  {link}\n  Post : {asli[:200]}\n  Draf : {draf}")


def cmd_setujui(id_):
    con = db()
    row = con.execute("SELECT target_id, username, draf FROM antrian WHERE id=? AND status='menunggu'",
                      (id_,)).fetchone()
    if not row:
        sys.exit("Draf tidak ditemukan.")
    publish(row[2], reply_to_id=row[0])
    if DRY_RUN:
        log("DRY_RUN aktif: draf tidak benar-benar dikirim & tetap di antrian.")
        return
    con.execute("UPDATE antrian SET status='terkirim' WHERE id=?", (id_,))
    catat_balasan(con, row[0], row[0], row[1], row[2])
    log(f"Draf #{id_} terkirim ke @{row[1]}")


def cmd_tolak(id_):
    con = db()
    con.execute("UPDATE antrian SET status='ditolak' WHERE id=?", (id_,))
    con.commit()
    log(f"Draf #{id_} dibuang.")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    c = sys.argv[1]
    if c != "refresh-token":
        refresh_token()
    if c == "cek":
        cmd_cek()
    elif c == "posting":
        cmd_posting()
    elif c == "balas":
        cmd_balas()
    elif c == "cari":
        cmd_cari()
    elif c == "antrian":
        cmd_antrian()
    elif c == "setujui" and len(sys.argv) > 2:
        cmd_setujui(int(sys.argv[2]))
    elif c == "tolak" and len(sys.argv) > 2:
        cmd_tolak(int(sys.argv[2]))
    elif c == "refresh-token":
        refresh_token(force=True)
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
