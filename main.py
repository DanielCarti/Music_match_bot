# main.py — двухшаговый диалог: сначала A, потом B
import os
import re
import csv
import io
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import telebot
from telebot import types
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import vkpymusic

# === токен только из env ===
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise SystemExit("ERROR: set TELEGRAM_BOT_TOKEN env var")

bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome Safari")
HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json,*/*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
    "Referer": "https://music.yandex.ru/",
    "Connection": "keep-alive",
}
PAGE_SIZE = 200
MAX_PAGES = 80
WINDOW = 6
HTTP_TIMEOUT = 10
WALL_CLOCK_LIMIT = 70

RE_YANDEX_USERS = re.compile(r'https?://music\.yandex\.ru/users/([^/]+)/playlists/(\d+)')
RE_YANDEX_IFR   = re.compile(r'https?://music\.yandex\.ru/iframe/playlist/([^/]+)/(\d+)')
RE_SPOTIFY = re.compile(r'https?://open\.spotify\.com/playlist/([a-zA-Z0-9]+)')
RE_VK = re.compile(r'https?://vk\.com/music/playlist/([0-9]+)_([0-9]+)')
RE_SRC   = re.compile(r'src="([^"]+)"')

# Spotify API credentials
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")


# ===== простейшее состояние диалога по пользователю =====
# state[user_id] = {"stage": "await_a"|"await_b"|None, "a": str|None, "b": str|None}
state: dict[int, dict] = {}

def reset_state(uid: int):
    state[uid] = {"stage": "await_a", "a": None, "b": None}

def canon_url(s: str) -> tuple[str, str]:
    s = s.strip()
    m = RE_SRC.search(s)
    if m:
        s = m.group(1)

    m_ya_ifr = RE_YANDEX_IFR.search(s)
    if m_ya_ifr:
        return "yandex", f"https://music.yandex.ru/users/{m_ya_ifr.group(1)}/playlists/{m_ya_ifr.group(2)}"

    m_ya_users = RE_YANDEX_USERS.search(s)
    if m_ya_users:
        return "yandex", m_ya_users.group(0)

    m_spotify = RE_SPOTIFY.search(s)
    if m_spotify:
        return "spotify", m_spotify.group(0)

    m_vk = RE_VK.search(s)
    if m_vk:
        return "vk", m_vk.group(0)

    raise ValueError("Нужна ссылка на плейлист Яндекс.Музыки, Spotify или VK")

def normalize(txt: str) -> str:
    x = txt.lower()
    x = re.sub(r'[\(\[\{].*?[\)\]\}]', '', x)
    x = re.sub(r'\bfeat\.?\b|\bft\.?\b|\bfeaturing\b', '', x)
    x = x.replace('&', 'and')
    x = re.sub(r'[^a-zа-я0-9\s\-]+', ' ', x, flags=re.U)
    x = re.sub(r'\s+', ' ', x).strip()
    return x

def ru_tracks(n: int) -> str:
    m10, m100 = n % 10, n % 100
    if m10 == 1 and m100 != 11: return "трек"
    if 2 <= m10 <= 4 and not (12 <= m100 <= 14): return "трека"
    return "треков"

session = requests.Session()
session.headers.update(HEADERS)

def fetch_yandex_page(user: str, pid: str, page: int, use_controller: bool):
    base = "playlist-controller.jsx" if use_controller else "playlist.jsx"
    url = (f"https://music.yandex.ru/handlers/{base}"
           f"?owner={user}&kinds={pid}&light=false&rich-tracks=true"
           f"&lang=ru&external-domain=music.yandex.ru&overembed=false"
           f"&page={page}&page-size={PAGE_SIZE}")
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []
        data = r.json()
        if use_controller:
            lst = data.get("tracks")
        else:
            pl = data.get("playlist", data) or {}
            lst = pl.get("tracks")
        if not isinstance(lst, list) or not lst:
            return []
        out = []
        for item in lst:
            t = item if use_controller else (item.get("track") if isinstance(item, dict) else None)
            if not isinstance(t, dict):
                t = item if isinstance(item, dict) else None
            if not t:
                continue
            title = str(t.get("title", "")).strip()
            a = t.get("artists")
            if isinstance(a, list):
                artist = ", ".join([(x.get("name") if isinstance(x, dict) else str(x)) for x in a if str(x).strip()])
            else:
                artist = str(a or "").strip()
            if not title or not artist:
                continue
            out.append((artist, title, normalize(f"{artist} - {title}")))
        return out
    except Exception:
        return []

def fetch_yandex_tracks(canon: str):
    m = RE_YANDEX_USERS.search(canon)
    if not m:
        raise ValueError("Плохая ссылка")
    user, pid = m.group(1), m.group(2)

    start_ts = time.time()
    all_tracks = []
    use_controller = True

    for start in range(0, MAX_PAGES, WINDOW):
        if time.time() - start_ts > WALL_CLOCK_LIMIT:
            break

        pages = list(range(start, min(start + WINDOW, MAX_PAGES)))
        window_out = []

        with ThreadPoolExecutor(max_workers=WINDOW) as ex:
            futures = [ex.submit(fetch_yandex_page, user, pid, p, use_controller) for p in pages]
            for f in as_completed(futures):
                res = f.result()
                if res:
                    window_out.append(res)

        if not window_out:
            if use_controller:
                use_controller = False
                with ThreadPoolExecutor(max_workers=WINDOW) as ex:
                    futures = [ex.submit(fetch_yandex_page, user, pid, p, use_controller) for p in pages]
                    for f in as_completed(futures):
                        res = f.result()
                        if res:
                            window_out.append(res)
            if not window_out:
                break

        for lst in window_out:
            all_tracks.extend(lst)

        last = next((lst for lst in reversed(window_out) if lst), [])
        if last and len(last) < PAGE_SIZE:
            break

    # dedup по нормализованной строке
    seen, uniq = set(), []
    for a, t, n in all_tracks:
        if n in seen: continue
        seen.add(n)
        uniq.append((a, t, n))
    return uniq

def fetch_spotify_tracks(canon: str):
    if not SPOTIPY_CLIENT_ID or not SPOTIPY_CLIENT_SECRET:
        raise ValueError("Spotify API credentials (SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET) not set")

    auth_manager = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    m = RE_SPOTIFY.search(canon)
    if not m:
        raise ValueError("Плохая ссылка на плейлист Spotify")
    playlist_id = m.group(1)

    results = sp.playlist_tracks(playlist_id)
    tracks = results['items']
    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])

    out = []
    for item in tracks:
        track = item.get('track')
        if not track:
            continue
        title = str(track.get("name", "")).strip()
        artists = track.get("artists")
        if isinstance(artists, list):
            artist = ", ".join([a.get("name") for a in artists if a.get("name")])
        else:
            artist = str(artists or "").strip()
        if not title or not artist:
            continue
        out.append((artist, title, normalize(f"{artist} - {title}")))
    return out

def reset_state(uid: int):
    state[uid] = {"stage": "await_a", "a_url": None, "b_url": None, "a_service": None, "b_service": None}


def fetch_vk_tracks(canon: str):
    access_token = os.getenv("VK_ACCESS_TOKEN")
    if not access_token:
        raise ValueError("VK access token (VK_ACCESS_TOKEN) not set")

    vk = vkpymusic.Service(access_token=access_token)
    m = RE_VK.search(canon)
    if not m:
        raise ValueError("Плохая ссылка на плейлист VK")
    owner_id, playlist_id = m.group(1), m.group(2)

    playlist = vk.playlist(owner_id=int(owner_id), playlist_id=int(playlist_id))
    if not playlist:
        return []

    out = []
    for track in playlist["tracks"]:
        title = str(track.get("title", "")).strip()
        artist = str(track.get("artist", "")).strip()
        if not title or not artist:
            continue
        out.append((artist, title, normalize(f"{artist} - {title}")))
    return out

def fetch_tracks(service: str, canon: str):
    if service == "yandex":
        return fetch_yandex_tracks(canon)
    if service == "spotify":
        return fetch_spotify_tracks(canon)
    if service == "vk":
        return fetch_vk_tracks(canon)
    raise ValueError(f"Unknown service: {service}")

def compare_exact(A, B):
    setB = {n for _,_,n in B}
    matches = [(a, t) for a, t, n in A if n in setB]
    ratio = len(matches) / max(1, len(A))
    return ratio, matches

# ========= команды =========
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m: types.Message):
    reset_state(m.from_user.id)
    bot.reply_to(
        m,
        "Кидай ссылку на <b>плейлист A</b> (Яндекс.Музыка, Spotify, VK Музыка), "
        "потом на <b>плейлист B</b>. Я пришлю совпадения в TXT+CSV.\n\n"
        "<b>Для работы с Spotify и VK Музыкой нужно настроить переменные окружения:</b>\n"
        "- `SPOTIPY_CLIENT_ID` и `SPOTIPY_CLIENT_SECRET` для Spotify. "
        "Их можно получить, создав приложение на "
        "<a href=\"https://developer.spotify.com/dashboard/applications\">Spotify Developer Dashboard</a>.\n"
        "- `VK_ACCESS_TOKEN` для VK Музыки. "
        "Его можно получить, например, через <a href=\"https://vkhost.github.io/\">VK Host</a>.\n\n"
        "Команды: /cancel — отменить диалог;"
    )

@bot.message_handler(commands=['cancel', 'reset'])
def cmd_cancel(m: types.Message):
    reset_state(m.from_user.id)
    bot.reply_to(m, "ок, отменил. пришли новую ссылку на плейлист A;")

# ========= основной текстовый хендлер =========
@bot.message_handler(content_types=['text'])
def handle_text(m: types.Message):
    uid = m.from_user.id
    if uid not in state:
        reset_state(uid)

    st = state[uid]["stage"]
    text = m.text.strip()

    # игнорируем многострочные сообщения — диалог теперь пошаговый
    if "\n" in text:
        bot.reply_to(m, "пришли одну ссылку в сообщении. сейчас ждём только один URL;")
        return

    try:
        service, url = canon_url(text)
    except Exception as e:
        bot.reply_to(m, f"ошибка в ссылке: {e}")
        return

    # шаг A
    if st == "await_a":
        state[uid]["a_service"] = service
        state[uid]["a_url"] = url
        state[uid]["stage"] = "await_b"
        bot.reply_to(m, "✅ принял плейлист A. теперь пришли ссылку на плейлист B;")
        return

    # шаг B
    if st == "await_b":
        state[uid]["b_service"] = service
        state[uid]["b_url"] = url
        state[uid]["stage"] = None
        bot.reply_to(m, "✅ принял плейлист B. начинаю сравнение…")

        service_a, url_a = state[uid]["a_service"], state[uid]["a_url"]
        service_b, url_b = state[uid]["b_service"], state[uid]["b_url"]
        status = bot.send_message(m.chat.id, "считаю… A").message_id

        def worker(chat_id: int, message_id: int, service_a: str, url_a: str, service_b: str, url_b: str):
            try:
                A = fetch_tracks(service_a, url_a)
                bot.edit_message_text(f"считаю… A: {len(A)} треков; тяну B", chat_id, message_id)
                B = fetch_tracks(service_b, url_b)

                ratio, matches = compare_exact(A, B)
                percent = f"{ratio*100:.2f}%"
                n = len(matches)
                header = f"совместимость: <b>{percent}</b>\nсовпадений: <b>{n} {ru_tracks(n)}</b>;"
                bot.edit_message_text(header, chat_id, message_id)

                # CSV
                buf_csv = io.StringIO()
                w = csv.writer(buf_csv)
                w.writerow(["artist", "title"])
                w.writerows(matches)
                data_csv = io.BytesIO(buf_csv.getvalue().encode("utf-8"))
                data_csv.name = "matches.csv"

                # TXT
                txt_header = f"У вас совпало {n} {ru_tracks(n)}!\nСовместимость: {percent}\n\n"
                txt_body = "\n".join(f"{a} — {t}" for a, t in matches)
                data_txt = io.BytesIO((txt_header + txt_body).encode("utf-8"))
                data_txt.name = "matches.txt"

                bot.send_document(chat_id, data_txt, caption="TXT со списком совпадений;")
                bot.send_document(chat_id, data_csv, caption="CSV со списком совпадений;")

            except Exception as ex:
                bot.send_message(chat_id, f"упал на подсчёте: {ex}")
            finally:
                # после работы возвращаемся к ожиданию A
                reset_state(uid)

        threading.Thread(
            target=worker, args=(m.chat.id, status, service_a, url_a, service_b, url_b), daemon=True
        ).start()
        return

    # если состояние неизвестно — сброс в начало
    reset_state(uid)
    bot.reply_to(m, "начнём заново. пришли ссылку на плейлист A;")

# ===== устойчивый long-polling =====
if __name__ == "__main__":
    print("Bot started...")
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=60)
        except Exception:
            time.sleep(2)
