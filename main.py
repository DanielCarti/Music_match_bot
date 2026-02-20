# main.py — Музыкальный бот для сравнения плейлистов
import os, re, csv, io, time, json, html, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, telebot, vkpymusic
from telebot import types
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN: raise SystemExit("ERROR: set TELEGRAM_BOT_TOKEN")

bot = telebot.TeleBot(TOKEN, parse_mode="HTML")
UA = "KateMobileAndroid/112 (Android 14; SDK 34; arm64-v8a; samsung SM-G998B; ru)"
HEADERS = {"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9"}

RE_YANDEX = re.compile(r'music\.yandex\.ru/(?:users/([^/]+)/playlists/(\d+)|iframe/playlist/([^/]+)/(\d+))')
RE_SPOTIFY = re.compile(r'open\.spotify\.com/(playlist|album|track)/[a-zA-Z0-9]+')
RE_VK_PLAYLIST = re.compile(r'vk\.com/music/(?:playlist|album)/(-?[0-9]+)_([0-9]+)')
RE_VK_AUDIO = re.compile(r'audio_playlist(-?[0-9]+)_([0-9]+)')
RE_VK_AUDIOS_PAGE = re.compile(r'vk\.com/audios(-?\d+)')
RE_VK_PROFILE = re.compile(r'vk\.com/([a-zA-Z0-9._]+)')
RE_BOOM = re.compile(r'share\.boom\.ru/playlist/(\d+)')
RE_IFRAME_SRC = re.compile(r'src="([^"]+)"')

state = {}

def ru_tracks(n: int) -> str:
    m10, m100 = n % 10, n % 100
    if m10 == 1 and m100 != 11: return "трек"
    if 2 <= m10 <= 4 and not (12 <= m100 <= 14): return "трека"
    return "треков"

def reset_state(uid):
    state[uid] = {"stage": "await_a", "a_url": None, "b_url": None, "a_service": None, "b_service": None, "a_tracks": None, "b_tracks": None, "matches": None}

def canon_url(s):
    if '<iframe' in s:
        m = RE_IFRAME_SRC.search(s)
        if m: s = m.group(1)
    s = s.strip().split('?')[0]
    if RE_YANDEX.search(s):
        m = RE_YANDEX.search(s)
        u, p = (m.group(1), m.group(2)) if m.group(1) else (m.group(3), m.group(4))
        return "yandex", f"https://music.yandex.ru/users/{u}/playlists/{p}"
    if RE_SPOTIFY.search(s): return "spotify", s
    if RE_VK_PLAYLIST.search(s): return "vk", RE_VK_PLAYLIST.search(s).group(0)
    if RE_VK_AUDIO.search(s):
        m = RE_VK_AUDIO.search(s)
        return "vk", f"https://vk.com/music/playlist/{m.group(1)}_{m.group(2)}"
    if RE_VK_AUDIOS_PAGE.search(s): return "vk", f"https://vk.com/audios{RE_VK_AUDIOS_PAGE.search(s).group(1)}"
    if RE_BOOM.search(s):
        try:
            r = requests.get(f"https://{RE_BOOM.search(s).group(0)}", headers=HEADERS, timeout=10, allow_redirects=True)
            m = RE_VK_PLAYLIST.search(r.text) or RE_VK_PLAYLIST.search(r.url)
            if m: return "vk", m.group(0)
        except: pass
        raise ValueError("Не удалось развернуть ссылку BOOM.")
    if RE_VK_PROFILE.search(s):
        name = RE_VK_PROFILE.search(s).group(1)
        if name not in ["feed", "im", "groups", "video", "settings", "music", "audios"]: return "vk", f"https://vk.com/{name}"
    raise ValueError("Нужна ссылка на Яндекс, Spotify или VK")

def normalize(artist, title):
    a_list = re.split(r'[,&/]| and |\bfeat\.?\b|\bft\.?\b', artist.lower())
    clean_a = sorted([re.sub(r'[^a-zа-я0-9]+', '', a, flags=re.U) for a in a_list if re.sub(r'[^a-zа-я0-9]+', '', a, flags=re.U)])
    t = re.sub(r'[^a-zа-я0-9]+', '', re.sub(r'[\(\[\{].*?[\)\]\}]|\bfeat\.?\b|\bft\.?\b', '', title.lower()), flags=re.U)
    return "|".join(clean_a) + "||" + t

def fetch_yandex_tracks(url):
    m = re.search(r'users/([^/]+)/playlists/(\d+)', url)
    u, pid = m.group(1), m.group(2)
    all_t, seen_ids = [], set()
    for p in range(30):
        try:
            r = requests.get(f"https://music.yandex.ru/handlers/playlist.jsx?owner={u}&kinds={pid}&light=false&page={p}&page-size=200", headers=HEADERS, timeout=10)
            data = r.json()
            lst = data.get("playlist", {}).get("tracks", [])
            if not lst: break
            new_added = 0
            for item in lst:
                t = item.get("track", item)
                tid = str(t.get("id"))
                if tid in seen_ids: continue
                seen_ids.add(tid)
                title, artist = t.get("title", ""), ", ".join([a.get("name") for a in t.get("artists", [])])
                if title and artist:
                    all_t.append((artist, title, normalize(artist, title)))
                    new_added += 1
            if new_added == 0 or len(lst) < 200: break
        except: break
    return all_t

def vk_resolve_id(name):
    token = os.getenv("VK_ACCESS_TOKEN")
    try:
        r = requests.get(f"https://api.vk.com/method/utils.resolveScreenName?screen_name={name}&access_token={token}&v=5.131", timeout=10)
        data = r.json()
        if "response" in data and isinstance(data["response"], dict): return data["response"].get("object_id")
    except: pass
    return None

def fetch_vk_tracks(url):
    vk = vkpymusic.Service(user_agent=UA, token=os.getenv("VK_ACCESS_TOKEN"))
    all_t = []
    m_pl = RE_VK_PLAYLIST.search(url) or RE_VK_AUDIO.search(url)
    if m_pl and "audios" not in url:
        try:
            oid, pid, akey = (int(m_pl.group(1)), int(m_pl.group(2)), m_pl.group(3)) if "audio_playlist" in url else (int(m_pl.group(2)), int(m_pl.group(3)), m_pl.group(4) if len(m_pl.groups())>=4 else "")
            for off in range(0, 5000, 100):
                tracks = vk.get_songs_by_playlist_id(user_id=oid, playlist_id=pid, access_key=akey, count=100, offset=off)
                if not tracks: break
                all_t.extend(tracks); 
                if len(tracks) < 10: break
            if all_t: return process_vk_tracks(all_t)
        except: pass
    uid = None
    m_audios = RE_VK_AUDIOS_PAGE.search(url)
    if m_audios: uid = int(m_audios.group(1))
    else:
        m_prof = RE_VK_PROFILE.search(url)
        if m_prof:
            name = m_prof.group(1)
            uid = int(name[2:]) if name.startswith("id") and name[2:].isdigit() else (int(name) if name.isdigit() else vk_resolve_id(name))
    if uid:
        try:
            for off in range(0, 5000, 200):
                tracks = vk.get_songs_by_userid(user_id=uid, count=200, offset=off)
                if not tracks: break
                all_t.extend(tracks); 
                if len(tracks) < 50: break
            if all_t: return process_vk_tracks(all_t)
        except: pass
    raise ValueError("VK аудио недоступны. Проверьте настройки приватности.")

def process_vk_tracks(tracks):
    out, seen = [], set()
    for t in tracks:
        title = str(getattr(t, "title", "") or (t.get("title") if isinstance(t, dict) else "")).strip()
        artist = str(getattr(t, "artist", "") or (t.get("artist") if isinstance(t, dict) else "")).strip()
        if title and artist:
            n = normalize(artist, title); 
            if n not in seen: seen.add(n); out.append((artist, title, n))
    return out

def main_menu_markup():
    markup = types.InlineKeyboardMarkup(row_width=3)
    markup.add(types.InlineKeyboardButton("🟢 Spotify", callback_data="info_spotify"),
               types.InlineKeyboardButton("🟡 Яндекс", callback_data="info_yandex"),
               types.InlineKeyboardButton("🔵 VK", callback_data="info_vk"))
    return markup

def back_menu_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Назад к выбору", callback_data="back_to_help"))
    return markup

def show_main_menu(chat_id, message_id=None):
    text = (
        "👋 <b>Привет! Я бот для сравнения плейлистов.</b>\n\n"
        "Я помогу найти общие треки в Яндекс.Музыке, VK и Spotify.\n\n"
        "🚀 <b>Главная фишка:</b> Вы можете сравнивать плейлисты из <b>разных сервисов</b> между собой (например, Яндекс ↔ VK).\n\n"
        "Чтобы начать, просто пришлите мне <b>первую ссылку</b> или <b>файл</b>.\n"
        "Помощь по кнопкам ниже:"
    )
    try:
        if message_id: bot.edit_message_text(text, chat_id, message_id, reply_markup=main_menu_markup())
        else: bot.send_message(chat_id, text, reply_markup=main_menu_markup())
    except: pass

@bot.message_handler(commands=['start', 'help'])
def cmd_start(m): reset_state(m.from_user.id); show_main_menu(m.chat.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("info_") or call.data == "back_to_help")
def info_handler(call):
    if call.data == "back_to_help": show_main_menu(call.message.chat.id, call.message.message_id); return
    if call.data == "info_spotify":
        text = (
            "🟢 <b>Как сравнить плейлист Spotify:</b>\n\n"
            "Spotify блокирует автоматическое чтение плейлистов ботами, поэтому используйте экспорт в файл:\n"
            "1. Зайдите на сайт <a href=\"https://www.tunemymusic.com/\">TuneMyMusic</a> или <a href=\"https://exportify.net/\">Exportify</a>.\n"
            "2. Выберите свой плейлист и экспортируйте его в формат <b>TXT</b> или <b>CSV</b>.\n"
            "3. Просто <b>отправьте этот файл мне</b> в чат!"
        )
    elif call.data == "info_yandex":
        text = (
            "🟡 <b>Инструкция для Яндекс.Музыки:</b>\n\n"
            "💻 <b>На компьютере или в браузере:</b>\n"
            "Зайдите в раздел 'Коллекция' -> 'Мне нравится', нажмите на три точки (слева от кнопки загрузки) -> выберите 'HTML-код' -> нажмите желтую кнопку <b>'Скопировать'</b>. Пришлите этот текст мне!\n\n"
            "📱 <b>В мобильном приложении:</b>\n"
            "Зайдите в 'Коллекции' (иконка сердечка справа внизу) -> выберите 'Мне нравится', нажмите на три точки вверху -> <b>'Поделиться'</b> -> выберите Telegram или скопируйте ссылку."
        )
    elif call.data == "info_vk":
        text = (
            "🔵 <b>Инструкция для VK Музыки:</b>\n\n"
            "📱 <b>На телефоне:</b>\n"
            "В приложении нет кнопки 'Поделиться' для всех аудио сразу. Просто <b>пришлите ссылку на свой профиль</b> (Три точки в профиле -> Скопировать ссылку).\n\n"
            "💻 <b>На компьютере:</b>\n"
            "Зайдите в раздел 'Мои Аудиозаписи' и скопируйте ссылку из адресной строки браузера или пришлите ссылку на любой созданный вами плейлист.\n\n"
            "⚠️ <b>Важно:</b> Убедитесь, что в настройках приватности ваш список аудиозаписей открыт для <b>Всех пользователей</b>."
        )
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=back_menu_markup(), disable_web_page_preview=True)
    except: pass

@bot.message_handler(content_types=['document'])
def handle_file(m):
    uid = m.from_user.id
    if uid not in state: reset_state(uid)
    
    if state[uid].get("stage") == "await_format":
        bot.reply_to(m, "⚠️ <b>Сначала выберите формат для списка совпадений выше</b> или нажмите кнопку «🔄 Новый Мэтч».")
        return

    if not m.document.file_name.lower().endswith(('.txt', '.csv')): return
    try:
        file_info = bot.get_file(m.document.file_id)
        downloaded = bot.download_file(file_info.file_path).decode('utf-8', 'ignore')
        tracks = [l.strip() for l in downloaded.splitlines() if l.strip()]
        parsed, seen = [], set()
        for t in tracks:
            t = re.sub(r'^\d+\.?\s*', '', t); a, title = (t.split(" - ", 1)[0], t.split(" - ", 1)[1]) if " - " in t else ("", t)
            n = normalize(a, title); 
            if n not in seen: seen.add(n); parsed.append((a, title, n))
        if state[uid]["stage"] == "await_a":
            state[uid].update({"a_service": "file", "a_tracks": parsed, "stage": "await_b"})
            bot.reply_to(m, f"✅ <b>Файл с треками принят!</b> ({len(parsed)} {ru_tracks(len(parsed))})\n\nТеперь пришлите вторую ссылку или еще один файл.")
        else:
            state[uid].update({"b_service": "file", "b_tracks": parsed, "stage": None})
            start_comparison(m, uid)
    except Exception as e: bot.reply_to(m, f"❌ Ошибка файла: {e}")

@bot.message_handler(content_types=['text'])
def handle_text(m):
    uid, text = m.from_user.id, m.text.strip()
    if uid not in state: reset_state(uid)
    
    if state[uid].get("stage") == "await_format":
        bot.reply_to(m, "⚠️ <b>Сначала выберите формат для списка совпадений выше</b> или нажмите кнопку «🔄 Новый Мэтч».")
        return

    try:
        service, url = canon_url(text)
        if service == "spotify": bot.reply_to(m, "⚠️ Для Spotify нужны файлы. См. инструкцию в меню."); return
        
        service_names = {"yandex": "Яндекс.Музыки", "vk": "VK Музыки"}
        s_name = service_names.get(service, service.capitalize())

        if state[uid]["stage"] == "await_a":
            state[uid].update({"a_service": service, "a_url": url, "stage": "await_b"})
            bot.reply_to(m, f"✅ <b>Плейлист из {s_name} принят!</b>\n\nТеперь пришлите вторую ссылку или файл.")
        else:
            state[uid].update({"b_service": service, "b_url": url, "stage": None})
            start_comparison(m, uid)
    except Exception as e: bot.reply_to(m, f"❌ {e}")

def start_comparison(m, uid):
    status = bot.send_message(m.chat.id, "⌛ Считаю…").message_id
    def work():
        try:
            A_raw = state[uid]["a_tracks"] if state[uid].get("a_tracks") else fetch_yandex_tracks(state[uid]["a_url"]) if state[uid]["a_service"] == "yandex" else fetch_vk_tracks(state[uid]["a_url"])
            A, seen_a = [], set()
            for item in A_raw:
                if item[2] not in seen_a: seen_a.add(item[2]); A.append(item)
            bot.edit_message_text(f"считаю… A: <b>{len(A)}</b> {ru_tracks(len(A))};\nтяну B…", m.chat.id, status)
            B_raw = state[uid]["b_tracks"] if state[uid].get("b_tracks") else fetch_yandex_tracks(state[uid]["b_url"]) if state[uid]["b_service"] == "yandex" else fetch_vk_tracks(state[uid]["b_url"])
            B, seen_b = [], set()
            for item in B_raw:
                if item[2] not in seen_b: seen_b.add(item[2]); B.append(item)
            B_set = {x[2] for x in B}
            matches = [(a, t) for a, t, n in A if n in B_set]
            state[uid]["matches"] = matches; la, lb, n = len(A), len(B), len(matches)
            
            p_a = (n / la * 100) if la > 0 else 0
            p_b = (n / lb * 100) if lb > 0 else 0
            
            # Переводим в стадию ожидания формата
            state[uid]["stage"] = "await_format"
            
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(types.InlineKeyboardButton("📄 TXT", callback_data="get_txt"), types.InlineKeyboardButton("📊 CSV", callback_data="get_csv"),
                       types.InlineKeyboardButton("📦 Оба", callback_data="get_both"), types.InlineKeyboardButton("🔄 Новый Мэтч", callback_data="start_new"))
            
            bot.edit_message_text(
                f"📊 <b>Результат сравнения:</b>\n\n"
                f"▪️ Плейлист A: <b>{la}</b> {ru_tracks(la)}\n"
                f"▪️ Плейлист B: <b>{lb}</b> {ru_tracks(lb)}\n"
                f"✅ Общих треков: <b>{n}</b>\n\n"
                f"📈 <b>Сходство:</b>\n"
                f"Плейлист A похож на B на <b>{p_a:.1f}%</b>\n"
                f"Плейлист B похож на A на <b>{p_b:.1f}%</b>\n\n"
                f"<i>Выберите формат для списка совпадений:</i>",
                m.chat.id, status, reply_markup=markup
            )
        except Exception as e: bot.send_message(m.chat.id, "❌ Произошла системная ошибка.")
    threading.Thread(target=work).start()

@bot.callback_query_handler(func=lambda call: True)
def cb_handler(call):
    uid = call.from_user.id
    if call.data == "start_new": reset_state(uid); bot.send_message(call.message.chat.id, "🔄 Готов! Пришли первую ссылку или файл."); return
    if call.data == "to_main": reset_state(uid); show_main_menu(call.message.chat.id); return
    if uid not in state or not state[uid].get("matches"): bot.answer_callback_query(call.id, "Устарело."); return
    m = state[uid]["matches"]
    
    if call.data == "get_txt":
        content = f"Совпало: {len(m)}\n\n" + "\n".join([f"{x[0]} - {x[1]}" for x in m])
        buf = io.BytesIO(content.encode("utf-8")); buf.name = "matches.txt"; bot.send_document(call.message.chat.id, buf)
    elif call.data == "get_csv":
        out = io.StringIO(); csv.writer(out).writerows([["Artist", "Title"]] + m)
        buf = io.BytesIO(out.getvalue().encode("utf-8")); buf.name = "matches.csv"; bot.send_document(call.message.chat.id, buf)
    elif call.data == "get_both":
        content = f"Совпало: {len(m)}\n\n" + "\n".join([f"{x[0]} - {x[1]}" for x in m])
        txt = io.BytesIO(content.encode("utf-8")); txt.name = "matches.txt"; bot.send_document(call.message.chat.id, txt)
        out = io.StringIO(); csv.writer(out).writerows([["Artist", "Title"]] + m)
        csv_f = io.BytesIO(out.getvalue().encode("utf-8")); csv_f.name = "matches.csv"
        bot.send_document(call.message.chat.id, csv_f)
    
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(); markup.add(types.InlineKeyboardButton("🏠 В начало", callback_data="to_main"), types.InlineKeyboardButton("🔄 Новый Мэтч", callback_data="start_new"))
    bot.send_message(call.message.chat.id, "Сравним еще что-нибудь?", reply_markup=markup)

if __name__ == "__main__":
    print("Bot started..."); bot.infinity_polling()
