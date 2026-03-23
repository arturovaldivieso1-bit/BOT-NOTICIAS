import os
import time
import json
import logging
import threading
import requests
import feedparser
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from bs4 import BeautifulSoup
from flask import Flask, jsonify

# ==================== CONFIGURACIÓN ====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Intervalos (en minutos)
MACRO_INTERVAL_MINUTES = int(os.getenv("MACRO_INTERVAL_MINUTES", "60"))
NEWS_INTERVAL_MINUTES = int(os.getenv("NEWS_INTERVAL_MINUTES", "30"))

# Palabras clave para noticias
KEYWORDS = [
    "bitcoin", "btc", "crypto", "ethereum", "fed", "rate", "inflation",
    "sec", "etf", "jpmorgan", "blackrock", "attack", "war", "iran",
    "fomc", "powell", "emergency"
]

# Fuentes RSS
RSS_FEEDS = [
    "https://www.reuters.com/rss/topNews",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://news.google.com/rss?q=bitcoin+OR+btc+OR+federal+reserve+OR+sec&hl=en-US&gl=US&ceid=US:en"
]

# Archivos
SIGNAL_FILE = "signal.json"
SENT_MACRO_FILE = "sent_macro.json"
SENT_NEWS_FILE = "sent_news.txt"

# Umbrales
SENTIMENT_THRESHOLD = 0.3
HIGH_IMPACT_WORDS = ['emergency', 'attack', 'hike', 'sec', 'etf', 'fomc', 'powell']

# ==================== FUNCIONES COMUNES ====================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }, timeout=10)
        logging.info("Mensaje enviado a Telegram")
    except Exception as e:
        logging.error(f"Telegram error: {e}")

def load_json(file, default=None):
    if os.path.exists(file):
        try:
            with open(file, 'r') as f:
                return json.load(f)
        except:
            pass
    return default if default is not None else {}

def save_json(file, data):
    try:
        with open(file, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logging.error(f"Error guardando {file}: {e}")

def save_sent_link(link):
    with open(SENT_NEWS_FILE, 'a') as f:
        f.write(link + "\n")

def load_sent_links():
    if os.path.exists(SENT_NEWS_FILE):
        with open(SENT_NEWS_FILE, 'r') as f:
            return set(line.strip() for line in f)
    return set()

# ==================== MÓDULO MACRO ====================
def parse_event_datetime(date_str, time_str):
    try:
        combined = f"{date_str} {time_str}".strip()
        for fmt in ["%b %d %H:%M", "%b %d, %Y %H:%M"]:
            try:
                dt = datetime.strptime(combined, fmt)
                if dt.year == 1900:
                    dt = dt.replace(year=datetime.now().year)
                return dt.replace(tzinfo=timezone.utc)
            except:
                continue
    except:
        pass
    return None

def fetch_macro_events():
    url = "https://www.investing.com/economic-calendar/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'economicCalendarData'})
        if not table:
            table = soup.find('table', class_='ecoCal')
        if not table:
            return []

        events = []
        now = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=1)
        rows = table.find('tbody').find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 5:
                continue
            date_cell = cells[0].get_text(strip=True)
            time_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            event_dt = parse_event_datetime(date_cell, time_cell)
            if not event_dt or event_dt < now or event_dt > tomorrow:
                continue
            country = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            event_name = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            impact_cell = cells[4]
            impact = "Low"
            if impact_cell.find('span', class_='high'):
                impact = "High"
            elif impact_cell.find('span', class_='medium'):
                impact = "Medium"
            if impact not in ['High', 'Medium']:
                continue
            events.append({
                "datetime": event_dt,
                "event": event_name,
                "country": country,
                "impact": impact,
                "forecast": cells[6].get_text(strip=True) if len(cells) > 6 else "",
                "previous": cells[7].get_text(strip=True) if len(cells) > 7 else ""
            })
        return events
    except Exception as e:
        logging.error(f"Error macro: {e}")
        return []

def macro_job():
    logging.info("Ejecutando macro_job...")
    events = fetch_macro_events()
    if not events:
        return {"has_high_impact": False, "events": []}

    sent = load_json(SENT_MACRO_FILE, {})
    now = datetime.now(timezone.utc)
    signal_macro = {"has_high_impact": False, "events": []}
    for ev in events:
        event_id = ev["datetime"].isoformat() + "_" + ev["event"]
        if event_id not in sent:
            if ev["impact"] == "High":
                msg = (
                    f"📅 *EVENTO MACRO*\n"
                    f"{ev['event']}\n"
                    f"🗓️ {ev['datetime'].strftime('%d/%m %H:%M UTC')}\n"
                    f"🌍 {ev['country']}\n"
                    f"⚡ Impacto: 🔴 ALTO\n"
                    f"📊 Esperado: {ev['forecast']} | Previo: {ev['previous']}"
                )
                send_telegram(msg)
                sent[event_id] = True
                save_json(SENT_MACRO_FILE, sent)
                time.sleep(1)

        minutes_left = (ev["datetime"] - now).total_seconds() / 60
        if minutes_left <= 180 and ev["impact"] == "High":
            signal_macro["has_high_impact"] = True
            signal_macro["events"].append({
                "event": ev["event"],
                "time_minutes": round(minutes_left),
                "impact": ev["impact"],
                "bias": "bearish_if_hot" if "CPI" in ev["event"] else "volatile"
            })
    return signal_macro

# ==================== MÓDULO NOTICIAS ====================
def analyze_sentiment_simple(text):
    positive = ["rally", "surge", "gain", "up", "bull", "green", "approve", "good", "strong"]
    negative = ["crash", "drop", "down", "bear", "red", "reject", "fail", "emergency", "attack", "war"]
    words = text.lower().split()
    score = 0
    for w in words:
        if w in positive:
            score += 0.1
        elif w in negative:
            score -= 0.1
    return max(-1, min(1, score))

def fetch_news():
    sent_links = load_sent_links()
    new_items = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:15]:
                title = entry.get('title', '')
                link = entry.get('link', '')
                if not link or link in sent_links:
                    continue
                if any(kw in title.lower() for kw in KEYWORDS):
                    sentiment = analyze_sentiment_simple(title)
                    new_items.append({
                        "title": title,
                        "link": link,
                        "source": feed_url,
                        "sentiment": sentiment
                    })
        except Exception as e:
            logging.error(f"Error RSS {feed_url}: {e}")
    return new_items

def news_job():
    logging.info("Ejecutando news_job...")
    news = fetch_news()
    if not news:
        return {"recent_count": 0, "latest_sentiment": 0, "has_emergency": False}

    signal_news = {"recent_count": 0, "latest_sentiment": 0, "has_emergency": False}
    for item in news:
        title_lower = item["title"].lower()
        is_high_impact = any(w in title_lower for w in HIGH_IMPACT_WORDS)
        if abs(item["sentiment"]) > SENTIMENT_THRESHOLD or is_high_impact:
            emoji = "🟢 ALCISTA" if item["sentiment"] > 0 else "🔴 BAJISTA" if item["sentiment"] < 0 else "🔵 NEUTRAL"
            msg = (
                f"📰 *NOTICIA*\n"
                f"📌 {item['title']}\n"
                f"🔗 [Leer más]({item['link']})\n"
                f"🏷️ Fuente: {item['source']}\n"
                f"📊 Sentimiento: {emoji} ({item['sentiment']:.2f})"
            )
            send_telegram(msg)
            save_sent_link(item["link"])
            time.sleep(1)

        signal_news["recent_count"] += 1
        signal_news["latest_sentiment"] = item["sentiment"]
        if "emergency" in title_lower or "attack" in title_lower:
            signal_news["has_emergency"] = True
    return signal_news

# ==================== GENERADOR DE SEÑAL GLOBAL ====================
def update_signal():
    macro_signal = macro_job() or {"has_high_impact": False, "events": []}
    news_signal = news_job() or {"recent_count": 0, "latest_sentiment": 0, "has_emergency": False}

    alert_level = 0
    message_parts = []
    if macro_signal["has_high_impact"]:
        alert_level += 2
        ev = macro_signal["events"][0] if macro_signal["events"] else {}
        message_parts.append(f"Evento {ev.get('event','macro')} en {ev.get('time_minutes',0)} min (Alto impacto).")
    if news_signal["has_emergency"]:
        alert_level += 1
        message_parts.append("Noticia de emergencia detectada.")
    if abs(news_signal["latest_sentiment"]) > 0.5:
        alert_level += 1
        message_parts.append(f"Sentimiento extremo: {news_signal['latest_sentiment']:.2f}.")

    combined_message = " ".join(message_parts) if message_parts else "Sin alertas significativas."

    signal_data = {
        "last_update": datetime.utcnow().isoformat() + "Z",
        "macro": macro_signal,
        "news": news_signal,
        "alert_level": alert_level,
        "message": combined_message
    }
    save_json(SIGNAL_FILE, signal_data)
    logging.info(f"Señal actualizada: nivel {alert_level}")

# ==================== SERVIDOR FLASK ====================
app = Flask(__name__)

@app.route('/signal')
def get_signal():
    try:
        with open(SIGNAL_FILE, 'r') as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "No signal available", "detail": str(e)}), 404

def run_flask():
    app.run(host='0.0.0.0', port=5000)

# ==================== FUNCIÓN DE PRUEBA INICIAL ====================
def enviar_status_inicial():
    """Envía un mensaje a Telegram con el estado actual (próximos eventos y noticias recientes)."""
    logging.info("Generando status inicial...")
    macro_events = fetch_macro_events()
    news_items = fetch_news()

    # Construir mensaje
    lines = []
    lines.append("🤖 *Bot Fundamental iniciado*")
    lines.append(f"📅 Macro: revisión cada {MACRO_INTERVAL_MINUTES} min")
    lines.append(f"📰 Noticias: revisión cada {NEWS_INTERVAL_MINUTES} min")
    lines.append("")

    if macro_events:
        lines.append("*Próximos eventos macro:*")
        for ev in macro_events[:5]:  # mostrar hasta 5
            lines.append(f"• {ev['event']} ({ev['impact']}) - {ev['datetime'].strftime('%d/%m %H:%M UTC')}")
        if len(macro_events) > 5:
            lines.append(f"... y {len(macro_events)-5} más.")
    else:
        lines.append("No hay eventos macro relevantes en las próximas 24h.")

    lines.append("")
    if news_items:
        lines.append("*Noticias recientes detectadas:*")
        for n in news_items[:3]:
            lines.append(f"• {n['title'][:80]}... (sentimiento {n['sentiment']:.2f})")
        if len(news_items) > 3:
            lines.append(f"... y {len(news_items)-3} más.")
    else:
        lines.append("No se encontraron noticias con palabras clave.")

    send_telegram("\n".join(lines))

# ==================== INICIO ====================
if __name__ == "__main__":
    # Enviar mensaje de inicio con status actual
    enviar_status_inicial()

    # Iniciar servidor Flask
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logging.info("Servidor Flask iniciado en puerto 5000")

    # Programar tareas
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_signal, 'interval', minutes=30)
    scheduler.start()
    logging.info("Bot fundamental unificado corriendo. Scheduler activo.")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
