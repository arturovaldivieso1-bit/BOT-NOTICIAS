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
from deep_translator import GoogleTranslator
from collections import Counter
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ==================== CONFIGURACIÓN ====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Intervalos (en minutos)
MACRO_INTERVAL_MINUTES = int(os.getenv("MACRO_INTERVAL_MINUTES", "60"))
NEWS_INTERVAL_MINUTES = int(os.getenv("NEWS_INTERVAL_MINUTES", "30"))

# Palabras clave para noticias (español e inglés)
KEYWORDS = [
    "bitcoin", "btc", "cripto", "ethereum", "fed", "tasa", "inflación",
    "sec", "etf", "jpmorgan", "blackrock", "ataque", "guerra", "irán",
    "fomc", "powell", "emergencia", "interest rates", "rate hike"
]

# Fuentes RSS con pesos (dominio -> peso)
RSS_FEEDS = {
    "https://www.reuters.com/rss/topNews": 1.5,
    "https://www.coindesk.com/arc/outboundfeeds/rss/": 1.2,
    "https://news.google.com/rss?q=bitcoin+OR+btc+OR+federal+reserve+OR+sec&hl=en-US&gl=US&ceid=US:en": 0.8
}

# Archivos
SIGNAL_FILE = "signal.json"
SENT_MACRO_FILE = "sent_macro.json"
SENT_NEWS_FILE = "sent_news.txt"

# Umbrales y constantes
SENTIMENT_THRESHOLD = 0.2          # para alertas de Telegram
HIGH_IMPACT_WORDS = ['emergency', 'attack', 'hike', 'sec', 'etf', 'fomc', 'powell', 'ataque', 'emergencia', 'guerra']
WEIGHTED_INTENSITY_WINDOW_HOURS = 6   # ventana de tiempo para intensidad de noticias
MACRO_CONTRIBUTION_BASE = 30          # base de intensidad por evento macro alto

# Inicializar analizador VADER
sentiment_analyzer = SentimentIntensityAnalyzer()

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

# ==================== TRADUCCIÓN ====================
translator = GoogleTranslator(source='en', target='es')

def translate_title(title):
    try:
        return translator.translate(title[:500])
    except Exception as e:
        logging.warning(f"Error traduciendo: {e}")
        return title

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
            # Guardamos más información para la intensidad
            signal_macro["events"].append({
                "event": ev["event"],
                "time_minutes": round(minutes_left),
                "impact": ev["impact"],
                "bias": "bajista si sorprende al alza" if "CPI" in ev["event"] else "volátil",
                "intensity_contribution": MACRO_CONTRIBUTION_BASE * (1 - minutes_left/180)  # más cerca = más intensidad
            })
    return signal_macro

# ==================== MÓDULO NOTICIAS ====================
# Almacenamiento de noticias recientes para intensidad temporal
recent_news = []  # cada elemento: {"timestamp": datetime, "weighted_sentiment": float, "source_weight": float}

def clean_old_news():
    """Elimina noticias más antiguas que WEIGHTED_INTENSITY_WINDOW_HOURS."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=WEIGHTED_INTENSITY_WINDOW_HOURS)
    global recent_news
    recent_news = [n for n in recent_news if n["timestamp"] > cutoff]

def fetch_news():
    sent_links = load_sent_links()
    new_items = []
    for feed_url, weight in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:15]:
                title = entry.get('title', '')
                link = entry.get('link', '')
                if not link or link in sent_links:
                    continue
                if any(kw in title.lower() for kw in KEYWORDS):
                    # Obtener sentimiento VADER (compound)
                    sentiment = sentiment_analyzer.polarity_scores(title)["compound"]
                    title_es = translate_title(title)
                    triggered_keywords = [kw for kw in KEYWORDS if kw in title.lower()]
                    new_items.append({
                        "title_original": title,
                        "title": title_es,
                        "link": link,
                        "source": feed_url,
                        "sentiment": sentiment,
                        "source_weight": weight,
                        "keywords": triggered_keywords[:3],
                        "timestamp": datetime.now(timezone.utc)
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
        # Guardar para intensidad temporal
        weighted_sent = item["sentiment"] * item["source_weight"]
        recent_news.append({
            "timestamp": item["timestamp"],
            "weighted_sentiment": weighted_sent,
            "source_weight": item["source_weight"]
        })
        # Limpiar viejos
        clean_old_news()

        # Enviar alerta si sentimiento fuerte o palabras de alto impacto
        title_lower = (item["title"] + " " + item["title_original"]).lower()
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
        if "emergency" in title_lower or "attack" in title_lower or "emergencia" in title_lower or "ataque" in title_lower:
            signal_news["has_emergency"] = True

    return signal_news

# ==================== INTENSIDAD Y SEÑAL CONTINUA ====================
def compute_intensity(macro_signal):
    """
    Calcula la intensidad actual basada en:
    - Noticias recientes (suma de |weighted_sentiment|)
    - Eventos macro próximos (contribución inversa al tiempo)
    """
    # Intensidad de noticias: suma absoluta de weighted_sentiment en ventana
    news_intensity = sum(abs(n["weighted_sentiment"]) for n in recent_news)

    # Intensidad macro: suma de contribuciones de eventos (cada evento da hasta MACRO_CONTRIBUTION_BASE)
    macro_intensity = sum(ev.get("intensity_contribution", 0) for ev in macro_signal.get("events", []))

    total_intensity = news_intensity + macro_intensity
    return total_intensity

def compute_score_and_state(total_intensity):
    """
    Convierte intensidad en un score 0-100 y estado de mercado.
    Se usa una escala empírica: ajustar según observación.
    """
    # Escala máxima esperada (ajustable)
    max_expected = 50.0   # ajusta según pruebas
    score = min(100, int((total_intensity / max_expected) * 100))
    score = max(0, score)

    if score < 20:
        state = "Calm"
        volatility_spike = False
        panic_mode = False
    elif score < 40:
        state = "Build-up"
        volatility_spike = False
        panic_mode = False
    elif score < 70:
        state = "Volatility spike"
        volatility_spike = True
        panic_mode = False
    else:
        state = "Panic mode"
        volatility_spike = True
        panic_mode = True

    return score, state, volatility_spike, panic_mode

# ==================== GENERADOR DE SEÑAL GLOBAL ====================
def update_signal():
    macro_signal = macro_job() or {"has_high_impact": False, "events": []}
    news_signal = news_job() or {"recent_count": 0, "latest_sentiment": 0, "has_emergency": False}

    # Calcular intensidad total
    total_intensity = compute_intensity(macro_signal)
    score, state, volatility_spike, panic_mode = compute_score_and_state(total_intensity)

    # Para compatibilidad con la antigua señal, mantenemos alert_level
    # Alert level basado en score
    if score >= 70:
        alert_level = 3
    elif score >= 40:
        alert_level = 2
    elif score >= 20:
        alert_level = 1
    else:
        alert_level = 0

    # Construir mensaje resumen para el campo "message"
    message_parts = []
    if macro_signal["has_high_impact"]:
        ev = macro_signal["events"][0] if macro_signal["events"] else {}
        message_parts.append(f"Evento {ev.get('event','macro')} en {ev.get('time_minutes',0)} min.")
    if news_signal["has_emergency"]:
        message_parts.append("Noticia de emergencia.")
    if abs(news_signal["latest_sentiment"]) > 0.5:
        message_parts.append(f"Sentimiento extremo: {news_signal['latest_sentiment']:.2f}.")
    combined_message = " ".join(message_parts) if message_parts else "Sin alertas significativas."

    signal_data = {
        "last_update": datetime.utcnow().isoformat() + "Z",
        "macro": macro_signal,
        "news": news_signal,
        # Nuevos campos para señal continua
        "intensity": round(total_intensity, 2),
        "score": score,
        "market_state": state,
        "volatility_spike": volatility_spike,
        "panic_mode": panic_mode,
        # Campos legacy (por compatibilidad)
        "alert_level": alert_level,
        "message": combined_message
    }
    save_json(SIGNAL_FILE, signal_data)
    logging.info(f"Señal actualizada: score={score}, state={state}")

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

# ==================== REPORTE MEJORADO ====================
def enviar_status_inicial():
    logging.info("Generando status inicial mejorado...")
    macro_events = fetch_macro_events()
    news_items = fetch_news()  # esto ya llena recent_news con sus timestamps
    clean_old_news()

    # Calcular intensidad inicial
    macro_signal = macro_job() or {"has_high_impact": False, "events": []}
    total_intensity = compute_intensity(macro_signal)
    score, state, vol_spike, panic = compute_score_and_state(total_intensity)

    # Palabras clave más frecuentes
    all_keywords = []
    for n in news_items:
        all_keywords.extend(n["keywords"])
    keyword_counts = Counter(all_keywords).most_common(5)

    # Sentimiento promedio (pesado por fuente)
    avg_sentiment = sum(n["sentiment"] * n["source_weight"] for n in news_items) / (sum(n["source_weight"] for n in news_items) or 1)

    lines = []
    lines.append("🤖 *Bot Fundamental - Estado actual*")
    lines.append("")
    lines.append(f"📅 *Macro*: revisión cada {MACRO_INTERVAL_MINUTES} min")
    lines.append(f"📰 *Noticias*: revisión cada {NEWS_INTERVAL_MINUTES} min")
    lines.append("")
    lines.append(f"🔔 *Score de mercado*: `{score}` (0-100)")
    lines.append(f"   └─ Estado: *{state}*")
    if vol_spike:
        lines.append("   └─ ⚡ Volatilidad inminente")
    if panic:
        lines.append("   └─ 🚨 Modo pánico")
    lines.append("")
    if macro_events:
        lines.append("*📆 Próximos eventos macro:*")
        for ev in macro_events[:5]:
            lines.append(f"   • {ev['event']} ({ev['impact']}) - {ev['datetime'].strftime('%d/%m %H:%M UTC')}")
        if len(macro_events) > 5:
            lines.append(f"   ... y {len(macro_events)-5} más.")
    else:
        lines.append("*📆 No hay eventos macro relevantes en las próximas 24h.*")

    lines.append("")
    if news_items:
        lines.append(f"*📰 Noticias detectadas:* {len(news_items)} en total")
        lines.append(f"   📊 Sentimiento promedio ponderado: {avg_sentiment:.2f}")
        lines.append(f"   🔥 Intensidad noticias (últimas {WEIGHTED_INTENSITY_WINDOW_HOURS}h): {total_intensity:.2f}")
        if keyword_counts:
            lines.append("   🔑 Palabras clave más frecuentes:")
            for kw, count in keyword_counts:
                lines.append(f"      - {kw} ({count} veces)")
        lines.append("")
        lines.append("*Ejemplos:*")
        for n in news_items[:3]:
            kw_str = ", ".join(n["keywords"]) if n["keywords"] else "ninguna"
            lines.append(f"   📌 *{n['title'][:70]}...*")
            lines.append(f"      🔑 {kw_str} | 📊 sentimiento {n['sentiment']:.2f} (peso {n['source_weight']:.1f})")
        if len(news_items) > 3:
            lines.append(f"   ... y {len(news_items)-3} más.")
    else:
        lines.append("*📰 No se encontraron noticias con palabras clave.*")

    lines.append("")
    lines.append("🔄 *Fuentes RSS con peso:*")
    for src, w in RSS_FEEDS.items():
        lines.append(f"   • {src.split('/')[2]} (peso {w})")

    send_telegram("\n".join(lines))

# ==================== INICIO ====================
if __name__ == "__main__":
    enviar_status_inicial()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logging.info("Servidor Flask iniciado en puerto 5000")

    scheduler = BackgroundScheduler()
    scheduler.add_job(update_signal, 'interval', minutes=30)
    scheduler.start()
    logging.info("Bot fundamental unificado corriendo. Scheduler activo.")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
