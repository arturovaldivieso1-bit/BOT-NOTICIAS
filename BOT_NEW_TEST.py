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
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ==================== CONFIGURACIÓN ====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.warning("Faltan variables TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")

# Intervalos (en minutos)
MACRO_INTERVAL_MINUTES = int(os.getenv("MACRO_INTERVAL_MINUTES", "60"))
NEWS_INTERVAL_MINUTES  = int(os.getenv("NEWS_INTERVAL_MINUTES",  "30"))

# ── CONTROL DE ALERTAS DIARIAS ──────────────────────────────────────────────
MAX_ALERTS_PER_DAY = 3          # techo duro: máximo 3 alertas de noticias por día
alerts_today: list[datetime] = []   # timestamps de alertas enviadas hoy

def _purge_old_alerts():
    """Elimina alertas con más de 24 h de antigüedad."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    global alerts_today
    alerts_today = [t for t in alerts_today if t > cutoff]

def can_send_alert() -> bool:
    _purge_old_alerts()
    return len(alerts_today) < MAX_ALERTS_PER_DAY

def register_alert():
    alerts_today.append(datetime.now(timezone.utc))

# ── KEYWORDS DE ALTO IMPACTO REAL ───────────────────────────────────────────
# Solo macro + riesgo sistémico. Eliminado: bitcoin, btc, crypto, ethereum, etc.
KEYWORDS = [
    # Macro / bancos centrales
    "fed", "federal reserve", "fomc", "powell", "interest rate", "rate hike",
    "rate cut", "inflation", "cpi", "pce", "gdp", "recession", "nonfarm",
    "payroll", "unemployment", "ecb", "lagarde", "boj", "bank of japan",
    # Riesgo sistémico / geopolítico
    "attack", "war", "crisis", "emergency", "sanctions", "default",
    "collapse", "invasion", "missile", "nuclear", "ataque", "guerra",
    "emergencia", "colapso",
    # Regulación de alto impacto (solo las que mueven mercado)
    "sec enforcement", "sec lawsuit", "ban", "blackrock etf", "etf approved",
    "etf rejected",
]

# Pesos: solo palabras que tienen impacto real y documentado en precio
KEYWORD_WEIGHTS: dict[str, float] = {
    "fomc":             2.0,
    "federal reserve":  2.0,
    "powell":           1.8,
    "rate hike":        2.0,
    "rate cut":         2.0,
    "inflation":        1.6,
    "cpi":              1.7,
    "recession":        1.8,
    "default":          2.0,
    "collapse":         2.0,
    "emergency":        2.0,
    "attack":           1.9,
    "war":              1.9,
    "invasion":         2.0,
    "nuclear":          2.0,
    "sanctions":        1.7,
    "ban":              1.8,
    "etf approved":     1.9,
    "etf rejected":     1.9,
    "blackrock etf":    1.8,
    "sec enforcement":  1.8,
    "sec lawsuit":      1.8,
}

# ── FUENTES RSS ──────────────────────────────────────────────────────────────
# Eliminadas: CryptoPanic (mucho ruido retail) y CoinDesk (baja señal/ruido).
# Mantenidas solo las de mayor rigor periodístico para macro y riesgo.
RSS_FEEDS = {
    "https://www.reuters.com/rss/topNews":    {"level": 1, "base_weight": 2.0},
    "https://www.forexlive.com/feed/":        {"level": 1, "base_weight": 1.8},
    "https://www.theblock.co/rss":            {"level": 2, "base_weight": 1.2},
}

# ── ARCHIVOS ─────────────────────────────────────────────────────────────────
SIGNAL_FILE      = "signal.json"
SENT_MACRO_FILE  = "sent_macro.json"
SENT_NEWS_FILE   = "sent_news.txt"

# ── UMBRALES ─────────────────────────────────────────────────────────────────
# Antes: 0.2 / 0.4  →  Ahora: 0.65 / 0.85
# El weighted_sentiment debe ser alto Y venir de una keyword de peso ≥ 1.6
SENTIMENT_THRESHOLD_LEVEL1 = 0.65
SENTIMENT_THRESHOLD_LEVEL2 = 0.85

# Un artículo solo pasa si su keyword más pesada supera este umbral
MIN_KEYWORD_WEIGHT_TO_QUALIFY = 1.6

# Cooldown por keyword: 6 h (antes 2 h) — evita duplicados del mismo tema
COOLDOWN_MINUTES = 360

# Palabras que elevan prioridad pero siguen necesitando sentimiento mínimo
HIGH_IMPACT_WORDS = [
    "emergency", "attack", "invasion", "nuclear", "default", "collapse",
    "emergencia", "ataque", "invasion", "colapso",
]

WEIGHTED_INTENSITY_WINDOW_HOURS = 6
MACRO_CONTRIBUTION_BASE         = 30

# ==================== INICIALIZACIÓN ====================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sentiment_analyzer = SentimentIntensityAnalyzer()
translator         = GoogleTranslator(source='en', target='es')

# ==================== FUNCIONES COMUNES ====================
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=10)
        logging.info("Mensaje enviado a Telegram")
    except Exception as e:
        logging.error(f"Telegram error: {e}")

def load_json(file, default=None):
    if os.path.exists(file):
        try:
            with open(file) as f:
                return json.load(f)
        except Exception:
            pass
    return default if default is not None else {}

def save_json(file, data):
    try:
        with open(file, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logging.error(f"Error guardando {file}: {e}")

def save_sent_link(link: str):
    with open(SENT_NEWS_FILE, 'a') as f:
        f.write(link + "\n")

def load_sent_links() -> set:
    if os.path.exists(SENT_NEWS_FILE):
        with open(SENT_NEWS_FILE) as f:
            return {line.strip() for line in f}
    return set()

def translate_title(title: str) -> str:
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
            except Exception:
                continue
    except Exception:
        pass
    return None

def fetch_macro_events():
    """Eventos de alto/medio impacto en las próximas 24 horas."""
    url     = "https://www.investing.com/economic-calendar/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp  = requests.get(url, headers=headers, timeout=15)
        soup  = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'economicCalendarData'}) or \
                soup.find('table', class_='ecoCal')
        if not table:
            return []

        events  = []
        now     = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=1)

        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 5:
                continue
            event_dt = parse_event_datetime(
                cells[0].get_text(strip=True),
                cells[1].get_text(strip=True) if len(cells) > 1 else "",
            )
            if not event_dt or not (now <= event_dt <= tomorrow):
                continue
            impact_cell = cells[4]
            impact = "Low"
            if impact_cell.find('span', class_='high'):
                impact = "High"
            elif impact_cell.find('span', class_='medium'):
                impact = "Medium"
            if impact not in ('High', 'Medium'):
                continue
            events.append({
                "datetime": event_dt,
                "event":    cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "country":  cells[2].get_text(strip=True) if len(cells) > 2 else "",
                "impact":   impact,
                "forecast": cells[6].get_text(strip=True) if len(cells) > 6 else "",
                "previous": cells[7].get_text(strip=True) if len(cells) > 7 else "",
            })
        return events
    except Exception as e:
        logging.error(f"Error macro: {e}")
        return []

def fetch_macro_events_week():
    """Eventos de ALTO impacto en los próximos 7 días (para reporte semanal)."""
    url     = "https://www.investing.com/economic-calendar/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp  = requests.get(url, headers=headers, timeout=15)
        soup  = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', {'id': 'economicCalendarData'}) or \
                soup.find('table', class_='ecoCal')
        if not table:
            return []

        events  = []
        now     = datetime.now(timezone.utc)
        week_ahead = now + timedelta(days=7)

        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')
            if len(cells) < 5:
                continue
            event_dt = parse_event_datetime(
                cells[0].get_text(strip=True),
                cells[1].get_text(strip=True) if len(cells) > 1 else "",
            )
            if not event_dt or not (now <= event_dt <= week_ahead):
                continue
            impact_cell = cells[4]
            impact = "Low"
            if impact_cell.find('span', class_='high'):
                impact = "High"
            elif impact_cell.find('span', class_='medium'):
                impact = "Medium"
            if impact != "High":  # solo alto impacto en el parte semanal
                continue
            events.append({
                "datetime": event_dt,
                "event":    cells[3].get_text(strip=True) if len(cells) > 3 else "",
                "country":  cells[2].get_text(strip=True) if len(cells) > 2 else "",
                "impact":   impact,
            })
        events.sort(key=lambda e: e["datetime"])
        return events
    except Exception as e:
        logging.error(f"Error macro week: {e}")
        return []

def macro_job():
    logging.info("Ejecutando macro_job...")
    events = fetch_macro_events()
    if not events:
        return {"has_high_impact": False, "events": []}

    sent = load_json(SENT_MACRO_FILE, {})
    now  = datetime.now(timezone.utc)
    signal_macro = {"has_high_impact": False, "events": []}

    for ev in events:
        event_id = ev["datetime"].isoformat() + "_" + ev["event"]
        if event_id not in sent and ev["impact"] == "High":
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
                "event":                ev["event"],
                "time_minutes":         round(minutes_left),
                "impact":               ev["impact"],
                "bias":                 "bajista si sorprende al alza" if "CPI" in ev["event"] else "volátil",
                "intensity_contribution": MACRO_CONTRIBUTION_BASE * (1 - minutes_left / 180),
            })
    return signal_macro

# ==================== MÓDULO NOTICIAS ====================
recent_news: list[dict]          = []
last_alert_by_keyword: dict      = {}

def clean_old_news():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WEIGHTED_INTENSITY_WINDOW_HOURS)
    global recent_news
    recent_news = [n for n in recent_news if n["timestamp"] > cutoff]

def compute_keyword_boost(title: str) -> tuple[float, float]:
    """
    Devuelve (boost_acumulado, peso_max_keyword).
    boost: multiplicador de sentimiento.
    peso_max: la keyword individual más pesada detectada.
    """
    title_lower = title.lower()
    boost    = 1.0
    max_kw_w = 0.0
    for kw, weight in KEYWORD_WEIGHTS.items():
        if kw in title_lower:
            boost    += (weight - 1)
            max_kw_w  = max(max_kw_w, weight)
    return boost, max_kw_w

def _keyword_in_cooldown(title_lower: str) -> bool:
    """True si alguna keyword fuerte del título está dentro del cooldown."""
    for kw in KEYWORD_WEIGHTS:
        if kw in title_lower and kw in last_alert_by_keyword:
            elapsed = (datetime.now(timezone.utc) - last_alert_by_keyword[kw]).total_seconds() / 60
            if elapsed < COOLDOWN_MINUTES:
                return True
    return False

def fetch_news():
    sent_links = load_sent_links()
    new_items  = []
    for feed_url, feed_info in RSS_FEEDS.items():
        level       = feed_info["level"]
        base_weight = feed_info["base_weight"]
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:15]:
                title = entry.get('title', '')
                link  = entry.get('link', '')
                if not link or link in sent_links:
                    continue
                title_lower = title.lower()
                # Filtro primario: debe contener al menos una keyword relevante
                if not any(kw in title_lower for kw in KEYWORDS):
                    continue
                sentiment              = sentiment_analyzer.polarity_scores(title)["compound"]
                boost, max_kw_weight   = compute_keyword_boost(title)
                weighted_sentiment     = sentiment * boost * base_weight
                title_es               = translate_title(title)
                triggered_keywords     = [kw for kw in KEYWORDS if kw in title_lower]
                new_items.append({
                    "title_original":    title,
                    "title":             title_es,
                    "link":              link,
                    "source":            feed_url,
                    "level":             level,
                    "base_weight":       base_weight,
                    "keyword_boost":     boost,
                    "max_kw_weight":     max_kw_weight,
                    "sentiment":         sentiment,
                    "weighted_sentiment": weighted_sentiment,
                    "keywords":          triggered_keywords[:3],
                    "timestamp":         datetime.now(timezone.utc),
                })
        except Exception as e:
            logging.error(f"Error RSS {feed_url}: {e}")
    return new_items

def news_job():
    logging.info("Ejecutando news_job...")
    news = fetch_news()
    if not news:
        return {
            "recent_count": 0, "latest_sentiment": 0,
            "has_emergency": False, "latest_weighted_sentiment": 0,
        }

    signal_news = {
        "recent_count":              0,
        "latest_sentiment":          0,
        "latest_weighted_sentiment": 0,
        "has_emergency":             False,
        "top_keywords":              [],
    }
    all_keywords = []

    for item in news:
        recent_news.append({
            "timestamp":         item["timestamp"],
            "weighted_sentiment": item["weighted_sentiment"],
            "source_name":       item["source"],
        })
        clean_old_news()

        title_lower = (item["title"] + " " + item["title_original"]).lower()
        is_high_impact = any(w in title_lower for w in HIGH_IMPACT_WORDS)

        # ── FILTRO 1: la keyword más pesada debe superar el mínimo ──────────
        if item["max_kw_weight"] < MIN_KEYWORD_WEIGHT_TO_QUALIFY and not is_high_impact:
            continue  # noticia descartada — no tiene keywords de suficiente peso

        # ── FILTRO 2: umbral de sentimiento ponderado ────────────────────────
        threshold = SENTIMENT_THRESHOLD_LEVEL1 if item["level"] == 1 else SENTIMENT_THRESHOLD_LEVEL2
        passes_sentiment = abs(item["weighted_sentiment"]) >= threshold

        # Las HIGH_IMPACT_WORDS sí pasan, pero solo si el sentimiento no es
        # completamente neutro (evita artículos de archivo o contexto vacío)
        if is_high_impact:
            passes_sentiment = passes_sentiment or abs(item["sentiment"]) >= 0.15

        if not passes_sentiment:
            continue

        # ── FILTRO 3: cooldown por keyword ───────────────────────────────────
        if _keyword_in_cooldown(title_lower):
            logging.info(f"Cooldown activo para: {item['title_original'][:60]}")
            continue

        # ── FILTRO 4: techo diario de alertas ────────────────────────────────
        if not can_send_alert():
            logging.info("Techo diario de alertas alcanzado. Noticia descartada.")
            continue

        # ── ENVIAR ────────────────────────────────────────────────────────────
        emoji = "🟢 ALCISTA" if item["sentiment"] > 0 else \
                "🔴 BAJISTA" if item["sentiment"] < 0 else "🔵 NEUTRAL"
        msg = (
            f"📰 *NOTICIA DE ALTO IMPACTO*\n"
            f"📌 {item['title']}\n"
            f"🔗 [Leer más]({item['link']})\n"
            f"🏷️ Fuente: {item['source'].split('/')[2]} (Nivel {item['level']})\n"
            f"📊 Sentimiento: {emoji} ({item['sentiment']:.2f})\n"
            f"⚡ Score ponderado: {item['weighted_sentiment']:.2f}"
        )
        send_telegram(msg)
        save_sent_link(item["link"])
        register_alert()
        time.sleep(1)

        # Actualizar cooldown
        for kw in KEYWORD_WEIGHTS:
            if kw in title_lower:
                last_alert_by_keyword[kw] = datetime.now(timezone.utc)

        # Actualizar señal
        signal_news["recent_count"]              += 1
        signal_news["latest_sentiment"]           = item["sentiment"]
        signal_news["latest_weighted_sentiment"]  = item["weighted_sentiment"]
        if any(w in title_lower for w in ["emergency", "attack", "emergencia", "ataque"]):
            signal_news["has_emergency"] = True
        all_keywords.extend(item["keywords"])

    if all_keywords:
        signal_news["top_keywords"] = [kw for kw, _ in Counter(all_keywords).most_common(5)]

    return signal_news

# ==================== INTENSIDAD Y SEÑAL CONTINUA ====================
def compute_intensity(macro_signal: dict) -> float:
    news_intensity  = sum(abs(n["weighted_sentiment"]) for n in recent_news)
    macro_intensity = sum(ev.get("intensity_contribution", 0) for ev in macro_signal.get("events", []))
    return news_intensity + macro_intensity

def compute_score_and_state(total_intensity: float):
    max_expected = 50.0
    score = max(0, min(100, int((total_intensity / max_expected) * 100)))

    if score < 20:
        state, vol_spike, panic = "Calm",             False, False
    elif score < 40:
        state, vol_spike, panic = "Build-up",         False, False
    elif score < 70:
        state, vol_spike, panic = "Volatility spike", True,  False
    else:
        state, vol_spike, panic = "Panic mode",       True,  True

    return score, state, vol_spike, panic

# ==================== GENERADOR DE SEÑAL GLOBAL ====================
def update_signal():
    macro_signal = macro_job() or {"has_high_impact": False, "events": []}
    news_signal  = news_job()  or {
        "recent_count": 0, "latest_sentiment": 0,
        "has_emergency": False, "latest_weighted_sentiment": 0,
    }

    total_intensity = compute_intensity(macro_signal)
    score, state, vol_spike, panic = compute_score_and_state(total_intensity)

    alert_level = 3 if score >= 70 else 2 if score >= 40 else 1 if score >= 20 else 0

    parts = []
    if macro_signal["has_high_impact"]:
        ev = macro_signal["events"][0] if macro_signal["events"] else {}
        parts.append(f"Evento {ev.get('event','macro')} en {ev.get('time_minutes',0)} min.")
    if news_signal["has_emergency"]:
        parts.append("Noticia de emergencia.")
    if abs(news_signal.get("latest_weighted_sentiment", 0)) > 0.5:
        parts.append(f"Sentimiento extremo: {news_signal['latest_weighted_sentiment']:.2f}.")

    signal_data = {
        "last_update":     datetime.utcnow().isoformat() + "Z",
        "macro":           macro_signal,
        "news":            news_signal,
        "intensity":       round(total_intensity, 2),
        "score":           score,
        "market_state":    state,
        "volatility_spike": vol_spike,
        "panic_mode":      panic,
        "alert_level":     alert_level,
        "message":         " ".join(parts) if parts else "Sin alertas significativas.",
        "alerts_sent_today": len(alerts_today),
        "alerts_remaining":  max(0, MAX_ALERTS_PER_DAY - len(alerts_today)),
    }
    save_json(SIGNAL_FILE, signal_data)
    logging.info(f"Señal actualizada: score={score}, state={state}, alertas_hoy={len(alerts_today)}/{MAX_ALERTS_PER_DAY}")

# ==================== REPORTE SEMANAL ====================
def weekly_report_job():
    """Envía todos los domingos a las 22:00 UTC el parte semanal."""
    logging.info("Generando reporte semanal...")
    try:
        with open(SIGNAL_FILE) as f:
            signal = json.load(f)
    except:
        signal = {"score": 0, "market_state": "Desconocido", "volatility_spike": False,
                  "panic_mode": False, "alerts_sent_today": 0, "news": {}}

    score   = signal.get("score", 0)
    state   = signal.get("market_state", "Calm")
    vol_ok  = "⚠️" if signal.get("volatility_spike") else "✅"
    panic_ok = "🚨" if signal.get("panic_mode") else "✅"
    alerts_hoy = signal.get("alerts_sent_today", 0)
    news_signal = signal.get("news", {})
    kws = ", ".join(news_signal.get("top_keywords", [])) or "ninguna"

    # Eventos macro de la semana (alto impacto)
    weekly_events = fetch_macro_events_week()
    if weekly_events:
        events_lines = []
        for ev in weekly_events[:10]:
            dt_str = ev["datetime"].strftime('%a %d/%m %H:%M UTC')
            events_lines.append(f"• {dt_str} - {ev['event']} ({ev['country']})")
        events_text = "\n".join(events_lines)
    else:
        events_text = "No hay eventos de alto impacto previstos."

    msg = (
        f"📅 *Parte semanal del Bot Fundamental*\n\n"
        f"📊 Estado actual: *{state}* (score: {score})\n"
        f"   Volatilidad: {vol_ok}  Pánico: {panic_ok}\n"
        f"   Alertas de alto impacto hoy: {alerts_hoy}\n"
        f"   Keywords recientes: {kws}\n\n"
        f"🗓️ *Eventos macro de la semana (Alto impacto)*:\n{events_text}"
    )
    send_telegram(msg)

# ==================== SERVIDOR FLASK ====================
flask_app = Flask(__name__)

@flask_app.route('/signal')
def get_signal():
    try:
        with open(SIGNAL_FILE) as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": "No signal available", "detail": str(e)}), 404

def run_flask():
    flask_app.run(host='0.0.0.0', port=5000)

# ==================== COMANDO TELEGRAM /stats ====================
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with open(SIGNAL_FILE) as f:
            data = json.load(f)
    except Exception:
        await update.message.reply_text("📡 Señal no disponible todavía. Espera unos minutos.")
        return

    score   = data.get("score", 0)
    state   = data.get("market_state", "Calm")
    vol_ok  = "⚠️" if data.get("volatility_spike") else "✅"
    panic_ok = "🚨" if data.get("panic_mode") else "✅"
    macro   = data.get("macro", {})
    news    = data.get("news",  {})
    sent_t  = data.get("alerts_sent_today", 0)
    remain  = data.get("alerts_remaining",  MAX_ALERTS_PER_DAY)

    macro_text = "\n".join(
        f"   • {e['event']} en {e['time_minutes']} min" for e in macro.get("events", [])[:3]
    ) or "   No hay eventos de alto impacto próximos."

    kws = ", ".join(news.get("top_keywords", [])) or "ninguna"

    msg = (
        f"📊 *Bot Fundamental — Estado*\n"
        f"🕒 Actualizado: {data.get('last_update','?')[:16]}\n"
        f"📈 Score: `{score}` | Estado: *{state}*\n"
        f"   Volatilidad: {vol_ok}  Pánico: {panic_ok}\n\n"
        f"📅 *Eventos macro próximos:*\n{macro_text}\n\n"
        f"📰 *Noticias calificadas hoy:* {news.get('recent_count',0)}\n"
        f"   Sentimiento último: {news.get('latest_sentiment',0):.2f}\n"
        f"   Keywords: {kws}\n\n"
        f"🔔 *Alertas enviadas hoy:* {sent_t}/{MAX_ALERTS_PER_DAY} "
        f"({remain} restantes)"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

def run_telegram_bot():
    if not TELEGRAM_BOT_TOKEN:
        logging.warning("No se iniciará bot de comandos: falta TELEGRAM_BOT_TOKEN")
        return
    tg_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    tg_app.add_handler(CommandHandler("stats", stats_command))
    tg_app.run_polling()

# ==================== REPORTE INICIAL ====================
def enviar_status_inicial():
    logging.info("Generando status inicial...")
    macro_events = fetch_macro_events()
    news_items   = fetch_news()
    clean_old_news()

    macro_signal    = macro_job() or {"has_high_impact": False, "events": []}
    total_intensity = compute_intensity(macro_signal)
    score, state, vol_spike, panic = compute_score_and_state(total_intensity)

    all_kws = []
    for n in news_items:
        all_kws.extend(n["keywords"])
    kw_counts  = Counter(all_kws).most_common(5)
    avg_w_sent = sum(n["weighted_sentiment"] for n in news_items) / (len(news_items) or 1)

    lines = [
        "🤖 *Bot Fundamental — Estado inicial*", "",
        f"📅 Macro: cada {MACRO_INTERVAL_MINUTES} min | 📰 Noticias: cada {NEWS_INTERVAL_MINUTES} min",
        f"🔔 Techo diario de alertas: {MAX_ALERTS_PER_DAY}", "",
        f"📈 Score: `{score}` | Estado: *{state}*",
        "   └─ ⚡ Volatilidad inminente" if vol_spike else "",
        "   └─ 🚨 Modo pánico"           if panic     else "", "",
        "*Umbrales activos:*",
        f"   • Nivel 1 (Reuters/ForexLive): weighted_sentiment ≥ {SENTIMENT_THRESHOLD_LEVEL1}",
        f"   • Nivel 2 (TheBlock):          weighted_sentiment ≥ {SENTIMENT_THRESHOLD_LEVEL2}",
        f"   • Keyword mínima requerida:    peso ≥ {MIN_KEYWORD_WEIGHT_TO_QUALIFY}",
        f"   • Cooldown por keyword:        {COOLDOWN_MINUTES // 60} h", "",
    ]

    if macro_events:
        lines.append("*📆 Próximos eventos macro:*")
        for ev in macro_events[:5]:
            lines.append(f"   • {ev['event']} ({ev['impact']}) — {ev['datetime'].strftime('%d/%m %H:%M UTC')}")
        if len(macro_events) > 5:
            lines.append(f"   ... y {len(macro_events)-5} más.")
    else:
        lines.append("*📆 No hay eventos macro relevantes en las próximas 24 h.*")

    lines.append("")
    if news_items:
        qualified = [n for n in news_items if n["max_kw_weight"] >= MIN_KEYWORD_WEIGHT_TO_QUALIFY]
        lines += [
            f"*📰 Noticias totales detectadas:* {len(news_items)}",
            f"   Califican para alerta: {len(qualified)}",
            f"   Sentimiento ponderado promedio: {avg_w_sent:.2f}",
        ]
        if kw_counts:
            lines.append("   Keywords más frecuentes: " + ", ".join(f"{k}({v})" for k, v in kw_counts))
    else:
        lines.append("*📰 No se encontraron noticias con keywords relevantes.*")

    lines += [
        "", "*🔍 Fuentes activas:*",
        *[f"   • {src.split('/')[2]} (Nivel {info['level']}, peso {info['base_weight']})"
          for src, info in RSS_FEEDS.items()],
    ]

    send_telegram("\n".join(l for l in lines if l is not None))

# ==================== INICIO ====================
if __name__ == "__main__":
    enviar_status_inicial()

    threading.Thread(target=run_flask,        daemon=True).start()
    threading.Thread(target=run_telegram_bot, daemon=True).start()
    logging.info("Flask en :5000 | Bot Telegram iniciado")

    scheduler = BackgroundScheduler()
    scheduler.add_job(update_signal, 'interval', minutes=30)
    # NUEVO: reporte semanal cada domingo a las 22:00 UTC
    scheduler.add_job(weekly_report_job, 'cron', day_of_week='sun', hour=22, minute=0)
    scheduler.start()
    logging.info("Scheduler activo — actualización cada 30 min + reporte semanal domingos 22:00 UTC")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
