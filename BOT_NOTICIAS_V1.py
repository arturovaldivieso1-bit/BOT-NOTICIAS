import os
import time
import logging
import requests
import feedparser
from apscheduler.schedulers.background import BackgroundScheduler

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Intervalo en minutos (cada 30 minutos)
INTERVAL_MINUTES = int(os.getenv("NEWS_INTERVAL_MINUTES", "30"))

# Palabras clave para filtrar
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

# Archivo para recordar noticias ya enviadas (basado en el link)
SENT_FILE = "sent_news.txt"

logging.basicConfig(level=logging.INFO)

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
    except Exception as e:
        logging.error(f"Telegram error: {e}")

def load_sent():
    if os.path.exists(SENT_FILE):
        with open(SENT_FILE, 'r') as f:
            return set(line.strip() for line in f)
    return set()

def save_sent(link):
    with open(SENT_FILE, 'a') as f:
        f.write(link + "\n")

def is_relevant(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in KEYWORDS)

def fetch_news():
    new_news = []
    sent = load_sent()
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:10]:
                title = entry.get('title', '')
                link = entry.get('link', '')
                if not link or link in sent:
                    continue
                if is_relevant(title):
                    new_news.append((title, link, feed_url))
        except Exception as e:
            logging.error(f"Error en {feed_url}: {e}")
    return new_news

def main_job():
    logging.info("Ejecutando búsqueda de noticias...")
    news = fetch_news()
    if not news:
        logging.info("No se encontraron noticias nuevas relevantes.")
        return
    for title, link, source in news:
        msg = f"📰 *NOTICIA RELEVANTE*\n📌 *{title}*\n🔗 [Leer más]({link})\n🏷️ Fuente: {source}"
        send_telegram(msg)
        save_sent(link)
        time.sleep(1)
    logging.info(f"Enviadas {len(news)} noticias.")

if __name__ == "__main__":
    send_telegram("📰 *Bot de Noticias Simple iniciado*")
    scheduler = BackgroundScheduler()
    scheduler.add_job(main_job, 'interval', minutes=INTERVAL_MINUTES)
    scheduler.start()
    logging.info(f"Bot iniciado, interval {INTERVAL_MINUTES} min.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
