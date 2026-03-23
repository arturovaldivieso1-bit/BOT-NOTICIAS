import os
import time
import logging
import requests
import feedparser
from apscheduler.schedulers.background import BackgroundScheduler

# Variables de entorno
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))   # cada 30 min

# Palabras clave para filtrar noticias
KEYWORDS = [
    "bitcoin", "btc", "crypto", "ethereum", "fed", "rate", "inflation",
    "sec", "etf", "jpmorgan", "blackrock", "attack", "war", "iran",
    "fomc", "powell", "emergency"
]

# Fuentes RSS gratuitas
RSS_FEEDS = [
    "https://www.reuters.com/rss/topNews",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://news.google.com/rss?q=bitcoin+OR+btc+OR+federal+reserve+OR+sec&hl=en-US&gl=US&ceid=US:en"
]

# Archivo para guardar enlaces ya enviados (evita duplicados)
SENT_LINKS_FILE = "sent_links.txt"

# Configurar logging
logging.basicConfig(level=logging.INFO)

def send_telegram(message):
    """Envía un mensaje a Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Faltan variables de Telegram")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }, timeout=10)
        resp.raise_for_status()
        logging.info("Mensaje enviado a Telegram")
    except Exception as e:
        logging.error(f"Error enviando mensaje: {e}")

def load_sent_links():
    """Carga el conjunto de enlaces ya enviados."""
    if os.path.exists(SENT_LINKS_FILE):
        with open(SENT_LINKS_FILE, "r") as f:
            return set(line.strip() for line in f)
    return set()

def save_sent_link(link):
    """Guarda un enlace en el archivo de enviados."""
    with open(SENT_LINKS_FILE, "a") as f:
        f.write(link + "\n")

def is_relevant(text):
    """Verifica si el texto contiene alguna palabra clave."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in KEYWORDS)

def fetch_news():
    """Recorre los RSS y devuelve una lista de (título, enlace, fuente)."""
    sent = load_sent_links()
    new_news = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:15]:  # últimos 15
                title = entry.get("title", "")
                link = entry.get("link", "")
                if not link or link in sent:
                    continue
                if is_relevant(title):
                    new_news.append((title, link, feed_url))
        except Exception as e:
            logging.error(f"Error en feed {feed_url}: {e}")
    return new_news

def main_job():
    logging.info("Buscando noticias relevantes...")
    news = fetch_news()
    if not news:
        logging.info("No hay noticias nuevas.")
        return
    for title, link, source in news:
        mensaje = f"📰 *NOTICIA RELEVANTE*\n📌 *{title}*\n🔗 [Leer más]({link})\n🏷️ Fuente: {source}"
        send_telegram(mensaje)
        save_sent_link(link)
        time.sleep(1)
    logging.info(f"Enviadas {len(news)} noticias.")

if __name__ == "__main__":
    # Mensaje de inicio
    send_telegram("🤖 *Bot de Noticias Simple iniciado*")
    # Programar la tarea cada INTERVAL_MINUTES minutos
    scheduler = BackgroundScheduler()
    scheduler.add_job(main_job, 'interval', minutes=INTERVAL_MINUTES)
    scheduler.start()
    logging.info(f"Bot iniciado. Intervalo: {INTERVAL_MINUTES} minutos.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
