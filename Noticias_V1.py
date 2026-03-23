import os
import json
import time
import logging
import hashlib
import requests
import feedparser
from datetime import datetime, timedelta
from textblob import TextBlob
from apscheduler.schedulers.background import BackgroundScheduler

# ==================== CONFIGURACIÓN ====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Intervalo en minutos entre ejecuciones (para RSS y NewsAPI)
NEWS_INTERVAL_MINUTES = int(os.getenv("NEWS_INTERVAL_MINUTES", "60"))

# Twitter (opcional) - solo si se define el token
TWITTER_BEARER = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_INTERVAL_MINUTES = int(os.getenv("TWITTER_INTERVAL_MINUTES", "360"))  # 6 horas

# NewsAPI key (opcional)
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")

# Palabras clave para filtrar relevancia
KEYWORDS = [
    "bitcoin", "btc", "crypto", "ethereum", "fed", "rate", "inflation",
    "sec", "etf", "jpmorgan", "blackrock", "attack", "war", "iran",
    "interest rates", "fomc", "powell", "emergency"
]

# Umbral de sentimiento para enviar alerta (absoluto > 0.3)
SENTIMENT_THRESHOLD = 0.3

# Archivos para persistencia
SENT_NEWS_FILE = "sent_news.json"            # hashes de noticias ya enviadas
SENT_TITLES_CACHE = "sent_titles_cache.json"  # títulos normalizados con timestamp

# Fuentes RSS (puedes agregar o quitar según prefieras)
RSS_FEEDS = [
    "https://www.reuters.com/rss/topNews",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://news.google.com/rss?q=bitcoin+OR+btc+OR+federal+reserve+OR+sec&hl=en-US&gl=US&ceid=US:en"
]

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==================== FUNCIONES AUXILIARES ====================
def send_telegram(message):
    """Envía mensaje a Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Faltan variables de entorno TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        logging.info("Mensaje enviado a Telegram")
    except Exception as e:
        logging.error(f"Error enviando mensaje: {e}")

def load_json(filename, default=None):
    """Carga un archivo JSON, devuelve default si no existe o error."""
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error cargando {filename}: {e}")
    return default if default is not None else {}

def save_json(filename, data):
    """Guarda datos en un archivo JSON."""
    try:
        with open(filename, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logging.error(f"Error guardando {filename}: {e}")

def get_news_id(title, source, date_str):
    """Genera un ID único para una noticia basado en título, fuente y fecha."""
    unique = f"{source}|{title}|{date_str}"
    return hashlib.md5(unique.encode()).hexdigest()

def normalize_title(title):
    """Normaliza el título para comparar duplicados entre fuentes."""
    return title.lower().strip()

def is_duplicate_title(title, cache_file, max_age_hours=24):
    """
    Verifica si el título normalizado ya fue enviado en las últimas max_age_hours.
    También limpia entradas expiradas.
    """
    cache = load_json(cache_file, {})
    normalized = normalize_title(title)
    now = datetime.utcnow()
    # Limpiar entradas expiradas
    expired = []
    for t, ts_str in cache.items():
        ts = datetime.fromisoformat(ts_str)
        if now - ts > timedelta(hours=max_age_hours):
            expired.append(t)
    for t in expired:
        del cache[t]
    # Guardar caché limpia (opcional, pero no obligatorio aquí)
    # No guardamos ahora para no hacer escritura innecesaria; lo haremos al marcar como enviado.
    # Si el título ya existe, es duplicado
    if normalized in cache:
        return True
    return False

def mark_title_as_sent(title, cache_file):
    """Registra que un título ha sido enviado."""
    cache = load_json(cache_file, {})
    normalized = normalize_title(title)
    cache[normalized] = datetime.utcnow().isoformat()
    save_json(cache_file, cache)

def analyze_sentiment(text):
    """Retorna polaridad de sentimiento con TextBlob."""
    blob = TextBlob(text)
    return blob.sentiment.polarity

def is_relevant(title, description):
    """Verifica si el texto contiene alguna palabra clave."""
    text = (title + " " + description).lower()
    for kw in KEYWORDS:
        if kw in text:
            return True
    return False

def format_news_message(item):
    """Formatea una noticia para Telegram."""
    sentiment = item['sentiment']
    if sentiment > SENTIMENT_THRESHOLD:
        sentiment_emoji = "🟢 ALCISTA"
    elif sentiment < -SENTIMENT_THRESHOLD:
        sentiment_emoji = "🔴 BAJISTA"
    else:
        sentiment_emoji = "🔵 NEUTRAL"

    message = (
        f"📰 *NOTICIA RELEVANTE*\n"
        f"📌 *{item['title']}*\n"
        f"📝 {item['summary'][:250]}...\n"
        f"🔗 [Leer más]({item['link']})\n"
        f"📊 Sentimiento: {sentiment_emoji} ({sentiment:.2f})\n"
        f"🏷️ Fuente: {item['source']}\n"
        f"🕒 {item['published'][:16]}"
    )
    return message

# ==================== FUENTES DE NOTICIAS ====================
def fetch_rss_news():
    """Extrae noticias de los feeds RSS."""
    all_news = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:15]:  # top 15 de cada feed
                title = entry.get('title', '')
                summary = entry.get('summary', '')
                link = entry.get('link', '')
                published = entry.get('published', datetime.utcnow().isoformat())
                if is_relevant(title, summary):
                    news_id = get_news_id(title, feed_url, published)
                    polarity = analyze_sentiment(title + " " + summary)
                    all_news.append({
                        'id': news_id,
                        'title': title,
                        'summary': summary[:250],
                        'link': link,
                        'source': feed_url,
                        'published': published,
                        'sentiment': polarity
                    })
        except Exception as e:
            logging.error(f"Error en RSS {feed_url}: {e}")
    return all_news

def fetch_newsapi_news():
    """Usa NewsAPI (gratis) para noticias de las últimas horas."""
    if not NEWSAPI_KEY:
        logging.info("NewsAPI no configurada, omitiendo.")
        return []
    try:
        # Buscar noticias de las últimas 2 horas (evitar solapamientos)
        from_time = (datetime.utcnow() - timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%SZ')
        url = "https://newsapi.org/v2/everything"
        params = {
            'q': 'bitcoin OR btc OR cryptocurrency OR "federal reserve" OR sec OR etf',
            'apiKey': NEWSAPI_KEY,
            'language': 'en',
            'pageSize': 20,
            'from': from_time,
            'sortBy': 'publishedAt'
        }
        resp = requests.get(url, params=params, timeout=15)
        data = resp.json()
        if data['status'] == 'ok':
            news = []
            for article in data['articles']:
                title = article.get('title', '')
                description = article.get('description', '')
                if not is_relevant(title, description):
                    continue
                if title == "[Removed]":
                    continue
                news_id = get_news_id(title, article['source']['name'], article['publishedAt'])
                polarity = analyze_sentiment(title + " " + description)
                news.append({
                    'id': news_id,
                    'title': title,
                    'summary': description[:250],
                    'link': article['url'],
                    'source': article['source']['name'],
                    'published': article['publishedAt'],
                    'sentiment': polarity
                })
            return news
        else:
            logging.error(f"NewsAPI error: {data.get('message', 'Unknown error')}")
            return []
    except Exception as e:
        logging.error(f"NewsAPI exception: {e}")
        return []

def fetch_twitter_news():
    """Obtiene tweets recientes con palabras clave (límite: pocas veces al día)."""
    if not TWITTER_BEARER:
        return []
    try:
        import tweepy
        client = tweepy.Client(bearer_token=TWITTER_BEARER)
        start_time = (datetime.utcnow() - timedelta(hours=TWITTER_INTERVAL_MINUTES/60)).isoformat() + "Z"
        query = "(" + " OR ".join(KEYWORDS) + ") -is:retweet lang:en"
        tweets = client.search_recent_tweets(
            query=query,
            max_results=10,
            start_time=start_time,
            tweet_fields=['created_at']
        )
        if tweets.data:
            result = []
            for tweet in tweets.data:
                text = tweet.text
                news_id = get_news_id(text, "twitter", str(tweet.id))
                polarity = analyze_sentiment(text)
                result.append({
                    'id': news_id,
                    'title': text[:100],
                    'summary': text,
                    'link': f"https://twitter.com/i/web/status/{tweet.id}",
                    'source': "Twitter",
                    'published': str(tweet.created_at),
                    'sentiment': polarity
                })
            return result
    except Exception as e:
        logging.error(f"Twitter API error: {e}")
    return []

# ==================== ORQUESTADOR PRINCIPAL ====================
def process_and_send_news(news_list):
    """Recibe lista de noticias, filtra duplicados y envía las relevantes."""
    if not news_list:
        return 0

    # Cargar IDs ya enviados (para evitar duplicados exactos)
    sent_ids = load_json(SENT_NEWS_FILE, [])
    # Cargar caché de títulos (para evitar mismas noticias de distintas fuentes)
    title_cache_file = SENT_TITLES_CACHE

    new_count = 0
    for news in news_list:
        # 1. Evitar duplicados por ID
        if news['id'] in sent_ids:
            continue
        # 2. Evitar duplicados por título normalizado
        if is_duplicate_title(news['title'], title_cache_file):
            continue
        # 3. Decidir si enviar según sentimiento o palabras de alto impacto
        high_impact_keywords = ['emergency', 'attack', 'hike', 'sec', 'etf', 'fomc', 'powell']
        title_lower = news['title'].lower()
        has_high_impact = any(kw in title_lower for kw in high_impact_keywords)
        if abs(news['sentiment']) > SENTIMENT_THRESHOLD or has_high_impact:
            msg = format_news_message(news)
            send_telegram(msg)
            # Registrar como enviado
            sent_ids.append(news['id'])
            mark_title_as_sent(news['title'], title_cache_file)
            new_count += 1
            time.sleep(1)  # evitar rate limit de Telegram

    # Guardar sent_ids actualizados (como lista, no como set para JSON)
    save_json(SENT_NEWS_FILE, sent_ids)
    return new_count

def news_job():
    """Tarea principal: recolecta noticias de todas las fuentes y las envía."""
    logging.info("Ejecutando news_job...")

    all_news = []
    # RSS siempre se ejecuta
    rss_news = fetch_rss_news()
    all_news.extend(rss_news)

    # NewsAPI se ejecuta solo si tenemos clave
    if NEWSAPI_KEY:
        api_news = fetch_newsapi_news()
        all_news.extend(api_news)

    # Twitter no se incluye aquí porque tiene su propio job separado
    processed = process_and_send_news(all_news)
    logging.info(f"Noticias procesadas: RSS: {len(rss_news)} + NewsAPI: {len(api_news) if NEWSAPI_KEY else 0}. Nuevas enviadas: {processed}")

def twitter_job():
    """Tarea específica para Twitter, con intervalo propio."""
    if not TWITTER_BEARER:
        return
    logging.info("Ejecutando twitter_job...")
    twitter_news = fetch_twitter_news()
    processed = process_and_send_news(twitter_news)
    logging.info(f"Twitter: {len(twitter_news)} noticias obtenidas, nuevas enviadas: {processed}")

# ==================== INICIO ====================
if __name__ == "__main__":
    # Mensaje de inicio
    inicio_msg = "📰 *Bot de Noticias v2 iniciado*\n\n✅ RSS + NewsAPI"
    if TWITTER_BEARER:
        inicio_msg += " + Twitter"
    inicio_msg += f"\n⏱️ Intervalo NewsAPI/RSS: {NEWS_INTERVAL_MINUTES} min"
    if TWITTER_BEARER:
        inicio_msg += f"\n🐦 Twitter cada {TWITTER_INTERVAL_MINUTES} min"
    send_telegram(inicio_msg)

    # Scheduler principal (RSS y NewsAPI)
    scheduler = BackgroundScheduler()
    scheduler.add_job(news_job, 'interval', minutes=NEWS_INTERVAL_MINUTES)

    # Scheduler para Twitter (si está configurado)
    if TWITTER_BEARER:
        scheduler.add_job(twitter_job, 'interval', minutes=TWITTER_INTERVAL_MINUTES)

    scheduler.start()
    logging.info(f"Bot de noticias iniciado. Intervalo principal: {NEWS_INTERVAL_MINUTES} min, Twitter: {TWITTER_INTERVAL_MINUTES if TWITTER_BEARER else 'no configurado'} min.")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Deteniendo scheduler...")
        scheduler.shutdown()
