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

# Configuración
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
INTERVAL_MINUTES = int(os.getenv("NEWS_INTERVAL_MINUTES", "10"))
TWITTER_BEARER = os.getenv("TWITTER_BEARER_TOKEN")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")

# Fuentes RSS (pueden variar)
RSS_FEEDS = [
    "https://www.reuters.com/rss/topNews",
    "https://www.bloomberg.com/feed/podcast/etf-report.xml",  # limitado
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://news.google.com/rss?q=bitcoin+OR+btc+OR+fed+OR+sec&hl=en-US&gl=US&ceid=US:en"
]

# Palabras clave para filtrar relevancia
KEYWORDS = ["bitcoin", "btc", "crypto", "ethereum", "fed", "rate", "inflation", "sec", "etf", "jpmorgan", "blackrock"]

# Archivo para recordar noticias ya enviadas
SENT_NEWS_FILE = "sent_news.json"

def load_sent_news():
    if os.path.exists(SENT_NEWS_FILE):
        with open(SENT_NEWS_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_sent_news(news_id):
    sent = load_sent_news()
    sent.add(news_id)
    with open(SENT_NEWS_FILE, 'w') as f:
        json.dump(list(sent), f)

def send_telegram(message):
    # igual que en macro bot
    pass

def get_news_id(title, source, date):
    unique = f"{source}|{title}|{date}"
    return hashlib.md5(unique.encode()).hexdigest()

def analyze_sentiment(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity  # -1..1
    return polarity

def is_relevant(title, description):
    text = (title + " " + description).lower()
    for kw in KEYWORDS:
        if kw in text:
            return True
    return False

def fetch_rss_news():
    news = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:10]:  # top 10
                title = entry.get('title', '')
                summary = entry.get('summary', '')
                link = entry.get('link', '')
                published = entry.get('published', '')
                if is_relevant(title, summary):
                    news_id = get_news_id(title, feed_url, published)
                    polarity = analyze_sentiment(title + " " + summary)
                    news.append({
                        'id': news_id,
                        'title': title,
                        'summary': summary[:200],
                        'link': link,
                        'source': feed_url,
                        'published': published,
                        'sentiment': polarity
                    })
        except Exception as e:
            logging.error(f"Error en RSS {feed_url}: {e}")
    return news

def fetch_twitter_news():
    if not TWITTER_BEARER:
        return []
    try:
        client = tweepy.Client(bearer_token=TWITTER_BEARER)
        # Buscar tweets con palabras clave de los últimos 10 minutos
        start_time = (datetime.utcnow() - timedelta(minutes=INTERVAL_MINUTES)).isoformat() + "Z"
        query = "(" + " OR ".join(KEYWORDS) + ") -is:retweet"
        tweets = client.search_recent_tweets(query=query, max_results=10, start_time=start_time)
        if tweets.data:
            result = []
            for tweet in tweets.data:
                news_id = get_news_id(tweet.text, "twitter", tweet.id)
                polarity = analyze_sentiment(tweet.text)
                result.append({
                    'id': news_id,
                    'title': tweet.text[:100],
                    'summary': tweet.text,
                    'link': f"https://twitter.com/i/web/status/{tweet.id}",
                    'source': 'twitter',
                    'published': str(tweet.created_at),
                    'sentiment': polarity
                })
            return result
    except Exception as e:
        logging.error(f"Twitter API error: {e}")
    return []

def fetch_newsapi_news():
    if not NEWSAPI_KEY:
        return []
    try:
        url = "https://newsapi.org/v2/everything"
        params = {
            'q': 'bitcoin OR btc OR cryptocurrency OR federal reserve',
            'apiKey': NEWSAPI_KEY,
            'language': 'en',
            'pageSize': 10,
            'from': (datetime.utcnow() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'sortBy': 'publishedAt'
        }
        resp = requests.get(url, params=params)
        data = resp.json()
        if data['status'] == 'ok':
            news = []
            for article in data['articles']:
                title = article.get('title', '')
                description = article.get('description', '')
                if is_relevant(title, description):
                    news_id = get_news_id(title, article['source']['name'], article['publishedAt'])
                    polarity = analyze_sentiment(title + " " + description)
                    news.append({
                        'id': news_id,
                        'title': title,
                        'summary': description[:200],
                        'link': article['url'],
                        'source': article['source']['name'],
                        'published': article['publishedAt'],
                        'sentiment': polarity
                    })
            return news
    except Exception as e:
        logging.error(f"NewsAPI error: {e}")
    return []

def format_news_message(item):
    sentiment_emoji = "🔵 NEUTRAL"
    polarity = item['sentiment']
    if polarity > 0.2:
        sentiment_emoji = "🟢 ALCISTA"
    elif polarity < -0.2:
        sentiment_emoji = "🔴 BAJISTA"
    message = (
        f"📰 *NOTICIA RELEVANTE*\n"
        f"📌 *{item['title']}*\n"
        f"📝 {item['summary']}\n"
        f"🔗 [Leer más]({item['link']})\n"
        f"📊 Sentimiento: {sentiment_emoji} ({polarity:.2f})\n"
        f"🏷️ Fuente: {item['source']}\n"
        f"🕒 {item['published'][:16]}"
    )
    return message

def main_job():
    logging.info("Ejecutando news monitor...")
    all_news = []
    all_news.extend(fetch_rss_news())
    all_news.extend(fetch_twitter_news())
    all_news.extend(fetch_newsapi_news())

    sent_ids = load_sent_news()
    new_news = [n for n in all_news if n['id'] not in sent_ids]

    # Filtrar noticias con sentimiento significativo o palabras clave muy relevantes
    for news in new_news:
        # Enviar si sentimiento fuerte (|polarity| > 0.3) o si tiene palabras de alto impacto
        if abs(news['sentiment']) > 0.3 or any(kw in news['title'].lower() for kw in ['emergency', 'hike', 'attack', 'sec', 'etf']):
            msg = format_news_message(news)
            send_telegram(msg)
            save_sent_news(news['id'])
            time.sleep(1)
    logging.info(f"Procesadas {len(all_news)} noticias, enviadas {len(new_news)} nuevas.")

if __name__ == "__main__":
    send_telegram("📰 *Bot de Noticias v1 iniciado*")
    scheduler = BackgroundScheduler()
    scheduler.add_job(main_job, 'interval', minutes=INTERVAL_MINUTES)
    scheduler.start()
    while True:
        time.sleep(60)
