import os
import json
import logging
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler

# ==================== CONFIGURACIÓN ====================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# Intervalo en horas entre ejecuciones (se puede cambiar por variable de entorno)
INTERVAL_HOURS = int(os.getenv("MACRO_BOT_INTERVAL_HOURS", "2"))

# Archivo para recordar eventos ya notificados (se crea en el mismo directorio)
SENT_EVENTS_FILE = "sent_events.json"

# Headers para evitar bloqueos por parte de Investing.com
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

CALENDAR_URL = "https://www.investing.com/economic-calendar/"

# Mapeo de impacto a emojis
IMPACT_MAP = {
    "High": "🔴 ALTO",
    "Medium": "🟡 MEDIO",
    "Low": "⚪ BAJO"
}

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==================== FUNCIONES AUXILIARES ====================
def send_telegram_message(message):
    """Envía un mensaje al chat de Telegram configurado."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Faltan variables de entorno TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        logging.info("Mensaje enviado a Telegram")
    except Exception as e:
        logging.error(f"Error enviando mensaje a Telegram: {e}")

def load_sent_events():
    """Carga la lista de IDs de eventos ya notificados desde el archivo JSON."""
    if os.path.exists(SENT_EVENTS_FILE):
        try:
            with open(SENT_EVENTS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error cargando {SENT_EVENTS_FILE}: {e}")
            return []
    return []

def save_sent_event(event_id):
    """Guarda un ID de evento como ya notificado."""
    sent = load_sent_events()
    if event_id not in sent:
        sent.append(event_id)
        try:
            with open(SENT_EVENTS_FILE, 'w') as f:
                json.dump(sent, f)
        except Exception as e:
            logging.error(f"Error guardando {SENT_EVENTS_FILE}: {e}")

def parse_event_datetime(date_str, time_str):
    """
    Convierte strings de fecha y hora (ej. 'Mar 25', '14:30') en un objeto datetime UTC.
    """
    try:
        combined = f"{date_str} {time_str}".strip()
        # Intentar con formato sin año
        for fmt in ["%b %d %H:%M", "%b %d, %Y %H:%M"]:
            try:
                dt = datetime.strptime(combined, fmt)
                if dt.year == 1900:
                    dt = dt.replace(year=datetime.now().year)
                # Asumimos que la hora está en UTC (Investing.com usa hora del servidor, pero simplificamos)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        logging.warning(f"No se pudo parsear fecha/hora: {date_str} / {time_str}")
        return None
    except Exception as e:
        logging.error(f"Error parseando datetime: {e}")
        return None

def get_bias(event):
    """Devuelve un sesgo orientativo según el tipo de evento."""
    name = event['event']
    if "CPI" in name or "PPI" in name:
        return "⚠️ Bajista si sorprende al alza"
    if "Nonfarm Payrolls" in name:
        return "⚠️ Bajista si muy fuerte"
    if "Fed" in name or "FOMC" in name:
        return "⚠️ Alta volatilidad, dirección incierta"
    if "GDP" in name:
        return "✅ Alcista si supera expectativas"
    if "Unemployment" in name:
        return "📉 Puede generar volatilidad"
    return "🔄 Esperar volatilidad"

def format_event_message(event):
    """Formatea un evento para enviarlo por Telegram."""
    impact_text = IMPACT_MAP.get(event['impact'], "⚪ DESCONOCIDO")
    dt = event['datetime']
    time_str = dt.strftime("%H:%M UTC")
    date_str = dt.strftime("%d/%m")
    message = (
        f"📅 *{event['event']}*\n"
        f"🗓️ {date_str} - {time_str}\n"
        f"🌍 {event['country']}\n"
        f"⚡ Impacto: {impact_text}\n"
        f"📊 Esperado: {event['forecast']} | Previo: {event['previous']}\n"
        f"📈 Sesgo: {get_bias(event)}\n"
    )
    return message

def fetch_events():
    """
    Scrapea Investing.com y devuelve una lista de eventos de las próximas 24h
    que tengan impacto 'High' o 'Medium'.
    """
    try:
        response = requests.get(CALENDAR_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # La tabla de eventos suele tener id 'economicCalendarData' o clase 'ecoCal'
        table = soup.find('table', {'id': 'economicCalendarData'})
        if not table:
            table = soup.find('table', class_='ecoCal')
        if not table:
            logging.error("No se encontró la tabla de eventos")
            return []

        events = []
        now = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=1)

        rows = table.find('tbody').find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 5:
                continue

            # Fecha y hora
            date_cell = cells[0].get_text(strip=True)
            time_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            event_dt = parse_event_datetime(date_cell, time_cell)
            if not event_dt:
                continue

            # Solo eventos en las próximas 24h
            if event_dt < now or event_dt > tomorrow:
                continue

            # País (a veces hay una bandera)
            country = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            # Nombre del evento
            event_name = cells[3].get_text(strip=True) if len(cells) > 3 else ""

            # Impacto
            impact_cell = cells[4]
            impact = "Low"
            if impact_cell.find('span', class_='high'):
                impact = "High"
            elif impact_cell.find('span', class_='medium'):
                impact = "Medium"

            # Datos económicos
            actual = cells[5].get_text(strip=True) if len(cells) > 5 else ""
            forecast = cells[6].get_text(strip=True) if len(cells) > 6 else ""
            previous = cells[7].get_text(strip=True) if len(cells) > 7 else ""

            # Solo nos interesan eventos de impacto alto o medio
            if impact not in ['High', 'Medium']:
                continue

            event_id = f"{event_dt.isoformat()}_{event_name}"
            events.append({
                "id": event_id,
                "datetime": event_dt,
                "country": country,
                "event": event_name,
                "impact": impact,
                "actual": actual,
                "forecast": forecast,
                "previous": previous
            })

        return events
    except Exception as e:
        logging.error(f"Error en fetch_events: {e}")
        return []

def main_job():
    """Tarea principal: obtiene eventos y notifica los nuevos."""
    logging.info("Ejecutando macro_hunter...")
    events = fetch_events()
    if not events:
        logging.info("No se encontraron eventos relevantes en las próximas 24h.")
        return

    sent_ids = load_sent_events()
    new_events = [e for e in events if e['id'] not in sent_ids]

    for event in new_events:
        msg = format_event_message(event)
        send_telegram_message(msg)
        save_sent_event(event['id'])
        time.sleep(1)  # evitar rate limit de Telegram

    logging.info(f"Procesados {len(events)} eventos, notificados {len(new_events)} nuevos.")

# ==================== INICIO ====================
if __name__ == "__main__":
    # Mensaje de inicio para confirmar que el bot está corriendo
    send_telegram_message("🤖 *Bot Macro Hunter iniciado* 🤖\n\nMonitoreando eventos macroeconómicos cada {} hora(s).".format(INTERVAL_HOURS))

    # Configurar scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(main_job, 'interval', hours=INTERVAL_HOURS)
    scheduler.start()
    logging.info(f"Macro Hunter iniciado. Intervalo: cada {INTERVAL_HOURS} hora(s).")

    try:
        # Mantener el script en ejecución
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("Deteniendo scheduler...")
        scheduler.shutdown()
