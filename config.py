"""
Configuración del bot premium: variables de entorno y constantes de negocio.

Módulo "hoja" sin dependencias internas (solo `os`), importado por el resto.
El estado mutable en memoria (rate limiting, avisos enviados) y BOT_USERNAME
(que se reasigna al arrancar) NO viven aquí: se quedan en premium_bot.py.
"""
import os

# ── Secretos / entorno ──────────────────────────────────────────────────────
TOKEN        = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# DB de solo lectura del bot de picks (para estadísticas reales)
PICKS_DATABASE_URL = os.getenv("PICKS_DATABASE_URL")

# ── Administración ──────────────────────────────────────────────────────────
ADMIN_IDS = [9330181]

# ── Canales de Telegram ─────────────────────────────────────────────────────
CANAL_CORNERS_ID = -1003895151594
CANAL_GOLES_ID   = -1003818905455
CANAL_PRE_ID     = -1003837149453   # Over 2.5 FT prepartido — análisis manual

LINK_FREE = "https://t.me/+WhIkP2PstS1kMDVk"

# ── Precios y métodos de pago ───────────────────────────────────────────────
PRECIO_GOLES   = "20€"
PRECIO_CORNERS = "20€"
PRECIO_COMBO   = "30€"
PRECIO_PRE     = "20€"

BIZUM        = "+34660426660"
PAYPAL_LINK  = "https://paypal.me/erikenobi"
REVOLUT_LINK = "https://revolut.me/ericblasco9"

STRIPE_GOLES   = "https://buy.stripe.com/aFa8wObuQ9MbdgA00x08g01"
STRIPE_CORNERS = "https://buy.stripe.com/bJe3cugPaf6vdgA5kR08g02"
STRIPE_COMBO   = "https://buy.stripe.com/4gM7sK8iE0bBgsMfZv08g03"
STRIPE_PRE     = "https://buy.stripe.com/aFafZg9mI6zZccw00x08g04"

# ── Suscripciones / trials / accesos ────────────────────────────────────────
PLAN_DAYS    = 30
TRIAL_DAYS   = 3
INVITE_EXPIRY_HOURS = 1

# Referidos: el referidor gana REFERIDOR_DIAS gratis y el recomendado recibe
# 2x1 (REFERIDO_MULTIPLICADOR × los días normales) en su primer pago.
REFERIDOR_DIAS = 30
REFERIDO_MULTIPLICADOR = 2

# Cada hora: reduce a ≤1h la ventana de acceso residual de un usuario ya
# caducado (antes 12h). La expulsión es idempotente y, gracias al flag
# acceso_revocado, no se re-banean usuarios ya expulsados con éxito.
CHECK_EXPIRATIONS_EVERY_SECONDS = 3600  # 1h
# Ventana del reintento automático de expulsión: solo se reintenta con
# caducados recientes (los fallos antiguos se fuerzan a mano con /reexpulsar).
REEXPULSION_RETRY_DAYS = 7

# Máximo de enlaces de acceso que un usuario puede auto-generar por periodo
# de suscripción. Limita el reparto de enlaces a terceros. El contador se
# reinicia con cada aprobación/renovación/regalo (registrar_acceso_pendiente).
MAX_GENERACIONES_ACCESO = 3

TIMEZONE = "Europe/Madrid"

DEPLOYMENT_COMMIT = (
    os.getenv("RAILWAY_GIT_COMMIT_SHA")
    or os.getenv("RAILWAY_GIT_COMMIT_MESSAGE")
    or os.getenv("RAILWAY_DEPLOYMENT_ID")
    or "local"
)

# ── Rate limiting: límite por acción (máx_llamadas, ventana_segundos) ────────
RATE_LIMITS: dict[str, tuple[int, int]] = {
    "start":       (5, 30),    # comando /start
    "menu":        (20, 20),   # navegación de botones (callbacks)
    "trial":       (3, 30),    # activación de prueba gratuita
    "acceso":      (2, 20),    # generación de enlaces (llama a la API Telegram)
    "comprobante": (4, 60),    # reenvío de comprobantes al admin
}

# Meses en español para formateo de stats
_MESES_ES = {
    "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
}
