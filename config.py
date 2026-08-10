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

# Ping pong (tenis de mesa) — canal premium independiente. Por defecto apunta
# al canal OFICIAL de ping pong del bot de picks (-1004259041662); overridable
# por entorno. Si valiera 0, el plan quedaría OCULTO en el menú y
# get_plan_channels no devolvería canal (no se intenta invitar/expulsar).
# NOTA: el bot de pagos debe ser ADMIN de ese canal (invitar y expulsar).
CANAL_PINPON_ID  = int(os.getenv("CANAL_PINPON_ID", "-1004259041662") or "0")

# Enlace al canal FREE de ping pong (t.me/... público o de invitación). Si se
# define, aparece un botón "🏓 Ping Pong FREE" en el menú del bot. Vacío = sin botón.
PINPON_FREE_URL = (os.getenv("PINPON_FREE_URL") or "").strip()

LINK_FREE = "https://t.me/+WhIkP2PstS1kMDVk"

# ── Precios y métodos de pago ───────────────────────────────────────────────
PRECIO_GOLES   = "20€"
PRECIO_CORNERS = "20€"
PRECIO_COMBO   = "30€"
PRECIO_PRE     = "20€"
PRECIO_PINPON  = "50€"

# Planes que ofrecen prueba gratis. Ping pong (50€) también ofrece prueba:
# 3 días son más que suficientes para un método con ~90% de acierto.
TRIAL_PLANS = ("goles", "corners", "combo", "pre", "pinpon")

BIZUM        = "+34660426660"
PAYPAL_LINK  = "https://paypal.me/erikenobi"
REVOLUT_LINK = "https://revolut.me/ericblasco9"

STRIPE_GOLES   = "https://buy.stripe.com/aFa8wObuQ9MbdgA00x08g01"
STRIPE_CORNERS = "https://buy.stripe.com/bJe3cugPaf6vdgA5kR08g02"
STRIPE_COMBO   = "https://buy.stripe.com/4gM7sK8iE0bBgsMfZv08g03"
STRIPE_PRE     = "https://buy.stripe.com/aFafZg9mI6zZccw00x08g04"
# Enlace de pago de Ping Pong (50€). Overridable por entorno; si quedara vacío,
# el bot no muestra el botón de tarjeta (PayPal/Bizum/Revolut siguen disponibles).
STRIPE_PINPON  = os.getenv("STRIPE_PINPON", "https://buy.stripe.com/6oUcN4gPa2jJb8s8x308g05")

# ── Suscripciones / trials / accesos ────────────────────────────────────────
PLAN_DAYS    = 30
# Duración de la prueba gratuita. Parametrizable por entorno (default 3 días).
# Solo afecta a trials NUEVOS: los ya activos conservan su fecha_fin.
TRIAL_DAYS   = int(os.getenv("TRIAL_DAYS", "3"))
# Validez del enlace de invitación (horas). Antes 1h — demasiado corto: si el
# usuario no lo abría a tiempo, caducaba y quemaba intentos. Ahora 24h.
INVITE_EXPIRY_HOURS = int(os.getenv("INVITE_EXPIRY_HOURS", "24"))

# Referidos: el referidor gana REFERIDOR_DIAS gratis por cada amigo que se
# suscriba. El recomendado NO recibe días extra (multiplicador 1): se regalaba
# demasiado en un servicio con ~90% de acierto.
REFERIDOR_DIAS = 15
REFERIDO_MULTIPLICADOR = 1

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
MAX_GENERACIONES_ACCESO = int(os.getenv("MAX_GENERACIONES_ACCESO", "6"))

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
