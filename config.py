"""
Configuración del bot premium: variables de entorno y constantes de negocio.

Módulo "hoja" sin dependencias internas (solo `os`), importado por el resto.
El estado mutable en memoria (rate limiting, avisos enviados) y BOT_USERNAME
(que se reasigna al arrancar) NO viven aquí: se quedan en premium_bot.py.
"""
import os

from shared import product_config as cfg

# Fuente única de verdad de precios, límites, identidad y textos legales.
# Ver shared/README.md. Si product.json no cumple el esquema, esto lanza
# ConfigError y el bot no arranca: mejor caer al inicio que cobrar mal.
cfg.verify_startup()

_FUTBOL = cfg.product("futbol")
_PINPON = cfg.product("pinpon")

# ── Secretos / entorno ──────────────────────────────────────────────────────
TOKEN        = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# DB de solo lectura del bot de picks (para estadísticas reales)
PICKS_DATABASE_URL = os.getenv("PICKS_DATABASE_URL")

# ── Administración ──────────────────────────────────────────────────────────
ADMIN_IDS = [9330181]

# ── Canales de Telegram ─────────────────────────────────────────────────────
CANAL_CORNERS_ID = _FUTBOL["channels"]["premium_corners"]
CANAL_GOLES_ID   = _FUTBOL["channels"]["premium_goles"]
# C5: en el bot de picks este canal se llama premium_pre_over25. El nombre
# canónico vive ahora en shared/, así que "CANAL_PRE_ID" ya no significa dos
# cosas distintas según el repo.
CANAL_PRE_ID     = _FUTBOL["channels"]["premium_pre_over25"]

# Ping pong (tenis de mesa) — canal premium independiente. Por defecto apunta
# al canal OFICIAL de ping pong del bot de picks (-1004259041662); overridable
# por entorno. Si valiera 0, el plan quedaría OCULTO en el menú y
# get_plan_channels no devolvería canal (no se intenta invitar/expulsar).
# NOTA: el bot de pagos debe ser ADMIN de ese canal (invitar y expulsar).
CANAL_PINPON_ID  = int(
    os.getenv("CANAL_PINPON_ID") or _PINPON["channels"]["premium_official"]
)

# Enlace al canal FREE de ping pong (t.me/... público o de invitación). Aparece
# un botón "🏓 Canal FREE Ping Pong" en el menú del bot. Overridable por entorno.
PINPON_FREE_URL = (
    os.getenv("PINPON_FREE_URL") or _PINPON["telegram_channel_free"]
).strip()

LINK_FREE = _FUTBOL["telegram_channel_free"]

# ── Precios y métodos de pago ───────────────────────────────────────────────
# Se calculan siempre desde amount_cents + currency: el precio formateado no se
# almacena en ningún sitio. Cambiar un precio se hace en shared/product.json,
# y en el panel de Stripe el mismo día (ver shared/README.md).
PRECIO_GOLES   = cfg.price("goles")
PRECIO_CORNERS = cfg.price("corners")
PRECIO_COMBO   = cfg.price("combo")
PRECIO_PRE     = cfg.price("pre")
PRECIO_PINPON  = cfg.price("pinpon")

# Planes que ofrecen prueba gratis, según lo declarado en shared/.
TRIAL_PLANS = tuple(
    plan["id"]
    for producto in cfg.config()["products"].values()
    for plan in producto["plans"]
    if plan["trial_enabled"]
)

_METODOS = {m["id"]: m["value"] for m in cfg.config()["payment_methods"]}
BIZUM        = _METODOS["bizum"]
PAYPAL_LINK  = _METODOS["paypal"]
REVOLUT_LINK = _METODOS["revolut"]

STRIPE_GOLES   = cfg.plan("goles")["provider_payment_link"]
STRIPE_CORNERS = cfg.plan("corners")["provider_payment_link"]
STRIPE_COMBO   = cfg.plan("combo")["provider_payment_link"]
STRIPE_PRE     = cfg.plan("pre")["provider_payment_link"]
# Overridable por entorno; si quedara vacío, el bot no muestra el botón de
# tarjeta (PayPal/Bizum/Revolut siguen disponibles).
STRIPE_PINPON  = os.getenv("STRIPE_PINPON", cfg.plan("pinpon")["provider_payment_link"])

# ── Suscripciones / trials / accesos ────────────────────────────────────────
PLAN_DAYS    = cfg.config()["access"]["plan_interval_days"]
# Duración de la prueba gratuita POR PRODUCTO. Solo afecta a trials NUEVOS:
# los ya activos conservan su fecha_fin.
TRIAL_DAYS        = _FUTBOL["trial"]["duration_days"]
PINPON_TRIAL_DAYS = _PINPON["trial"]["duration_days"]
# Validez del enlace de invitación (horas). Antes 1h — demasiado corto: si el
# usuario no lo abría a tiempo, caducaba y quemaba intentos.
INVITE_EXPIRY_HOURS = cfg.config()["access"]["invite_expiry_hours"]

# Referidos: el referidor gana REFERIDOR_DIAS gratis por cada amigo que se
# suscriba. El recomendado NO recibe días extra (multiplicador 1).
_REFERIDOS = cfg.config()["access"]["referral"]
REFERIDOR_DIAS = _REFERIDOS["referrer_bonus_days"]
REFERIDO_MULTIPLICADOR = _REFERIDOS["referred_multiplier"]

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
MAX_GENERACIONES_ACCESO = cfg.config()["access"]["max_access_generations_per_period"]

# Encuesta de SATISFACCIÓN (CSAT) a clientes ACTIVOS: se envía una sola vez,
# tras NPS_DELAY_DAYS días desde el alta, en lotes de NPS_LOTE por barrido (cada
# hora) para no escribir a todos de golpe, y solo entre NPS_HORA_MIN y
# NPS_HORA_MAX (hora Madrid) para no molestar de madrugada.
NPS_DELAY_DAYS = int(os.getenv("NPS_DELAY_DAYS", "12"))
NPS_LOTE       = int(os.getenv("NPS_LOTE", "10"))
NPS_HORA_MIN   = int(os.getenv("NPS_HORA_MIN", "11"))
NPS_HORA_MAX   = int(os.getenv("NPS_HORA_MAX", "21"))

TIMEZONE = cfg.config()["timezone"]

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
