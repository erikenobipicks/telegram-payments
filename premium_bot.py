import asyncio
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from time import monotonic
from urllib.parse import quote
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ==============================
# LOGGING
# ==============================

def configurar_logging() -> None:
    fmt_consola = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    fmt_archivo = "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("premium_bot.log", encoding="utf-8"),
    ]
    handlers[0].setLevel(logging.INFO)
    handlers[0].setFormatter(logging.Formatter(fmt_consola, datefmt=date_fmt))
    handlers[1].setLevel(logging.DEBUG)
    handlers[1].setFormatter(logging.Formatter(fmt_archivo, datefmt=date_fmt))

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in handlers:
        root.addHandler(h)

    for lib in ("httpx", "httpcore", "telegram.ext", "apscheduler"):
        logging.getLogger(lib).setLevel(logging.WARNING)

configurar_logging()
logger = logging.getLogger(__name__)


# ==============================
# CONFIG
# ==============================

TOKEN        = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# DB de solo lectura del bot de picks (para estadísticas reales)
PICKS_DATABASE_URL = os.getenv("PICKS_DATABASE_URL")

ADMIN_IDS = [9330181]

CANAL_CORNERS_ID = -1003895151594
CANAL_GOLES_ID   = -1003818905455
CANAL_PRE_ID     = -1003837149453   # Over 2.5 FT prepartido — análisis manual

LINK_FREE = "https://t.me/+WhIkP2PstS1kMDVk"

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

PLAN_DAYS    = 30
TRIAL_DAYS   = 3
INVITE_EXPIRY_HOURS = 1

# Referidos: el referidor gana REFERIDOR_DIAS gratis y el recomendado recibe
# 2x1 (REFERIDO_MULTIPLICADOR × los días normales) en su primer pago.
REFERIDOR_DIAS = 30
REFERIDO_MULTIPLICADOR = 2

# Username del bot (sin @), para construir los enlaces de referido. Se rellena
# al arrancar (post_init) con get_me(); también puede fijarse por entorno.
BOT_USERNAME = os.getenv("BOT_USERNAME")

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

# Avisos de expiración ya enviados en este proceso (evita duplicados entre
# las dos ejecuciones diarias del job). Se pierde en reinicio, lo cual es
# aceptable: en el peor caso se envía el aviso dos veces tras un restart.
_avisos_enviados: set[tuple[int, str]] = set()

# ── Rate limiting en memoria ────────────────────────────────────────────────
# Bot de proceso único (polling), así que un limitador en memoria basta.
# Mapea (user_id, acción) -> timestamps monotónicos recientes; se purga al
# vuelo. Límite por acción: (máx_llamadas, ventana_segundos).
_rate_buckets: dict[tuple[int, str], list[float]] = {}

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


# ==============================
# DB — BOT PREMIUM
# ==============================

_pool: ConnectionPool | None = None


def init_pool() -> None:
    global _pool
    if not DATABASE_URL:
        raise ValueError("Falta DATABASE_URL en variables de entorno.")
    _pool = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        # Holgado para que varias consultas offloaded a hilos (ver _run_db)
        # puedan ejecutarse en paralelo sin esperar a una conexión libre.
        max_size=10,
        kwargs={"row_factory": dict_row},
    )
    _pool.wait()
    logger.info("Pool de conexiones DB inicializado.")


def get_conn():
    if _pool is not None:
        return _pool.connection()
    if not DATABASE_URL:
        raise ValueError("Falta DATABASE_URL en variables de entorno.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


async def _run_db(func, *args, **kwargs):
    """
    Ejecuta una función de DB SÍNCRONA en un hilo aparte para no bloquear el
    event loop de asyncio mientras espera a Postgres. El pool de psycopg es
    thread-safe, así que es seguro obtener conexiones desde estos hilos.
    Uso: `fila = await _run_db(get_pending_payment, user_id)`.
    """
    return await asyncio.to_thread(func, *args, **kwargs)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    telegram_user_id BIGINT PRIMARY KEY,
                    username         TEXT,
                    full_name        TEXT,
                    plan             TEXT NOT NULL,
                    fecha_inicio     DATE NOT NULL,
                    fecha_fin        DATE NOT NULL,
                    estado           TEXT NOT NULL DEFAULT 'activo',
                    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at       TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            # Flag de control de expulsión: TRUE cuando el acceso a los canales
            # ya se ha revocado con éxito. Evita re-banear en cada ciclo del job
            # a usuarios ya expulsados (clave al bajar el intervalo a 1h). Se
            # reinicia a FALSE cuando el usuario se reactiva (ver _UPSERT_USER_SQL).
            cur.execute(
                """
                ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS acceso_revocado BOOLEAN NOT NULL DEFAULT FALSE;
                """
            )
            # Motivo de la baja para estados 'cancelado' / 'reembolsado'
            # (texto libre del admin). NULL mientras la suscripción está activa.
            cur.execute(
                """
                ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS motivo_baja TEXT;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_payments (
                    telegram_user_id BIGINT PRIMARY KEY,
                    username         TEXT,
                    full_name        TEXT,
                    plan             TEXT NOT NULL,
                    created_at       TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_access (
                    telegram_user_id BIGINT PRIMARY KEY,
                    plan             TEXT NOT NULL,
                    approved_at      TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            # Columnas para limitar y auditar la auto-generación de enlaces
            # (control de la fuga de acceso por reparto de enlaces de 1 uso).
            cur.execute(
                """
                ALTER TABLE pending_access
                    ADD COLUMN IF NOT EXISTS generaciones INTEGER NOT NULL DEFAULT 0;
                """
            )
            cur.execute(
                """
                ALTER TABLE pending_access
                    ADD COLUMN IF NOT EXISTS ultimos_enlaces TEXT;
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trials (
                    telegram_user_id BIGINT PRIMARY KEY,
                    plan             TEXT NOT NULL,
                    used_at          TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_visitors (
                    telegram_user_id BIGINT PRIMARY KEY,
                    username         TEXT,
                    full_name        TEXT,
                    first_seen_at    TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS encuestas (
                    telegram_user_id BIGINT PRIMARY KEY,
                    plan             TEXT,
                    sent_at          TIMESTAMP NOT NULL DEFAULT NOW(),
                    razon            TEXT,
                    valoracion       INTEGER,
                    sugerencia       TEXT,
                    responded_at     TIMESTAMP,
                    awaiting_sugerencia BOOLEAN NOT NULL DEFAULT FALSE
                );
                """
            )
            # Registro de auditoría persistente: a diferencia del log en
            # fichero (efímero, se pierde en cada redeploy), esta tabla guarda
            # de forma permanente los eventos financieros y de acceso para
            # poder reconstruir el historial de cada usuario (soporte, disputas).
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_log (
                    id             BIGSERIAL PRIMARY KEY,
                    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    event          TEXT NOT NULL,
                    actor_id       BIGINT,
                    actor_tipo     TEXT NOT NULL DEFAULT 'sistema',
                    target_user_id BIGINT,
                    plan           TEXT,
                    fecha_fin      DATE,
                    detalle        TEXT
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_target
                ON audit_log (target_user_id, created_at DESC);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_audit_event
                ON audit_log (event, created_at DESC);
                """
            )
            # Referidos: cada persona puede ser recomendada una sola vez
            # (referred_user_id es PK). estado: 'pendiente' (registrado, aún no
            # pagó) → 'recompensado' (pagó y se aplicaron las recompensas).
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS referrals (
                    referred_user_id  BIGINT PRIMARY KEY,
                    referrer_user_id  BIGINT NOT NULL,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    estado            TEXT NOT NULL DEFAULT 'pendiente',
                    rewarded_at       TIMESTAMPTZ
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_referrals_referrer
                ON referrals (referrer_user_id);
                """
            )
    logger.info("Base de datos inicializada.")


# ==============================
# DB — ESTADÍSTICAS REALES (picks DB)
# ==============================

def get_picks_conn():
    """Conexión de solo lectura a la DB del bot de picks."""
    if not PICKS_DATABASE_URL:
        logger.debug("PICKS_DATABASE_URL no configurada — stats reales no disponibles.")
        return None
    try:
        return psycopg.connect(PICKS_DATABASE_URL, row_factory=dict_row)
    except Exception as e:
        logger.error(f"Error conectando a picks DB: {e}")
        return None


def calcular_strike(hits: int, misses: int) -> float:
    resueltos = hits + misses
    return round((hits / resueltos) * 100, 1) if resueltos > 0 else 0.0


# Caché en memoria de las stats reales: estas 3 consultas GROUP BY son
# pesadas y se piden en cada vista de plan/stats. Con TTL evitamos golpear
# la picks DB en cada clic. Los datos cambian poco (resultados de picks),
# así que un TTL de minutos es de sobra.
_STATS_CACHE_TTL_SECONDS = 600  # 10 min
_stats_cache: dict = {"data": None, "ts": 0.0}


def get_stats_reales() -> dict | None:
    """
    Obtiene estadísticas reales del bot de picks (con caché de
    _STATS_CACHE_TTL_SECONDS):
      - globales: strike total por tipo_pick
      - ultimo_mes: stats del mes anterior cerrado
      - mes_label: "YYYY-MM" del último mes
      - evolucion: stats agrupadas por mes/tipo de los últimos 6 meses
    Si la conexión o la consulta falla, devuelve lo último cacheado (aunque
    esté algo viejo) o None si no hay nada cacheado.
    """
    now = monotonic()
    cached = _stats_cache["data"]
    if cached is not None and (now - _stats_cache["ts"]) < _STATS_CACHE_TTL_SECONDS:
        return cached

    conn = get_picks_conn()
    if not conn:
        return cached

    try:
        with conn:
            with conn.cursor() as cur:

                # Stats globales por tipo (solo picks resueltos)
                cur.execute("""
                    SELECT
                        tipo_pick,
                        COUNT(*)                                              AS total,
                        SUM(CASE WHEN resultado = 'HIT'  THEN 1 ELSE 0 END) AS hits,
                        SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END) AS misses,
                        SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END) AS voids
                    FROM picks
                    WHERE resultado IS NOT NULL
                    GROUP BY tipo_pick;
                """)
                globales = {row["tipo_pick"]: row for row in cur.fetchall()}

                # Último mes cerrado (zona horaria Madrid)
                cur.execute("""
                    SELECT
                        TO_CHAR(
                            date_trunc('month',
                                (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid')::date
                                - INTERVAL '1 month'
                            ),
                            'YYYY-MM'
                        ) AS mes,
                        tipo_pick,
                        COUNT(*)                                              AS total,
                        SUM(CASE WHEN resultado = 'HIT'  THEN 1 ELSE 0 END) AS hits,
                        SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END) AS misses,
                        SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END) AS voids
                    FROM picks
                    WHERE resultado IS NOT NULL
                      AND date_trunc('month', fecha) =
                          date_trunc('month',
                              (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid')::date
                              - INTERVAL '1 month'
                          )
                    GROUP BY mes, tipo_pick;
                """)
                ultimo_mes_rows = cur.fetchall()
                ultimo_mes  = {row["tipo_pick"]: row for row in ultimo_mes_rows}
                mes_label   = ultimo_mes_rows[0]["mes"] if ultimo_mes_rows else None

                # Evolución mensual (últimos 6 meses)
                cur.execute("""
                    SELECT
                        TO_CHAR(fecha, 'YYYY-MM') AS mes,
                        tipo_pick,
                        COUNT(*)                                              AS total,
                        SUM(CASE WHEN resultado = 'HIT'  THEN 1 ELSE 0 END) AS hits,
                        SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END) AS misses
                    FROM picks
                    WHERE resultado IS NOT NULL
                      AND fecha >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 months')
                    GROUP BY mes, tipo_pick
                    ORDER BY mes DESC, tipo_pick;
                """)
                evolucion = cur.fetchall()

        result = {
            "globales":   globales,
            "ultimo_mes": ultimo_mes,
            "mes_label":  mes_label,
            "evolucion":  evolucion,
        }
        _stats_cache["data"] = result
        _stats_cache["ts"] = now
        return result

    except Exception as e:
        logger.error(f"Error obteniendo stats reales: {e}")
        return cached


def _get_strike_tipo(stats: dict | None, tipo: str) -> str | None:
    """
    Devuelve el strike del último mes para un tipo_pick ("gol" / "corner").
    Si no hay datos del último mes, usa el global.
    Devuelve None si no hay datos en absoluto.

    El `.` decimal va escapado para que el valor pueda inyectarse
    directamente en mensajes con parse_mode="MarkdownV2".
    """
    if not stats:
        return None
    ultimo_mes = stats.get("ultimo_mes", {})
    if tipo in ultimo_mes:
        row = ultimo_mes[tipo]
        return f"{calcular_strike(row['hits'], row['misses'])}%".replace(".", "\\.")
    globales = stats.get("globales", {})
    if tipo in globales:
        row = globales[tipo]
        return f"{calcular_strike(row['hits'], row['misses'])}%".replace(".", "\\.")
    return None


def _formatear_stats_reales(stats: dict) -> str:
    """
    Formatea el mensaje de estadísticas reales para el menú del bot premium.
    Incluye: último mes cerrado, strike global y evolución mensual en tabla.
    """
    globales   = stats["globales"]
    ultimo_mes = stats["ultimo_mes"]
    mes_label  = stats["mes_label"]
    evolucion  = stats["evolucion"]

    lineas = ["📊 *Rendimiento real del servicio*\n"]

    # ── Último mes cerrado ──────────────────────────────────────────────
    if ultimo_mes:
        if mes_label:
            mes_nombre = (
                _MESES_ES.get(mes_label[5:], mes_label[5:]) + " " + mes_label[:4]
            )
        else:
            mes_nombre = "—"

        lineas.append(f"📅 *Último mes cerrado — {mes_nombre}*")

        if "gol" in ultimo_mes:
            g = ultimo_mes["gol"]
            s = calcular_strike(g["hits"], g["misses"])
            lineas.append(f"⚽ Goles: {g['total']} picks | ✅ {g['hits']} HITs | 📈 {s}%")

        if "corner" in ultimo_mes:
            c = ultimo_mes["corner"]
            s = calcular_strike(c["hits"], c["misses"])
            lineas.append(f"🚩 Corners: {c['total']} picks | ✅ {c['hits']} HITs | 📈 {s}%")

        lineas.append("")

    # ── Strike global ───────────────────────────────────────────────────
    lineas.append("🏆 *Strike acumulado (todos los picks resueltos)*")

    if "gol" in globales:
        g = globales["gol"]
        s = calcular_strike(g["hits"], g["misses"])
        lineas.append(f"⚽ Goles: {g['total']} picks | {s}%")

    if "corner" in globales:
        c = globales["corner"]
        s = calcular_strike(c["hits"], c["misses"])
        lineas.append(f"🚩 Corners: {c['total']} picks | {s}%")

    lineas.append("")

    # ── Evolución mensual ───────────────────────────────────────────────
    if evolucion:
        lineas.append("📈 *Evolución — últimos 6 meses*")
        lineas.append("```")
        lineas.append(f"{'Mes':<8}  {'Goles':>7}  {'Corners':>9}")
        lineas.append("─" * 28)

        por_mes: dict[str, dict] = defaultdict(dict)
        for row in evolucion:
            por_mes[row["mes"]][row["tipo_pick"]] = row

        for mes_key in sorted(por_mes.keys(), reverse=True):
            nombre_mes = (
                _MESES_ES.get(mes_key[5:], mes_key[5:]) + " " + mes_key[2:4]
            )
            datos = por_mes[mes_key]

            g_str = "  —  "
            if "gol" in datos:
                g = datos["gol"]
                g_str = f"{calcular_strike(g['hits'], g['misses'])}%"

            c_str = "  —  "
            if "corner" in datos:
                c = datos["corner"]
                c_str = f"{calcular_strike(c['hits'], c['misses'])}%"

            lineas.append(f"{nombre_mes:<8}  {g_str:>7}  {c_str:>9}")

        lineas.append("```")
        lineas.append("")

    # ── Aviso legal ─────────────────────────────────────────────────────
    lineas.append(
        "⚠️ _Porcentajes calculados sobre picks con resultado ya conocido. "
        "El rendimiento pasado no garantiza resultados futuros. "
        "Este servicio es únicamente informativo._"
    )

    return "\n".join(lineas)


# ==============================
# UTILS
# ==============================

def today_date():
    """Fecha de hoy en zona horaria Europe/Madrid."""
    return datetime.now(ZoneInfo(TIMEZONE)).date()


def now_utc():
    return datetime.now(timezone.utc)


def parse_date(date_str: str):
    return datetime.strptime(str(date_str), "%Y-%m-%d").date()


def rate_limited(user_id: int, accion: str) -> bool:
    """
    Devuelve True si la acción debe BLOQUEARSE por exceder su límite (ventana
    deslizante en memoria). Los admins nunca se limitan. Acciones sin límite
    configurado nunca bloquean.
    """
    if user_id in ADMIN_IDS:
        return False
    max_calls, per_seconds = RATE_LIMITS.get(accion, (0, 0))
    if max_calls <= 0:
        return False

    now = monotonic()
    cutoff = now - per_seconds
    bucket = _rate_buckets.get((user_id, accion))
    if bucket is None:
        bucket = []
        _rate_buckets[(user_id, accion)] = bucket

    # Purga los timestamps fuera de la ventana (el bucket está ordenado).
    drop = 0
    for ts in bucket:
        if ts >= cutoff:
            break
        drop += 1
    if drop:
        del bucket[:drop]

    if len(bucket) >= max_calls:
        return True
    bucket.append(now)
    return False


def _limpiar_rate_buckets() -> None:
    """Elimina buckets vacíos o totalmente expirados para acotar la memoria."""
    now = monotonic()
    for key in list(_rate_buckets.keys()):
        _, accion = key
        _, per_seconds = RATE_LIMITS.get(accion, (0, 0))
        bucket = _rate_buckets.get(key)
        if not bucket or bucket[-1] < now - per_seconds:
            _rate_buckets.pop(key, None)


# Algunos usuarios antiguos están registrados con nombres de plan que ya
# no se usan en el bot (p.ej. `pre_o25` para PREPARTIDO Over 2.5). Este
# diccionario los normaliza al nombre canónico antes de cualquier lookup.
_PLAN_ALIASES = {
    "pre_o25": "pre",
}


def canonical_plan(plan: str | None) -> str | None:
    if plan is None:
        return None
    return _PLAN_ALIASES.get(plan, plan)


def get_plan_channels(plan: str) -> list[tuple[str, int]]:
    plan = canonical_plan(plan)
    if plan == "goles":
        return [("⚽ GOLES", CANAL_GOLES_ID)]
    if plan == "corners":
        return [("🚩 CORNERS", CANAL_CORNERS_ID)]
    if plan == "pre":
        return [("📊 PREPARTIDO", CANAL_PRE_ID)]
    if plan == "combo":
        return [("⚽ GOLES", CANAL_GOLES_ID), ("🚩 CORNERS", CANAL_CORNERS_ID)]
    if plan == "total":
        return [
            ("⚽ GOLES", CANAL_GOLES_ID),
            ("🚩 CORNERS", CANAL_CORNERS_ID),
            ("📊 PREPARTIDO", CANAL_PRE_ID),
        ]
    return []


async def _desbanear_de_canales(
    context: ContextTypes.DEFAULT_TYPE, user_id: int, plan: str
) -> None:
    """
    Levanta cualquier ban previo del usuario en los canales de su plan
    (only_if_banned=True, no-op si no estaba baneado). Un usuario baneado no
    puede ni solicitar entrada; una expulsión cuyo unban falló lo deja atascado.
    """
    for _, chat_id in get_plan_channels(plan):
        try:
            await context.bot.unban_chat_member(
                chat_id=chat_id, user_id=user_id, only_if_banned=True
            )
        except Exception as e:
            logger.warning(
                "No se pudo desbanear preventivamente a %s en %s: %s",
                user_id, chat_id, e,
            )


async def generar_enlaces_acceso(
    context: ContextTypes.DEFAULT_TYPE, plan: str, user_id: int | None = None
) -> list[tuple[str, str]]:
    """
    Genera enlaces de invitación frescos en el momento de la llamada.

    Los enlaces usan `creates_join_request`: al pulsarlos, el usuario SOLICITA
    entrar y el ChatJoinRequestHandler le aprueba al instante si tiene
    suscripción activa que cubra el canal. Ventajas frente a member_limit=1:
      - No se "consumen" con un intento fallido (adiós al falso 'enlace expirado').
      - Funcionan tengan o no el canal activado 'Aprobar nuevos miembros'.
      - Verifican la entrada real y rechazan a quien no tiene suscripción
        (defensa contra reparto del enlace a terceros).

    Si se pasa user_id, antes de crear el enlace se desbanea al usuario
    (necesario: un usuario baneado ni siquiera puede solicitar entrada).
    """
    if user_id is not None:
        await _desbanear_de_canales(context, user_id, plan)

    canales = get_plan_channels(plan)
    enlaces = []
    for titulo, chat_id in canales:
        invite = await context.bot.create_chat_invite_link(
            chat_id=chat_id,
            name=f"{plan}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            creates_join_request=True,
            expire_date=now_utc() + timedelta(hours=INVITE_EXPIRY_HOURS),
        )
        enlaces.append((titulo, invite.invite_link))
        logger.info(f"Enlace (solicitud) generado para {titulo} — caduca en {INVITE_EXPIRY_HOURS}h")
    return enlaces


async def _revocar_enlaces(context: ContextTypes.DEFAULT_TYPE, enlaces_json: str | None) -> None:
    """
    Revoca (best-effort) los enlaces de invitación emitidos en la generación
    anterior, almacenados como JSON [[chat_id, link], ...]. Así solo el último
    set de enlaces queda vivo y no se pueden acumular varios de 1 uso a la vez.
    """
    if not enlaces_json:
        return
    try:
        enlaces = json.loads(enlaces_json)
    except Exception:
        return
    for item in enlaces:
        try:
            chat_id, link = item[0], item[1]
            await context.bot.revoke_chat_invite_link(chat_id, link)
            logger.info("Enlace previo revocado en %s", chat_id)
        except Exception as e:
            logger.debug("No se pudo revocar enlace %s: %s", item, e)


# ==============================
# DB — PENDING ACCESS
# ==============================

_REGISTRAR_ACCESO_SQL = """
    INSERT INTO pending_access (telegram_user_id, plan, approved_at, generaciones, ultimos_enlaces)
    VALUES (%s, %s, NOW(), 0, NULL)
    ON CONFLICT (telegram_user_id)
    DO UPDATE SET
        plan = EXCLUDED.plan,
        approved_at = NOW(),
        generaciones = 0,
        ultimos_enlaces = NULL;
"""


def _registrar_acceso_cur(cur, user_id: int, plan: str) -> None:
    """Versión por-cursor de registrar_acceso_pendiente, para participar en
    una transacción mayor (mantiene el reset de generaciones/ultimos_enlaces)."""
    cur.execute(_REGISTRAR_ACCESO_SQL, (user_id, plan))


def registrar_acceso_pendiente(user_id: int, plan: str) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                _registrar_acceso_cur(cur, user_id, plan)
    except Exception as e:
        logger.error("Error registrando acceso pendiente para %s: %s", user_id, e)
        raise


def get_acceso_pendiente(user_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, generaciones, ultimos_enlaces
                FROM pending_access WHERE telegram_user_id = %s
                """,
                (user_id,),
            )
            return cur.fetchone()


def guardar_enlaces_generados(user_id: int, enlaces_con_chat: list) -> None:
    """
    Incrementa el contador de generaciones y guarda los enlaces emitidos
    (con su chat_id) para poder revocarlos en la siguiente generación.
    """
    try:
        payload = json.dumps(enlaces_con_chat)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pending_access
                    SET generaciones    = generaciones + 1,
                        ultimos_enlaces = %s
                    WHERE telegram_user_id = %s
                    """,
                    (payload, user_id),
                )
    except Exception as e:
        logger.error("Error guardando enlaces generados para %s: %s", user_id, e)


def borrar_acceso_pendiente(user_id: int) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pending_access WHERE telegram_user_id = %s",
                    (user_id,),
                )
    except Exception as e:
        logger.error("Error borrando acceso pendiente para %s: %s", user_id, e)


# ==============================
# DB — PENDING PAYMENTS
# ==============================

def upsert_pending_payment(user_id: int, username: str | None, full_name: str, plan: str) -> None:
    """Registra/actualiza el pago pendiente cuando el usuario elige un plan."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pending_payments (telegram_user_id, username, full_name, plan, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (telegram_user_id)
                    DO UPDATE SET
                        username   = EXCLUDED.username,
                        full_name  = EXCLUDED.full_name,
                        plan       = EXCLUDED.plan,
                        created_at = NOW();
                    """,
                    (user_id, username, full_name, plan),
                )
    except Exception as e:
        logger.error("Error guardando pago pendiente para %s: %s", user_id, e)


def get_pending_payment(user_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, username, full_name, plan, created_at
                FROM pending_payments WHERE telegram_user_id = %s
                """,
                (user_id,),
            )
            return cur.fetchone()


def delete_pending_payment(user_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pending_payments WHERE telegram_user_id = %s",
                (user_id,),
            )


def _claim_pending_cur(cur, user_id: int):
    """
    Borra y devuelve el pago pendiente (DELETE ... RETURNING) sobre un cursor
    existente. Garantiza idempotencia en la aprobación: solo la PRIMERA llamada
    obtiene la fila; aprobaciones duplicadas (doble clic, varias capturas o
    carrera entre procesos) reciben None. Al ejecutarse dentro de la misma
    transacción que la extensión, si esta falla, el claim también se revierte.
    """
    cur.execute(
        """
        DELETE FROM pending_payments
        WHERE telegram_user_id = %s
        RETURNING telegram_user_id, username, full_name, plan, created_at
        """,
        (user_id,),
    )
    return cur.fetchone()


# ==============================
# DB — AUDITORÍA
# ==============================

def _registrar_evento_cur(
    cur,
    event: str,
    target_user_id: int | None = None,
    actor_id: int | None = None,
    actor_tipo: str = "sistema",
    plan: str | None = None,
    fecha_fin=None,
    detalle: str | None = None,
) -> None:
    """Inserta un evento en audit_log usando un cursor existente (para que la
    auditoría forme parte de la misma transacción que la operación auditada)."""
    cur.execute(
        """
        INSERT INTO audit_log
            (event, actor_id, actor_tipo, target_user_id, plan, fecha_fin, detalle)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (event, actor_id, actor_tipo, target_user_id, plan, fecha_fin, detalle),
    )


def registrar_evento(
    event: str,
    target_user_id: int | None = None,
    actor_id: int | None = None,
    actor_tipo: str = "sistema",
    plan: str | None = None,
    fecha_fin=None,
    detalle: str | None = None,
) -> None:
    """
    Inserta un evento en audit_log en su propia transacción. Best-effort:
    nunca lanza, para no romper el flujo principal si la auditoría falla.
    Eventos típicos: 'aprobacion', 'rechazo', 'renovacion', 'regalo',
    'trial', 'caducidad', 'expulsion_manual', 'acceso_entregado'.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                _registrar_evento_cur(
                    cur, event, target_user_id=target_user_id, actor_id=actor_id,
                    actor_tipo=actor_tipo, plan=plan, fecha_fin=fecha_fin, detalle=detalle,
                )
    except Exception as e:
        logger.error(
            "Error registrando evento de auditoría '%s' (target=%s): %s",
            event, target_user_id, e,
        )


# ==============================
# DB — TRIALS
# ==============================

def has_used_trial(user_id: int) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM trials WHERE telegram_user_id = %s",
                (user_id,),
            )
            return cur.fetchone() is not None


def es_trial_actual(user_id: int, fecha_inicio, fecha_fin) -> bool:
    """
    Determina si la fila actual de `users` para `user_id` corresponde
    al periodo de prueba (no a una suscripción de pago).

    El trial fija fecha_inicio = used_at::date y fecha_fin = used_at + TRIAL_DAYS.
    Cualquier pago/renovación posterior reescribe fecha_inicio al día de la
    operación (vía _extend_user_cur), así que la igualdad se rompe.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT used_at FROM trials WHERE telegram_user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return False

    used_at = row["used_at"]
    used_date = used_at.date() if hasattr(used_at, "date") else parse_date(str(used_at)[:10])

    if isinstance(fecha_inicio, str):
        fecha_inicio = parse_date(fecha_inicio)
    if isinstance(fecha_fin, str):
        fecha_fin = parse_date(fecha_fin)

    return (
        fecha_inicio == used_date
        and fecha_fin == used_date + timedelta(days=TRIAL_DAYS)
    )


def tiene_suscripcion_activa(user_id: int) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT fecha_fin FROM users WHERE telegram_user_id = %s AND estado = 'activo'",
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return False
    fecha_fin = row["fecha_fin"]
    if isinstance(fecha_fin, str):
        fecha_fin = parse_date(fecha_fin)
    return fecha_fin >= today_date()


def usuario_activo_para_canal(user_id: int, chat_id: int) -> bool:
    """
    True si el usuario tiene suscripción activa (no caducada/cancelada) cuyo
    plan incluye `chat_id`. Base de la auto-aprobación de solicitudes de unión.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT plan, fecha_fin FROM users "
                "WHERE telegram_user_id = %s AND estado = 'activo'",
                (user_id,),
            )
            row = cur.fetchone()
    if not row:
        return False
    fecha_fin = row["fecha_fin"]
    if isinstance(fecha_fin, str):
        fecha_fin = parse_date(fecha_fin)
    if fecha_fin < today_date():
        return False
    canales = {cid for _, cid in get_plan_channels(row["plan"])}
    return chat_id in canales


def start_trial(user_id: int, username: str | None, full_name: str, plan: str):
    """
    Activa una prueba gratuita de TRIAL_DAYS días.
    No extiende suscripciones existentes: la fecha_fin se fija a today+TRIAL_DAYS.
    Marca al usuario en la tabla trials para impedir reclamarla otra vez.
    """
    today = today_date()
    new_expiry = today + timedelta(days=TRIAL_DAYS)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (
                        telegram_user_id, username, full_name, plan,
                        fecha_inicio, fecha_fin, estado, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'activo', NOW(), NOW())
                    ON CONFLICT (telegram_user_id)
                    DO UPDATE SET
                        username     = EXCLUDED.username,
                        full_name    = EXCLUDED.full_name,
                        plan         = EXCLUDED.plan,
                        fecha_inicio = EXCLUDED.fecha_inicio,
                        fecha_fin    = EXCLUDED.fecha_fin,
                        estado       = 'activo',
                        updated_at   = NOW()
                    RETURNING telegram_user_id, username, full_name, plan, fecha_inicio, fecha_fin, estado
                    """,
                    (user_id, username, full_name, plan, today, new_expiry),
                )
                record = cur.fetchone()
                cur.execute(
                    """
                    INSERT INTO trials (telegram_user_id, plan, used_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (telegram_user_id) DO NOTHING
                    """,
                    (user_id, plan),
                )
                return record
    except Exception as e:
        logger.error("Error en start_trial para %s: %s", user_id, e)
        raise


# ==============================
# DB — ENCUESTAS DE SATISFACCIÓN
# ==============================

# Mapeo de razones (callback → texto humano).
RAZONES_ENCUESTA = {
    "precio":      "💸 Precio",
    "aciertos":    "🎯 Pocos aciertos",
    "actividad":   "😴 Poca actividad",
    "afi":         "🚫 Cambié de afición",
    "otro":        "❓ Otro motivo",
}


def get_encuesta(user_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, sent_at, razon, valoracion,
                       sugerencia, responded_at, awaiting_sugerencia
                FROM encuestas WHERE telegram_user_id = %s
                """,
                (user_id,),
            )
            return cur.fetchone()


def crear_encuesta(user_id: int, plan: str | None) -> bool:
    """
    Crea la fila de encuesta para este usuario si no existía.
    Devuelve True si era nueva, False si ya estaba.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO encuestas (telegram_user_id, plan)
                    VALUES (%s, %s)
                    ON CONFLICT (telegram_user_id) DO NOTHING
                    """,
                    (user_id, plan),
                )
                return cur.rowcount == 1
    except Exception as e:
        logger.error("Error creando encuesta para %s: %s", user_id, e)
        return False


def marcar_encuesta_rechazada(user_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE encuestas SET responded_at = NOW(), razon = 'rechazada'
                WHERE telegram_user_id = %s
                """,
                (user_id,),
            )


def guardar_razon_encuesta(user_id: int, razon: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE encuestas SET razon = %s WHERE telegram_user_id = %s",
                (razon, user_id),
            )


def guardar_valoracion_encuesta(user_id: int, valoracion: int) -> None:
    """
    Guarda la valoración y marca awaiting_sugerencia=TRUE para que
    el siguiente mensaje de texto del usuario se trate como sugerencia.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE encuestas
                SET valoracion = %s, awaiting_sugerencia = TRUE
                WHERE telegram_user_id = %s
                """,
                (valoracion, user_id),
            )


def guardar_sugerencia_encuesta(user_id: int, sugerencia: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE encuestas
                SET sugerencia = %s,
                    awaiting_sugerencia = FALSE,
                    responded_at = NOW()
                WHERE telegram_user_id = %s
                """,
                (sugerencia, user_id),
            )


def cerrar_encuesta_sin_sugerencia(user_id: int) -> None:
    """Cierra la encuesta dejando sugerencia=NULL y respondida=NOW."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE encuestas
                SET awaiting_sugerencia = FALSE, responded_at = NOW()
                WHERE telegram_user_id = %s
                """,
                (user_id,),
            )


# ==============================
# DB — USERS / SUBSCRIPTIONS
# ==============================

# SQL de upsert de la fila de users, compartido por todos los caminos
# ('extend' y 'set'). Devuelve la fila resultante.
_UPSERT_USER_SQL = """
    INSERT INTO users (
        telegram_user_id, username, full_name, plan,
        fecha_inicio, fecha_fin, estado, created_at, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, 'activo', NOW(), NOW())
    ON CONFLICT (telegram_user_id)
    DO UPDATE SET
        username        = EXCLUDED.username,
        full_name       = EXCLUDED.full_name,
        plan            = EXCLUDED.plan,
        fecha_inicio    = EXCLUDED.fecha_inicio,
        fecha_fin       = EXCLUDED.fecha_fin,
        estado          = 'activo',
        acceso_revocado = FALSE,
        motivo_baja     = NULL,
        updated_at      = NOW()
    RETURNING telegram_user_id, username, full_name, plan, fecha_inicio, fecha_fin, estado
"""


def marcar_acceso_revocado(user_id: int, revocado: bool = True) -> None:
    """Marca si el acceso a los canales ya se ha revocado (best-effort)."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET acceso_revocado = %s, updated_at = NOW() "
                    "WHERE telegram_user_id = %s",
                    (revocado, user_id),
                )
    except Exception as e:
        logger.error("Error marcando acceso_revocado para %s: %s", user_id, e)


def _extend_user_cur(cur, user_id, username, full_name, plan, today, dias=PLAN_DAYS):
    """
    Upsert 'extend' sobre un cursor existente: suma `dias` (por defecto
    PLAN_DAYS) sobre la fecha de fin vigente (o sobre hoy si ya caducó / no
    existe). Pensado para participar en una transacción mayor.
    """
    cur.execute(
        "SELECT fecha_fin FROM users WHERE telegram_user_id = %s",
        (user_id,),
    )
    existing = cur.fetchone()
    if existing and existing["fecha_fin"]:
        old_expiry = existing["fecha_fin"]
        if isinstance(old_expiry, str):
            old_expiry = parse_date(old_expiry)
        base_date = old_expiry if old_expiry >= today else today
        new_expiry = base_date + timedelta(days=dias)
    else:
        new_expiry = today + timedelta(days=dias)
    cur.execute(_UPSERT_USER_SQL, (user_id, username, full_name, plan, today, new_expiry))
    return cur.fetchone()


def _recompensar_referidor_cur(cur, referrer_id, plan_fallback, today, dias):
    """
    Suma `dias` gratis al referidor sobre un cursor existente, preservando su
    identidad y plan. Si el referidor no tenía suscripción, le crea una del
    plan del recomendado (plan_fallback). Devuelve la fila resultante.
    """
    cur.execute(
        "SELECT username, full_name, plan FROM users WHERE telegram_user_id = %s",
        (referrer_id,),
    )
    row = cur.fetchone()
    if row:
        username, full_name, plan = row["username"], row["full_name"], row["plan"]
    else:
        cur.execute(
            "SELECT username, full_name FROM bot_visitors WHERE telegram_user_id = %s",
            (referrer_id,),
        )
        v = cur.fetchone()
        username = v["username"] if v else None
        full_name = (v["full_name"] if v else None) or f"Usuario {referrer_id}"
        plan = plan_fallback
    return _extend_user_cur(cur, referrer_id, username, full_name, plan, today, dias)


def _set_user_cur(cur, user_id, username, full_name, plan, today, days):
    """Upsert 'set' sobre un cursor existente: fecha_fin = hoy + days (pisa)."""
    new_expiry = today + timedelta(days=days)
    cur.execute(_UPSERT_USER_SQL, (user_id, username, full_name, plan, today, new_expiry))
    return cur.fetchone()


# ── Operaciones compuestas ATÓMICAS ─────────────────────────────────────────
# Reclamar pago + extender/asignar suscripción + registrar acceso + auditar,
# todo en UNA transacción. Si algo falla, se revierte todo (incluido el claim
# del pago pendiente), evitando estados parciales. Los efectos externos de
# Telegram (expulsión por cambio de plan, avisos) los ejecuta el llamador
# DESPUÉS del commit, porque no son transaccionales.

def aprobar_pago_tx(user_id: int, plan: str, actor_id: int, via: str):
    """
    Aprobación atómica de un pago pendiente. Devuelve
    (record, plan_anterior, pending, referido_info) o None si no había pago
    pendiente (idempotencia: una segunda aprobación no extiende nada).

    Si el usuario vino recomendado y aún no se ha recompensado el referido,
    en la MISMA transacción: aplica el 2x1 al recomendado (días dobles),
    suma REFERIDOR_DIAS al referidor y marca el referido como recompensado.
    `referido_info` es None si no había referido pendiente, o un dict con
    {referrer_id, referrer_record} para que el llamador avise por Telegram.
    """
    today = today_date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            pending = _claim_pending_cur(cur, user_id)
            if not pending:
                return None

            cur.execute(
                "SELECT plan FROM users WHERE telegram_user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            plan_anterior = row["plan"] if row else None

            # ¿Tiene un referido pendiente? (su primer pago activa el 2x1)
            cur.execute(
                "SELECT referrer_user_id FROM referrals "
                "WHERE referred_user_id = %s AND estado = 'pendiente'",
                (user_id,),
            )
            ref_row = cur.fetchone()
            es_referido = ref_row is not None
            dias = PLAN_DAYS * REFERIDO_MULTIPLICADOR if es_referido else PLAN_DAYS

            record = _extend_user_cur(
                cur, user_id, pending["username"], pending["full_name"],
                plan, today, dias,
            )
            _registrar_acceso_cur(cur, user_id, plan)

            detalle = via if not plan_anterior else f"{via} plan_anterior={plan_anterior}"
            if es_referido:
                detalle += f" | 2x1 referido ({dias}d)"
            _registrar_evento_cur(
                cur, "aprobacion", target_user_id=user_id, actor_id=actor_id,
                actor_tipo="admin", plan=plan, fecha_fin=record["fecha_fin"],
                detalle=detalle,
            )

            referido_info = None
            if es_referido:
                referrer_id = ref_row["referrer_user_id"]
                referrer_record = _recompensar_referidor_cur(
                    cur, referrer_id, plan, today, REFERIDOR_DIAS
                )
                _registrar_acceso_cur(cur, referrer_id, referrer_record["plan"])
                cur.execute(
                    "UPDATE referrals SET estado = 'recompensado', rewarded_at = NOW() "
                    "WHERE referred_user_id = %s",
                    (user_id,),
                )
                _registrar_evento_cur(
                    cur, "referido_recompensa", target_user_id=referrer_id,
                    actor_tipo="sistema", plan=referrer_record["plan"],
                    fecha_fin=referrer_record["fecha_fin"],
                    detalle=f"por recomendar a {user_id} (+{REFERIDOR_DIAS}d)",
                )
                referido_info = {
                    "referrer_id": referrer_id,
                    "referrer_record": referrer_record,
                }
    return record, plan_anterior, pending, referido_info


def renovar_tx(user_id: int, plan: str, username: str | None, full_name: str, actor_id: int):
    """Renovación manual atómica: extiende + registra acceso + audita.
    Devuelve (record, plan_anterior)."""
    today = today_date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT plan FROM users WHERE telegram_user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            plan_anterior = row["plan"] if row else None

            record = _extend_user_cur(cur, user_id, username, full_name, plan, today)
            _registrar_acceso_cur(cur, user_id, plan)
            _registrar_evento_cur(
                cur, "renovacion", target_user_id=user_id, actor_id=actor_id,
                actor_tipo="admin", plan=plan, fecha_fin=record["fecha_fin"],
                detalle="via /renovar",
            )
    return record, plan_anterior


def regalar_tx(
    user_id: int, plan: str, days: int,
    username: str | None, full_name: str, actor_id: int,
):
    """Regalo atómico: asigna (set) + registra acceso + audita.
    Devuelve (record, plan_anterior)."""
    today = today_date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT plan FROM users WHERE telegram_user_id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            plan_anterior = row["plan"] if row else None

            record = _set_user_cur(cur, user_id, username, full_name, plan, today, days)
            _registrar_acceso_cur(cur, user_id, plan)
            _registrar_evento_cur(
                cur, "regalo", target_user_id=user_id, actor_id=actor_id,
                actor_tipo="admin", plan=plan, fecha_fin=record["fecha_fin"],
                detalle=f"{days} días",
            )
    return record, plan_anterior


async def expulsar_de_canales(context: ContextTypes.DEFAULT_TYPE, user_id: int, plan: str) -> bool:
    """
    Intenta expulsar al usuario de todos los canales que cubre el plan.
    Devuelve True si TODOS los bans salieron bien, False en cuanto uno falle
    (o si el plan no tiene canales asociados).
    """
    canales = get_plan_channels(plan)
    if not canales:
        logger.error(
            "Plan desconocido '%s' para user %s — no hay canales asociados, no se puede expulsar",
            plan, user_id,
        )
        return False

    all_ok = True
    for _, chat_id in canales:
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            logger.info(f"Usuario {user_id} expulsado de {chat_id}")
        except Exception as e:
            logger.error(f"Error expulsando {user_id} de {chat_id}: {e}")
            all_ok = False
    return all_ok


async def _expulsar_canales_obsoletos(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    plan_viejo: str | None,
    plan_nuevo: str,
) -> None:
    """
    Expulsa al usuario de los canales que tenía con el plan anterior
    y que NO están incluidos en el nuevo plan.
    Ejemplo: COMBO → GOLES expulsa del canal CORNERS.
    """
    if not plan_viejo or plan_viejo == plan_nuevo:
        return
    canales_viejos = {chat_id for _, chat_id in get_plan_channels(plan_viejo)}
    canales_nuevos = {chat_id for _, chat_id in get_plan_channels(plan_nuevo)}
    for chat_id in canales_viejos - canales_nuevos:
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            logger.info(
                "Usuario %s expulsado de %s por cambio de plan %s → %s",
                user_id, chat_id, plan_viejo, plan_nuevo,
            )
        except Exception as e:
            logger.error(
                "Error expulsando %s de %s en cambio de plan: %s",
                user_id, chat_id, e,
            )


# ==============================
# MARKUPS
# ==============================

def menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ℹ️ Cómo funciona", callback_data="info"),
            InlineKeyboardButton("📊 Estadísticas",  callback_data="stats"),
        ],
        [InlineKeyboardButton("📋 Guía de pago",     callback_data="guia")],
        [InlineKeyboardButton("🆓 Canal FREE",        callback_data="free")],
        [
            InlineKeyboardButton("⚽ GOLES — 20€",   callback_data="goles"),
            InlineKeyboardButton("🚩 CORNERS — 20€", callback_data="corners"),
        ],
        [InlineKeyboardButton("🔥 GOLES + CORNERS — 30€", callback_data="combo")],
        [InlineKeyboardButton("📊 PREPARTIDO — 20€", callback_data="pre")],
        [InlineKeyboardButton("🎁 Invitar amigos (gana 1 mes)", callback_data="referido")],
        [InlineKeyboardButton("💬 Contacto",          url="https://t.me/erikenobi")],
    ])


def volver_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]]
    )


def pago_markup(plan: str) -> InlineKeyboardMarkup:
    precios = {"goles": "20", "corners": "20", "combo": "30", "pre": "20"}
    stripes = {"goles": STRIPE_GOLES, "corners": STRIPE_CORNERS, "combo": STRIPE_COMBO, "pre": STRIPE_PRE}
    importe = precios.get(plan, "")

    stripe_url = stripes.get(plan, "")
    keyboard = [
        [InlineKeyboardButton(
            f"🎁 Probar gratis {TRIAL_DAYS} días",
            callback_data=f"trial:{plan}",
        )],
    ]
    if stripe_url:
        keyboard.append([InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=stripe_url)])
    keyboard += [
        [InlineKeyboardButton("🅿️ Pagar con PayPal",           url=f"{PAYPAL_LINK}/{importe}")],
        [InlineKeyboardButton("📲 Bizum",   callback_data=f"bizum:{plan}"),
         InlineKeyboardButton("🟣 Revolut", callback_data=f"revolut:{plan}")],
        [InlineKeyboardButton("📋 ¿Cómo activo el acceso?", callback_data="guia")],
        [InlineKeyboardButton("⬅️ Volver al menú",          callback_data="menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def admin_approval_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Aprobar GOLES",   callback_data=f"approve:goles:{user_id}"),
            InlineKeyboardButton("✅ Aprobar CORNERS", callback_data=f"approve:corners:{user_id}"),
        ],
        [
            InlineKeyboardButton("✅ Aprobar PRE",   callback_data=f"approve:pre:{user_id}"),
            InlineKeyboardButton("✅ Aprobar COMBO", callback_data=f"approve:combo:{user_id}"),
        ],
        [InlineKeyboardButton("❌ Rechazar", callback_data=f"reject:{user_id}")],
    ])


def acceso_listo_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔑 Obtener mi acceso", callback_data="obtener_acceso")]]
    )


def registrar_visitante(user_id: int, username: str | None, full_name: str) -> bool:
    """
    Registra al usuario en bot_visitors si es la primera vez.
    Devuelve True si era nuevo, False si ya estaba.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bot_visitors (telegram_user_id, username, full_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (telegram_user_id) DO NOTHING
                    """,
                    (user_id, username, full_name),
                )
                return cur.rowcount == 1
    except Exception as e:
        logger.error("Error registrando visitante %s: %s", user_id, e)
        return False


# ==============================
# DB — REFERIDOS
# ==============================

def registrar_referido(referred_user_id: int, referrer_user_id: int) -> bool:
    """
    Registra que `referred_user_id` viene recomendado por `referrer_user_id`.
    Solo si: no es autorreferencia, el recomendado no estaba ya referido y aún
    no es un suscriptor (no se puede 'referir' a alguien que ya está en users).
    Devuelve True si se registró el referido.
    """
    if referred_user_id == referrer_user_id:
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM users WHERE telegram_user_id = %s",
                    (referred_user_id,),
                )
                if cur.fetchone():
                    return False  # ya es/ha sido suscriptor: no aplica
                cur.execute(
                    """
                    INSERT INTO referrals (referred_user_id, referrer_user_id)
                    VALUES (%s, %s)
                    ON CONFLICT (referred_user_id) DO NOTHING
                    """,
                    (referred_user_id, referrer_user_id),
                )
                return cur.rowcount == 1
    except Exception as e:
        logger.error("Error registrando referido %s→%s: %s",
                     referrer_user_id, referred_user_id, e)
        return False


def stats_referidos(user_id: int) -> dict:
    """Devuelve {'total': N, 'recompensados': M} de los referidos de un usuario."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN estado = 'recompensado' THEN 1 ELSE 0 END) AS recompensados
                    FROM referrals WHERE referrer_user_id = %s
                    """,
                    (user_id,),
                )
                row = cur.fetchone() or {}
                return {
                    "total": row.get("total") or 0,
                    "recompensados": row.get("recompensados") or 0,
                }
    except Exception as e:
        logger.error("Error obteniendo stats de referidos de %s: %s", user_id, e)
        return {"total": 0, "recompensados": 0}


def referral_link(user_id: int) -> str | None:
    """Enlace de referido del usuario. None si aún no se conoce el username del bot."""
    if not BOT_USERNAME:
        return None
    return f"https://t.me/{BOT_USERNAME}?start=ref{user_id}"


# ==============================
# USER FLOW
# ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    # Anti-spam: si el usuario martillea /start, ignoramos en silencio para
    # no amplificar (no registramos visitante de nuevo ni reenviamos al admin).
    if user and rate_limited(user.id, "start"):
        return

    vino_referido = False
    if user:
        es_nuevo = await _run_db(registrar_visitante, user.id, user.username, user.full_name)
        if es_nuevo:
            username_admin = f"@{user.username}" if user.username else "(sin username)"
            texto_admin = (
                "👤 Nuevo usuario en el bot\n\n"
                f"Nombre: {user.full_name}\n"
                f"Username: {username_admin}\n"
                f"User ID: {user.id}"
            )
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=texto_admin)
                except Exception as e:
                    logger.error(f"Error avisando nuevo usuario al admin {admin_id}: {e}")

        # Deep link de referido: /start ref<referrer_id>. Solo cuenta si el
        # usuario es nuevo (no farmear referidos con cuentas ya existentes).
        if es_nuevo and context.args and context.args[0].startswith("ref"):
            try:
                referrer_id = int(context.args[0][3:])
            except ValueError:
                referrer_id = None
            if referrer_id and await _run_db(registrar_referido, user.id, referrer_id):
                vino_referido = True
                logger.info("Referido registrado: %s recomendado por %s", user.id, referrer_id)
                for admin_id in ADMIN_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=(
                                f"🔗 Nuevo referido: {user.id} ({user.full_name}) "
                                f"viene recomendado por {referrer_id}."
                            ),
                        )
                    except Exception as e:
                        logger.error("Error avisando referido al admin %s: %s", admin_id, e)

        acceso = await _run_db(get_acceso_pendiente, user.id)
        if acceso:
            await update.message.reply_text(
                "🎉 Tienes un acceso aprobado pendiente de recoger.\n"
                "Pulsa el botón para obtener tu enlace de entrada.",
                reply_markup=acceso_listo_markup(),
            )
            return

    bonus_referido = (
        "🎁 *¡Vienes recomendado!* Tu primera suscripción es *2x1*: "
        "paga 1 mes y llévate 2.\n\n" if vino_referido else ""
    )
    texto = (
        "🔥 *Erikenobi Picks Premium*\n\n"
        f"{bonus_referido}"
        "Alertas de fútbol en tiempo real con análisis estadístico avanzado.\n\n"
        "⚽ *GOLES* — Alertas de gol en directo\n"
        "🚩 *CORNERS* — Mercados de córners en vivo\n"
        "📊 *PREPARTIDO* — Análisis manual Over 2\\.5 FT\n"
        "🔥 *COMBO* — GOLES \\+ CORNERS\n\n"
        "Elige un plan o consulta la información:"
    )
    await update.message.reply_text(
        texto,
        reply_markup=menu_markup(),
        parse_mode="Markdown",
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Usa /start para ver los planes.\n"
        "Si ya has pagado, envía el comprobante en este chat privado."
    )


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    username = f"@{user.username}" if user.username else "(sin username)"
    await update.message.reply_text(
        f"Tu user_id es: {user.id}\nUsername: {username}"
    )


async def _panel_referido(user_id: int):
    """Construye el texto y el teclado del panel de referidos de un usuario."""
    link = referral_link(user_id)
    if not link:
        return (
            "⚠️ El sistema de referidos se está iniciando. Inténtalo en un minuto.",
            volver_markup(),
        )
    stats = await _run_db(stats_referidos, user_id)
    texto = (
        "🎁 *Invita y gana*\n\n"
        "Comparte tu enlace personal. Cuando un amigo *nuevo* se suscriba:\n"
        f"• Tú ganas *{REFERIDOR_DIAS} días* gratis.\n"
        "• Tu amigo recibe *2x1* en su primera suscripción (paga 1 mes, lleva 2).\n\n"
        "🔗 Tu enlace (tócalo para copiarlo):\n"
        f"`{link}`\n\n"
        f"👥 Invitados: *{stats['total']}*  ·  💰 Suscritos: *{stats['recompensados']}*"
    )
    share_text = (
        "Te invito a Erikenobi Picks Premium: con mi enlace tu primera "
        "suscripción es 2x1 (paga 1 mes y llévate 2)."
    )
    share_url = (
        "https://t.me/share/url?url=" + quote(link, safe="")
        + "&text=" + quote(share_text, safe="")
    )
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Compartir enlace", url=share_url)],
        [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
    ])
    return texto, markup


async def referido_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra al usuario su enlace de referido y estadísticas. Uso: /referido"""
    user = update.effective_user
    if not user:
        return
    await _run_db(registrar_visitante, user.id, user.username, user.full_name)
    texto, markup = await _panel_referido(user.id)
    await update.message.reply_text(texto, reply_markup=markup, parse_mode="Markdown")


async def seleccionar_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = query.from_user

    # Anti-spam de botones: si pulsa demasiado rápido, avisamos con un toast
    # (sin enviar mensaje nuevo) y no procesamos el callback.
    if rate_limited(user.id, "menu"):
        await query.answer("Vas muy rápido, espera un momento ⏳", show_alert=False)
        return
    await query.answer()

    plan = query.data

    # Registrar en pendientes cuando el usuario elige un plan de pago
    if plan in ("goles", "corners", "combo", "pre"):
        await _run_db(upsert_pending_payment, user.id, user.username, user.full_name, plan)

    if plan == "menu":
        await query.edit_message_text(
            "🔥 *Erikenobi Picks Premium*\n\n"
            "Alertas de fútbol en tiempo real con análisis estadístico avanzado\\.\n\n"
            "⚽ *GOLES* — Alertas de gol en directo\n"
            "🚩 *CORNERS* — Mercados de córners en vivo\n"
            "📊 *PREPARTIDO* — Análisis manual Over 2\\.5 FT\n"
            "🔥 *COMBO* — GOLES \\+ CORNERS\n\n"
            "Elige un plan o consulta la información:",
            reply_markup=menu_markup(),
            parse_mode="MarkdownV2",
        )
        return

    if plan == "guia":
        await query.edit_message_text(
            "📋 *Guía de pago — paso a paso*\n\n"
            "1️⃣ Elige tu plan en el menú \\(GOLES, CORNERS o COMBO\\)\n"
            "2️⃣ Selecciona el método de pago\n"
            "3️⃣ Realiza el pago por el importe indicado\n"
            "4️⃣ Haz una *captura de pantalla* del comprobante\n"
            "5️⃣ *Envía la captura aquí, en este chat* ⬅️\n\n"
            "⏱ En cuanto valide el pago \\(normalmente el mismo día\\) "
            "recibirás un botón para acceder al canal\\.\n\n"
            "💳 *Métodos disponibles:*\n"
            "Stripe · PayPal · Bizum · Revolut\n\n"
            "❓ ¿Algún problema? Escríbeme: @erikenobi",
            reply_markup=volver_markup(),
            parse_mode="MarkdownV2",
        )
        return

    if plan == "info":
        await query.edit_message_text(
            "ℹ️ *Cómo funciona*\n\n"
            "⚽ *GOLES — 20€/mes*\n"
            "Alertas de gol en directo\\.\n\n"
            "🚩 *CORNERS — 20€/mes*\n"
            "Alertas especializadas en mercados de córners en vivo\\.\n\n"
            "📊 *PREPARTIDO — 20€/mes*\n"
            "Análisis manual Over 2\\.5 FT\\. Canal independiente\\.\n\n"
            "🔥 *COMBO — 30€/mes*\n"
            "Acceso completo a GOLES \\+ CORNERS\\.\n\n"
            "📲 *Métodos de pago*\n"
            "Stripe · PayPal · Bizum · Revolut\n\n"
            "🔑 *Activación*\n"
            "Tras pagar, envía la captura del comprobante en este chat\\. "
            "El acceso se activa en cuanto valide el pago\\.\n\n"
            "⚠️ *Aviso*\n"
            "Servicio únicamente informativo\\. "
            "Cada usuario es responsable de sus propias decisiones\\.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Guía de pago", callback_data="guia")],
                [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
            ]),
            parse_mode="MarkdownV2",
        )
        return

    # ── Stats reales ────────────────────────────────────────────────────
    if plan == "stats":
        stats = await _run_db(get_stats_reales)

        if stats and (stats.get("globales") or stats.get("ultimo_mes")):
            texto = _formatear_stats_reales(stats)
        else:
            # Fallback si no hay conexión a la picks DB o aún no hay datos
            texto = (
                "📊 *Rendimiento del servicio*\n\n"
                "⚽ *GOLES*\n"
                "Acierto estimado actual: *+70%*\n"
                "Incluye alertas de gol en directo y prepartido over 2.5.\n\n"
                "⛳ *CORNERS*\n"
                "Acierto estimado actual: *+80%*\n"
                "Alertas en vivo basadas en estadísticas y momentum.\n\n"
                "🔥 *COMBO*\n"
                "Rendimiento estimado combinado: *+75%*\n"
                "Acceso completo a GOLES + CORNERS.\n\n"
                "⚠️ _Los datos en tiempo real no están disponibles en este momento. "
                "Inténtalo más tarde._"
            )

        await query.edit_message_text(
            texto,
            reply_markup=volver_markup(),
            parse_mode="Markdown",
        )
        return

    if plan == "free":
        await query.edit_message_text(
            "🆓 *Canal FREE*\n\n"
            "Accede a una selección gratuita de picks para ver cómo funciona el servicio\\.\n\n"
            "Si quieres recibir todas las alertas, echa un vistazo a los planes premium\\.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("👉 Entrar al canal FREE", url=LINK_FREE)],
                [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
            ]),
            parse_mode="MarkdownV2",
        )
        return

    if plan == "referido":
        texto, markup = await _panel_referido(user.id)
        await query.edit_message_text(texto, reply_markup=markup, parse_mode="Markdown")
        return

    _GUIA_PAGO = (
        "\n\n"
        "─────────────────\n"
        "📋 *¿Cómo activar el acceso?*\n"
        "1️⃣ Paga por *cualquier método* \\(Stripe, PayPal, Bizum o Revolut\\)\n"
        "2️⃣ Captura del comprobante o del email de confirmación\n"
        "3️⃣ *Envía la captura aquí, en este chat* ⬅️\n"
        "⏱ Acceso activado el mismo día \\(también si pagas con tarjeta vía Stripe\\)"
    )

    # ── Plan GOLES ──────────────────────────────────────────────────────
    if plan == "goles":
        stats       = await _run_db(get_stats_reales)
        strike_real = _get_strike_tipo(stats, "gol")
        strike_txt  = f"*{strike_real}* \\(último mes\\)" if strike_real else "*\\+70% estimado*"

        await query.edit_message_text(
            f"⚽ *PLAN GOLES*\n\n"
            f"📈 Strike: {strike_txt}\n"
            f"💰 Precio: *{PRECIO_GOLES}/mes*\n\n"
            "✅ Incluye:\n"
            "• Alertas de gol en directo\n"
            "• Selecciones prepartido over 2\\.5\n"
            "• Estadísticas del partido en vivo\n"
            + _GUIA_PAGO + "\n\n"
            "Selecciona tu método de pago:",
            reply_markup=pago_markup("goles"),
            parse_mode="MarkdownV2",
        )
        return

    # ── Plan CORNERS ────────────────────────────────────────────────────
    if plan == "corners":
        stats       = await _run_db(get_stats_reales)
        strike_real = _get_strike_tipo(stats, "corner")
        strike_txt  = f"*{strike_real}* \\(último mes\\)" if strike_real else "*\\+80% estimado*"

        await query.edit_message_text(
            f"🚩 *PLAN CORNERS*\n\n"
            f"📈 Strike: {strike_txt}\n"
            f"💰 Precio: *{PRECIO_CORNERS}/mes*\n\n"
            "✅ Incluye:\n"
            "• Alertas de córners en vivo\n"
            "• Datos de momentum y presión ofensiva\n"
            "• Estadísticas del partido en tiempo real\n"
            + _GUIA_PAGO + "\n\n"
            "Selecciona tu método de pago:",
            reply_markup=pago_markup("corners"),
            parse_mode="MarkdownV2",
        )
        return

    # ── Plan PREPARTIDO ─────────────────────────────────────────────────
    if plan == "pre":
        await query.edit_message_text(
            "📊 *PLAN PREPARTIDO*\n\n"
            "💰 Precio: *20€/mes*\n\n"
            "✅ Incluye:\n"
            "• Análisis manual prepartido\n"
            "• Selecciones Over 2\\.5 FT\n"
            "• Picks con estadísticas y contexto del partido\n\n"
            "📌 Canal independiente de GOLES y CORNERS\\."
            + _GUIA_PAGO + "\n\n"
            "Selecciona tu método de pago:",
            reply_markup=pago_markup("pre"),
            parse_mode="MarkdownV2",
        )
        return

    # ── Plan COMBO ──────────────────────────────────────────────────────
    if plan == "combo":
        stats         = await _run_db(get_stats_reales)
        strike_goles  = _get_strike_tipo(stats, "gol")
        strike_corner = _get_strike_tipo(stats, "corner")

        if strike_goles and strike_corner:
            strike_txt = f"⚽ {strike_goles} · 🚩 {strike_corner} \\(último mes\\)"
        elif strike_goles or strike_corner:
            strike_txt = f"*{strike_goles or strike_corner}* \\(último mes\\)"
        else:
            strike_txt = "*\\+75% estimado*"

        await query.edit_message_text(
            f"🔥 *PLAN COMBO*\n\n"
            f"📈 Strike: {strike_txt}\n"
            f"💰 Precio: *{PRECIO_COMBO}/mes*\n\n"
            "✅ Incluye acceso completo a:\n"
            "• ⚽ Canal GOLES\n"
            "• 🚩 Canal CORNERS\n\n"
            "La opción más completa para seguir todos los mercados "
            "y elegir según el volumen del día\\."
            + _GUIA_PAGO + "\n\n"
            "Selecciona tu método de pago:",
            reply_markup=pago_markup("combo"),
            parse_mode="MarkdownV2",
        )
        return

    if plan.startswith("trial:"):
        _, plan_real = plan.split(":", 1)
        if plan_real not in ("goles", "corners", "combo", "pre"):
            await query.edit_message_text(
                "Plan no válido para la prueba.",
                reply_markup=volver_markup(),
            )
            return

        if rate_limited(user.id, "trial"):
            await query.edit_message_text(
                "⏳ Vas muy rápido. Espera unos segundos e inténtalo de nuevo.",
                reply_markup=volver_markup(),
            )
            return

        if await _run_db(has_used_trial, user.id):
            await query.edit_message_text(
                "🎁 *Prueba gratuita ya usada*\n\n"
                "Solo se permite una prueba de 3 días por usuario y ya has reclamado la tuya\\.\n\n"
                "Si quieres seguir disfrutando del servicio, elige un plan en el menú\\.",
                reply_markup=volver_markup(),
                parse_mode="MarkdownV2",
            )
            return

        if await _run_db(tiene_suscripcion_activa, user.id):
            await query.edit_message_text(
                "Ya tienes una suscripción activa, así que no necesitas la prueba 🙌\n\n"
                "Si quieres cambiar de plan, escríbeme: @erikenobi",
                reply_markup=volver_markup(),
            )
            return

        try:
            record = await _run_db(
                start_trial, user.id, user.username, user.full_name, plan_real
            )
        except Exception as e:
            logger.error("Error iniciando trial para %s: %s", user.id, e)
            await query.edit_message_text(
                "⚠️ Ha habido un error activando tu prueba. Escríbeme: @erikenobi",
                reply_markup=volver_markup(),
            )
            return

        await _run_db(registrar_acceso_pendiente, user.id, plan_real)
        await _run_db(delete_pending_payment, user.id)
        await _run_db(
            registrar_evento, "trial",
            target_user_id=user.id, actor_id=user.id,
            actor_tipo="user", plan=plan_real, fecha_fin=record["fecha_fin"],
        )

        username_admin = f"@{user.username}" if user.username else "(sin username)"
        texto_admin = (
            "🎁 Trial activado\n\n"
            f"Usuario: {user.full_name}\n"
            f"Username: {username_admin}\n"
            f"User ID: {user.id}\n"
            f"Plan: {plan_real}\n"
            f"Válido hasta: {record['fecha_fin']}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(chat_id=admin_id, text=texto_admin)
            except Exception as e:
                logger.error(f"Error avisando trial al admin {admin_id}: {e}")

        await query.edit_message_text(
            "✅ *Prueba gratuita activada*\n\n"
            f"Plan: *{plan_real.upper()}*\n"
            f"Duración: *{TRIAL_DAYS} días*\n"
            f"Válida hasta: *{record['fecha_fin']}*\n\n"
            "Pulsa el botón para obtener tu acceso al canal.\n"
            "Cuando caduque podrás seguir suscrito eligiendo un plan de pago.",
            reply_markup=acceso_listo_markup(),
            parse_mode="Markdown",
        )
        logger.info(
            "Trial activado: user %s | plan %s | hasta %s",
            user.id, plan_real, record["fecha_fin"],
        )
        return

    if plan.startswith("bizum:"):
        _, plan_real = plan.split(":", 1)
        importes = {"goles": PRECIO_GOLES, "corners": PRECIO_CORNERS, "combo": PRECIO_COMBO, "pre": PRECIO_PRE}
        importe  = importes.get(plan_real, "consultar")
        # Formatear número Bizum limpio
        bizum_fmt = BIZUM.replace("+34", "\\+34")
        await query.edit_message_text(
            f"📲 *Pago por Bizum*\n\n"
            f"Plan: *{plan_real.upper()}*\n"
            f"Importe: *{importe}*\n"
            f"Número: *{bizum_fmt}*\n\n"
            "─────────────────\n"
            "📋 *Pasos a seguir:*\n\n"
            "1️⃣ Abre tu app bancaria\n"
            f"2️⃣ Envía *{importe}* al número de arriba\n"
            "3️⃣ Haz una captura de pantalla del pago\n"
            "4️⃣ *Envía la captura aquí, en este chat* ⬅️\n\n"
            "⏱ Recibirás el acceso en cuanto valide el pago\\.",
            reply_markup=volver_markup(),
            parse_mode="MarkdownV2",
        )
        return

    if plan.startswith("revolut:"):
        _, plan_real = plan.split(":", 1)
        importes = {"goles": PRECIO_GOLES, "corners": PRECIO_CORNERS, "combo": PRECIO_COMBO, "pre": PRECIO_PRE}
        importe  = importes.get(plan_real, "consultar")
        await query.edit_message_text(
            f"🟣 *Pago por Revolut*\n\n"
            f"Plan: *{plan_real.upper()}*\n"
            f"Importe: *{importe}*\n\n"
            f"🔗 [Abrir Revolut]({REVOLUT_LINK})\n\n"
            "─────────────────\n"
            "📋 *Pasos a seguir:*\n\n"
            f"1️⃣ Pulsa el enlace de arriba y envía *{importe}*\n"
            "2️⃣ Haz una captura de pantalla del pago\n"
            "3️⃣ *Envía la captura aquí, en este chat* ⬅️\n\n"
            "⏱ Recibirás el acceso en cuanto valide el pago\\.",
            reply_markup=volver_markup(),
            parse_mode="MarkdownV2",
        )
        return

    if plan == "obtener_acceso":
        await callback_obtener_acceso(update, context)
        return

    # Callback desconocido (botón antiguo en el historial del chat)
    logger.warning("callback_data desconocido: %r (user %s)", plan, user.id)
    await query.edit_message_text(
        "Este botón ha caducado. Usa /start para ver el menú actualizado.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Ir al menú", callback_data="menu")]]
        ),
    )


# ==============================
# ENCUESTA DE SATISFACCIÓN — UI
# ==============================

def _encuesta_inicial_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Sí, te ayudo",   callback_data="enc:start")],
        [InlineKeyboardButton("❌ No, gracias",    callback_data="enc:no")],
    ])


def _encuesta_razon_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(texto, callback_data=f"enc:razon:{key}")]
        for key, texto in RAZONES_ENCUESTA.items()
    ]
    return InlineKeyboardMarkup(keyboard)


def _encuesta_valoracion_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⭐",     callback_data="enc:val:1"),
            InlineKeyboardButton("⭐⭐",   callback_data="enc:val:2"),
            InlineKeyboardButton("⭐⭐⭐", callback_data="enc:val:3"),
        ],
        [
            InlineKeyboardButton("⭐⭐⭐⭐",   callback_data="enc:val:4"),
            InlineKeyboardButton("⭐⭐⭐⭐⭐", callback_data="enc:val:5"),
        ],
    ])


def _encuesta_skip_sugerencia_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("Saltar — sin sugerencia", callback_data="enc:skip")]]
    )


async def enviar_encuesta_inicial(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    nombre: str,
    plan: str | None,
) -> bool:
    """
    Envía el mensaje inicial de la encuesta al usuario si no se le había
    enviado ya. Devuelve True si se envió.
    """
    if not await _run_db(crear_encuesta, user_id, plan):
        return False

    saludo = f"👋 Hola {nombre}" if nombre else "👋 Hola"
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"{saludo},\n\n"
                "Vi que tu suscripción terminó hace unos días y no la has renovado. "
                "¿Te importa responder *2 preguntas rápidas* para mejorar el servicio? "
                "Me ayudaría mucho saber qué podemos mejorar."
            ),
            reply_markup=_encuesta_inicial_markup(),
            parse_mode="Markdown",
        )
        return True
    except Exception as e:
        logger.error("No se pudo enviar la encuesta inicial a %s: %s", user_id, e)
        return False


async def encuesta_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Gestiona todos los callbacks `enc:*` de la encuesta."""
    query = update.callback_query
    await query.answer()

    data = query.data
    user = query.from_user

    if data == "enc:no":
        await _run_db(marcar_encuesta_rechazada, user.id)
        await query.edit_message_text(
            "Entendido. ¡Gracias igualmente! 🙏\n"
            "Si en algún momento quieres volver, escribe /start."
        )
        return

    if data == "enc:start":
        await query.edit_message_text(
            "*1/2 — ¿Cuál fue el motivo principal para no renovar?*",
            reply_markup=_encuesta_razon_markup(),
            parse_mode="Markdown",
        )
        return

    if data.startswith("enc:razon:"):
        razon = data.split(":", 2)[2]
        if razon not in RAZONES_ENCUESTA:
            await query.edit_message_text("Opción no válida.")
            return
        await _run_db(guardar_razon_encuesta, user.id, razon)
        await query.edit_message_text(
            "*2/2 — ¿Cómo valoras el servicio en general?*",
            reply_markup=_encuesta_valoracion_markup(),
            parse_mode="Markdown",
        )
        return

    if data.startswith("enc:val:"):
        try:
            valoracion = int(data.split(":", 2)[2])
        except ValueError:
            await query.edit_message_text("Valoración no válida.")
            return
        if valoracion < 1 or valoracion > 5:
            await query.edit_message_text("Valoración fuera de rango.")
            return
        await _run_db(guardar_valoracion_encuesta, user.id, valoracion)
        await query.edit_message_text(
            "¡Gracias! Última cosa (opcional):\n\n"
            "Si tienes alguna *sugerencia o mejora*, escríbela aquí en este chat. "
            "Si no, puedes saltarla.",
            reply_markup=_encuesta_skip_sugerencia_markup(),
            parse_mode="Markdown",
        )
        return

    if data == "enc:skip":
        await _run_db(cerrar_encuesta_sin_sugerencia, user.id)
        await query.edit_message_text(
            "¡Gracias por tu feedback! 🙏\n"
            "Si en algún momento quieres volver, escribe /start."
        )
        return


async def callback_obtener_acceso(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user = query.from_user

    # Anti-spam estricto: generar enlaces llama a la API de Telegram. Evita
    # martilleo del botón "Generar nuevo enlace".
    if rate_limited(user.id, "acceso"):
        await query.edit_message_text(
            "⏳ Vas muy rápido. Espera unos segundos antes de generar otro enlace.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔄 Generar nuevo enlace", callback_data="obtener_acceso")]]
            ),
        )
        return

    acceso = await _run_db(get_acceso_pendiente, user.id)

    if not acceso:
        await query.edit_message_text(
            "No tienes ningún acceso pendiente. Usa /start para ver los planes."
        )
        return

    plan = acceso["plan"]

    # Desbaneo preventivo SIEMPRE, antes del límite de generaciones: si una
    # expulsión previa dejó al usuario baneado, no podría ni solicitar entrada,
    # y la trampa era que el tope de generaciones impedía llegar a desbanearlo.
    await _desbanear_de_canales(context, user.id, plan)

    # Límite de auto-generación: impide que un suscriptor genere enlaces de
    # forma indefinida para repartirlos a terceros. Se reinicia con cada
    # aprobación/renovación/regalo (registrar_acceso_pendiente). Ya no es un
    # callejón sin salida: el usuario queda desbaneado y se avisa al admin.
    if (acceso.get("generaciones") or 0) >= MAX_GENERACIONES_ACCESO:
        await query.edit_message_text(
            "⚠️ Ya has generado varios enlaces para esta suscripción.\n"
            "Si tu último enlace sigue vigente, púlsalo y *Solicitar acceso* "
            "(te aprobaré automáticamente).\n\n"
            "Si no te funciona, escríbeme: @erikenobi",
            parse_mode="Markdown",
        )
        logger.warning(
            "Usuario %s alcanzó el límite de generación de enlaces (%s)",
            user.id, MAX_GENERACIONES_ACCESO,
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"⚠️ El usuario {user.id} no consigue entrar "
                        f"(plan {plan}) y agotó las generaciones de enlace.\n"
                        f"Lo he desbaneado por si acaso. Usa /link {user.id} para "
                        "darle un enlace fresco, o añádelo al canal manualmente."
                    ),
                )
            except Exception as e:
                logger.error("Error avisando tope de enlaces al admin %s: %s", admin_id, e)
        return

    # Revocar los enlaces emitidos antes (best-effort): así el botón siempre
    # entrega el enlace más reciente.
    await _revocar_enlaces(context, acceso.get("ultimos_enlaces"))

    try:
        # El desbaneo ya se hizo arriba, no hace falta repetirlo en generar.
        enlaces = await generar_enlaces_acceso(context, plan)
    except Exception as e:
        logger.error(f"Error generando enlaces para {user.id}: {e}")
        await query.edit_message_text(
            "⚠️ Ha habido un error generando tu enlace. Por favor, contáctame: @erikenobi"
        )
        return

    # Persistir los enlaces (con su chat_id) y contar esta generación. El orden
    # de `enlaces` coincide con el de get_plan_channels(plan), así que podemos
    # emparejar cada link con su chat_id por posición.
    canales = get_plan_channels(plan)
    enlaces_con_chat = [
        [chat_id, link]
        for (_, chat_id), (_, link) in zip(canales, enlaces)
    ]
    await _run_db(guardar_enlaces_generados, user.id, enlaces_con_chat)
    await _run_db(
        registrar_evento, "acceso_entregado",
        target_user_id=user.id, actor_id=user.id,
        actor_tipo="user", plan=plan,
        detalle=f"generacion={(acceso.get('generaciones') or 0) + 1}",
    )

    texto = (
        "✅ *Acceso activado*\n\n"
        f"Plan: *{plan.upper()}*\n\n"
        "Pulsa tu enlace y luego *Solicitar acceso* — te aprobaré "
        "automáticamente al instante:\n\n"
    )
    for titulo, link in enlaces:
        texto += f"{titulo}\n{link}\n\n"

    texto += (
        "⏱ El enlace caduca en 1 hora. Si caduca, pulsa el botón de abajo "
        "para generar otro."
    )

    # No borramos pending_access: así el usuario puede regenerar el enlace
    # si caduca antes de usarlo (hasta MAX_GENERACIONES_ACCESO veces).
    # Se limpia cuando la suscripción expira.
    await query.edit_message_text(
        texto,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔄 Generar nuevo enlace", callback_data="obtener_acceso")]]
        ),
    )
    logger.info(f"Acceso entregado a usuario {user.id} para plan {plan}")


async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Auto-aprueba las solicitudes de unión a los canales: aprueba si el usuario
    tiene suscripción activa que cubra el canal, y rechaza en caso contrario
    (defensa contra reparto del enlace). Verifica la entrada real del usuario.
    """
    req = update.chat_join_request
    if req is None:
        return
    user_id = req.from_user.id
    chat_id = req.chat.id

    try:
        permitido = await _run_db(usuario_activo_para_canal, user_id, chat_id)
    except Exception as e:
        logger.error("Error comprobando suscripción de %s para %s: %s", user_id, chat_id, e)
        permitido = False

    if permitido:
        try:
            await context.bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            logger.info("Solicitud de unión APROBADA: user %s | canal %s", user_id, chat_id)
        except Exception as e:
            logger.error("Error aprobando solicitud de %s en %s: %s", user_id, chat_id, e)
    else:
        try:
            await context.bot.decline_chat_join_request(chat_id=chat_id, user_id=user_id)
            logger.info(
                "Solicitud de unión RECHAZADA (sin suscripción activa): user %s | canal %s",
                user_id, chat_id,
            )
        except Exception as e:
            logger.error("Error rechazando solicitud de %s en %s: %s", user_id, chat_id, e)
        # Avisar al admin: puede ser alguien con el enlace compartido o un pago
        # pendiente de validar.
        for admin_id in ADMIN_IDS:
            try:
                username = f"@{req.from_user.username}" if req.from_user.username else "(sin username)"
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        "🚷 Solicitud de acceso rechazada (sin suscripción activa)\n\n"
                        f"Usuario: {req.from_user.full_name}\n"
                        f"Username: {username}\n"
                        f"User ID: {user_id}\n"
                        f"Canal: {chat_id}"
                    ),
                )
            except Exception as e:
                logger.error("Error avisando solicitud rechazada al admin %s: %s", admin_id, e)


async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user
    chat    = update.effective_chat

    if message is None or user is None or chat is None:
        return
    if chat.type != "private":
        return

    # Si el usuario está en mitad de la encuesta esperando una sugerencia
    # y manda un mensaje de texto, lo guardamos como sugerencia y avisamos
    # al admin. Las fotos/documentos siguen el flujo normal de comprobantes.
    if message.text:
        encuesta = await _run_db(get_encuesta, user.id)
        if encuesta and encuesta.get("awaiting_sugerencia"):
            sugerencia = (message.text or "").strip()
            if sugerencia:
                await _run_db(guardar_sugerencia_encuesta, user.id, sugerencia[:1000])
                await message.reply_text(
                    "¡Gracias por tu feedback! 🙏\n"
                    "Si en algún momento quieres volver, escribe /start."
                )
                for admin_id in ADMIN_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=(
                                "📝 Sugerencia de encuesta\n\n"
                                f"User ID: {user.id}\n"
                                f"Nombre: {user.full_name}\n"
                                f"Razón: {RAZONES_ENCUESTA.get(encuesta.get('razon') or '', encuesta.get('razon'))}\n"
                                f"Valoración: {encuesta.get('valoracion')}/5\n"
                                f"Sugerencia: {sugerencia[:1000]}"
                            ),
                        )
                    except Exception as e:
                        logger.error("Error reenviando sugerencia al admin %s: %s", admin_id, e)
                return

    pending = await _run_db(get_pending_payment, user.id)

    if not pending:
        await message.reply_text(
            "Antes de enviar el comprobante, usa /start y selecciona un plan."
        )
        return

    # Anti-spam: limita el reenvío de comprobantes al admin para evitar que
    # un usuario inunde el chat del admin con muchas capturas seguidas.
    if rate_limited(user.id, "comprobante"):
        await message.reply_text(
            "He recibido tu mensaje. Espera la validación, por favor 🙏"
        )
        return

    plan     = pending["plan"]
    username = f"@{user.username}" if user.username else "(sin username)"

    await message.reply_text(
        "Perfecto. He recibido tu comprobante.\n"
        "En cuanto lo revise te enviaré el acceso."
    )

    texto_admin = (
        "📥 Nuevo pago pendiente\n\n"
        f"Usuario: {user.full_name}\n"
        f"Username: {username}\n"
        f"User ID: {user.id}\n"
        f"Plan solicitado: {plan}"
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=texto_admin,
                reply_markup=admin_approval_markup(user.id),
            )
            await message.forward(chat_id=admin_id)
        except Exception as e:
            logger.error(f"Error avisando al admin {admin_id}: {e}")


# ==============================
# ADMIN FLOW
# ==============================

async def _notificar_recompensa_referido(context: ContextTypes.DEFAULT_TYPE, referido_info) -> None:
    """Avisa al referidor (y al admin) de su mes gratis. No-op si no hubo referido."""
    if not referido_info:
        return
    referrer_id = referido_info["referrer_id"]
    rec = referido_info["referrer_record"]
    try:
        await context.bot.send_message(
            chat_id=referrer_id,
            text=(
                "🎁 *¡Tu recomendación se ha suscrito!*\n\n"
                f"Te hemos regalado *{REFERIDOR_DIAS} días* de servicio.\n"
                f"Plan: *{rec['plan'].upper()}*\n"
                f"Válido hasta: {rec['fecha_fin']}\n\n"
                "Pulsa el botón para asegurar tu acceso al canal."
            ),
            reply_markup=acceso_listo_markup(),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("Error avisando recompensa de referido a %s: %s", referrer_id, e)
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"🎁 Referido recompensado: el referidor {referrer_id} gana "
                    f"{REFERIDOR_DIAS} días ({rec['plan']}, hasta {rec['fecha_fin']})."
                ),
            )
        except Exception as e:
            logger.error("Error avisando referido al admin %s: %s", admin_id, e)


async def admin_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    admin = query.from_user
    if admin.id not in ADMIN_IDS:
        await query.edit_message_text("No tienes permisos para esta acción.")
        return

    data = query.data

    try:
        if data.startswith("approve:"):
            parts = data.split(":")
            if len(parts) != 3:
                await query.edit_message_text("Error en datos del botón.")
                return

            _, plan, user_id = parts
            user_id_int = int(user_id)

            if plan not in ("goles", "corners", "combo", "pre"):
                await query.edit_message_text("Plan no válido.")
                return

            # Aprobación ATÓMICA: reclamar pago + extender + registrar acceso
            # + auditar en una transacción. Idempotente: un segundo clic (o el
            # botón de otra captura) recibe None y no vuelve a extender. Si la
            # transacción falla, no se aplica nada (el pago sigue pendiente).
            result = await _run_db(aprobar_pago_tx, user_id_int, plan, actor_id=admin.id, via="botón")
            if result is None:
                await query.edit_message_text(
                    f"⚠️ El usuario {user_id_int} ya no está en pendientes "
                    "(¿ya aprobado o rechazado?)."
                )
                return
            record, plan_anterior, _, referido_info = result

            # Efectos externos (Telegram) DESPUÉS del commit. Si cambió de plan,
            # expulsar de los canales que ya no le corresponden.
            await _expulsar_canales_obsoletos(context, user_id_int, plan_anterior, plan)

            bonus_txt = (
                "\n🎁 *Incluye tu 2x1 por venir recomendado* (¡el doble de tiempo!)."
                if referido_info else ""
            )
            try:
                await context.bot.send_message(
                    chat_id=user_id_int,
                    text=(
                        "✅ *Pago aprobado*\n\n"
                        f"Plan activo: *{plan.upper()}*\n"
                        f"Válido hasta: {record['fecha_fin']}"
                        f"{bonus_txt}\n\n"
                        "Pulsa el botón cuando estés listo para entrar al canal.\n"
                        "El enlace se generará en el momento y tendrá 1 hora de validez."
                    ),
                    reply_markup=acceso_listo_markup(),
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"Error avisando acceso a {user_id_int}: {e}")

            await _notificar_recompensa_referido(context, referido_info)

            extra_admin = (
                f"\n🎁 Era un referido: 2x1 aplicado y +{REFERIDOR_DIAS}d al referidor "
                f"{referido_info['referrer_id']}." if referido_info else ""
            )
            await query.edit_message_text(
                f"✅ Usuario {user_id_int} aprobado para {plan.upper()}.\n"
                f"Activo hasta {record['fecha_fin']}.\n"
                "El usuario recibirá el enlace cuando pulse 'Obtener mi acceso'."
                + extra_admin
            )
            logger.info(f"Pago aprobado: user {user_id_int} | plan {plan} | hasta {record['fecha_fin']}")
            return

        if data.startswith("reject:"):
            parts = data.split(":")
            if len(parts) != 2:
                await query.edit_message_text("Error en datos del botón.")
                return

            _, user_id = parts
            user_id_int = int(user_id)

            try:
                await context.bot.send_message(
                    chat_id=user_id_int,
                    text="❌ No he podido validar el pago. Escríbeme si quieres revisarlo.",
                )
            except Exception as e:
                logger.error(f"Error avisando rechazo a {user_id_int}: {e}")

            await _run_db(delete_pending_payment, user_id_int)
            await _run_db(
                registrar_evento, "rechazo",
                target_user_id=user_id_int, actor_id=admin.id, actor_tipo="admin",
            )
            await query.edit_message_text(f"❌ Usuario {user_id_int} rechazado.")
            logger.info(f"Pago rechazado: user {user_id_int}")
            return

    except Exception as e:
        logger.error(f"Error en admin_action_callback: {e}")
        await query.edit_message_text("⚠️ Ha ocurrido un error procesando la acción.")


# ==============================
# COMANDOS ADMIN
# ==============================

def _check_admin(update: Update) -> bool:
    return update.effective_user and update.effective_user.id in ADMIN_IDS


async def aprobar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if len(context.args) < 2:
        await update.message.reply_text("Uso correcto: /aprobar user_id plan")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    plan = context.args[1].lower()
    if plan not in ("goles", "corners", "combo", "pre", "total"):
        await update.message.reply_text("Plan no válido. Usa: goles, corners, pre, combo o total")
        return

    # Aprobación ATÓMICA e idempotente (ver aprobar_pago_tx). Una segunda
    # ejecución de /aprobar para el mismo usuario no vuelve a extender.
    try:
        result = await _run_db(
            aprobar_pago_tx,
            target_user_id, plan, actor_id=update.effective_user.id, via="/aprobar"
        )
    except Exception as e:
        await update.message.reply_text(
            f"⚠️ Error al aprobar (no se aplicó nada): {e}. Reinténtalo."
        )
        return

    if result is None:
        await update.message.reply_text(
            "Ese usuario no está en pendientes (¿ya aprobado o rechazado?)."
        )
        return
    record, plan_anterior, _, referido_info = result

    # Si cambió de plan, expulsar de los canales que ya no le corresponden
    await _expulsar_canales_obsoletos(context, target_user_id, plan_anterior, plan)

    bonus_txt = (
        "\n🎁 *Incluye tu 2x1 por venir recomendado* (¡el doble de tiempo!)."
        if referido_info else ""
    )
    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=(
                "✅ *Pago aprobado*\n\n"
                f"Plan activo: *{plan.upper()}*\n"
                f"Válido hasta: {record['fecha_fin']}"
                f"{bonus_txt}\n\n"
                "Pulsa el botón cuando estés listo para entrar al canal.\n"
                "El enlace se generará en el momento y tendrá 1 hora de validez."
            ),
            reply_markup=acceso_listo_markup(),
            parse_mode="Markdown",
        )
        await update.message.reply_text(
            f"Usuario {target_user_id} aprobado para {plan} hasta {record['fecha_fin']}."
            + (f"\n🎁 Referido: 2x1 + {REFERIDOR_DIAS}d al referidor "
               f"{referido_info['referrer_id']}." if referido_info else "")
        )
    except Exception as e:
        await update.message.reply_text(f"Error avisando al usuario: {e}")

    await _notificar_recompensa_referido(context, referido_info)


async def rechazar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso correcto: /rechazar user_id")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text="No he podido validar el pago. Escríbeme de nuevo si quieres revisarlo.",
        )
    except Exception as e:
        logger.error(f"Error avisando rechazo a {target_user_id}: {e}")

    await _run_db(delete_pending_payment, target_user_id)
    await _run_db(
        registrar_evento, "rechazo",
        target_user_id=target_user_id,
        actor_id=update.effective_user.id, actor_tipo="admin",
        detalle="via /rechazar",
    )
    await update.message.reply_text(f"Usuario {target_user_id} rechazado.")


async def estado(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso correcto: /estado user_id")
        return

    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, plan, fecha_inicio, fecha_fin, estado,
                           acceso_revocado, motivo_baja
                    FROM users WHERE telegram_user_id = %s
                    """,
                    (user_id,),
                )
                return cur.fetchone()

    record = await _run_db(_q)

    if not record:
        await update.message.reply_text("Ese usuario no tiene suscripción registrada.")
        return

    texto = (
        f"Usuario: {record['telegram_user_id']}\n"
        f"Plan: {record['plan']}\n"
        f"Inicio: {record['fecha_inicio']}\n"
        f"Fin: {record['fecha_fin']}\n"
        f"Estado: {record['estado']}"
    )
    if record["estado"] != "activo":
        texto += f"\nAcceso revocado: {'sí' if record['acceso_revocado'] else 'no'}"
    if record.get("motivo_baja"):
        texto += f"\nMotivo: {record['motivo_baja']}"
    await update.message.reply_text(texto)


# Emojis por tipo de evento para que el historial se lea de un vistazo.
_EVENT_EMOJI = {
    "aprobacion":       "✅",
    "rechazo":          "❌",
    "renovacion":       "🔁",
    "regalo":           "🎁",
    "trial":            "🆓",
    "caducidad":        "⌛",
    "expulsion_manual": "🚷",
    "cancelacion":      "🛑",
    "reembolso":        "💸",
    "referido_recompensa": "🎁",
    "acceso_entregado": "🔑",
}


def _formatear_evento(row: dict, incluir_target: bool = False) -> str:
    emoji = _EVENT_EMOJI.get(row["event"], "•")
    cuando = row["created_at"]
    cuando_str = cuando.strftime("%Y-%m-%d %H:%M") if hasattr(cuando, "strftime") else str(cuando)[:16]
    partes = [f"{emoji} {cuando_str} | {row['event']}"]
    if incluir_target and row.get("target_user_id"):
        partes.append(f"u={row['target_user_id']}")
    if row.get("plan"):
        partes.append(str(row["plan"]))
    if row.get("fecha_fin"):
        partes.append(f"→ {row['fecha_fin']}")
    if row.get("actor_tipo") and row["actor_tipo"] != "sistema":
        partes.append(f"por {row['actor_tipo']}")
    if row.get("detalle"):
        partes.append(f"({row['detalle']})")
    return " | ".join(partes)


async def historial(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra el historial de auditoría de un usuario. Uso: /historial user_id"""
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso: /historial user_id")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT created_at, event, actor_id, actor_tipo, target_user_id,
                           plan, fecha_fin, detalle
                    FROM audit_log
                    WHERE target_user_id = %s
                    ORDER BY created_at DESC
                    LIMIT 50
                    """,
                    (target_user_id,),
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text(
            f"No hay eventos de auditoría para el usuario {target_user_id}."
        )
        return

    lineas = [f"🧾 Historial de {target_user_id} (últimos {len(rows)}):\n"]
    for row in rows:
        lineas.append(_formatear_evento(row))
    await update.message.reply_text("\n".join(lineas)[:4000])


async def auditoria(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra los últimos eventos de auditoría de todos los usuarios."""
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    limite = 30
    if context.args:
        try:
            limite = max(1, min(100, int(context.args[0])))
        except ValueError:
            pass

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT created_at, event, actor_id, actor_tipo, target_user_id,
                           plan, fecha_fin, detalle
                    FROM audit_log
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limite,),
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay eventos de auditoría registrados.")
        return

    lineas = [f"🧾 Auditoría — últimos {len(rows)} eventos:\n"]
    for row in rows:
        lineas.append(_formatear_evento(row, incluir_target=True))
    await update.message.reply_text("\n".join(lineas)[:4000])


async def debug_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    await update.message.reply_text(
        "DEBUG PREMIUM\n\n"
        f"BIZUM: {BIZUM}\n"
        f"DEPLOYMENT: {DEPLOYMENT_COMMIT}\n"
        f"BOT_TOKEN cargado: {'si' if bool(TOKEN) else 'no'}\n"
        f"DATABASE_URL cargada: {'si' if bool(DATABASE_URL) else 'no'}\n"
        f"PICKS_DATABASE_URL cargada: {'si' if bool(PICKS_DATABASE_URL) else 'no'}"
    )


async def listar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, full_name, username, plan, estado, fecha_fin
                    FROM users ORDER BY fecha_fin ASC
                    """
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay usuarios guardados.")
        return

    lineas = ["📋 Usuarios activos/guardados:\n"]
    for row in rows:
        nombre   = row["full_name"] or "?"
        username = f"@{row['username']}" if row["username"] else "(sin @)"
        lineas.append(
            f"{row['telegram_user_id']} | {nombre} {username} | "
            f"{row['plan']} | {row['estado']} | hasta {row['fecha_fin']}"
        )
    await update.message.reply_text("\n".join(lineas)[:4000])


async def pendientes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, username, full_name, plan, created_at
                    FROM pending_payments ORDER BY created_at DESC
                    """
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay pagos pendientes.")
        return

    lineas = ["📥 Pagos pendientes:\n"]
    for row in rows:
        username = f"@{row['username']}" if row["username"] else "(sin username)"
        lineas.append(
            f"{row['telegram_user_id']} | {row['full_name']} | {username} | {row['plan']}"
        )
    await update.message.reply_text("\n".join(lineas)[:4000])


async def caducan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    today = today_date()

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, plan, fecha_fin
                    FROM users
                    WHERE estado = 'activo'
                      AND fecha_fin <= %s
                    ORDER BY fecha_fin ASC
                    """,
                    (today + timedelta(days=7),),
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay caducidades en los próximos 7 días.")
        return

    lineas = ["⏳ Próximas caducidades:\n"]
    for row in rows:
        end_date = row["fecha_fin"]
        if isinstance(end_date, str):
            end_date = parse_date(end_date)
        days_left = (end_date - today).days
        lineas.append(
            f"{row['telegram_user_id']} | {row['plan']} | {end_date} | faltan {days_left} días"
        )
    await update.message.reply_text("\n".join(lineas)[:4000])


async def activos(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, plan, fecha_fin
                    FROM users WHERE estado = 'activo'
                    ORDER BY fecha_fin ASC
                    """
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay usuarios activos.")
        return

    texto = "✅ Usuarios activos:\n\n"
    for row in rows:
        texto += f"{row['telegram_user_id']} | {row['plan']} | hasta {row['fecha_fin']}\n"
    await update.message.reply_text(texto[:4000])


async def trials_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lista los trials usados con un resumen de conversión a pago."""
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT t.telegram_user_id, t.plan AS plan_trial, t.used_at,
                           u.fecha_inicio, u.fecha_fin, u.estado, u.plan AS plan_actual,
                           u.full_name, u.username
                    FROM trials t
                    LEFT JOIN users u ON u.telegram_user_id = t.telegram_user_id
                    ORDER BY t.used_at DESC
                    """
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay trials registrados.")
        return

    today = today_date()
    total = len(rows)
    convertidos = 0
    en_trial    = 0
    expirados   = 0
    lineas      = []

    for row in rows:
        used_at = row["used_at"]
        used_date = used_at.date() if hasattr(used_at, "date") else parse_date(str(used_at)[:10])

        fecha_inicio = row["fecha_inicio"]
        fecha_fin    = row["fecha_fin"]
        if isinstance(fecha_inicio, str):
            fecha_inicio = parse_date(fecha_inicio)
        if isinstance(fecha_fin, str):
            fecha_fin = parse_date(fecha_fin)

        # "Es la fila del trial actual" = fecha_inicio == used_at::date
        # y fecha_fin <= used_at::date + TRIAL_DAYS (no se extendió)
        es_fila_trial = (
            fecha_inicio is not None
            and fecha_fin is not None
            and fecha_inicio == used_date
            and fecha_fin <= used_date + timedelta(days=TRIAL_DAYS)
        )

        if row["estado"] == "activo" and fecha_fin and fecha_fin >= today:
            if es_fila_trial:
                en_trial += 1
                marca = f"🎁 trial ({fecha_fin})"
            else:
                convertidos += 1
                marca = f"💰 pagó ({row['plan_actual']}, hasta {fecha_fin})"
        else:
            expirados += 1
            marca = "⌛ sin sub"

        nombre = row["full_name"] or f"User {row['telegram_user_id']}"
        username = f"@{row['username']}" if row["username"] else ""
        lineas.append(
            f"{row['telegram_user_id']} | {nombre} {username} | trial:{row['plan_trial']} | {marca}"
        )

    cabecera = (
        f"🎁 Trials: {total} totales\n"
        f"💰 Convertidos a pago: {convertidos}\n"
        f"🎁 Trial en curso: {en_trial}\n"
        f"⌛ Expirados sin pagar: {expirados}\n\n"
    )
    await update.message.reply_text((cabecera + "\n".join(lineas))[:4000])


async def encuestas_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra el resumen de respuestas de la encuesta de satisfacción."""
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT e.telegram_user_id, e.plan, e.sent_at, e.razon, e.valoracion,
                           e.sugerencia, e.responded_at, u.full_name, u.username
                    FROM encuestas e
                    LEFT JOIN users u ON u.telegram_user_id = e.telegram_user_id
                    ORDER BY e.sent_at DESC
                    """
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("Aún no se ha enviado ninguna encuesta.")
        return

    enviadas    = len(rows)
    respondidas = sum(1 for r in rows if r["responded_at"])
    rechazadas  = sum(1 for r in rows if r["razon"] == "rechazada")
    sin_responder = enviadas - respondidas

    razones = {}
    valoraciones = []
    for r in rows:
        if r["razon"] and r["razon"] != "rechazada":
            razones[r["razon"]] = razones.get(r["razon"], 0) + 1
        if r["valoracion"] is not None:
            valoraciones.append(r["valoracion"])

    media = (sum(valoraciones) / len(valoraciones)) if valoraciones else None

    lineas = [
        "📊 Encuestas — resumen\n",
        f"Enviadas: {enviadas}",
        f"Respondidas: {respondidas}",
        f"Rechazaron: {rechazadas}",
        f"Sin responder: {sin_responder}",
    ]
    if media is not None:
        lineas.append(f"Valoración media: {media:.1f}/5 ({len(valoraciones)} respuestas)")
    if razones:
        lineas.append("\nMotivos:")
        for key, count in sorted(razones.items(), key=lambda x: -x[1]):
            etiqueta = RAZONES_ENCUESTA.get(key, key)
            lineas.append(f"  • {etiqueta}: {count}")

    sugerencias = [r for r in rows if r["sugerencia"]]
    if sugerencias:
        lineas.append("\n💬 Sugerencias:")
        for r in sugerencias[-10:]:
            nombre = r["full_name"] or f"User {r['telegram_user_id']}"
            lineas.append(f"  • {nombre}: {r['sugerencia'][:200]}")

    await update.message.reply_text("\n".join(lineas)[:4000])


async def encuesta_pendientes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Envía la encuesta ahora mismo a todos los usuarios caducados que aún
    no la han recibido, ignorando el delay de 3 días. Útil para hacer una
    primera ronda con los caducados históricos.
    """
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.telegram_user_id, u.plan, u.full_name
                    FROM users u
                    LEFT JOIN encuestas e ON e.telegram_user_id = u.telegram_user_id
                    WHERE u.estado = 'caducado'
                      AND e.telegram_user_id IS NULL
                    ORDER BY u.fecha_fin ASC
                    """
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay caducados sin encuesta pendiente.")
        return

    enviadas = 0
    for r in rows:
        ok = await enviar_encuesta_inicial(
            context,
            int(r["telegram_user_id"]),
            r["full_name"] or "",
            r["plan"],
        )
        if ok:
            enviadas += 1

    await update.message.reply_text(
        f"📨 Encuesta enviada a {enviadas}/{len(rows)} usuarios caducados."
    )


async def renovar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Extiende manualmente la suscripción de un usuario sin pasar por el flujo de pago.
    Uso: /renovar user_id [plan]
    Si se omite el plan, usa el que ya tiene el usuario.
    """
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso: /renovar user_id [goles|corners|pre|combo]")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    # Buscar plan actual si no se especifica uno nuevo
    plan_nuevo = context.args[1].lower() if len(context.args) >= 2 else None

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT username, full_name, plan FROM users WHERE telegram_user_id = %s",
                    (target_user_id,),
                )
                return cur.fetchone()

    existing = await _run_db(_q)

    if not existing and not plan_nuevo:
        await update.message.reply_text(
            "Ese usuario no tiene suscripción previa. Especifica el plan: /renovar user_id goles"
        )
        return

    plan_anterior = existing["plan"] if existing else None
    plan          = plan_nuevo or plan_anterior
    if plan not in ("goles", "corners", "combo", "pre", "total"):
        await update.message.reply_text("Plan no válido. Usa: goles, corners, pre, combo o total")
        return

    username   = existing["username"] if existing else None
    full_name  = existing["full_name"] if existing else f"Usuario {target_user_id}"

    # Renovación ATÓMICA: extender + registrar acceso + auditar en una
    # transacción. Si falla, no se aplica nada.
    try:
        record, plan_anterior = await _run_db(
            renovar_tx,
            target_user_id, plan, username, full_name,
            actor_id=update.effective_user.id,
        )
    except Exception as e:
        await update.message.reply_text(
            f"⚠️ Error al renovar (no se aplicó nada): {e}. Reinténtalo."
        )
        return

    # Si cambió de plan, expulsar de los canales que ya no le corresponden
    await _expulsar_canales_obsoletos(context, target_user_id, plan_anterior, plan)

    # Generamos el invite link aquí mismo para devolvérselo al admin,
    # de forma que pueda compartirlo manualmente (WhatsApp, DM, etc.)
    # si el bot no puede mandar mensaje al usuario directamente.
    try:
        enlaces = await generar_enlaces_acceso(context, plan, user_id=target_user_id)
    except Exception as e:
        logger.error("Error generando enlaces en /renovar para %s: %s", target_user_id, e)
        enlaces = []

    aviso_al_user_ok = True
    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=(
                "✅ *Suscripción renovada*\n\n"
                f"Plan activo: *{plan.upper()}*\n"
                f"Válido hasta: {record['fecha_fin']}\n\n"
                "Pulsa el botón cuando estés listo para entrar al canal."
            ),
            reply_markup=acceso_listo_markup(),
            parse_mode="Markdown",
        )
    except Exception as e:
        aviso_al_user_ok = False
        logger.error(f"Error avisando renovación a {target_user_id}: {e}")

    respuesta = (
        f"✅ Suscripción renovada: usuario {target_user_id} | "
        f"{plan.upper()} | hasta {record['fecha_fin']}"
    )
    if not aviso_al_user_ok:
        respuesta += "\n\n⚠️ No he podido mandarle DM (¿bot bloqueado?). Comparte tú el link:"
    if enlaces:
        respuesta += "\n\nEnlaces de acceso (1 uso, 1 hora):"
        for titulo, link in enlaces:
            respuesta += f"\n{titulo}: {link}"

    await update.message.reply_text(respuesta)
    logger.info(f"Renovación manual: user {target_user_id} | plan {plan} | hasta {record['fecha_fin']}")


async def link_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Genera un invite link nuevo (1 uso, 1h) para un usuario con sub activa.
    Útil para reenviar el acceso por DM/WhatsApp si el original caducó o
    el usuario perdió el botón.
    Uso: /link user_id
    """
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso: /link user_id")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT plan, estado, fecha_fin FROM users WHERE telegram_user_id = %s",
                    (target_user_id,),
                )
                return cur.fetchone()

    row = await _run_db(_q)

    if not row:
        await update.message.reply_text(
            f"Usuario {target_user_id} no tiene suscripción registrada. "
            "Usa /renovar user_id plan para darle acceso."
        )
        return

    if row["estado"] != "activo":
        await update.message.reply_text(
            f"⚠️ Usuario {target_user_id} no está activo (estado={row['estado']}). "
            "Usa /renovar user_id [plan] primero."
        )
        return

    plan = row["plan"]
    try:
        enlaces = await generar_enlaces_acceso(context, plan, user_id=target_user_id)
    except Exception as e:
        logger.error("Error generando enlaces en /link para %s: %s", target_user_id, e)
        await update.message.reply_text(f"Error generando enlaces: {e}")
        return

    if not enlaces:
        await update.message.reply_text(
            f"No hay canales asociados al plan '{plan}'. Comprueba la configuración."
        )
        return

    respuesta = (
        f"🔑 Enlaces para usuario {target_user_id} "
        f"({plan.upper()}, hasta {row['fecha_fin']})\n"
        "Cada enlace es de *1 uso* y dura *1 hora*:\n"
    )
    for titulo, link in enlaces:
        respuesta += f"\n{titulo}: {link}"
    await update.message.reply_text(respuesta, parse_mode="Markdown")


async def regalar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Regala una suscripción a un usuario con fecha de fin FIJA: hoy + días.
    A diferencia de /renovar, sobreescribe la fecha existente (no la suma).
    Pensado para cortesía, regalos o arreglar suscripciones desfasadas.

    Uso: /regalar <user_id> <plan> <días>
      Plan: goles | corners | pre | combo | total
      Ejemplo: /regalar 6905572130 total 30
    """
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    if len(context.args) < 3:
        await update.message.reply_text(
            "Uso: /regalar <user_id> <plan> <días>\n"
            "Plan: goles | corners | pre | combo | total\n"
            "Ejemplo: /regalar 6905572130 total 30"
        )
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    plan = context.args[1].lower()
    if plan not in ("goles", "corners", "combo", "pre", "total"):
        await update.message.reply_text(
            "Plan no válido. Usa: goles, corners, pre, combo o total"
        )
        return

    try:
        days = int(context.args[2])
    except ValueError:
        await update.message.reply_text("Los días deben ser un número entero.")
        return
    if days <= 0 or days > 365:
        await update.message.reply_text("Días debe estar entre 1 y 365.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT username, full_name, plan FROM users WHERE telegram_user_id = %s",
                    (target_user_id,),
                )
                return cur.fetchone()

    existing = await _run_db(_q)

    username  = existing["username"] if existing else None
    full_name = existing["full_name"] if existing else f"Usuario {target_user_id}"

    # Regalo ATÓMICO: asignar (set) + registrar acceso + auditar en una
    # transacción. Si falla, no se aplica nada.
    try:
        record, plan_anterior = await _run_db(
            regalar_tx,
            target_user_id, plan, days, username, full_name,
            actor_id=update.effective_user.id,
        )
    except Exception as e:
        await update.message.reply_text(f"⚠️ Error al asignar la suscripción: {e}")
        return

    # Si el plan anterior incluía canales que ya no están, expulsar
    await _expulsar_canales_obsoletos(context, target_user_id, plan_anterior, plan)

    try:
        enlaces = await generar_enlaces_acceso(context, plan, user_id=target_user_id)
    except Exception as e:
        logger.error("Error generando enlaces en /regalar para %s: %s", target_user_id, e)
        enlaces = []

    dm_ok = True
    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=(
                "🎁 *Acceso de cortesía*\n\n"
                f"Plan: *{plan.upper()}*\n"
                f"Válido hasta: {record['fecha_fin']}\n\n"
                "Pulsa el botón cuando quieras entrar al canal."
            ),
            reply_markup=acceso_listo_markup(),
            parse_mode="Markdown",
        )
    except Exception as e:
        dm_ok = False
        logger.error(f"Error notificando regalo a {target_user_id}: {e}")

    respuesta = (
        f"🎁 Regalo activado: usuario {target_user_id} | {plan.upper()} | "
        f"{days} días | hasta {record['fecha_fin']}"
    )
    if not dm_ok:
        respuesta += "\n\n⚠️ No he podido mandarle DM. Comparte tú el link:"
    if enlaces:
        respuesta += "\n\nEnlaces de acceso (1 uso, 1 hora):"
        for titulo, link in enlaces:
            respuesta += f"\n{titulo}: {link}"

    await update.message.reply_text(respuesta)
    logger.info(
        "Regalo: user %s | plan %s | %s días | hasta %s",
        target_user_id, plan, days, record["fecha_fin"],
    )


async def expulsar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso correcto: /expulsar user_id")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    def _q_plan():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT plan FROM users WHERE telegram_user_id = %s",
                    (target_user_id,),
                )
                return cur.fetchone()

    record = await _run_db(_q_plan)

    if not record:
        await update.message.reply_text("Ese usuario no está registrado.")
        return

    ok = await expulsar_de_canales(context, target_user_id, record["plan"])
    await _run_db(borrar_acceso_pendiente, target_user_id)

    def _q_upd():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET estado = 'caducado', acceso_revocado = %s, "
                    "updated_at = NOW() WHERE telegram_user_id = %s",
                    (ok, target_user_id),
                )

    await _run_db(_q_upd)

    await _run_db(
        registrar_evento, "expulsion_manual",
        target_user_id=target_user_id,
        actor_id=update.effective_user.id, actor_tipo="admin",
        plan=record["plan"], detalle=f"ban_ok={ok}",
    )

    if ok:
        await update.message.reply_text(
            f"✅ Usuario {target_user_id} expulsado y marcado como caducado."
        )
    else:
        await update.message.reply_text(
            f"⚠️ Usuario {target_user_id} marcado como caducado, pero el ban falló "
            "en alguno de los canales. Revisa los permisos del bot y reintenta con /reexpulsar."
        )


async def desbanear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Levanta el ban de un usuario en TODOS los canales (only_if_banned).
    Útil cuando una expulsión anterior dejó al usuario baneado y no puede
    entrar ni con un enlace válido. Uso: /desbanear user_id
    """
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso: /desbanear user_id")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    canales = [
        ("⚽ GOLES", CANAL_GOLES_ID),
        ("🚩 CORNERS", CANAL_CORNERS_ID),
        ("📊 PREPARTIDO", CANAL_PRE_ID),
    ]
    lineas = []
    for titulo, chat_id in canales:
        try:
            await context.bot.unban_chat_member(
                chat_id=chat_id, user_id=target_user_id, only_if_banned=True
            )
            lineas.append(f"✅ {titulo}")
        except Exception as e:
            logger.error("Error desbaneando %s en %s: %s", target_user_id, chat_id, e)
            lineas.append(f"❌ {titulo}: {e}")

    await update.message.reply_text(
        f"🔓 Desbaneo de {target_user_id}:\n" + "\n".join(lineas) +
        "\n\nYa puede volver a usar su enlace de acceso."
    )
    logger.info("Admin desbaneó manualmente al usuario %s de todos los canales", target_user_id)


async def _baja_usuario(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    estado_nuevo: str,
    evento: str,
    etiqueta: str,
    cmd: str,
) -> None:
    """
    Lógica común a /cancelar y /reembolsar: expulsa de los canales, marca el
    estado correspondiente con su motivo, limpia el acceso pendiente y audita.
    Uso del comando: /<cmd> user_id [motivo libre]
    """
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    if len(context.args) < 1:
        await update.message.reply_text(f"Uso: /{cmd} user_id [motivo]")
        return

    try:
        target_user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id no es válido.")
        return

    motivo = " ".join(context.args[1:]).strip() or None

    def _q_rec():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT plan, estado FROM users WHERE telegram_user_id = %s",
                    (target_user_id,),
                )
                return cur.fetchone()

    record = await _run_db(_q_rec)

    if not record:
        await update.message.reply_text("Ese usuario no está registrado.")
        return

    ok = await expulsar_de_canales(context, target_user_id, record["plan"])
    await _run_db(borrar_acceso_pendiente, target_user_id)

    def _q_upd():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET estado = %s, motivo_baja = %s, acceso_revocado = %s, "
                    "updated_at = NOW() WHERE telegram_user_id = %s",
                    (estado_nuevo, motivo, ok, target_user_id),
                )

    await _run_db(_q_upd)

    await _run_db(
        registrar_evento, evento,
        target_user_id=target_user_id,
        actor_id=update.effective_user.id, actor_tipo="admin",
        plan=record["plan"],
        detalle=(motivo or "") + (f" | ban_ok={ok}" if not ok else ""),
    )

    detalle_motivo = f"\nMotivo: {motivo}" if motivo else ""
    if ok:
        await update.message.reply_text(
            f"✅ Usuario {target_user_id} marcado como {estado_nuevo.upper()} y expulsado.{detalle_motivo}"
        )
    else:
        await update.message.reply_text(
            f"⚠️ Usuario {target_user_id} marcado como {estado_nuevo.upper()}, pero el ban falló "
            f"en algún canal. Reintenta con /reexpulsar.{detalle_motivo}"
        )
    logger.info(
        "%s: user %s | plan %s | motivo=%s | ban_ok=%s",
        etiqueta, target_user_id, record["plan"], motivo, ok,
    )


async def cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancela la suscripción de un usuario (estado 'cancelado'). Uso: /cancelar user_id [motivo]"""
    await _baja_usuario(update, context, "cancelado", "cancelacion", "Cancelación", "cancelar")


async def reembolsar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Marca un reembolso y retira el acceso (estado 'reembolsado'). Uso: /reembolsar user_id [motivo]"""
    await _baja_usuario(update, context, "reembolsado", "reembolso", "Reembolso", "reembolsar")


async def reexpulsar(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Reintenta expulsar a todos los usuarios marcados como 'caducado'
    cuya fecha_fin ya pasó. Útil para recuperar a los que se quedaron
    atascados por errores transitorios del ban.
    """
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, plan, full_name, username, fecha_fin, estado
                    FROM users
                    WHERE estado <> 'activo' AND acceso_revocado = FALSE
                    ORDER BY fecha_fin ASC
                    """
                )
                return cur.fetchall()

    rows = await _run_db(_q)

    if not rows:
        await update.message.reply_text("No hay usuarios dados de baja pendientes de expulsar.")
        return

    lineas = []
    ok_count = 0
    for row in rows:
        user_id  = row["telegram_user_id"]
        plan     = row["plan"]
        nombre   = row["full_name"] or f"User {user_id}"
        try:
            ok = await expulsar_de_canales(context, int(user_id), plan)
        except Exception as e:
            logger.error("Error en /reexpulsar para %s: %s", user_id, e)
            ok = False
        if ok:
            ok_count += 1
            await _run_db(marcar_acceso_revocado, int(user_id), True)
        marca = "✅" if ok else "❌"
        lineas.append(f"{marca} {user_id} | {nombre} | {plan} | {row['estado']} | fin {row['fecha_fin']}")

    cabecera = f"🔁 Reexpulsión: {ok_count}/{len(rows)} OK\n\n"
    await update.message.reply_text((cabecera + "\n".join(lineas))[:4000])
    logger.info("Reexpulsión manual: %d/%d usuarios expulsados OK", ok_count, len(rows))


# ==============================
# HELPERS — AVISOS DE EXPIRACIÓN
# ==============================

def _instrucciones_renovacion(plan: str) -> str:
    """
    Devuelve el bloque de texto con precio, métodos de pago y pasos
    que se incluye en todos los avisos de expiración.
    """
    plan = canonical_plan(plan)

    # El plan TOTAL (GOLES + CORNERS + PRE) es siempre asignación manual
    # del admin, no tiene precio público y no se compra por menu.
    if plan == "total":
        return (
            "\n\nEste paquete completo se asigna manualmente. "
            "Habla con @erikenobi si quieres renovarlo."
        )

    precios  = {"goles": PRECIO_GOLES, "corners": PRECIO_CORNERS, "combo": PRECIO_COMBO, "pre": PRECIO_PRE}
    stripes  = {"goles": STRIPE_GOLES, "corners": STRIPE_CORNERS, "combo": STRIPE_COMBO, "pre": STRIPE_PRE}

    precio     = precios.get(plan, "20€")
    stripe_url = stripes.get(plan, "")

    stripe_linea = f"• 💳 Tarjeta (Stripe): {stripe_url}\n" if stripe_url else ""

    return (
        f"\n\n💰 *Precio:* {precio}/mes\n\n"
        "📋 *Para renovar:*\n"
        f"{stripe_linea}"
        f"• 🅿️ PayPal: {PAYPAL_LINK}\n"
        f"• 📲 Bizum: {BIZUM}\n"
        f"• 🟣 Revolut: {REVOLUT_LINK}\n\n"
        "Una vez pagado, *envíame aquí la captura del comprobante* ⬅️ "
        "y activo el acceso en cuanto lo vea."
    )


# ==============================
# JOB — EXPIRACIÓN AUTOMÁTICA
# ==============================

async def check_expirations(context: ContextTypes.DEFAULT_TYPE) -> None:
    today = today_date()

    def _q_proximas():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, plan, fecha_inicio, fecha_fin, estado
                    FROM users
                    WHERE estado = 'activo'
                      AND fecha_fin <= %s
                    """,
                    (today + timedelta(days=3),),
                )
                return cur.fetchall()

    rows = await _run_db(_q_proximas)

    for record in rows:
        try:
            user_id  = record["telegram_user_id"]
            end_date = record["fecha_fin"]
            if isinstance(end_date, str):
                end_date = parse_date(end_date)

            days_left = (end_date - today).days

            # Clave de deduplicación: (user_id, "aviso_N_dias", fecha_fin)
            # Incluir fecha_fin evita que una renovación anule los avisos del
            # siguiente ciclo (sin la fecha, las claves colisionarían).
            fecha_str  = str(end_date)
            plan_upper = record["plan"].upper()
            renovar    = _instrucciones_renovacion(record["plan"])
            es_trial   = await _run_db(es_trial_actual, user_id, record["fecha_inicio"], end_date)

            # Para usuarios en trial saltamos el aviso de 3 días: lo activan
            # y lo recibirían inmediatamente, lo que es ruido innecesario.
            if days_left == 3 and not es_trial:
                aviso_key = (user_id, "aviso_3", fecha_str)
                if aviso_key not in _avisos_enviados:
                    await context.bot.send_message(
                        chat_id=int(user_id),
                        text=(
                            f"⏳ Tu suscripción *{plan_upper}* caduca en 3 días ({end_date}).\n"
                            "Si quieres renovarla sin interrupciones, tienes tiempo de sobra."
                            + renovar
                        ),
                        parse_mode="Markdown",
                    )
                    _avisos_enviados.add(aviso_key)

            elif days_left == 2:
                aviso_key = (user_id, "aviso_2", fecha_str)
                if aviso_key not in _avisos_enviados:
                    if es_trial:
                        texto = (
                            f"🎁 Tu *prueba gratuita* de *{plan_upper}* caduca en 2 días ({end_date}).\n"
                            "Si quieres seguir disfrutando del servicio, elige un plan:"
                            + renovar
                        )
                    else:
                        texto = (
                            f"⏳ Tu suscripción *{plan_upper}* caduca en 2 días ({end_date}).\n"
                            "Renueva hoy para no perder el acceso."
                            + renovar
                        )
                    await context.bot.send_message(
                        chat_id=int(user_id),
                        text=texto,
                        parse_mode="Markdown",
                    )
                    _avisos_enviados.add(aviso_key)

            elif days_left == 1:
                aviso_key = (user_id, "aviso_1", fecha_str)
                if aviso_key not in _avisos_enviados:
                    if es_trial:
                        texto = (
                            f"🎁 Tu *prueba gratuita* de *{plan_upper}* caduca *mañana* ({end_date}).\n"
                            "Si quieres mantener el acceso, elige un plan hoy:"
                            + renovar
                        )
                    else:
                        texto = (
                            f"⚠️ Tu suscripción *{plan_upper}* caduca *mañana* ({end_date}).\n"
                            "Si renuevas hoy, el acceso no se interrumpe."
                            + renovar
                        )
                    await context.bot.send_message(
                        chat_id=int(user_id),
                        text=texto,
                        parse_mode="Markdown",
                    )
                    _avisos_enviados.add(aviso_key)

            elif days_left == 0:
                aviso_key = (user_id, "aviso_0", fecha_str)
                if aviso_key not in _avisos_enviados:
                    if es_trial:
                        texto = (
                            f"🎁 *Hoy es el último día* de tu prueba gratuita de *{plan_upper}* ({end_date}).\n"
                            "Si te ha gustado y quieres seguir, elige un plan antes de medianoche:"
                            + renovar
                        )
                    else:
                        texto = (
                            f"⚠️ Tu suscripción *{plan_upper}* caduca *hoy* ({end_date}).\n"
                            "Es el último día — si renuevas antes de medianoche el acceso continúa."
                            + renovar
                        )
                    await context.bot.send_message(
                        chat_id=int(user_id),
                        text=texto,
                        parse_mode="Markdown",
                    )
                    _avisos_enviados.add(aviso_key)

            elif days_left < 0:
                expulsado_ok = await expulsar_de_canales(context, int(user_id), record["plan"])
                await _run_db(borrar_acceso_pendiente, user_id)

                def _q_caducar():
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE users SET estado = 'caducado', acceso_revocado = %s, "
                                "updated_at = NOW() WHERE telegram_user_id = %s",
                                (expulsado_ok, user_id),
                            )

                await _run_db(_q_caducar)

                if es_trial:
                    texto = (
                        f"🎁 Tu prueba gratuita de *{plan_upper}* ha terminado.\n"
                        "Se ha retirado tu acceso al canal.\n\n"
                        "Si quieres seguir disfrutando del servicio, elige un plan:"
                        + renovar
                    )
                else:
                    texto = (
                        f"❌ Tu suscripción *{plan_upper}* ha caducado.\n"
                        "Se ha retirado tu acceso a los canales premium.\n\n"
                        "Si quieres volver a activarla:"
                        + renovar
                    )
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=texto,
                    parse_mode="Markdown",
                )
                logger.info(
                    "Suscripción caducada y usuario expulsado: %s (trial=%s, ban_ok=%s)",
                    user_id, es_trial, expulsado_ok,
                )
                await _run_db(
                    registrar_evento, "caducidad",
                    target_user_id=int(user_id), actor_tipo="sistema",
                    plan=record["plan"], fecha_fin=end_date,
                    detalle=f"trial={es_trial} ban_ok={expulsado_ok}",
                )

                # Si la expulsión falló, avisar al admin (la job reintentará
                # automáticamente cada ciclo en la segunda pasada).
                if not expulsado_ok:
                    for admin_id in ADMIN_IDS:
                        try:
                            await context.bot.send_message(
                                chat_id=admin_id,
                                text=(
                                    f"⚠️ No se pudo expulsar al usuario {user_id} "
                                    f"({record['plan']}) tras caducar.\n"
                                    "Está marcado como caducado pero sigue en el canal.\n"
                                    "Se reintentará automáticamente cada hora. "
                                    "Usa /reexpulsar para forzarlo ahora."
                                ),
                            )
                        except Exception as e:
                            logger.error("Error avisando expulsión fallida al admin %s: %s", admin_id, e)

        except Exception as e:
            logger.error(f"Error revisando expiración de {record.get('telegram_user_id')}: {e}")

    # ── Segunda pasada: reintentar expulsión de cualquier usuario dado de baja
    # recientemente (caducado/cancelado/reembolsado) cuyo acceso aún no se ha
    # revocado con éxito. El flag acceso_revocado evita re-banear en cada ciclo
    # a los ya expulsados; la recencia por updated_at (REEXPULSION_RETRY_DAYS)
    # evita re-escanear el histórico antiguo y cubre las bajas anticipadas
    # (cancelaciones antes de fecha_fin).
    def _q_pendientes():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT telegram_user_id, plan
                    FROM users
                    WHERE estado <> 'activo'
                      AND acceso_revocado = FALSE
                      AND updated_at >= NOW() - (%s * INTERVAL '1 day')
                    """,
                    (REEXPULSION_RETRY_DAYS,),
                )
                return cur.fetchall()

    pendientes = await _run_db(_q_pendientes)

    for record in pendientes:
        try:
            user_id = record["telegram_user_id"]
            ok = await expulsar_de_canales(context, int(user_id), record["plan"])
            if ok:
                await _run_db(marcar_acceso_revocado, int(user_id), True)
                logger.info("Reintento de expulsión exitoso: %s (%s)", user_id, record["plan"])
        except Exception as e:
            logger.error(
                "Error en reintento de expulsión de %s: %s",
                record.get("telegram_user_id"), e,
            )

    # ── Tercera pasada: enviar encuesta de satisfacción a usuarios
    # que caducaron hace al menos ENCUESTA_DELAY_DAYS días y aún no
    # la han recibido.
    delay_days = 3

    def _q_candidatos():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.telegram_user_id, u.plan, u.full_name
                    FROM users u
                    LEFT JOIN encuestas e ON e.telegram_user_id = u.telegram_user_id
                    WHERE u.estado = 'caducado'
                      AND u.fecha_fin <= %s
                      AND e.telegram_user_id IS NULL
                    """,
                    (today - timedelta(days=delay_days),),
                )
                return cur.fetchall()

    candidatos = await _run_db(_q_candidatos)

    for c in candidatos:
        try:
            await enviar_encuesta_inicial(
                context,
                int(c["telegram_user_id"]),
                c["full_name"] or "",
                c["plan"],
            )
        except Exception as e:
            logger.error(
                "Error enviando encuesta a %s: %s",
                c.get("telegram_user_id"), e,
            )

    # Limpieza de buckets de rate limiting expirados (acota memoria).
    _limpiar_rate_buckets()


async def limpiar_pending_payments_antiguos(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Elimina entradas de pending_payments con más de 7 días.
    Evita que usuarios que abrieron el bot pero nunca pagaron acumulen
    registros huérfanos indefinidamente.
    """
    def _q():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pending_payments WHERE created_at < NOW() - INTERVAL '7 days'"
                )
                return cur.rowcount

    try:
        borrados = await _run_db(_q)
        if borrados:
            logger.info("Limpieza pending_payments: %d registros huérfanos eliminados.", borrados)
    except Exception as e:
        logger.error("Error en limpieza de pending_payments: %s", e)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Error capturado: {context.error}", exc_info=context.error)


async def _post_init(app) -> None:
    """Al arrancar: resuelve el @username del bot para los enlaces de referido."""
    global BOT_USERNAME
    if not BOT_USERNAME:
        try:
            me = await app.bot.get_me()
            BOT_USERNAME = me.username
            logger.info("Bot username resuelto: @%s", BOT_USERNAME)
        except Exception as e:
            logger.error("No se pudo resolver el username del bot: %s", e)


# ==============================
# MAIN
# ==============================

def main() -> None:
    if not TOKEN:
        logger.critical("Falta BOT_TOKEN en variables de entorno.")
        sys.exit(1)
    if not DATABASE_URL:
        logger.critical("Falta DATABASE_URL en variables de entorno.")
        sys.exit(1)
    if not PICKS_DATABASE_URL:
        logger.warning(
            "PICKS_DATABASE_URL no configurada — las estadísticas reales "
            "no estarán disponibles en el bot premium."
        )

    init_pool()
    init_db()

    app = ApplicationBuilder().token(TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start",         start))
    app.add_handler(CommandHandler("help",          help_command))
    app.add_handler(CommandHandler("whoami",        whoami))
    app.add_handler(CommandHandler("referido",      referido_command))
    app.add_handler(CommandHandler("aprobar",       aprobar))
    app.add_handler(CommandHandler("rechazar",      rechazar))
    app.add_handler(CommandHandler("estado",        estado))
    app.add_handler(CommandHandler("historial",     historial))
    app.add_handler(CommandHandler("auditoria",     auditoria))
    app.add_handler(CommandHandler("debugpremium",  debug_premium))
    app.add_handler(CommandHandler("listar",        listar))
    app.add_handler(CommandHandler("pendientes",    pendientes))
    app.add_handler(CommandHandler("caducan",       caducan))
    app.add_handler(CommandHandler("trials",        trials_admin))
    app.add_handler(CommandHandler("encuestas",     encuestas_admin))
    app.add_handler(CommandHandler("encuesta_pendientes", encuesta_pendientes))
    app.add_handler(CommandHandler("activos",       activos))
    app.add_handler(CommandHandler("expulsar",      expulsar))
    app.add_handler(CommandHandler("reexpulsar",    reexpulsar))
    app.add_handler(CommandHandler("desbanear",     desbanear))
    app.add_handler(CommandHandler("cancelar",      cancelar))
    app.add_handler(CommandHandler("reembolsar",    reembolsar))
    app.add_handler(CommandHandler("renovar",       renovar))
    app.add_handler(CommandHandler("regalar",       regalar))
    app.add_handler(CommandHandler("link",          link_admin))

    app.add_handler(CallbackQueryHandler(admin_action_callback, pattern=r"^(approve:|reject:)"))
    app.add_handler(CallbackQueryHandler(encuesta_callback, pattern=r"^enc:"))
    app.add_handler(CallbackQueryHandler(seleccionar_plan))

    # Auto-aprobación de solicitudes de unión a los canales (enlaces con
    # creates_join_request). Aprueba a suscriptores activos, rechaza al resto.
    app.add_handler(ChatJoinRequestHandler(on_join_request))

    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE
            & ((filters.TEXT & ~filters.COMMAND) | filters.PHOTO | filters.Document.ALL),
            recibir_comprobante,
        )
    )

    app.add_error_handler(error_handler)

    app.job_queue.run_repeating(
        check_expirations,
        interval=CHECK_EXPIRATIONS_EVERY_SECONDS,
        first=20,
    )
    app.job_queue.run_repeating(
        limpiar_pending_payments_antiguos,
        interval=7 * 24 * 60 * 60,   # cada 7 días
        first=300,
    )

    logger.info(
        "Bot premium iniciado. BIZUM=%s | deployment=%s",
        BIZUM,
        DEPLOYMENT_COMMIT,
    )
    # No descartamos los updates acumulados durante una caída/redeploy: así no
    # se pierden comprobantes ni mensajes enviados mientras el bot estaba abajo.
    # allowed_updates explícito para garantizar la recepción de las solicitudes
    # de unión (chat_join_request), necesarias para la auto-aprobación.
    app.run_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
