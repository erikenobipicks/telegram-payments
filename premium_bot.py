import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
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

LINK_FREE = "https://t.me/+WhIkP2PstS1kMDVk"

PRECIO_GOLES   = "20€"
PRECIO_CORNERS = "20€"
PRECIO_COMBO   = "30€"

BIZUM        = "+34660426660"
PAYPAL_LINK  = "https://paypal.me/erikenobi"
REVOLUT_LINK = "https://revolut.me/ericblasco9"

STRIPE_GOLES   = "https://buy.stripe.com/aFa8wObuQ9MbdgA00x08g01"
STRIPE_CORNERS = "https://buy.stripe.com/bJe3cugPaf6vdgA5kR08g02"
STRIPE_COMBO   = "https://buy.stripe.com/4gM7sK8iE0bBgsMfZv08g03"

PLAN_DAYS    = 30
INVITE_EXPIRY_HOURS = 1
CHECK_EXPIRATIONS_EVERY_SECONDS = 43200  # 12h

TIMEZONE = "Europe/Madrid"

DEPLOYMENT_COMMIT = (
    os.getenv("RAILWAY_GIT_COMMIT_SHA")
    or os.getenv("RAILWAY_GIT_COMMIT_MESSAGE")
    or os.getenv("RAILWAY_DEPLOYMENT_ID")
    or "local"
)

# Meses en español para formateo de stats
_MESES_ES = {
    "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
}


# ==============================
# DB — BOT PREMIUM
# ==============================

def get_conn():
    if not DATABASE_URL:
        raise ValueError("Falta DATABASE_URL en variables de entorno.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


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


def get_stats_reales() -> dict | None:
    """
    Obtiene estadísticas reales del bot de picks:
      - globales: strike total por tipo_pick
      - ultimo_mes: stats del mes anterior cerrado
      - mes_label: "YYYY-MM" del último mes
      - evolucion: stats agrupadas por mes/tipo de los últimos 6 meses
    Devuelve None si la conexión falla o no hay datos.
    """
    conn = get_picks_conn()
    if not conn:
        return None

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

                # Último mes cerrado
                cur.execute("""
                    SELECT
                        TO_CHAR(
                            date_trunc('month', CURRENT_DATE - INTERVAL '1 month'),
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
                          date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
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

        return {
            "globales":   globales,
            "ultimo_mes": ultimo_mes,
            "mes_label":  mes_label,
            "evolucion":  evolucion,
        }

    except Exception as e:
        logger.error(f"Error obteniendo stats reales: {e}")
        return None


def _get_strike_tipo(stats: dict | None, tipo: str) -> str | None:
    """
    Devuelve el strike del último mes para un tipo_pick ("gol" / "corner").
    Si no hay datos del último mes, usa el global.
    Devuelve None si no hay datos en absoluto.
    """
    if not stats:
        return None
    ultimo_mes = stats.get("ultimo_mes", {})
    if tipo in ultimo_mes:
        row = ultimo_mes[tipo]
        return f"{calcular_strike(row['hits'], row['misses'])}%"
    globales = stats.get("globales", {})
    if tipo in globales:
        row = globales[tipo]
        return f"{calcular_strike(row['hits'], row['misses'])}%"
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


def get_plan_channels(plan: str) -> list[tuple[str, int]]:
    if plan == "goles":
        return [("⚽ GOLES", CANAL_GOLES_ID)]
    if plan == "corners":
        return [("⛳ CORNERS", CANAL_CORNERS_ID)]
    if plan == "combo":
        return [("⚽ GOLES", CANAL_GOLES_ID), ("⛳ CORNERS", CANAL_CORNERS_ID)]
    return []


async def generar_enlaces_acceso(context: ContextTypes.DEFAULT_TYPE, plan: str) -> list[tuple[str, str]]:
    """
    Genera enlaces de invitación frescos en el momento de la llamada.
    Cada enlace tiene 1 uso y caduca en INVITE_EXPIRY_HOURS horas.
    """
    canales = get_plan_channels(plan)
    enlaces = []
    for titulo, chat_id in canales:
        invite = await context.bot.create_chat_invite_link(
            chat_id=chat_id,
            name=f"{plan}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            member_limit=1,
            expire_date=now_utc() + timedelta(hours=INVITE_EXPIRY_HOURS),
        )
        enlaces.append((titulo, invite.invite_link))
        logger.info(f"Enlace generado para {titulo} — caduca en {INVITE_EXPIRY_HOURS}h")
    return enlaces


# ==============================
# DB — PENDING ACCESS
# ==============================

def registrar_acceso_pendiente(user_id: int, plan: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pending_access (telegram_user_id, plan, approved_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (telegram_user_id)
                DO UPDATE SET plan = EXCLUDED.plan, approved_at = NOW();
                """,
                (user_id, plan),
            )


def get_acceso_pendiente(user_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT telegram_user_id, plan FROM pending_access WHERE telegram_user_id = %s",
                (user_id,),
            )
            return cur.fetchone()


def borrar_acceso_pendiente(user_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pending_access WHERE telegram_user_id = %s",
                (user_id,),
            )


# ==============================
# DB — PENDING PAYMENTS
# ==============================

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


# ==============================
# DB — USERS / SUBSCRIPTIONS
# ==============================

def extend_subscription(user_id: int, username: str | None, full_name: str, plan: str):
    today = today_date()

    with get_conn() as conn:
        with conn.cursor() as cur:
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
                new_expiry = base_date + timedelta(days=PLAN_DAYS)
            else:
                new_expiry = today + timedelta(days=PLAN_DAYS)

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
            return cur.fetchone()


async def expulsar_de_canales(context: ContextTypes.DEFAULT_TYPE, user_id: int, plan: str) -> None:
    for _, chat_id in get_plan_channels(plan):
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            logger.info(f"Usuario {user_id} expulsado de {chat_id}")
        except Exception as e:
            logger.error(f"Error expulsando {user_id} de {chat_id}: {e}")


# ==============================
# MARKUPS
# ==============================

def menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ℹ️ Info", callback_data="info"),
            InlineKeyboardButton("📊 Stats", callback_data="stats"),
        ],
        [InlineKeyboardButton("💬 Contacto", url="https://t.me/erikenobi")],
        [InlineKeyboardButton("🆓 FREE", callback_data="free")],
        [
            InlineKeyboardButton("⚽ GOLES", callback_data="goles"),
            InlineKeyboardButton("⛳ CORNERS", callback_data="corners"),
        ],
        [InlineKeyboardButton("🔥 COMBO", callback_data="combo")],
    ])


def volver_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]]
    )


def pago_markup(plan: str) -> InlineKeyboardMarkup:
    precios = {"goles": "20", "corners": "20", "combo": "30"}
    stripes = {"goles": STRIPE_GOLES, "corners": STRIPE_CORNERS, "combo": STRIPE_COMBO}
    importe = precios.get(plan, "")

    keyboard = [
        [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=stripes.get(plan, ""))],
        [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/{importe}")],
        [InlineKeyboardButton("📲 Pagar con Bizum", callback_data=f"bizum:{plan}")],
        [InlineKeyboardButton("🟣 Pagar con Revolut", callback_data=f"revolut:{plan}")],
        [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def admin_approval_markup(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Aprobar GOLES",   callback_data=f"approve:goles:{user_id}"),
            InlineKeyboardButton("✅ Aprobar CORNERS", callback_data=f"approve:corners:{user_id}"),
        ],
        [InlineKeyboardButton("✅ Aprobar COMBO", callback_data=f"approve:combo:{user_id}")],
        [InlineKeyboardButton("❌ Rechazar",      callback_data=f"reject:{user_id}")],
    ])


def acceso_listo_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔑 Obtener mi acceso", callback_data="obtener_acceso")]]
    )


# ==============================
# USER FLOW
# ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user

    if user:
        acceso = get_acceso_pendiente(user.id)
        if acceso:
            await update.message.reply_text(
                "🎉 Tienes un acceso aprobado pendiente de recoger.\n"
                "Pulsa el botón para obtener tu enlace de entrada.",
                reply_markup=acceso_listo_markup(),
            )
            return

    texto = (
        "🔥 *Erikenobi Picks Premium*\n\n"
        "Alertas de fútbol centradas en *GOLES* y *CORNERS*, "
        "con opción de acceso combinado.\n\n"
        "Selecciona una opción:"
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


async def seleccionar_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    plan = query.data
    user = query.from_user

    # Registrar en pendientes cuando el usuario elige un plan de pago
    if plan in ("goles", "corners", "combo"):
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
                    (user.id, user.username, user.full_name, plan),
                )

    if plan == "menu":
        await query.edit_message_text(
            "🔥 *Erikenobi Picks Premium*\n\n"
            "Alertas de fútbol centradas en *GOLES* y *CORNERS*, "
            "con opción de acceso combinado.\n\n"
            "Selecciona una opción:",
            reply_markup=menu_markup(),
            parse_mode="Markdown",
        )
        return

    if plan == "info":
        await query.edit_message_text(
            "ℹ️ *Cómo funciona*\n\n"
            "Este servicio ofrece alertas basadas en análisis estadístico "
            "y seguimiento de partidos en tiempo real.\n\n"
            "⚽ *GOLES*\n"
            "Incluye alertas de gol en directo y también selecciones "
            "prepartido de over 2.5 goles.\n\n"
            "⛳ *CORNERS*\n"
            "Alertas especializadas en mercados de córners en vivo.\n\n"
            "🔥 *COMBO*\n"
            "Acceso completo a GOLES + CORNERS.\n\n"
            "💳 *Métodos de pago disponibles*\n"
            "Stripe · PayPal · Bizum · Revolut\n\n"
            "💳 El acceso premium se activa tras validar el pago.\n\n"
            "⚠️ *Aviso de responsabilidad*\n"
            "Este servicio es únicamente informativo. "
            "Cada usuario es responsable de sus propias decisiones.",
            reply_markup=volver_markup(),
            parse_mode="Markdown",
        )
        return

    # ── Stats reales ────────────────────────────────────────────────────
    if plan == "stats":
        stats = get_stats_reales()

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
            "🆓 *Canal FREE*\n\nAquí puedes acceder al canal gratuito.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Entrar al canal FREE", url=LINK_FREE)],
                [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
            ]),
            parse_mode="Markdown",
        )
        return

    # ── Plan GOLES ──────────────────────────────────────────────────────
    if plan == "goles":
        stats       = get_stats_reales()
        strike_real = _get_strike_tipo(stats, "gol")
        strike_txt  = f"*{strike_real}* (último mes)" if strike_real else "*+70% estimado*"

        await query.edit_message_text(
            f"⚽ *PLAN GOLES*\n\n"
            f"📈 Strike real: {strike_txt}\n"
            f"💰 Precio: *{PRECIO_GOLES}*\n\n"
            "Incluye:\n"
            "• Alertas de gol en directo\n"
            "• Selecciones prepartido over 2.5\n"
            "• Información estadística del partido\n\n"
            "Selecciona tu método de pago preferido:",
            reply_markup=pago_markup("goles"),
            parse_mode="Markdown",
        )
        return

    # ── Plan CORNERS ────────────────────────────────────────────────────
    if plan == "corners":
        stats       = get_stats_reales()
        strike_real = _get_strike_tipo(stats, "corner")
        strike_txt  = f"*{strike_real}* (último mes)" if strike_real else "*+80% estimado*"

        await query.edit_message_text(
            f"⛳ *PLAN CORNERS*\n\n"
            f"📈 Strike real: {strike_txt}\n"
            f"💰 Precio: *{PRECIO_CORNERS}*\n\n"
            "Incluye:\n"
            "• Alertas especializadas en córners\n"
            "• Datos de momentum y presión ofensiva\n"
            "• Estadísticas del partido en vivo\n\n"
            "Selecciona tu método de pago preferido:",
            reply_markup=pago_markup("corners"),
            parse_mode="Markdown",
        )
        return

    # ── Plan COMBO ──────────────────────────────────────────────────────
    if plan == "combo":
        stats         = get_stats_reales()
        strike_goles  = _get_strike_tipo(stats, "gol")
        strike_corner = _get_strike_tipo(stats, "corner")

        if strike_goles and strike_corner:
            strike_txt = f"⚽ {strike_goles} goles | 🚩 {strike_corner} corners (último mes)"
        elif strike_goles or strike_corner:
            strike_txt = f"*{strike_goles or strike_corner}* (último mes)"
        else:
            strike_txt = "*+75% estimado*"

        await query.edit_message_text(
            f"🔥 *PLAN COMBO*\n\n"
            f"📈 Strike real: {strike_txt}\n"
            f"💰 Precio: *{PRECIO_COMBO}*\n\n"
            "Incluye acceso completo a:\n"
            "⚽ GOLES\n"
            "⛳ CORNERS\n\n"
            "Perfecto para seguir ambos tipos de alertas y elegir "
            "según el volumen del día.\n\n"
            "Selecciona tu método de pago preferido:",
            reply_markup=pago_markup("combo"),
            parse_mode="Markdown",
        )
        return

    if plan.startswith("bizum:"):
        _, plan_real = plan.split(":", 1)
        await query.edit_message_text(
            f"📲 *Pago por Bizum*\n\n"
            f"Plan seleccionado: *{plan_real.upper()}*\n"
            f"Número Bizum: *{BIZUM}*\n\n"
            "Realiza el pago y envía el comprobante en este chat.\n"
            "Una vez validado recibirás el acceso automáticamente.",
            reply_markup=volver_markup(),
            parse_mode="Markdown",
        )
        return

    if plan.startswith("revolut:"):
        _, plan_real = plan.split(":", 1)
        importes = {"goles": PRECIO_GOLES, "corners": PRECIO_CORNERS, "combo": PRECIO_COMBO}
        importe  = importes.get(plan_real, "consultar")
        await query.edit_message_text(
            f"🟣 *Pago por Revolut*\n\n"
            f"Plan seleccionado: *{plan_real.upper()}*\n"
            f"Importe: *{importe}*\n\n"
            f"Enlace de pago:\n{REVOLUT_LINK}\n\n"
            "Realiza el pago y envía el comprobante en este chat.\n"
            "Una vez validado recibirás el acceso automáticamente.",
            reply_markup=volver_markup(),
            parse_mode="Markdown",
        )
        return

    if plan == "obtener_acceso":
        await callback_obtener_acceso(update, context)
        return


async def callback_obtener_acceso(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    user   = query.from_user
    acceso = get_acceso_pendiente(user.id)

    if not acceso:
        await query.edit_message_text(
            "No tienes ningún acceso pendiente. Usa /start para ver los planes."
        )
        return

    plan = acceso["plan"]

    try:
        enlaces = await generar_enlaces_acceso(context, plan)
    except Exception as e:
        logger.error(f"Error generando enlaces para {user.id}: {e}")
        await query.edit_message_text(
            "⚠️ Ha habido un error generando tu enlace. Por favor, contáctame: @erikenobi"
        )
        return

    texto = (
        "✅ *Acceso activado*\n\n"
        f"Plan: *{plan.upper()}*\n\n"
        "Aquí tienes tu enlace de acceso (válido durante 1 hora):\n\n"
    )
    for titulo, link in enlaces:
        texto += f"{titulo}\n{link}\n\n"

    texto += "⚠️ El enlace es de un solo uso. Úsalo cuanto antes."

    await query.edit_message_text(texto, parse_mode="Markdown")
    borrar_acceso_pendiente(user.id)
    logger.info(f"Acceso entregado a usuario {user.id} para plan {plan}")


async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user
    chat    = update.effective_chat

    if message is None or user is None or chat is None:
        return
    if chat.type != "private":
        return

    pending = get_pending_payment(user.id)

    if not pending:
        await message.reply_text(
            "Antes de enviar el comprobante, usa /start y selecciona un plan."
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

            if plan not in ("goles", "corners", "combo"):
                await query.edit_message_text("Plan no válido.")
                return

            pending = get_pending_payment(user_id_int)
            if not pending:
                await query.edit_message_text(
                    f"⚠️ El usuario {user_id_int} ya no está en pendientes."
                )
                return

            record = extend_subscription(
                user_id=user_id_int,
                username=pending["username"],
                full_name=pending["full_name"],
                plan=plan,
            )

            registrar_acceso_pendiente(user_id_int, plan)
            delete_pending_payment(user_id_int)

            try:
                await context.bot.send_message(
                    chat_id=user_id_int,
                    text=(
                        "✅ *Pago aprobado*\n\n"
                        f"Plan activo: *{plan.upper()}*\n"
                        f"Válido hasta: {record['fecha_fin']}\n\n"
                        "Pulsa el botón cuando estés listo para entrar al canal.\n"
                        "El enlace se generará en el momento y tendrá 1 hora de validez."
                    ),
                    reply_markup=acceso_listo_markup(),
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"Error avisando acceso a {user_id_int}: {e}")

            await query.edit_message_text(
                f"✅ Usuario {user_id_int} aprobado para {plan.upper()}.\n"
                f"Activo hasta {record['fecha_fin']}.\n"
                "El usuario recibirá el enlace cuando pulse 'Obtener mi acceso'."
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

            delete_pending_payment(user_id_int)
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
    if plan not in ("goles", "corners", "combo"):
        await update.message.reply_text("Plan no válido. Usa: goles, corners o combo")
        return

    pending = get_pending_payment(target_user_id)
    if not pending:
        await update.message.reply_text("Ese usuario no está en pendientes.")
        return

    record = extend_subscription(
        user_id=target_user_id,
        username=pending["username"],
        full_name=pending["full_name"],
        plan=plan,
    )

    registrar_acceso_pendiente(target_user_id, plan)
    delete_pending_payment(target_user_id)

    try:
        await context.bot.send_message(
            chat_id=target_user_id,
            text=(
                "✅ *Pago aprobado*\n\n"
                f"Plan activo: *{plan.upper()}*\n"
                f"Válido hasta: {record['fecha_fin']}\n\n"
                "Pulsa el botón cuando estés listo para entrar al canal.\n"
                "El enlace se generará en el momento y tendrá 1 hora de validez."
            ),
            reply_markup=acceso_listo_markup(),
            parse_mode="Markdown",
        )
        await update.message.reply_text(
            f"Usuario {target_user_id} aprobado para {plan} hasta {record['fecha_fin']}."
        )
    except Exception as e:
        await update.message.reply_text(f"Error avisando al usuario: {e}")


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

    delete_pending_payment(target_user_id)
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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, fecha_inicio, fecha_fin, estado
                FROM users WHERE telegram_user_id = %s
                """,
                (user_id,),
            )
            record = cur.fetchone()

    if not record:
        await update.message.reply_text("Ese usuario no tiene suscripción registrada.")
        return

    await update.message.reply_text(
        f"Usuario: {record['telegram_user_id']}\n"
        f"Plan: {record['plan']}\n"
        f"Inicio: {record['fecha_inicio']}\n"
        f"Fin: {record['fecha_fin']}\n"
        f"Estado: {record['estado']}"
    )


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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, estado, fecha_fin
                FROM users ORDER BY fecha_fin ASC
                """
            )
            rows = cur.fetchall()

    if not rows:
        await update.message.reply_text("No hay usuarios guardados.")
        return

    lineas = ["📋 Usuarios activos/guardados:\n"]
    for row in rows:
        lineas.append(
            f"{row['telegram_user_id']} | {row['plan']} | {row['estado']} | hasta {row['fecha_fin']}"
        )
    await update.message.reply_text("\n".join(lineas)[:4000])


async def pendientes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _check_admin(update):
        await update.message.reply_text("No tienes permisos.")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, username, full_name, plan, created_at
                FROM pending_payments ORDER BY created_at DESC
                """
            )
            rows = cur.fetchall()

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
            rows = cur.fetchall()

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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, fecha_fin
                FROM users WHERE estado = 'activo'
                ORDER BY fecha_fin ASC
                """
            )
            rows = cur.fetchall()

    if not rows:
        await update.message.reply_text("No hay usuarios activos.")
        return

    texto = "✅ Usuarios activos:\n\n"
    for row in rows:
        texto += f"{row['telegram_user_id']} | {row['plan']} | hasta {row['fecha_fin']}\n"
    await update.message.reply_text(texto[:4000])


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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT plan FROM users WHERE telegram_user_id = %s",
                (target_user_id,),
            )
            record = cur.fetchone()

    if not record:
        await update.message.reply_text("Ese usuario no está registrado.")
        return

    await expulsar_de_canales(context, target_user_id, record["plan"])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET estado = 'caducado', updated_at = NOW() WHERE telegram_user_id = %s",
                (target_user_id,),
            )

    await update.message.reply_text(
        f"Usuario {target_user_id} expulsado y marcado como caducado."
    )
    logger.info(f"Admin expulsó manualmente al usuario {target_user_id}")


# ==============================
# JOB — EXPIRACIÓN AUTOMÁTICA
# ==============================

async def check_expirations(context: ContextTypes.DEFAULT_TYPE) -> None:
    today = today_date()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, fecha_fin, estado
                FROM users
                WHERE estado = 'activo'
                  AND fecha_fin <= %s
                """,
                (today + timedelta(days=3),),
            )
            rows = cur.fetchall()

    for record in rows:
        try:
            user_id  = record["telegram_user_id"]
            end_date = record["fecha_fin"]
            if isinstance(end_date, str):
                end_date = parse_date(end_date)

            days_left = (end_date - today).days

            if days_left == 3:
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"⏳ Tu suscripción {record['plan'].upper()} caduca en 3 días "
                        f"({end_date}).\n"
                        "Cuando renueves, envíame el comprobante aquí."
                    ),
                )

            elif days_left == 0:
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"⚠️ Tu suscripción {record['plan'].upper()} caduca hoy "
                        f"({end_date}).\n"
                        "Si quieres renovar, envíame el comprobante aquí."
                    ),
                )

            elif days_left < 0:
                await expulsar_de_canales(context, int(user_id), record["plan"])

                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE users SET estado = 'caducado', updated_at = NOW() WHERE telegram_user_id = %s",
                            (user_id,),
                        )

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"❌ Tu suscripción {record['plan'].upper()} ha caducado.\n"
                        "Se ha retirado tu acceso a los canales premium.\n"
                        "Si quieres volver a activarla, envía de nuevo el comprobante."
                    ),
                )
                logger.info(f"Suscripción caducada y usuario expulsado: {user_id}")

        except Exception as e:
            logger.error(f"Error revisando expiración de {record.get('telegram_user_id')}: {e}")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Error capturado: {context.error}", exc_info=context.error)


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

    init_db()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start",         start))
    app.add_handler(CommandHandler("help",          help_command))
    app.add_handler(CommandHandler("whoami",        whoami))
    app.add_handler(CommandHandler("aprobar",       aprobar))
    app.add_handler(CommandHandler("rechazar",      rechazar))
    app.add_handler(CommandHandler("estado",        estado))
    app.add_handler(CommandHandler("debugpremium",  debug_premium))
    app.add_handler(CommandHandler("listar",        listar))
    app.add_handler(CommandHandler("pendientes",    pendientes))
    app.add_handler(CommandHandler("caducan",       caducan))
    app.add_handler(CommandHandler("activos",       activos))
    app.add_handler(CommandHandler("expulsar",      expulsar))

    app.add_handler(CallbackQueryHandler(admin_action_callback, pattern=r"^(approve:|reject:)"))
    app.add_handler(CallbackQueryHandler(seleccionar_plan))

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

    logger.info(
        "Bot premium iniciado. BIZUM=%s | deployment=%s",
        BIZUM,
        DEPLOYMENT_COMMIT,
    )
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
