import os
from datetime import datetime, timedelta, timezone

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
# CONFIG
# ==============================

TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# PON TU USER ID REAL
ADMIN_IDS = [9330181]

CANAL_CORNERS_ID = -1003895151594
CANAL_GOLES_ID = -1003818905455

LINK_FREE = "https://t.me/+WhIkP2PstS1kMDVk"

PRECIO_GOLES = "20€"
PRECIO_CORNERS = "20€"
PRECIO_COMBO = "30€"

BIZUM = "688946111"

# STRIPE
STRIPE_GOLES = "https://buy.stripe.com/aFa8wObuQ9MbdgA00x08g01"
STRIPE_CORNERS = "https://buy.stripe.com/bJe3cugPaf6vdgA5kR08g02"
STRIPE_COMBO = "https://buy.stripe.com/4gM7sK8iE0bBgsMfZv08g03"

# PAYPAL
PAYPAL_LINK = "https://paypal.me/erikenobi"

# REVOLUT
REVOLUT_LINK = "https://revolut.me/ericblasco9"

PLAN_DAYS = 30
INVITE_EXPIRY_HOURS = 1
CHECK_EXPIRATIONS_EVERY_SECONDS = 43200  # 12h


# ==============================
# DB
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
                    username TEXT,
                    full_name TEXT,
                    plan TEXT NOT NULL,
                    fecha_inicio DATE NOT NULL,
                    fecha_fin DATE NOT NULL,
                    estado TEXT NOT NULL DEFAULT 'activo',
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_payments (
                    telegram_user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    plan TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )


# ==============================
# UTILS
# ==============================

def today_date():
    return datetime.now().date()


def now_utc():
    return datetime.now(timezone.utc)


def parse_date(date_str: str):
    return datetime.strptime(str(date_str), "%Y-%m-%d").date()


def get_plan_channels(plan: str):
    if plan == "goles":
        return [("⚽ GOLES", CANAL_GOLES_ID)]
    if plan == "corners":
        return [("⛳ CORNERS", CANAL_CORNERS_ID)]
    if plan == "combo":
        return [
            ("⚽ GOLES", CANAL_GOLES_ID),
            ("⛳ CORNERS", CANAL_CORNERS_ID),
        ]
    return []


async def get_plan_links(context: ContextTypes.DEFAULT_TYPE, plan: str):
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

    return enlaces


def menu_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("ℹ️ Info", callback_data="info"),
            InlineKeyboardButton("📊 Stats", callback_data="stats"),
        ],
        [InlineKeyboardButton("💬 Contacto", url="https://t.me/erikenobi")],
        [InlineKeyboardButton("🆓 FREE", callback_data="free")],
        [
            InlineKeyboardButton("⚽ GOLES | +70%", callback_data="goles"),
            InlineKeyboardButton("⛳ CORNERS | +80%", callback_data="corners"),
        ],
        [InlineKeyboardButton("🔥 COMBO | +75%", callback_data="combo")],
    ]
    return InlineKeyboardMarkup(keyboard)


def volver_markup():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]]
    )


def pago_markup(plan: str) -> InlineKeyboardMarkup:

    if plan == "goles":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_GOLES)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/20")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:goles")],
            [InlineKeyboardButton("🟣 Pagar con Revolut", callback_data="revolut:goles")],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
        ]

    elif plan == "corners":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_CORNERS)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/20")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:corners")],
            [InlineKeyboardButton("🟣 Pagar con Revolut", callback_data="revolut:corners")],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
        ]

    elif plan == "combo":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_COMBO)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/30")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:combo")],
            [InlineKeyboardButton("🟣 Pagar con Revolut", callback_data="revolut:combo")],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
        ]
    else:
        keyboard = [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]]

    return InlineKeyboardMarkup(keyboard)


def admin_approval_markup(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("✅ Aprobar GOLES", callback_data=f"approve:goles:{user_id}"),
            InlineKeyboardButton("✅ Aprobar CORNERS", callback_data=f"approve:corners:{user_id}"),
        ],
        [
            InlineKeyboardButton("✅ Aprobar COMBO", callback_data=f"approve:combo:{user_id}"),
        ],
        [
            InlineKeyboardButton("❌ Rechazar", callback_data=f"reject:{user_id}")
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def upsert_pending_payment(user_id: int, username: str | None, full_name: str, plan: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pending_payments (telegram_user_id, username, full_name, plan, created_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (telegram_user_id)
                DO UPDATE SET
                    username = EXCLUDED.username,
                    full_name = EXCLUDED.full_name,
                    plan = EXCLUDED.plan,
                    created_at = NOW();
                """,
                (user_id, username, full_name, plan),
            )


def get_pending_payment(user_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, username, full_name, plan, created_at
                FROM pending_payments
                WHERE telegram_user_id = %s
                """,
                (user_id,),
            )
            return cur.fetchone()


def delete_pending_payment(user_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM pending_payments
                WHERE telegram_user_id = %s
                """,
                (user_id,),
            )


def extend_subscription(user_id: int, username: str | None, full_name: str, plan: str):
    today = today_date()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fecha_fin
                FROM users
                WHERE telegram_user_id = %s
                """,
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
                    username = EXCLUDED.username,
                    full_name = EXCLUDED.full_name,
                    plan = EXCLUDED.plan,
                    fecha_inicio = EXCLUDED.fecha_inicio,
                    fecha_fin = EXCLUDED.fecha_fin,
                    estado = 'activo',
                    updated_at = NOW()
                RETURNING telegram_user_id, username, full_name, plan, fecha_inicio, fecha_fin, estado
                """,
                (user_id, username, full_name, plan, today, new_expiry),
            )
            return cur.fetchone()


async def expulsar_de_canales(context: ContextTypes.DEFAULT_TYPE, user_id: int, plan: str):
    canales = get_plan_channels(plan)

    for _, chat_id in canales:
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            print(f"✅ Usuario {user_id} expulsado de {chat_id}")
        except Exception as e:
            print(f"❌ Error expulsando {user_id} de {chat_id}: {e}")


# ==============================
# USER FLOW
# ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Usa /start para ver los planes.\n"
        "Si ya has pagado, envía el comprobante en este chat privado."
    )


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    username = f"@{user.username}" if user.username else "(sin username)"
    await update.message.reply_text(
        f"Tu user_id es: {user.id}\nUsername: {username}"
    )


async def seleccionar_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    await query.answer()

    plan = query.data
    user = query.from_user

    if plan in ("goles", "corners", "combo"):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pending_payments (telegram_user_id, username, full_name, plan, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (telegram_user_id)
                    DO UPDATE SET
                        username = EXCLUDED.username,
                        full_name = EXCLUDED.full_name,
                        plan = EXCLUDED.plan,
                        created_at = NOW();
                """, (user.id, user.username, user.full_name, plan))

    if plan == "menu":
        texto = (
            "🔥 *Erikenobi Picks Premium*\n\n"
            "Alertas de fútbol centradas en *GOLES* y *CORNERS*, "
            "con opción de acceso combinado.\n\n"
            "Selecciona una opción:"
        )

        await query.edit_message_text(
            texto,
            reply_markup=menu_markup(),
            parse_mode="Markdown"
        )
        return

    elif plan == "info":

        texto = (
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
            "Cada usuario es responsable de sus propias decisiones."
        )

        await query.edit_message_text(
            texto,
            reply_markup=volver_markup(),
            parse_mode="Markdown"
        )
        return

    elif plan == "stats":

        texto = (
            "📊 *Rendimiento estimado del servicio*\n\n"

            "⚽ *GOLES*\n"
            "Acierto estimado actual: *+70%*\n"
            "Incluye alertas de gol en directo y prepartido over 2.5.\n\n"

            "⛳ *CORNERS*\n"
            "Acierto estimado actual: *+80%*\n"
            "Alertas en vivo basadas en estadísticas y momentum.\n\n"

            "🔥 *COMBO*\n"
            "Rendimiento estimado combinado: *+75%*\n"
            "Acceso completo a GOLES + CORNERS.\n\n"

            "⚠️ *Aviso importante*\n"
            "Estos porcentajes son orientativos y pueden variar según el volumen "
            "de alertas, el momento de la temporada y las condiciones del mercado.\n\n"

            "Este servicio es únicamente informativo. Cada usuario es responsable "
            "de sus propias decisiones."
        )

        await query.edit_message_text(
            texto,
            reply_markup=volver_markup(),
            parse_mode="Markdown"
        )
        return

    elif plan == "free":
        keyboard = [
            [InlineKeyboardButton("Entrar al canal FREE", url=LINK_FREE)],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]
        ]

        await query.edit_message_text(
            "🆓 *Canal FREE*\n\nAquí puedes acceder al canal gratuito.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return

    elif plan == "goles":

        texto = (
            f"⚽ *PLAN GOLES | +70% estimado*\n\n"
            f"Precio: *{PRECIO_GOLES}*\n\n"
            "Incluye:\n"
            "• Alertas de gol en directo\n"
            "• Selecciones prepartido over 2.5\n"
            "• Información estadística del partido\n\n"
            "Selecciona tu método de pago preferido:"
        )

        await query.edit_message_text(
            texto,
            reply_markup=pago_markup("goles"),
            parse_mode="Markdown"
        )
        return

    elif plan == "corners":

        texto = (
            f"⛳ *PLAN CORNERS | +80% estimado*\n\n"
            f"Precio: *{PRECIO_CORNERS}*\n\n"
            "Incluye:\n"
            "• Alertas especializadas en córners\n"
            "• Datos de momentum y presión ofensiva\n"
            "• Estadísticas del partido en vivo\n\n"
            "Selecciona tu método de pago preferido:"
        )

        await query.edit_message_text(
            texto,
            reply_markup=pago_markup("corners"),
            parse_mode="Markdown"
        )
        return

    elif plan == "combo":

        texto = (
            f"🔥 *PLAN COMBO | +75% estimado*\n\n"
            f"Precio: *{PRECIO_COMBO}*\n\n"
            "Incluye acceso completo a:\n"
            "⚽ GOLES\n"
            "⛳ CORNERS\n\n"
            "Perfecto para poder seguir ambos tipos de alertas y elegir "
            "qué consumir según el volumen del día.\n\n"
            "Selecciona tu método de pago preferido:"
        )

        await query.edit_message_text(
            texto,
            reply_markup=pago_markup("combo"),
            parse_mode="Markdown"
        )
        return

    elif plan.startswith("bizum:"):

        _, plan_real = plan.split(":")

        texto = (
            f"📲 *Pago por Bizum*\n\n"
            f"Plan seleccionado: *{plan_real.upper()}*\n"
            f"Número Bizum: *{BIZUM}*\n\n"
            "Realiza el pago y envía el comprobante en este chat.\n"
            "Una vez validado recibirás el acceso automáticamente."
        )

        await query.edit_message_text(
            texto,
            reply_markup=volver_markup(),
            parse_mode="Markdown"
        )
        return

    elif plan.startswith("revolut:"):

        _, plan_real = plan.split(":")

        if plan_real == "goles":
            importe = PRECIO_GOLES
        elif plan_real == "corners":
            importe = PRECIO_CORNERS
        elif plan_real == "combo":
            importe = PRECIO_COMBO
        else:
            importe = "consultar"

        texto = (
            f"🟣 *Pago por Revolut*\n\n"
            f"Plan seleccionado: *{plan_real.upper()}*\n"
            f"Importe: *{importe}*\n\n"
            f"Enlace de pago:\n{REVOLUT_LINK}\n\n"
            "Realiza el pago y envía el comprobante en este chat.\n"
            "Una vez validado recibirás el acceso automáticamente."
        )

        await query.edit_message_text(
            texto,
            reply_markup=volver_markup(),
            parse_mode="Markdown"
        )
        return


async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat

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

    plan = pending["plan"]
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
        except Exception as e:
            print(f"Error avisando al admin {admin_id}: {e}")


# ==============================
# ADMIN FLOW
# ==============================

async def admin_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

            links = await get_plan_links(context, plan)

            texto_usuario = (
                "✅ Pago aprobado.\n\n"
                f"Plan activo: {plan.upper()}\n"
                f"Válido hasta: {record['fecha_fin']}\n\n"
                "Aquí tienes tu acceso:\n\n"
            )

            for title, link in links:
                texto_usuario += f"{title}\n{link}\n\n"

            try:
                await context.bot.send_message(chat_id=user_id_int, text=texto_usuario)
            except Exception as e:
                print(f"Error enviando acceso a {user_id_int}: {e}")

            delete_pending_payment(user_id_int)

            await query.edit_message_text(
                f"✅ Usuario {user_id_int} aprobado para {plan.upper()}.\n"
                f"Activo hasta {record['fecha_fin']}."
            )
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
                print(f"Error avisando rechazo a {user_id_int}: {e}")

            delete_pending_payment(user_id_int)

            await query.edit_message_text(f"❌ Usuario {user_id_int} rechazado.")
            return

    except Exception as e:
        print(f"Error admin_action_callback: {e}")
        await query.edit_message_text("⚠️ Ha ocurrido un error procesando la acción.")


async def aprobar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
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
    links = await get_plan_links(context, plan)

    texto = (
        "✅ Pago aprobado.\n\n"
        f"Plan activo: {plan.upper()}\n"
        f"Válido hasta: {record['fecha_fin']}\n\n"
        "Aquí tienes tu acceso:\n\n"
    )

    for title, link in links:
        texto += f"{title}\n{link}\n\n"

    try:
        await context.bot.send_message(chat_id=target_user_id, text=texto)
        delete_pending_payment(target_user_id)
        await update.message.reply_text(
            f"Usuario {target_user_id} aprobado/renovado para {plan} hasta {record['fecha_fin']}."
        )
    except Exception as e:
        await update.message.reply_text(f"Error enviando acceso: {e}")


async def rechazar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
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
        print(f"Error avisando rechazo a {target_user_id}: {e}")

    delete_pending_payment(target_user_id)
    await update.message.reply_text(f"Usuario {target_user_id} rechazado.")


async def estado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
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
                FROM users
                WHERE telegram_user_id = %s
                """,
                (user_id,),
            )
            record = cur.fetchone()

    if not record:
        await update.message.reply_text("Ese usuario no tiene suscripción activa.")
        return

    await update.message.reply_text(
        f"Usuario: {record['telegram_user_id']}\n"
        f"Plan: {record['plan']}\n"
        f"Inicio: {record['fecha_inicio']}\n"
        f"Fin: {record['fecha_fin']}\n"
        f"Estado: {record['estado']}"
    )


async def listar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, estado, fecha_fin
                FROM users
                ORDER BY fecha_fin ASC
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


async def pendientes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos.")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, username, full_name, plan, created_at
                FROM pending_payments
                ORDER BY created_at DESC
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


async def caducan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos.")
        return

    today = today_date()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, fecha_fin, estado
                FROM users
                WHERE estado = 'activo'
                ORDER BY fecha_fin ASC
                """
            )
            rows = cur.fetchall()

    lineas = ["⏳ Próximas caducidades:\n"]
    encontrados = 0

    for row in rows:
        end_date = row["fecha_fin"]
        if isinstance(end_date, str):
            end_date = parse_date(end_date)
        days_left = (end_date - today).days
        if 0 <= days_left <= 7:
            lineas.append(
                f"{row['telegram_user_id']} | {row['plan']} | {end_date} | faltan {days_left} días"
            )
            encontrados += 1

    if encontrados == 0:
        await update.message.reply_text("No hay caducidades en los próximos 7 días.")
        return

    await update.message.reply_text("\n".join(lineas)[:4000])


async def activos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos.")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, fecha_fin
                FROM users
                WHERE estado = 'activo'
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


async def expulsar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
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
                """
                SELECT plan
                FROM users
                WHERE telegram_user_id = %s
                """,
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
                """
                UPDATE users
                SET estado = 'caducado', updated_at = NOW()
                WHERE telegram_user_id = %s
                """,
                (target_user_id,),
            )

    await update.message.reply_text(
        f"Usuario {target_user_id} expulsado y marcado como caducado."
    )


# ==============================
# JOBS
# ==============================

async def check_expirations(context: ContextTypes.DEFAULT_TYPE):
    today = today_date()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT telegram_user_id, plan, fecha_fin, estado
                FROM users
                """
            )
            rows = cur.fetchall()

    for record in rows:
        try:
            user_id = record["telegram_user_id"]
            end_date = record["fecha_fin"]
            if isinstance(end_date, str):
                end_date = parse_date(end_date)

            days_left = (end_date - today).days

            if days_left == 3 and record["estado"] == "activo":
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"⏳ Tu suscripción {record['plan'].upper()} caduca en 3 días "
                        f"({end_date}).\n"
                        "Cuando renueves, envíame el comprobante aquí."
                    ),
                )

            elif days_left == 0 and record["estado"] == "activo":
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"⚠️ Tu suscripción {record['plan'].upper()} caduca hoy "
                        f"({end_date}).\n"
                        "Si quieres renovar, envíame el comprobante aquí."
                    ),
                )

            elif days_left < 0 and record["estado"] != "caducado":
                await expulsar_de_canales(context, int(user_id), record["plan"])

                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE users
                            SET estado = 'caducado', updated_at = NOW()
                            WHERE telegram_user_id = %s
                            """,
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

        except Exception as e:
            print(f"Error revisando expiración de {record.get('telegram_user_id')}: {e}")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"⚠️ Error capturado: {context.error}")


# ==============================
# MAIN
# ==============================

def main():
    if not TOKEN:
        raise ValueError("Falta BOT_TOKEN en variables de entorno.")
    if not DATABASE_URL:
        raise ValueError("Falta DATABASE_URL en variables de entorno.")

    init_db()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("aprobar", aprobar))
    app.add_handler(CommandHandler("rechazar", rechazar))
    app.add_handler(CommandHandler("estado", estado))
    app.add_handler(CommandHandler("listar", listar))
    app.add_handler(CommandHandler("pendientes", pendientes))
    app.add_handler(CommandHandler("caducan", caducan))
    app.add_handler(CommandHandler("activos", activos))
    app.add_handler(CommandHandler("expulsar", expulsar))

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

    print("Bot premium funcionando...")
    app.run_polling()


if __name__ == "__main__":
    main()
