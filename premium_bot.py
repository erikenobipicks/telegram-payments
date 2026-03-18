import os
import json
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

TOKEN = "TOKEN"

ADMIN_IDS = [9330181]

CANAL_CORNERS_ID = -1003895151594
CANAL_GOLES_ID = -1003818905455

LINK_FREE = "https://t.me/+WhIkP2PstS1kMDVk"

PRECIO_GOLES = "20€"
PRECIO_CORNERS = "20€"
PRECIO_COMBO = "30€"

BIZUM = "660426660"

# STRIPE
STRIPE_GOLES = "https://buy.stripe.com/aFa8wObuQ9MbdgA00x08g01"
STRIPE_CORNERS = "https://buy.stripe.com/bJe3cugPaf6vdgA5kR08g02"
STRIPE_COMBO = "https://buy.stripe.com/4gM7sK8iE0bBgsMfZv08g03"

# PAYPAL
PAYPAL_LINK = "https://paypal.me/erikenobi"

DATA_FILE = "premium_users.json"
PENDING_FILE = "premium_pending.json"

PLAN_DAYS = 30


def pago_markup(plan: str):

    if plan == "goles":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_GOLES)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/20")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:goles")],
            [InlineKeyboardButton("⬅️ Volver", callback_data="menu")]
        ]

    elif plan == "corners":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_CORNERS)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/20")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:corners")],
            [InlineKeyboardButton("⬅️ Volver", callback_data="menu")]
        ]

    elif plan == "combo":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_COMBO)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/30")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:combo")],
            [InlineKeyboardButton("⬅️ Volver", callback_data="menu")]
        ]

    return InlineKeyboardMarkup(keyboard)


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        print(f"Error cargando {path}: {e}")
        return default


def save_json(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error guardando {path}: {e}")


USERS_DB = load_json(DATA_FILE, {})
PENDING_DB = load_json(PENDING_FILE, {})


def save_all():
    save_json(DATA_FILE, USERS_DB)
    save_json(PENDING_FILE, PENDING_DB)


def menu_markup() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("ℹ️ Info", callback_data="info"),
            InlineKeyboardButton("📊 Stats", callback_data="stats"),
        ],
        [
            InlineKeyboardButton("💬 Contacto", url="https://t.me/erikenobi")
        ],
        [
            InlineKeyboardButton("🆓 FREE", callback_data="free")
        ],
        [
            InlineKeyboardButton("⚽ GOLES | +70%", callback_data="goles"),
            InlineKeyboardButton("⛳ CORNERS | +80%", callback_data="corners"),
        ],
        [
            InlineKeyboardButton("🔥 COMBO | +75%", callback_data="combo")
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def pago_markup(plan: str) -> InlineKeyboardMarkup:

    if plan == "goles":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_GOLES)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/20")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:goles")],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
        ]

    elif plan == "corners":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_CORNERS)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/20")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:corners")],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
        ]

    elif plan == "combo":
        keyboard = [
            [InlineKeyboardButton("💳 Pagar con tarjeta (Stripe)", url=STRIPE_COMBO)],
            [InlineKeyboardButton("🅿️ Pagar con PayPal", url=f"{PAYPAL_LINK}/30")],
            [InlineKeyboardButton("📲 Pagar con Bizum", callback_data="bizum:combo")],
            [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
        ]
    else:
        keyboard = [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]]

    return InlineKeyboardMarkup(keyboard)


def volver_markup():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]
    ])


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")

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
            expire_date=datetime.now() + timedelta(hours=1)
        )
        enlaces.append((titulo, invite.invite_link))

    return enlaces


def extend_subscription(user_id: str, plan: str):
    today = datetime.now().date()
    record = USERS_DB.get(user_id)

    if record:
        old_expiry = parse_date(record["fecha_fin"]).date()
        base_date = old_expiry if old_expiry >= today else today
        new_expiry = base_date + timedelta(days=PLAN_DAYS)
    else:
        new_expiry = today + timedelta(days=PLAN_DAYS)

    USERS_DB[user_id] = {
        "plan": plan,
        "fecha_inicio": today_str(),
        "fecha_fin": new_expiry.strftime("%Y-%m-%d"),
        "estado": "activo",
        "updated_at": now_str(),
    }
    save_all()
    return USERS_DB[user_id]


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
        parse_mode="Markdown"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Usa /start para ver los planes.\n"
        "Si ya has pagado, envía el comprobante aquí."
    )


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
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
        PENDING_DB[str(user.id)] = {
            "plan": plan,
            "username": user.username,
            "name": user.full_name,
            "created_at": now_str(),
        }
        save_all()

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
            "Estos porcentajes son orientativos y pueden variar según el volumen de alertas, "
            "el momento de la temporada y las condiciones del mercado.\n\n"

            "Este servicio es únicamente informativo. Cada usuario es responsable "
            "de sus propias decisiones."
        )

        await query.edit_message_text(
            texto,
            reply_markup=volver_markup(),
            parse_mode="Markdown"
        )

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

    elif plan == "goles":

        texto = (
            f"⚽ *PLAN GOLES | +70% estimado*\n\n"
            f"Precio: *{PRECIO_GOLES}*\n\n"
            "Incluye:\n"
            "• Alertas de gol en directo\n"
            "• Selecciones prepartido over 2.5\n"
            "• Información estadística del partido\n\n"
            "Selecciona método de pago:"
        )

        await query.edit_message_text(
            texto,
            reply_markup=pago_markup("goles"),
            parse_mode="Markdown"
        )

    elif plan == "corners":

        texto = (
            f"⛳ *PLAN CORNERS | +80% estimado*\n\n"
            f"Precio: *{PRECIO_CORNERS}*\n\n"
            "Incluye:\n"
            "• Alertas especializadas en córners\n"
            "• Datos de momentum y presión ofensiva\n"
            "• Estadísticas del partido en vivo\n\n"
            "Selecciona método de pago:"
        )

        await query.edit_message_text(
            texto,
            reply_markup=pago_markup("corners"),
            parse_mode="Markdown"
        )

    elif plan == "combo":

        texto = (
            f"🔥 *PLAN COMBO | +75% estimado*\n\n"
            f"Precio: *{PRECIO_COMBO}*\n\n"
            "Incluye acceso completo a:\n"
            "⚽ GOLES\n"
            "⛳ CORNERS\n\n"
            "Perfecto para poder seguir ambos tipos de alertas y elegir "
            "qué consumir según el volumen del día.\n\n"
            "Selecciona método de pago:"
        )

        await query.edit_message_text(
            texto,
            reply_markup=pago_markup("combo"),
            parse_mode="Markdown"
        )

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


async def recibir_comprobante(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)

    if user_id not in PENDING_DB:
        await update.message.reply_text(
            "Antes de enviar el comprobante, usa /start y selecciona un plan."
        )
        return

    plan = PENDING_DB[user_id]["plan"]
    username = f"@{user.username}" if user.username else "(sin username)"

    await update.message.reply_text(
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
                reply_markup=admin_approval_markup(user.id)
            )
        except Exception as e:
            print(f"Error avisando al admin {admin_id}: {e}")


async def admin_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    await query.answer()

    admin = query.from_user

    if admin.id not in ADMIN_IDS:
        await query.edit_message_text("No tienes permisos para esta acción.")
        return

    data = query.data

    try:

        # -------------------------
        # APROBAR PAGO
        # -------------------------

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

            if str(user_id_int) not in PENDING_DB:
                await query.edit_message_text(
                    f"⚠️ El usuario {user_id_int} ya no está en pendientes."
                )
                return

            # renovar o crear suscripción
            record = extend_subscription(str(user_id_int), plan)

            # obtener enlaces únicos del plan
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
                await context.bot.send_message(
                    chat_id=user_id_int,
                    text=texto_usuario
                )
            except Exception as e:
                print(f"Error enviando acceso a {user_id_int}: {e}")

            # eliminar pendiente
            PENDING_DB.pop(str(user_id_int), None)

            save_all()

            await query.edit_message_text(
                f"✅ Usuario {user_id_int} aprobado para {plan.upper()}.\n"
                f"Activo hasta {record['fecha_fin']}."
            )

        # -------------------------
        # RECHAZAR PAGO
        # -------------------------

        elif data.startswith("reject:"):

            parts = data.split(":")
            if len(parts) != 2:
                await query.edit_message_text("Error en datos del botón.")
                return

            _, user_id = parts
            user_id_int = int(user_id)

            try:
                await context.bot.send_message(
                    chat_id=user_id_int,
                    text="❌ No he podido validar el pago. Escríbeme si quieres revisarlo."
                )
            except Exception as e:
                print(f"Error avisando rechazo a {user_id_int}: {e}")

            PENDING_DB.pop(str(user_id_int), None)

            save_all()

            await query.edit_message_text(
                f"❌ Usuario {user_id_int} rechazado."
            )

    except Exception as e:

        print(f"Error admin_action_callback: {e}")

        await query.edit_message_text(
            "⚠️ Ha ocurrido un error procesando la acción."
        )


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

    record = extend_subscription(str(target_user_id), plan)
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
        await update.message.reply_text(
            f"Usuario {target_user_id} aprobado/renovado para {plan} hasta {record['fecha_fin']}."
        )
        PENDING_DB.pop(str(target_user_id), None)
        save_all()
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
            text="No he podido validar el pago. Escríbeme de nuevo si quieres revisarlo."
        )
    except Exception as e:
        print(f"Error avisando rechazo a {target_user_id}: {e}")

    PENDING_DB.pop(str(target_user_id), None)
    save_all()
    await update.message.reply_text(f"Usuario {target_user_id} rechazado.")


async def estado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin = update.effective_user

    if admin.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Uso correcto: /estado user_id")
        return

    user_id = context.args[0]
    record = USERS_DB.get(user_id)

    if not record:
        await update.message.reply_text("Ese usuario no tiene suscripción activa.")
        return

    await update.message.reply_text(
        f"Usuario: {user_id}\n"
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

    if not USERS_DB:
        await update.message.reply_text("No hay usuarios guardados.")
        return

    lineas = ["📋 Usuarios activos/guardados:\n"]
    for user_id, record in USERS_DB.items():
        lineas.append(
            f"{user_id} | {record['plan']} | {record['estado']} | hasta {record['fecha_fin']}"
        )

    texto = "\n".join(lineas)
    await update.message.reply_text(texto[:4000])


async def check_expirations(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now().date()

    for user_id, record in list(USERS_DB.items()):
        try:
            end_date = parse_date(record["fecha_fin"]).date()
            days_left = (end_date - today).days

            # 3 días antes
            if days_left == 3 and record["estado"] == "activo":
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"⏳ Tu suscripción {record['plan'].upper()} caduca en 3 días "
                        f"({record['fecha_fin']}).\n"
                        "Cuando renueves, envíame el comprobante aquí."
                    ),
                )

            # último día
            elif days_left == 0 and record["estado"] == "activo":
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"⚠️ Tu suscripción {record['plan'].upper()} caduca hoy "
                        f"({record['fecha_fin']}).\n"
                        "Si quieres renovar, envíame el comprobante aquí."
                    ),
                )

            # caducado
            elif days_left < 0 and record["estado"] != "caducado":
                record["estado"] = "caducado"
                record["updated_at"] = now_str()

                await expulsar_de_canales(context, int(user_id), record["plan"])

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        f"❌ Tu suscripción {record['plan'].upper()} ha caducado.\n"
                        "Se ha retirado tu acceso a los canales premium.\n"
                        "Si quieres volver a activarla, envía de nuevo el comprobante."
                    ),
                )

        except Exception as e:
            print(f"Error revisando expiración de {user_id}: {e}")

    save_all()


async def expulsar_de_canales(context: ContextTypes.DEFAULT_TYPE, user_id: int, plan: str):
    canales = get_plan_channels(plan)

    for _, chat_id in canales:
        try:
            await context.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
            await context.bot.unban_chat_member(chat_id=chat_id, user_id=user_id)
            print(f"✅ Usuario {user_id} expulsado de {chat_id}")
        except Exception as e:
            print(f"❌ Error expulsando {user_id} de {chat_id}: {e}")


def admin_approval_markup(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("✅ GOLES", callback_data=f"approve:goles:{user_id}"),
            InlineKeyboardButton("✅ CORNERS", callback_data=f"approve:corners:{user_id}"),
        ],
        [
            InlineKeyboardButton("🔥 COMBO", callback_data=f"approve:combo:{user_id}")
        ],
        [
            InlineKeyboardButton("❌ RECHAZAR", callback_data=f"reject:{user_id}")
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def main():
    if not TOKEN:
        raise ValueError("Falta BOT_TOKEN en las variables de entorno.")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("aprobar", aprobar))
    app.add_handler(CommandHandler("rechazar", rechazar))
    app.add_handler(CommandHandler("estado", estado))
    app.add_handler(CommandHandler("listar", listar))

    # primero callbacks del admin
    app.add_handler(CallbackQueryHandler(admin_action_callback, pattern=r"^(approve:|reject:)"))

    # luego callbacks normales del usuario
    app.add_handler(CallbackQueryHandler(seleccionar_plan))

    app.add_handler(
        MessageHandler(
            (filters.TEXT & ~filters.COMMAND) | filters.PHOTO | filters.Document.ALL,
            recibir_comprobante,
        )
    )

    # revisa expiraciones cada 12 horas
    app.job_queue.run_repeating(check_expirations, interval=43200, first=20)

    print("Bot premium funcionando...")
    app.run_polling()


if __name__ == "__main__":
    main()