"""
Teclados (InlineKeyboardMarkup) del bot. Capa de presentación: solo depende de
`config` y de telegram. Sin lógica de negocio ni acceso a DB.
"""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import (
    CANAL_PINPON_ID,
    PAYPAL_LINK,
    PINPON_FREE_URL,
    STRIPE_COMBO,
    STRIPE_CORNERS,
    STRIPE_GOLES,
    STRIPE_PINPON,
    STRIPE_PRE,
    TRIAL_PLANS,
)
from shared import product_config as cfg


def menu_markup() -> InlineKeyboardMarkup:
    filas = [
        [
            InlineKeyboardButton("ℹ️ Cómo funciona", callback_data="info"),
            InlineKeyboardButton("📊 Estadísticas",  callback_data="stats"),
        ],
        [InlineKeyboardButton("🤔 ¿Qué plan elijo?",  callback_data="que_plan")],
        [InlineKeyboardButton("📋 Guía de pago",     callback_data="guia")],
        [InlineKeyboardButton("⚽⛳ Canal FREE Goles y Córners", callback_data="free")],
        [
            InlineKeyboardButton(f"⚽ GOLES — {cfg.price('goles')}",     callback_data="goles"),
            InlineKeyboardButton(f"🚩 CORNERS — {cfg.price('corners')}", callback_data="corners"),
        ],
        [InlineKeyboardButton(f"🔥 GOLES + CORNERS — {cfg.price('combo')}", callback_data="combo")],
        [InlineKeyboardButton(f"📊 PREPARTIDO — {cfg.price('pre')}", callback_data="pre")],
    ]
    # Botón al canal FREE de ping pong (si hay enlace configurado).
    if PINPON_FREE_URL:
        filas.append([InlineKeyboardButton("🏓 Canal FREE Ping Pong", url=PINPON_FREE_URL)])
    # El botón de Ping Pong (premium) solo aparece cuando su canal está configurado.
    if CANAL_PINPON_ID:
        filas.append(
            [InlineKeyboardButton(f"🏓 PING PONG — {cfg.price('pinpon')}", callback_data="pinpon")]
        )
    filas += [
        [InlineKeyboardButton("🎁 Invitar amigos", callback_data="referido")],
        [
            InlineKeyboardButton("🔒 Privacidad", callback_data="privacidad"),
            InlineKeyboardButton("💬 Contacto",   url=cfg.config()["brand"]["support_url"]),
        ],
    ]
    return InlineKeyboardMarkup(filas)


def volver_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]]
    )


def pago_markup(plan: str) -> InlineKeyboardMarkup:
    stripes = {"goles": STRIPE_GOLES, "corners": STRIPE_CORNERS, "combo": STRIPE_COMBO,
               "pre": STRIPE_PRE, "pinpon": STRIPE_PINPON}
    # PayPal quiere el importe en la URL: se deriva de amount_cents, no de una
    # tabla de precios paralela (era la tercera copia de los importes).
    try:
        importe = f"{cfg.plan(plan)['amount_cents'] // 100}"
    except cfg.ConfigError:
        importe = ""

    stripe_url = stripes.get(plan, "")
    keyboard = []
    # La prueba gratis solo se ofrece en los planes con trial (ver TRIAL_PLANS).
    # Los días salen del producto al que pertenece el plan.
    if plan in TRIAL_PLANS:
        vertical = cfg.plan(plan)["vertical"]
        keyboard.append([InlineKeyboardButton(
            cfg.text("trial.cta_button", product=vertical),
            callback_data=f"trial:{plan}",
        )])
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
        [InlineKeyboardButton("✅ Aprobar PING PONG", callback_data=f"approve:pinpon:{user_id}")],
        [InlineKeyboardButton("❌ Rechazar", callback_data=f"reject:{user_id}")],
    ])


def acceso_listo_markup(con_planes: bool = False) -> InlineKeyboardMarkup:
    filas = [[InlineKeyboardButton("🔑 Obtener mi acceso", callback_data="obtener_acceso")]]
    # En la prueba (o con acceso activo), el usuario debe poder llegar al menú de
    # planes para pagar/suscribirse; si no, /start se quedaba solo en esta pantalla.
    if con_planes:
        filas.append([InlineKeyboardButton("💳 Ver planes de pago", callback_data="menu")])
    return InlineKeyboardMarkup(filas)


def _privacidad_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Borrar mis datos", callback_data="borrar:pedir")],
        [InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")],
    ])


def _confirmar_borrado_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Sí, borrar todo", callback_data="borrar:confirm")],
        [InlineKeyboardButton("❌ No, cancelar", callback_data="menu")],
    ])
