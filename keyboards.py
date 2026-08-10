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
    TRIAL_DAYS,
    PINPON_TRIAL_DAYS,
    TRIAL_PLANS,
)


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
            InlineKeyboardButton("⚽ GOLES — 20€",   callback_data="goles"),
            InlineKeyboardButton("🚩 CORNERS — 20€", callback_data="corners"),
        ],
        [InlineKeyboardButton("🔥 GOLES + CORNERS — 30€", callback_data="combo")],
        [InlineKeyboardButton("📊 PREPARTIDO — 20€", callback_data="pre")],
    ]
    # Botón al canal FREE de ping pong (si hay enlace configurado).
    if PINPON_FREE_URL:
        filas.append([InlineKeyboardButton("🏓 Canal FREE Ping Pong", url=PINPON_FREE_URL)])
    # El botón de Ping Pong (premium) solo aparece cuando su canal está configurado.
    if CANAL_PINPON_ID:
        filas.append([InlineKeyboardButton("🏓 PING PONG — 50€", callback_data="pinpon")])
    filas += [
        [InlineKeyboardButton("🎁 Invitar amigos", callback_data="referido")],
        [
            InlineKeyboardButton("🔒 Privacidad", callback_data="privacidad"),
            InlineKeyboardButton("💬 Contacto",   url="https://t.me/erikenobi"),
        ],
    ]
    return InlineKeyboardMarkup(filas)


def volver_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Volver al menú", callback_data="menu")]]
    )


def pago_markup(plan: str) -> InlineKeyboardMarkup:
    precios = {"goles": "20", "corners": "20", "combo": "30", "pre": "20", "pinpon": "50"}
    stripes = {"goles": STRIPE_GOLES, "corners": STRIPE_CORNERS, "combo": STRIPE_COMBO,
               "pre": STRIPE_PRE, "pinpon": STRIPE_PINPON}
    importe = precios.get(plan, "")

    stripe_url = stripes.get(plan, "")
    keyboard = []
    # La prueba gratis solo se ofrece en los planes con trial (ver TRIAL_PLANS).
    # Ping Pong: 3 días; el resto: 7.
    if plan in TRIAL_PLANS:
        _dias = PINPON_TRIAL_DAYS if plan == "pinpon" else TRIAL_DAYS
        keyboard.append([InlineKeyboardButton(
            f"🎁 Probar gratis {_dias} días",
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


def acceso_listo_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔑 Obtener mi acceso", callback_data="obtener_acceso")]]
    )


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
