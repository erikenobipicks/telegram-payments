"""
Cargador de la fuente única de verdad (`shared/`).

Contrato:

- El JSON se lee **una sola vez** y se cachea.
- Se **valida contra el esquema** en la primera carga. Si falla, se lanza
  `ConfigError` y el proceso no arranca: caer ruidosamente al inicio es
  preferible a servir un precio incorrecto a un usuario.
- `text()` resuelve los marcadores de `copy.es.json` con los valores de
  `product.json` y **lanza error si queda alguno sin resolver**.
- `format_price()` deriva el precio mostrable de `amount_cents` + `currency`.
  El precio formateado **no se almacena nunca**: se calcula siempre.

Llama a `verify_startup()` al arrancar cada servicio.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_DIR = Path(__file__).resolve().parent
_MARCADOR = re.compile(r"\{([^{}]+)\}")
_SIMBOLO_MONEDA = {"EUR": "€"}


class ConfigError(RuntimeError):
    """La configuración compartida es inválida o se ha pedido algo que no existe."""


# ──────────────────────────────────────────────────────────────────────────
# Carga y validación
# ──────────────────────────────────────────────────────────────────────────
def _leer(nombre: str) -> dict:
    ruta = _DIR / nombre
    try:
        return json.loads(ruta.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ConfigError(f"falta {ruta}") from e
    except json.JSONDecodeError as e:
        raise ConfigError(f"{ruta} no es JSON válido: {e}") from e


@lru_cache(maxsize=1)
def config() -> dict:
    """`product.json` validado. Se lee y valida una sola vez."""
    datos = _leer("product.json")
    esquema = _leer("product.schema.json")
    try:
        from jsonschema import Draft202012Validator
    except ImportError as e:  # sin validador no hay garantía: no arrancamos
        raise ConfigError(
            "falta la dependencia 'jsonschema'; sin ella no se puede validar "
            "product.json y el proceso no debe arrancar"
        ) from e

    validador = Draft202012Validator(esquema)
    errores = _formatear_errores(validador.iter_errors(datos))
    if errores:
        raise ConfigError("product.json no cumple el esquema:\n" + "\n".join(errores))
    return datos


_RUIDO = ("unevaluatedProperties", "additionalProperties")


def _formatear_errores(errores) -> list[str]:
    """
    Deja el error legible.

    Cuando un `allOf` falla, `unevaluatedProperties` reporta además TODAS las
    propiedades del bloque como inesperadas, lo que sepulta la causa real. Se
    suprime ese ruido cuando ya hay un error más específico dentro del mismo
    subárbol, y se eliminan los duplicados que generan `allOf` e `if/then`.
    """
    errores = sorted(errores, key=lambda e: list(e.absolute_path))
    concretas = {
        tuple(e.absolute_path) for e in errores if e.validator not in _RUIDO
    }
    salida, vistos = [], set()
    for e in errores:
        ruta = tuple(e.absolute_path)
        if e.validator in _RUIDO and any(
            r[: len(ruta)] == ruta and len(r) > len(ruta) for r in concretas
        ):
            continue
        clave = (ruta, e.message)
        if clave in vistos:
            continue
        vistos.add(clave)
        salida.append(f"  {'.'.join(str(x) for x in ruta) or '(raíz)'}: {e.message}")
    return salida


@lru_cache(maxsize=1)
def copy() -> dict:
    """`copy.es.json` crudo, sin resolver. Se lee una sola vez."""
    return _leer("copy.es.json")


# ──────────────────────────────────────────────────────────────────────────
# Accesores
# ──────────────────────────────────────────────────────────────────────────
def product(vertical: str) -> dict:
    try:
        return config()["products"][vertical]
    except KeyError as e:
        disponibles = ", ".join(config()["products"])
        raise ConfigError(f"producto desconocido {vertical!r}; hay: {disponibles}") from e


def stats(clave: str):
    """
    Un valor del bloque global `stats`: cómo se publica el rendimiento y con qué
    política de riesgo (umbral de racha, cuota de referencia en directo, stake
    recomendado, tope de drawdown y muestra mínima).

    Es global a propósito. Medir las dos verticales con varas distintas —una
    racha que se anuncia a partir de 3 aciertos en una y de 5 en la otra— es una
    comparación tramposa aunque cada número por separado sea cierto.
    """
    try:
        return config()["stats"][clave]
    except KeyError as e:
        disponibles = ", ".join(config()["stats"])
        raise ConfigError(f"stats desconocido {clave!r}; hay: {disponibles}") from e


def stake_recomendado(bank: float) -> float:
    """
    Stake 1 recomendado: un porcentaje fijo del bank, igual para los cuatro
    métodos. Conservador a propósito y sin depender de que el pasado se repita.
    """
    if bank <= 0:
        raise ConfigError("el bank debe ser positivo")
    return bank * float(stats("recommended_stake_pct")) / 100.0


def stake_maximo(bank: float, drawdown_u: float, muestra: int) -> float | None:
    """
    Stake 1 máximo aplicable a un método, o None si todavía no puede calcularse.

    Sale del PEOR bache histórico real (drawdown, en unidades) y de cuánto bank
    se acepta perder en él: un método que llegó a caer 30u aguanta un stake
    mucho menor que uno que cayó 8u, aunque los dos acaben en verde. Es un tope
    de riesgo, no una recomendación.

    Devuelve None sin muestra suficiente: con pocos picks resueltos el drawdown
    no dice cuál es el peor bache del método, solo cuál ha sido hasta ahora, y
    publicar un tope basado en eso invita a apostar de más.
    """
    if bank <= 0:
        raise ConfigError("el bank debe ser positivo")
    if muestra < int(stats("min_sample_for_risk")) or drawdown_u <= 0:
        return None
    return bank * float(stats("max_drawdown_bank_pct")) / 100.0 / float(drawdown_u)


def plan(plan_id: str) -> dict:
    """Devuelve el plan (o bundle) con su vertical en la clave 'vertical'."""
    for vertical, datos in config()["products"].items():
        for p in datos["plans"]:
            if p["id"] == plan_id:
                return {**p, "vertical": vertical}
    for b in config().get("bundles", []):
        if b["id"] == plan_id:
            return {**b, "vertical": None}
    raise ConfigError(f"plan desconocido {plan_id!r}")


def market(vertical: str, market_id: str) -> dict:
    for m in product(vertical).get("markets", []):
        if m["id"] == market_id:
            return m
    raise ConfigError(f"mercado desconocido {market_id!r} en {vertical!r}")


def free_picks_per_day(vertical: str) -> int:
    """
    Picks gratis al día del producto, según su modo activo.

    Lanza `ConfigError` si el modo activo no tiene tope (`null`): un texto que
    anuncia un número de picks no aplica a un modo sin límite, y decirlo mal es
    exactamente el bug que este módulo existe para impedir.
    """
    libre = product(vertical)["free_tier"]
    modo = libre.get("mode")
    valor = libre["modes"][modo]["picks_per_day"] if modo else libre["picks_per_day"]
    if valor is None:
        raise ConfigError(
            f"{vertical}: el modo free {modo!r} no tiene tope de picks; "
            f"no uses un texto que anuncie un número"
        )
    return valor


# ──────────────────────────────────────────────────────────────────────────
# Precios — siempre calculados, nunca almacenados
# ──────────────────────────────────────────────────────────────────────────
def format_price(amount_cents: int, currency: str | None = None) -> str:
    """2000 → '20€' · 1999 → '19,99€' (convención es-ES)."""
    moneda = currency or config()["currency"]
    try:
        simbolo = _SIMBOLO_MONEDA[moneda]
    except KeyError as e:
        raise ConfigError(f"moneda sin símbolo definido: {moneda!r}") from e
    if not isinstance(amount_cents, int) or amount_cents < 0:
        raise ConfigError(f"importe inválido: {amount_cents!r}")
    enteros, centimos = divmod(amount_cents, 100)
    return f"{enteros},{centimos:02d}{simbolo}" if centimos else f"{enteros}{simbolo}"


def price(plan_id: str) -> str:
    return format_price(plan(plan_id)["amount_cents"])


# ──────────────────────────────────────────────────────────────────────────
# Deep links — origen Y vertical
# ──────────────────────────────────────────────────────────────────────────
def _sanear(valor: Any) -> str:
    limpio = re.sub(r"[^a-z0-9_-]+", "-", str(valor or "").lower())
    return limpio.strip("-")[:24]


def start_param(intent: str, source: str, vertical: str | None = None,
                campaign: str | None = None, content: str | None = None) -> str:
    """
    Start param con tracking, respetando el tope de Telegram.

    Los tokens se añaden en el orden de `tracking.start_param_token_order`, que
    es también el orden de truncado: `src` y `vrt` van primero porque son los
    que deben sobrevivir al límite.
    """
    t = config()["tracking"]
    valores = {
        "src": _sanear(source),
        "vrt": _sanear(vertical),
        "cmp": _sanear(campaign or t["default_utm_campaign"]),
        "cnt": _sanear(content),
    }
    resultado = _sanear(intent) or "start"
    for token in t["start_param_token_order"]:
        if not valores.get(token):
            continue
        candidato = f"{resultado}__{token}-{valores[token]}"
        if len(candidato) > t["start_param_max_length"]:
            break
        resultado = candidato
    return resultado


def bot_deep_link(intent: str, source: str, vertical: str | None = None,
                  campaign: str | None = None, content: str | None = None) -> str:
    base = config()["brand"]["bot_url"]
    return f"{base}?start={start_param(intent, source, vertical, campaign, content)}"


def landing_url(source: str, campaign: str | None = None, content: str | None = None) -> str:
    """URL de la landing con UTM. Siempre al dominio canónico (con www)."""
    t = config()["tracking"]
    params = [
        f"utm_source={_sanear(source) or 'direct'}",
        f"utm_campaign={_sanear(campaign or t['default_utm_campaign'])}",
    ]
    if content:
        params.append(f"utm_content={_sanear(content)}")
    return f"{config()['brand']['website_url']}/?" + "&".join(params)


# ──────────────────────────────────────────────────────────────────────────
# Resolución de textos
# ──────────────────────────────────────────────────────────────────────────
def _dominio(url: str) -> str:
    return urlparse(url).netloc.removeprefix("www.")


def _contexto(vertical: str | None, plan_id: str | None,
              market_id: str | None, runtime: dict) -> dict:
    """
    Marcador → valor. Cada entrada es un *callable* para que un marcador que no
    aplica solo falle si el texto realmente lo usa.
    """
    c = config()
    ctx: dict[str, Any] = {
        "brand_premium_name": lambda: c["brand"]["premium_name"],
        "brand_name": lambda: c["brand"]["name"],
        "website_url": lambda: c["brand"]["website_url"],
        "support_contact": lambda: c["brand"]["support_contact"],
        "support_url": lambda: c["brand"]["support_url"],
        "bot_url": lambda: c["brand"]["bot_url"],
        "instagram_handle": lambda: c["brand"]["instagram_handle"],
        "min_age": lambda: c["compliance"]["min_age"],
        "responsible_gambling_url": lambda: c["compliance"]["responsible_gambling_url"],
        "responsible_gambling_domain": lambda: _dominio(c["compliance"]["responsible_gambling_url"]),
        "plan_interval_days": lambda: c["access"]["plan_interval_days"],
        "invite_expiry_hours": lambda: c["access"]["invite_expiry_hours"],
        "referrer_bonus_days": lambda: c["access"]["referral"]["referrer_bonus_days"],
        # Hay textos que hablan de los dos productos a la vez (la landing los
        # vende juntos) y no pueden resolverse con un solo producto en contexto.
        "trial_days_futbol": lambda: c["products"]["futbol"]["trial"]["duration_days"],
        "trial_days_pinpon": lambda: c["products"]["pinpon"]["trial"]["duration_days"],
    }

    if vertical:
        p = product(vertical)
        ctx.update({
            "product_display_name": lambda: p["display_name"],
            "stats_url": lambda: p["stats_url"],
            "channel_free_url": lambda: p["telegram_channel_free"],
            "trial_days": lambda: p["trial"]["duration_days"],
            "free_picks_per_day": lambda: free_picks_per_day(vertical),
            "free_hours_range": lambda: "{start}:00–{end}:00".format(**p["free_tier"]["publication_hours"]),
            "reveal_minutes_before": lambda: p["free_tier"]["reveal_minutes_before"],
            "alert_minutes_before": lambda: p["method"]["alert_minutes_before"],
            "bookmakers": lambda: p["method"]["bookmakers"],
            "stake_game_2": lambda: p["method"]["stake_game_2"],
            "stake_game_3": lambda: p["method"]["stake_game_3"],
            "leagues_list": lambda: ", ".join(x["name"] for x in p["leagues"]),
        })

    if plan_id:
        datos_plan = plan(plan_id)
        ctx["plan_display_name"] = lambda: datos_plan["display_name"]
        ctx["plan_price"] = lambda: format_price(datos_plan["amount_cents"])

    if vertical and market_id:
        datos_mercado = market(vertical, market_id)
        ctx["market_display_name"] = lambda: datos_mercado["display_name"]
        ctx["advance_hours_max"] = lambda: datos_mercado["advance_hours_max"]
        ctx["reminder_minutes_before"] = lambda: datos_mercado["reminder_minutes_before"]

    for clave, valor in runtime.items():
        ctx[clave] = (lambda v=valor: v)
    return ctx


def _clave_copy(clave: str) -> Any:
    nodo: Any = copy()
    for parte in clave.split("."):
        if not isinstance(nodo, dict) or parte not in nodo:
            raise ConfigError(f"clave de texto desconocida: {clave!r}")
        nodo = nodo[parte]
    return nodo


def _resolver(plantilla: str, ctx: dict, visitadas: frozenset) -> str:
    def sustituir(m: re.Match) -> str:
        nombre = m.group(1)
        if "." in nombre:  # referencia a otra clave de copy.es.json
            if nombre in visitadas:
                raise ConfigError(f"referencia circular en textos: {nombre!r}")
            destino = _clave_copy(nombre)
            if not isinstance(destino, str):
                raise ConfigError(f"la referencia {nombre!r} no apunta a un texto")
            return _resolver(destino, ctx, visitadas | {nombre})
        if nombre not in ctx:
            raise ConfigError(
                f"marcador {{{nombre}}} sin valor en este contexto; "
                f"¿falta pasar product=/plan=/market= o un valor de runtime?"
            )
        try:
            return str(ctx[nombre]())
        except ConfigError:
            raise
        except (KeyError, TypeError) as e:
            raise ConfigError(f"marcador {{{nombre}}} no resoluble: {e}") from e

    return _MARCADOR.sub(sustituir, plantilla)


def _resolver_plantilla(plantilla: str, clave: str, vertical: str | None,
                        plan_id: str | None, market_id: str | None,
                        runtime: dict) -> str:
    ctx = _contexto(vertical, plan_id, market_id, runtime)
    resuelto = _resolver(plantilla, ctx, frozenset({clave}))
    if _MARCADOR.search(resuelto):
        pendientes = ", ".join(sorted(set(_MARCADOR.findall(resuelto))))
        raise ConfigError(f"{clave!r} deja marcadores sin resolver: {pendientes}")
    return resuelto


def text(clave: str, *, product: str | None = None, plan: str | None = None,
         market: str | None = None, **runtime) -> str:
    """
    Texto de `copy.es.json` con todos sus marcadores resueltos.

    Lanza `ConfigError` si la clave no existe, si un marcador no tiene valor en
    el contexto dado, o si queda algún marcador sin resolver.
    """
    plantilla = _clave_copy(clave)
    if not isinstance(plantilla, str):
        raise ConfigError(f"{clave!r} no es un texto (es {type(plantilla).__name__})")
    return _resolver_plantilla(plantilla, clave, product, plan, market, runtime)


def texts(clave: str, *, product: str | None = None, plan: str | None = None,
          market: str | None = None, **runtime) -> list[str]:
    """Igual que `text()` para una clave que contiene una lista de textos."""
    valores = _clave_copy(clave)
    if not isinstance(valores, list):
        raise ConfigError(f"{clave!r} no es una lista de textos")
    return [_resolver_plantilla(v, clave, product, plan, market, runtime) for v in valores]


def legal(variante: str = "short") -> str:
    """Aviso legal canónico. Una sola redacción para todo el ecosistema."""
    return text(f"legal.{variante}")


# ──────────────────────────────────────────────────────────────────────────
# Arranque
# ──────────────────────────────────────────────────────────────────────────
def verify_startup(check_payment_links: bool = False, timeout: float = 10.0) -> None:
    """
    Fuerza la carga y validación. Llamar al arrancar cada servicio.

    Con `check_payment_links=True` comprueba además que cada Payment Link del
    proveedor responde. NO compara importes: hoy son Payment Links, no precios
    con `price_id`, así que el importe solo vive en el panel del proveedor. El
    procedimiento manual de cambio de precio está en `shared/README.md`.
    """
    config()
    copy()
    if not check_payment_links:
        return

    import urllib.error
    import urllib.request

    enlaces = [
        (p["id"], p["provider_payment_link"])
        for datos in config()["products"].values()
        for p in datos["plans"]
        if p.get("provider_payment_link")
    ]
    caidos = []
    for plan_id, url in enlaces:
        peticion = urllib.request.Request(url, method="HEAD")
        try:
            with urllib.request.urlopen(peticion, timeout=timeout) as r:
                if r.status >= 400:
                    caidos.append(f"{plan_id}: HTTP {r.status} en {url}")
        except urllib.error.HTTPError as e:
            caidos.append(f"{plan_id}: HTTP {e.code} en {url}")
        except Exception as e:  # red caída, DNS, timeout…
            caidos.append(f"{plan_id}: {type(e).__name__} en {url}: {e}")
    if caidos:
        raise ConfigError("enlaces de pago que no responden:\n  " + "\n  ".join(caidos))
