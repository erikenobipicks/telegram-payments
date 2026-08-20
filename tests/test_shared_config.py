"""
Tests de la fuente única de verdad (`shared/`).

Cubren tres cosas, todas ellas fallos que ya han ocurrido en este repo:

1. `product.json` valida contra `product.schema.json` (y el esquema es válido).
2. `.env.example` no contradice a `config.py` — los IDs de canal GOLES y CORNERS
   estaban intercambiados en el ejemplo, e `INVITE_EXPIRY_HOURS` decía 1 cuando
   el código usa 24.
3. `copy.es.json` no esconde ningún número dentro de un texto, y todos sus
   marcadores están declarados y son resolubles.
"""
import json
import re
from pathlib import Path

import pytest

import config

RAIZ = Path(__file__).resolve().parent.parent
SHARED = RAIZ / "shared"


def _cargar(nombre):
    return json.loads((SHARED / nombre).read_text(encoding="utf-8"))


# ──────────────────────────────────────────────────────────────────────────
# 1. Esquema
# ──────────────────────────────────────────────────────────────────────────
def test_product_json_valida_contra_el_esquema():
    jsonschema = pytest.importorskip("jsonschema")
    esquema = _cargar("product.schema.json")
    jsonschema.Draft202012Validator.check_schema(esquema)
    validador = jsonschema.Draft202012Validator(esquema)
    errores = [
        f"{list(e.absolute_path)}: {e.message}"
        for e in validador.iter_errors(_cargar("product.json"))
    ]
    assert not errores, "product.json no cumple el esquema:\n" + "\n".join(errores)


@pytest.mark.parametrize("ruta,valor", [
    (("compliance", "min_age"), 0),                      # entero no positivo
    (("access", "invite_expiry_hours"), 0),              # entero no positivo
    (("brand", "website_url"), "https://erikenobipicks.com"),  # sin www
    (("currency",), "USD"),                              # moneda no admitida
])
def test_el_esquema_rechaza_valores_invalidos(ruta, valor):
    """Un esquema que lo acepta todo no protege de nada."""
    jsonschema = pytest.importorskip("jsonschema")
    datos = _cargar("product.json")
    nodo = datos
    for clave in ruta[:-1]:
        nodo = nodo[clave]
    nodo[ruta[-1]] = valor
    validador = jsonschema.Draft202012Validator(_cargar("product.schema.json"))
    assert list(validador.iter_errors(datos)), f"el esquema aceptó {ruta} = {valor!r}"


# ──────────────────────────────────────────────────────────────────────────
# 2. .env.example vs configuración real
# ──────────────────────────────────────────────────────────────────────────
def _env_example():
    valores = {}
    for linea in (RAIZ / ".env.example").read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, _, resto = linea.partition("=")
        valor = resto.split("#")[0].strip()
        if valor:
            valores[clave.strip()] = valor
    return valores


@pytest.mark.parametrize("clave", [
    "CANAL_GOLES_ID", "CANAL_CORNERS_ID", "CANAL_PRE_ID",
    "INVITE_EXPIRY_HOURS", "TRIAL_DAYS", "TIMEZONE",
    "CHECK_EXPIRATIONS_EVERY_SECONDS",
])
def test_env_example_coincide_con_config(clave):
    """
    El .env.example es lo que alguien copia al desplegar. Si contradice a
    config.py, reintroduce el bug que config.py dice haber arreglado.
    """
    ejemplo = _env_example()
    assert clave in ejemplo, f"{clave} falta en .env.example"
    esperado = getattr(config, clave)
    assert str(ejemplo[clave]) == str(esperado), (
        f".env.example dice {clave}={ejemplo[clave]} pero config.py usa {esperado}"
    )


def test_env_example_no_cruza_goles_y_corners():
    """Regresión: el ejemplo tenía los dos IDs intercambiados."""
    ejemplo = _env_example()
    assert int(ejemplo["CANAL_GOLES_ID"]) == config.CANAL_GOLES_ID
    assert int(ejemplo["CANAL_CORNERS_ID"]) == config.CANAL_CORNERS_ID
    assert ejemplo["CANAL_GOLES_ID"] != ejemplo["CANAL_CORNERS_ID"]


# ──────────────────────────────────────────────────────────────────────────
# 3. copy.es.json — ni un número dentro de un texto
# ──────────────────────────────────────────────────────────────────────────
# Los dígitos de estos literales forman parte del NOMBRE del mercado; no son
# un valor configurable. Cualquier otra cifra suelta es un fallo.
EXCLUSIONES = ("Over 2.5", "Over 1.5", "J2/J3")
_NUMERO = re.compile(r"(?<![\w{])\d+(?![\w}])")


def _textos(nodo, ruta=""):
    if isinstance(nodo, dict):
        for clave, valor in nodo.items():
            if not clave.startswith("_"):
                yield from _textos(valor, f"{ruta}.{clave}" if ruta else clave)
    elif isinstance(nodo, list):
        for i, valor in enumerate(nodo):
            yield from _textos(valor, f"{ruta}[{i}]")
    elif isinstance(nodo, str):
        yield ruta, nodo


def _declarados(copy):
    ph = copy["_placeholders"]
    return set(ph["global"]) | set(ph["product"]) | set(ph["plan"]) | set(ph["market"]) | set(ph["runtime"])


def test_ningun_numero_dentro_de_un_texto():
    copy = _cargar("copy.es.json")
    fallos = []
    for ruta, texto in _textos(copy):
        limpio = re.sub(r"\{[^}]+\}", "", texto)
        for excluido in EXCLUSIONES:
            limpio = limpio.replace(excluido, "")
        for numero in _NUMERO.findall(limpio):
            fallos.append(f"{ruta}: '{numero}' escrito a mano en {texto[:60]!r}")
    assert not fallos, (
        "Un número dentro de un texto es exactamente el bug del '1 vs 2 picks'. "
        "Usa un marcador y añade el campo a product.json:\n" + "\n".join(fallos)
    )


def test_todos_los_marcadores_estan_declarados():
    copy = _cargar("copy.es.json")
    declarados = _declarados(copy)
    fallos = [
        f"{ruta}: {{{marcador}}}"
        for ruta, texto in _textos(copy)
        for marcador in re.findall(r"\{([^}]+)\}", texto)
        if "." not in marcador and marcador not in declarados
    ]
    assert not fallos, "marcadores sin declarar en _placeholders:\n" + "\n".join(fallos)


def test_no_hay_marcadores_declarados_sin_usar():
    copy = _cargar("copy.es.json")
    usados = {
        marcador
        for _, texto in _textos(copy)
        for marcador in re.findall(r"\{([^}]+)\}", texto)
        if "." not in marcador
    }
    sobran = sorted(_declarados(copy) - usados)
    assert not sobran, f"declarados y nunca usados (no inventes campos): {sobran}"


def test_las_referencias_entre_claves_existen():
    copy = _cargar("copy.es.json")
    fallos = []
    for ruta, texto in _textos(copy):
        for referencia in re.findall(r"\{([^}]+)\}", texto):
            if "." not in referencia:
                continue
            nodo = copy
            for parte in referencia.split("."):
                nodo = nodo.get(parte) if isinstance(nodo, dict) else None
            if not isinstance(nodo, str):
                fallos.append(f"{ruta}: referencia rota {{{referencia}}}")
    assert not fallos, "\n".join(fallos)


# ──────────────────────────────────────────────────────────────────────────
# 4. Coherencia entre product.json y la configuración viva
# ──────────────────────────────────────────────────────────────────────────
def test_product_json_no_contiene_secretos():
    """`shared/` debe poder publicarse sin riesgo."""
    crudo = (SHARED / "product.json").read_text(encoding="utf-8")
    sospechosos = ("BOT_TOKEN", "DATABASE_URL", "sk_live", "sk_test",
                   "ACCESS_TOKEN", "API_KEY", "SECRET", "postgres://",
                   "postgresql://")
    encontrados = [s for s in sospechosos if s.lower() in crudo.lower()]
    assert not encontrados, f"posible secreto en product.json: {encontrados}"


def test_los_precios_coinciden_con_la_configuracion_actual():
    """
    Mientras dure la migración, product.json y config.py conviven. Este test
    detecta que se cambie uno y no el otro.
    """
    producto = _cargar("product.json")
    planes = {
        plan["id"]: plan["amount_cents"]
        for vertical in producto["products"].values()
        for plan in vertical["plans"]
    }
    esperado = {
        "goles": config.PRECIO_GOLES, "corners": config.PRECIO_CORNERS,
        "combo": config.PRECIO_COMBO, "pre": config.PRECIO_PRE,
        "pinpon": config.PRECIO_PINPON,
    }
    for plan_id, precio_texto in esperado.items():
        centimos = int(precio_texto.replace("€", "").strip()) * 100
        assert planes[plan_id] == centimos, (
            f"plan {plan_id}: product.json dice {planes[plan_id]} céntimos "
            f"y config.py dice {precio_texto}"
        )


def test_trial_y_acceso_coinciden_con_la_configuracion_actual():
    producto = _cargar("product.json")
    assert producto["products"]["futbol"]["trial"]["duration_days"] == config.TRIAL_DAYS
    assert producto["products"]["pinpon"]["trial"]["duration_days"] == config.PINPON_TRIAL_DAYS
    assert producto["access"]["plan_interval_days"] == config.PLAN_DAYS
    assert producto["access"]["invite_expiry_hours"] == config.INVITE_EXPIRY_HOURS
    assert producto["access"]["referral"]["referrer_bonus_days"] == config.REFERIDOR_DIAS
    assert producto["timezone"] == config.TIMEZONE
