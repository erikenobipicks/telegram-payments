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
from shared import product_config as cfg

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


# ──────────────────────────────────────────────────────────────────────────
# Módulo 3: el bot de pagos lee de shared/
# ──────────────────────────────────────────────────────────────────────────
def test_los_precios_del_bot_salen_del_cargador():
    import config as c
    for plan_id, valor in (("goles", c.PRECIO_GOLES), ("corners", c.PRECIO_CORNERS),
                           ("combo", c.PRECIO_COMBO), ("pre", c.PRECIO_PRE),
                           ("pinpon", c.PRECIO_PINPON)):
        assert valor == cfg.price(plan_id)


def test_los_botones_de_pago_muestran_el_precio_del_plan():
    import keyboards
    etiquetas = [b.text for fila in keyboards.menu_markup().inline_keyboard for b in fila]
    for plan_id in ("goles", "corners", "combo", "pre", "pinpon"):
        precio = cfg.price(plan_id)
        assert any(precio in e for e in etiquetas), f"ningún botón muestra {precio}"


def test_el_enlace_de_paypal_lleva_el_importe_del_plan():
    """El importe iba en la URL desde una tabla de precios paralela."""
    import keyboards
    for plan_id in ("goles", "combo", "pinpon"):
        urls = [b.url for fila in keyboards.pago_markup(plan_id).inline_keyboard
                for b in fila if b.url and "paypal" in b.url]
        esperado = str(cfg.plan(plan_id)["amount_cents"] // 100)
        assert urls and urls[0].endswith("/" + esperado), f"{plan_id}: {urls}"


def test_el_boton_de_prueba_usa_los_dias_de_su_producto():
    import keyboards
    def etiqueta(plan_id):
        return keyboards.pago_markup(plan_id).inline_keyboard[0][0].text
    assert str(cfg.product("futbol")["trial"]["duration_days"]) in etiqueta("goles")
    assert str(cfg.product("pinpon")["trial"]["duration_days"]) in etiqueta("pinpon")


def test_sin_datos_reales_no_se_inventan_porcentajes():
    """
    El fallback de estadísticas mostraba +70%/+80%/+75% escritos a mano. Justo
    cuando la DB falla es cuando más engañan: en un servicio que vende
    transparencia, decir "ahora no puedo enseñártelos" vale más que un número
    inventado.
    """
    fuente = (RAIZ / "premium_bot.py").read_text(encoding="utf-8")
    prohibidos = re.findall(r"\+\s*[0-9]{2}%\s*estimad|estimado actual|estimado combinado", fuente)
    assert not prohibidos, f"porcentajes inventados en el fallback: {prohibidos}"


def test_el_escapado_de_markdownv2_sobrevive_a_los_valores_de_shared():
    """
    Los mensajes del bot van con parse_mode="MarkdownV2": un '+' o un '.' sin
    escapar hace que Telegram rechace el mensaje entero.
    """
    import premium_bot
    for valor in (cfg.legal("short"), cfg.price("goles"), str(config.PLAN_DAYS)):
        escapado = premium_bot._v2(valor)
        sueltos = re.findall(r"(?<!\\)[_\[\]()~`>#+=|{}.!-]", escapado)
        assert not sueltos, f"{valor!r} deja sin escapar: {sueltos}"


# ──────────────────────────────────────────────────────────────────────────
# Fase 4: guardas globales
# ──────────────────────────────────────────────────────────────────────────
# Las guardas de arriba comprueban los sitios que ya migré. Esta comprueba
# todos los demás: es la que impide que el problema vuelva por una puerta que
# todavía no existe.
FUENTES_COPY = ("config.py", "keyboards.py", "premium_bot.py")

# `Over 2.5` / `Over 1.5` son nombres de mercado, no cifras de producto.
_EXCLUSIONES = ("Over 2.5", "Over 1.5", "O25", "O15")
_PROHIBIDOS = (
    (r"\\?\+\s*18\b", "la edad mínima"),
    (r"jugarbien", "el recurso de ayuda"),
    (r"no garantizamos rentabilidad", "el aviso legal"),
    (r"\d+\s*(?:€|EUR\b)", "un precio"),
    (r"\d+\s*d[ií]as\s+(?:de\s+)?(?:prueba|gratis|acceso)", "una duración"),
    (r"t\.me/erikenobi_premiumbot", "el deep-link al bot"),
    (r"erikenobipicks\.com", "el dominio"),
)


def _cadenas_de_usuario(ruta: Path):
    """
    Literales de cadena que pueden acabar en un mensaje, con su línea.

    Se analiza el AST en vez del texto plano: los comentarios y docstrings de
    este repo explican precios y avisos legales a propósito, y no deben hacer
    fallar la guarda. Lo que importa es lo que se envía.
    """
    import ast
    arbol = ast.parse(ruta.read_text(encoding="utf-8"))
    docstrings = set()
    for nodo in ast.walk(arbol):
        cuerpo = getattr(nodo, "body", None)
        if isinstance(nodo, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if (cuerpo and isinstance(cuerpo[0], ast.Expr)
                    and isinstance(cuerpo[0].value, ast.Constant)
                    and isinstance(cuerpo[0].value.value, str)):
                docstrings.add(id(cuerpo[0].value))
    for nodo in ast.walk(arbol):
        if (isinstance(nodo, ast.Constant) and isinstance(nodo.value, str)
                and id(nodo) not in docstrings):
            yield nodo.lineno, nodo.value


@pytest.mark.parametrize("fichero", FUENTES_COPY)
def test_ningun_valor_de_producto_escrito_a_mano(fichero):
    fallos = []
    for linea, cadena in _cadenas_de_usuario(RAIZ / fichero):
        for excluido in _EXCLUSIONES:
            cadena = cadena.replace(excluido, "")
        for patron, descripcion in _PROHIBIDOS:
            for encontrado in re.findall(patron, cadena, re.IGNORECASE):
                fallos.append(f"{fichero}:{linea}: {descripcion} a mano ({encontrado!r})")
    assert not fallos, (
        "estos valores deben salir de shared/, no escribirse aquí:\n  "
        + "\n  ".join(fallos)
    )


def test_shared_no_ha_divergido_del_canonico():
    """
    Este ES el repo canónico, así que aquí la guarda comprueba lo contrario que
    en las copias: que el sello esté al día. Un sello obsoleto significa que
    `shared/` se ha tocado y todavía no se ha propagado, y mientras tanto los
    otros dos servicios anuncian los valores viejos.

    Se arregla propagando:
        python -m shared.sync --to ../erikenobi-telegram-bot
        python -m shared.sync --to ../erikenobi_picks_landing
    """
    from shared import sync
    problemas = sync.verificar_sello()
    assert not problemas, (
        "el sello de shared/ está obsoleto:\n  " + "\n  ".join(problemas)
    )
