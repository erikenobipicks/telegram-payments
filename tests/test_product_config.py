"""
Tests del cargador de la configuración compartida (`shared/product_config.py`)
y de su gemelo de navegador (`shared/product_config.js`).

Lo que se protege aquí:

- El precio mostrable se DERIVA de `amount_cents`; nunca se almacena.
- Un marcador sin resolver es un error, no un texto con `{trial_days}` dentro.
- Toda clave de `copy.es.json` resuelve en algún contexto razonable.
- Los dos cargadores producen exactamente lo mismo. Si divergen, un canal
  anuncia una cosa y la web otra — que es el problema que veníamos a arreglar.
"""
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from shared import product_config as cfg

RAIZ = Path(__file__).resolve().parent.parent
SHARED = RAIZ / "shared"
NODE = shutil.which("node")


# ──────────────────────────────────────────────────────────────────────────
# Carga y arranque
# ──────────────────────────────────────────────────────────────────────────
def test_verify_startup_no_toca_la_red_por_defecto(monkeypatch):
    """Arrancar no debe depender de que Stripe esté en pie."""
    import urllib.request

    def prohibido(*a, **k):
        raise AssertionError("verify_startup() ha intentado salir a la red")

    monkeypatch.setattr(urllib.request, "urlopen", prohibido)
    cfg.verify_startup()


def test_el_json_se_lee_una_sola_vez():
    cfg.config()
    antes = cfg.config.cache_info().currsize
    for _ in range(50):
        cfg.config()
    assert cfg.config.cache_info().currsize == antes == 1


# ──────────────────────────────────────────────────────────────────────────
# Precios
# ──────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("centimos,esperado", [
    (2000, "20€"), (3000, "30€"), (5000, "50€"),
    (1999, "19,99€"), (2050, "20,50€"), (5, "0,05€"), (0, "0€"),
])
def test_format_price(centimos, esperado):
    assert cfg.format_price(centimos) == esperado


@pytest.mark.parametrize("malo", [-1, 1.5, "20€", None])
def test_format_price_rechaza_importes_invalidos(malo):
    with pytest.raises(cfg.ConfigError):
        cfg.format_price(malo)


def test_el_precio_formateado_no_esta_almacenado():
    """Si alguien guarda '20€' en el JSON, este test lo caza."""
    crudo = (SHARED / "product.json").read_text(encoding="utf-8")
    assert "€" not in crudo, "product.json no debe contener precios ya formateados"


def test_price_deriva_del_plan():
    assert cfg.price("goles") == "20€"
    assert cfg.price("combo") == "30€"
    assert cfg.price("pinpon") == "50€"


# ──────────────────────────────────────────────────────────────────────────
# Resolución de textos
# ──────────────────────────────────────────────────────────────────────────
def test_un_marcador_sin_contexto_es_un_error():
    with pytest.raises(cfg.ConfigError, match="trial_days"):
        cfg.text("trial.cta_button")


def test_el_mismo_texto_cambia_con_el_producto():
    assert cfg.text("trial.cta_button", product="futbol") != \
           cfg.text("trial.cta_button", product="pinpon")


def test_free_reducido_anuncia_su_tope():
    """
    En modo 'reducido' el free de ping pong tiene tope (2/día): el aviso que
    anuncia el número SÍ aplica y resuelve el placeholder con el tope real.
    (El guard de "un modo sin tope no puede anunciar un número" vive en
    free_picks_per_day, que lanza ConfigError si picks_per_day es null.)
    """
    aviso = cfg.text("products.pinpon.free.limit_notice_reducido", product="pinpon")
    assert "{" not in aviso                                  # placeholder resuelto
    assert str(cfg.free_picks_per_day("pinpon")) in aviso    # anuncia el tope real


def test_modo_sin_tope_lanza_al_pedir_un_numero():
    """
    El guard sigue vivo a nivel de función: pedir el nº de picks de un modo sin
    tope (picks_per_day null) lanza ConfigError, para no anunciar un número que
    no aplica (el bug original). Se comprueba forzando el modo 'completo'.
    """
    libre = cfg.product("pinpon")["free_tier"]
    if libre["modes"]["completo"]["picks_per_day"] is not None:
        pytest.skip("el modo 'completo' ya no es sin tope")
    modo_original = libre["mode"]
    libre["mode"] = "completo"
    try:
        with pytest.raises(cfg.ConfigError, match="no tiene tope"):
            cfg.free_picks_per_day("pinpon")
    finally:
        libre["mode"] = modo_original


def test_las_referencias_entre_claves_se_resuelven():
    assert "{" not in cfg.text("landing.stats_note")
    assert "rendimiento pasado" in cfg.text("landing.stats_note")


def test_el_aviso_legal_es_uno_solo():
    corto, largo = cfg.legal("short"), cfg.legal("full")
    edad = str(cfg.config()["compliance"]["min_age"])
    for texto in (corto, largo):
        assert edad in texto
        assert "jugarbien.es" in texto
        assert "{" not in texto


@pytest.mark.parametrize("clave", ["legal.inventada", "no.existe", "products.tenis.x"])
def test_clave_desconocida_es_un_error(clave):
    with pytest.raises(cfg.ConfigError, match="desconocida"):
        cfg.text(clave)


# ──────────────────────────────────────────────────────────────────────────
# Cobertura: TODA clave de copy.es.json debe resolver
# ──────────────────────────────────────────────────────────────────────────
def _claves_de_texto():
    datos = json.loads((SHARED / "copy.es.json").read_text(encoding="utf-8"))

    def recorrer(nodo, ruta=""):
        if isinstance(nodo, dict):
            for k, v in nodo.items():
                if not k.startswith("_"):
                    yield from recorrer(v, f"{ruta}.{k}" if ruta else k)
        elif isinstance(nodo, list) and all(isinstance(x, str) for x in nodo):
            yield ruta, True
        elif isinstance(nodo, str):
            yield ruta, False

    return sorted(recorrer(datos))


# Valores que aporta quien llama (no viven en product.json).
_RUNTIME = {"racha": 5, "emoji_tipo": "⚽"}


def _contextos():
    """Todos los contextos con los que tiene sentido resolver un texto."""
    datos = cfg.config()
    contextos = [{}, dict(_RUNTIME)]
    for vertical, producto in datos["products"].items():
        contextos.append({"product": vertical, **_RUNTIME})
        for plan in producto["plans"]:
            contextos.append({"product": vertical, "plan": plan["id"], **_RUNTIME})
        for mercado in producto.get("markets", []):
            contextos.append({"product": vertical, "market": mercado["id"], **_RUNTIME})
    return contextos


@pytest.mark.parametrize("clave,es_lista", _claves_de_texto())
def test_toda_clave_resuelve_en_algun_contexto(clave, es_lista):
    resolver = cfg.texts if es_lista else cfg.text
    errores = []
    for contexto in _contextos():
        try:
            resultado = resolver(clave, **contexto)
        except cfg.ConfigError as e:
            errores.append(str(e))
            continue
        valores = resultado if es_lista else [resultado]
        assert all("{" not in v for v in valores), f"{clave} deja marcadores: {valores}"
        return
    pytest.fail(f"{clave} no resuelve en ningún contexto. Último error: {errores[-1]}")


# ──────────────────────────────────────────────────────────────────────────
# Deep links: origen Y vertical
# ──────────────────────────────────────────────────────────────────────────
def test_el_deep_link_lleva_origen_y_vertical():
    enlace = cfg.bot_deep_link("trial_7d", "free_channel", "futbol")
    assert "src-free_channel" in enlace
    assert "vrt-futbol" in enlace


def test_el_start_param_respeta_el_tope_de_telegram():
    tope = cfg.config()["tracking"]["start_param_max_length"]
    param = cfg.start_param(
        "trial_7d", "una-fuente-larguisima", "futbol",
        campaign="campaña-de-marketing-larguísima", content="contenido-larguísimo",
    )
    assert len(param) <= tope


def test_al_truncar_sobreviven_origen_y_vertical():
    """El orden de tokens es el orden de truncado: src y vrt van primero."""
    param = cfg.start_param(
        "trial_7d", "instagram", "pinpon",
        campaign="x" * 40, content="y" * 40,
    )
    assert "src-instagram" in param and "vrt-pinpon" in param


def test_la_landing_usa_el_dominio_canonico():
    assert cfg.landing_url("instagram").startswith("https://www.erikenobipicks.com/")


# ──────────────────────────────────────────────────────────────────────────
# Paridad Python ↔ JavaScript
# ──────────────────────────────────────────────────────────────────────────
# Los casos entran por stdin (son miles, no caben en la línea de comandos) y las
# rutas por entorno: con `node -e` no hay ruta de script en argv[1], y depender
# de ese desfase es una fuente de fallos silenciosos.
_GUION_NODE = r"""
const fs = require('fs');
const pc = require(process.env.PC_JS);
pc.init({
  product: JSON.parse(fs.readFileSync(process.env.PC_PRODUCT, 'utf8')),
  copy: JSON.parse(fs.readFileSync(process.env.PC_COPY, 'utf8')),
});
const casos = JSON.parse(fs.readFileSync(0, 'utf8'));
const salida = casos.map(function (c) {
  try {
    const fn = c.lista ? pc.texts : pc.text;
    return { ok: true, valor: fn(c.clave, c.ctx) };
  } catch (e) {
    return { ok: false, valor: null };
  }
});
process.stdout.write(JSON.stringify(salida));
"""


def test_los_dos_cargadores_dan_lo_mismo():
    """
    Dos cargadores que divergen son dos fuentes de verdad disfrazadas de una.
    Se resuelve cada clave con cada contexto en ambos y se comparan.

    En CI esto NO se salta: un skip silencioso es una guarda que desaparece sin
    avisar. En local sí, para no exigir node a quien solo toca Python.
    """
    if NODE is None:
        if os.environ.get("CI"):
            pytest.fail("node no está disponible en CI; la guarda de paridad no se ejecutaría")
        pytest.skip("node no está disponible (en CI sería un fallo)")

    casos, esperados = [], []
    for clave, es_lista in _claves_de_texto():
        for contexto in _contextos():
            casos.append({"clave": clave, "lista": es_lista, "ctx": contexto})
            resolver = cfg.texts if es_lista else cfg.text
            try:
                esperados.append({"ok": True, "valor": resolver(clave, **contexto)})
            except cfg.ConfigError:
                esperados.append({"ok": False, "valor": None})

    proceso = subprocess.run(
        [NODE, "-e", _GUION_NODE],
        input=json.dumps(casos),
        env={
            **os.environ,
            "PC_JS": str(SHARED / "product_config.js"),
            "PC_PRODUCT": str(SHARED / "product.json"),
            "PC_COPY": str(SHARED / "copy.es.json"),
        },
        capture_output=True, text=True, timeout=120, check=True,
    )
    obtenidos = json.loads(proceso.stdout)

    diferencias = [
        f"{c['clave']} {c['ctx']}: python={e['valor']!r} js={o['valor']!r}"
        for c, e, o in zip(casos, esperados, obtenidos)
        if e != o
    ]
    assert not diferencias, (
        f"{len(diferencias)} divergencias entre los cargadores:\n"
        + "\n".join(diferencias[:10])
    )


# ──────────────────────────────────────────────────────────────────────────
# Enlaces de pago (D2)
# ──────────────────────────────────────────────────────────────────────────
class _Respuesta:
    def __init__(self, status):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_verify_startup_comprueba_todos_los_enlaces_de_pago(monkeypatch):
    import urllib.request

    visitadas = []

    def falsa(peticion, timeout=None):
        visitadas.append(peticion.full_url)
        return _Respuesta(200)

    monkeypatch.setattr(urllib.request, "urlopen", falsa)
    cfg.verify_startup(check_payment_links=True)

    esperadas = {
        p["provider_payment_link"]
        for datos in cfg.config()["products"].values()
        for p in datos["plans"]
        if p.get("provider_payment_link")
    }
    assert set(visitadas) == esperadas
    assert len(esperadas) == 5, "los cinco planes deben tener enlace de pago"


def test_no_arranca_si_un_enlace_de_pago_esta_caido(monkeypatch):
    import urllib.request

    def falsa(peticion, timeout=None):
        if "08g03" in peticion.full_url:  # el enlace del plan combo
            return _Respuesta(404)
        return _Respuesta(200)

    monkeypatch.setattr(urllib.request, "urlopen", falsa)
    with pytest.raises(cfg.ConfigError, match="no responden"):
        cfg.verify_startup(check_payment_links=True)


def test_no_arranca_si_el_proveedor_no_contesta(monkeypatch):
    import urllib.request

    def falsa(peticion, timeout=None):
        raise TimeoutError("timeout")

    monkeypatch.setattr(urllib.request, "urlopen", falsa)
    with pytest.raises(cfg.ConfigError, match="no responden"):
        cfg.verify_startup(check_payment_links=True)


# ──────────────────────────────────────────────────────────────────────────
# Bloque global `stats`: rendimiento publicado y política de riesgo
# ──────────────────────────────────────────────────────────────────────────
_STATS_CLAVES = ("min_streak_to_show", "reference_odds_live",
                 "recommended_stake_pct", "max_drawdown_bank_pct",
                 "min_sample_for_risk")


@pytest.mark.parametrize("clave", _STATS_CLAVES)
def test_stats_expone_la_politica_de_riesgo(clave):
    assert isinstance(cfg.stats(clave), (int, float))


def test_stats_rechaza_una_clave_desconocida():
    with pytest.raises(cfg.ConfigError):
        cfg.stats("no_existe")


def test_el_umbral_de_racha_es_global_y_no_de_un_producto():
    """
    Vivía bajo products.pinpon porque las rachas eran cosa del ping pong. Ahora
    las publican los cuatro métodos: un umbral por producto serían dos varas de
    medir, y comparar rachas medidas distinto es tramposo aunque cada número por
    separado sea cierto.
    """
    for vertical in cfg.config()["products"]:
        metodo = cfg.product(vertical).get("method", {})
        assert "min_streak_to_show" not in metodo, f"{vertical} reintroduce el umbral"


def test_la_cuota_de_referencia_no_es_una_cuota_real():
    """
    Es la vara con la que se publica el beneficio de goles y córners (línea
    entera +1), no la cuota de un pick concreto. Cambiarla reescribe el
    histórico anunciado, así que el esquema la acota a un rango creíble.
    """
    cuota = cfg.stats("reference_odds_live")
    assert 1.01 < cuota < 5.0


def test_el_tope_de_drawdown_deja_bank_de_sobra():
    """Si el peor bache histórico pudiera llevarse el bank entero, el 'stake
    máximo' no sería un límite de riesgo sino una invitación a arruinarse."""
    assert 0 < cfg.stats("max_drawdown_bank_pct") <= 50


def test_los_dos_cargadores_dan_el_mismo_stats():
    """La calculadora de stake corre en el navegador con el cargador JS; el
    resto del sistema lee el de Python. Si divergen, la web recomienda un stake
    que el bot no reconocería."""
    if NODE is None:
        if os.environ.get("CI"):
            pytest.fail("node no está disponible en CI; la guarda de paridad no se ejecutaría")
        pytest.skip("node no está disponible (en CI sería un fallo)")

    guion = (
        "const fs=require('fs');const pc=require(process.env.PC_JS);"
        "pc.init({product:JSON.parse(fs.readFileSync(process.env.PC_PRODUCT,'utf8'))});"
        "const claves=JSON.parse(fs.readFileSync(0,'utf8'));"
        "process.stdout.write(JSON.stringify(claves.map(k=>pc.stats(k))));"
    )
    proceso = subprocess.run(
        [NODE, "-e", guion],
        input=json.dumps(list(_STATS_CLAVES)),
        env={**os.environ, "PC_JS": str(SHARED / "product_config.js"),
             "PC_PRODUCT": str(SHARED / "product.json")},
        capture_output=True, text=True, timeout=60, check=True,
    )
    assert json.loads(proceso.stdout) == [cfg.stats(k) for k in _STATS_CLAVES]


# ──────────────────────────────────────────────────────────────────────────
# Calculadora de stake
# ──────────────────────────────────────────────────────────────────────────
def test_el_stake_recomendado_es_el_porcentaje_del_bank():
    pct = cfg.stats("recommended_stake_pct")
    assert cfg.stake_recomendado(1000) == pytest.approx(1000 * pct / 100)


@pytest.mark.parametrize("bank", [0, -1, -1000])
def test_un_bank_no_positivo_es_un_error(bank):
    with pytest.raises(cfg.ConfigError):
        cfg.stake_recomendado(bank)
    with pytest.raises(cfg.ConfigError):
        cfg.stake_maximo(bank, 10, 500)


def test_el_stake_maximo_sale_del_peor_bache():
    """Con 1000€ de bank, un tope del 33% y un bache de 10u: 330€/10u = 33€/u."""
    bank, drawdown = 1000, 10
    esperado = bank * cfg.stats("max_drawdown_bank_pct") / 100 / drawdown
    assert cfg.stake_maximo(bank, drawdown, 500) == pytest.approx(esperado)


def test_peor_bache_significa_stake_mas_pequeno():
    """
    Es la razón de ser del cálculo: dos métodos con el mismo beneficio final no
    admiten el mismo stake si uno sufrió el triple por el camino.
    """
    suave = cfg.stake_maximo(1000, 8, 500)
    duro = cfg.stake_maximo(1000, 30, 500)
    assert duro < suave


def test_sin_muestra_suficiente_no_hay_stake_maximo():
    """
    Con pocos picks el drawdown no dice cuál es el peor bache del método, solo
    cuál ha sido hasta ahora. Publicar un tope con eso invita a apostar de más.
    """
    minimo = cfg.stats("min_sample_for_risk")
    assert cfg.stake_maximo(1000, 5, minimo - 1) is None
    assert cfg.stake_maximo(1000, 5, minimo) is not None


def test_un_metodo_que_nunca_ha_caido_no_publica_maximo():
    """Sin bache que medir no hay tope que calcular: dividir por cero daría un
    stake infinito, que es justo el consejo contrario al que toca."""
    assert cfg.stake_maximo(1000, 0, 500) is None


def test_el_recomendado_nunca_supera_al_maximo_en_un_metodo_normal():
    """El recomendado es prudente por definición. Si con un drawdown corriente
    superase al tope de riesgo, la calculadora se contradiría a sí misma."""
    bank = 1000
    assert cfg.stake_recomendado(bank) <= cfg.stake_maximo(bank, 10, 500)


def test_los_dos_cargadores_calculan_el_mismo_stake():
    """La calculadora corre en el navegador. Si el JS y el Python no dan lo
    mismo, la web recomienda una cantidad que el resto del sistema no avala."""
    if NODE is None:
        if os.environ.get("CI"):
            pytest.fail("node no está disponible en CI; la guarda de paridad no se ejecutaría")
        pytest.skip("node no está disponible (en CI sería un fallo)")

    casos = [(1000, 10, 500), (250, 3.5, 500), (5000, 27.4, 500),
             (100, 10, 5), (1000, 0, 500)]
    guion = (
        "const fs=require('fs');const pc=require(process.env.PC_JS);"
        "pc.init({product:JSON.parse(fs.readFileSync(process.env.PC_PRODUCT,'utf8'))});"
        "const cs=JSON.parse(fs.readFileSync(0,'utf8'));"
        "process.stdout.write(JSON.stringify(cs.map(c=>["
        "pc.stakeRecomendado(c[0]),pc.stakeMaximo(c[0],c[1],c[2])])));"
    )
    proceso = subprocess.run(
        [NODE, "-e", guion], input=json.dumps(casos),
        env={**os.environ, "PC_JS": str(SHARED / "product_config.js"),
             "PC_PRODUCT": str(SHARED / "product.json")},
        capture_output=True, text=True, timeout=60, check=True,
    )
    obtenido = json.loads(proceso.stdout)
    esperado = [[cfg.stake_recomendado(b), cfg.stake_maximo(b, d, n)] for b, d, n in casos]
    for caso, (rec_js, max_js), (rec_py, max_py) in zip(casos, obtenido, esperado):
        assert rec_js == pytest.approx(rec_py), f"stake recomendado difiere en {caso}"
        if max_py is None:
            assert max_js is None, f"js publica un tope donde python no lo hace: {caso}"
        else:
            assert max_js == pytest.approx(max_py), f"stake máximo difiere en {caso}"
