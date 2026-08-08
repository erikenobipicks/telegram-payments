"""
Tests unitarios de la lógica PURA del bot (sin DB ni red).

Importar `premium_bot` no abre conexiones: init_pool()/init_db()/main() solo se
llaman en el arranque real, no al importar. Estos tests cubren cálculo de
strike, normalización de planes, mapeo de canales, enlaces de referido, rate
limiting en memoria, formateo y construcción de teclados.
"""
import datetime as dt

import pytest

import premium_bot as p


# ──────────────────────────────────────────────────────────────────────────
# calcular_strike
# ──────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("hits,misses,esperado", [
    (7, 3, 70.0),
    (8, 2, 80.0),
    (0, 0, 0.0),     # sin resueltos → 0, no división por cero
    (1, 0, 100.0),
    (1, 2, 33.3),    # redondeo a 1 decimal
])
def test_calcular_strike(hits, misses, esperado):
    assert p.calcular_strike(hits, misses) == esperado


# ──────────────────────────────────────────────────────────────────────────
# canonical_plan
# ──────────────────────────────────────────────────────────────────────────
def test_canonical_plan_alias():
    assert p.canonical_plan("pre_o25") == "pre"


@pytest.mark.parametrize("plan", ["goles", "corners", "combo", "pre", "total"])
def test_canonical_plan_identidad(plan):
    assert p.canonical_plan(plan) == plan


def test_canonical_plan_none():
    assert p.canonical_plan(None) is None


# ──────────────────────────────────────────────────────────────────────────
# get_plan_channels
# ──────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("plan,n", [
    ("goles", 1), ("corners", 1), ("pre", 1),
    ("combo", 2), ("total", 3), ("desconocido", 0),
])
def test_get_plan_channels_cantidad(plan, n):
    assert len(p.get_plan_channels(plan)) == n


def test_get_plan_channels_combo_ids():
    ids = {cid for _, cid in p.get_plan_channels("combo")}
    assert ids == {p.CANAL_GOLES_ID, p.CANAL_CORNERS_ID}


def test_get_plan_channels_total_incluye_pre():
    ids = {cid for _, cid in p.get_plan_channels("total")}
    assert p.CANAL_PRE_ID in ids


def test_get_plan_channels_alias():
    # pre_o25 se normaliza a pre dentro de get_plan_channels
    assert len(p.get_plan_channels("pre_o25")) == 1


# ──────────────────────────────────────────────────────────────────────────
# parse_date
# ──────────────────────────────────────────────────────────────────────────
def test_parse_date():
    assert p.parse_date("2026-06-08") == dt.date(2026, 6, 8)


# ──────────────────────────────────────────────────────────────────────────
# referral_link
# ──────────────────────────────────────────────────────────────────────────
def test_referral_link_con_username(monkeypatch):
    monkeypatch.setattr(p, "BOT_USERNAME", "miBot")
    assert p.referral_link(12345) == "https://t.me/miBot?start=ref12345"


def test_referral_link_sin_username(monkeypatch):
    monkeypatch.setattr(p, "BOT_USERNAME", None)
    assert p.referral_link(12345) is None


# ──────────────────────────────────────────────────────────────────────────
# rate_limited
# ──────────────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _reset_rate_buckets():
    p._rate_buckets.clear()
    yield
    p._rate_buckets.clear()


def test_rate_limited_admin_exento():
    admin_id = p.ADMIN_IDS[0]
    # Muchas llamadas seguidas: el admin nunca se bloquea.
    assert all(p.rate_limited(admin_id, "start") is False for _ in range(50))


def test_rate_limited_bloquea_tras_limite():
    uid = 424242  # no admin
    maximo, _ = p.RATE_LIMITS["start"]
    # Las primeras `maximo` pasan, la siguiente se bloquea.
    for _ in range(maximo):
        assert p.rate_limited(uid, "start") is False
    assert p.rate_limited(uid, "start") is True


def test_rate_limited_accion_sin_limite():
    assert p.rate_limited(424242, "accion_inexistente") is False


# ──────────────────────────────────────────────────────────────────────────
# _get_strike_tipo
# ──────────────────────────────────────────────────────────────────────────
def test_get_strike_tipo_ultimo_mes():
    stats = {"ultimo_mes": {"gol": {"hits": 7, "misses": 3}}, "globales": {}}
    res = p._get_strike_tipo(stats, "gol")
    assert res is not None and res.endswith("%") and "\\." in res  # punto escapado MarkdownV2


def test_get_strike_tipo_fallback_global():
    stats = {"ultimo_mes": {}, "globales": {"corner": {"hits": 8, "misses": 2}}}
    assert p._get_strike_tipo(stats, "corner") is not None


def test_get_strike_tipo_sin_datos():
    assert p._get_strike_tipo(None, "gol") is None
    assert p._get_strike_tipo({"ultimo_mes": {}, "globales": {}}, "gol") is None


# ──────────────────────────────────────────────────────────────────────────
# _instrucciones_renovacion
# ──────────────────────────────────────────────────────────────────────────
def test_instrucciones_renovacion_total_es_manual():
    txt = p._instrucciones_renovacion("total")
    assert "@erikenobi" in txt and "manual" in txt.lower()


def test_instrucciones_renovacion_incluye_precio():
    txt = p._instrucciones_renovacion("goles")
    assert p.PRECIO_GOLES in txt and p.BIZUM in txt


# ──────────────────────────────────────────────────────────────────────────
# Teclados (markups)
# ──────────────────────────────────────────────────────────────────────────
def _callbacks(markup):
    return [b.callback_data for row in markup.inline_keyboard for b in row if b.callback_data]


def test_menu_markup_tiene_referido_y_planes():
    cbs = _callbacks(p.menu_markup())
    assert "referido" in cbs
    assert "privacidad" in cbs
    for plan in ("goles", "corners", "combo", "pre"):
        assert plan in cbs


def test_pago_markup_callbacks():
    cbs = _callbacks(p.pago_markup("goles"))
    assert "trial:goles" in cbs
    assert "bizum:goles" in cbs
    assert "revolut:goles" in cbs


def test_admin_approval_markup_incluye_user_id():
    cbs = _callbacks(p.admin_approval_markup(999))
    assert "approve:goles:999" in cbs
    assert "reject:999" in cbs


def test_admin_approval_markup_incluye_pinpon():
    cbs = _callbacks(p.admin_approval_markup(999))
    assert "approve:pinpon:999" in cbs


def test_pago_markup_pinpon_ofrece_prueba():
    # Ping pong (50€) ahora ofrece prueba gratis: métodos manuales presentes
    # y botón de trial disponible.
    cbs = _callbacks(p.pago_markup("pinpon"))
    assert "bizum:pinpon" in cbs
    assert "revolut:pinpon" in cbs
    assert "trial:pinpon" in cbs
    assert "pinpon" in p.TRIAL_PLANS


def test_pinpon_metricas_calculo():
    # jugadas = E1/E3/FALLO; aciertos = E1+E3; pct y roi derivados.
    row = {"jugadas": 25, "aciertos": 13, "staked": 50.0, "unidades": 6.15}
    m = p._pinpon_metricas(row)
    assert m["jugadas"] == 25
    assert m["aciertos"] == 13
    assert m["pct"] == 52.0
    assert m["unidades"] == 6.15
    assert m["roi"] == 12.3  # 6.15 / 50 * 100


def test_pinpon_metricas_sin_jugadas_no_divide_por_cero():
    m = p._pinpon_metricas({"jugadas": 0, "aciertos": 0, "staked": 0, "unidades": 0})
    assert m["pct"] == 0.0 and m["roi"] == 0.0


def test_pinpon_stats_hay_datos():
    assert p._pinpon_stats_hay_datos({"global": {"jugadas": 3}}) is True
    assert p._pinpon_stats_hay_datos({"global": {"jugadas": 0}}) is False
    assert p._pinpon_stats_hay_datos(None) is False


def test_v2_escapa_markdown():
    # Los valores dinámicos (%, +, ., -) deben quedar escapados para MarkdownV2.
    assert p._v2("+38.31u") == "\\+38\\.31u"
    assert p._v2("52.0%") == "52\\.0%"


def test_get_plan_channels_pinpon():
    # Con CANAL_PINPON_ID configurado (por defecto el canal OFICIAL), el plan
    # devuelve exactamente ese canal. Si estuviera a 0, devolvería [] (guard
    # para no operar sobre un chat_id inválido).
    canales = p.get_plan_channels("pinpon")
    if p.CANAL_PINPON_ID:
        assert len(canales) == 1
        assert canales[0][1] == p.CANAL_PINPON_ID
    else:
        assert canales == []


def test_volver_y_acceso_markups():
    assert "menu" in _callbacks(p.volver_markup())
    assert "obtener_acceso" in _callbacks(p.acceso_listo_markup())


# ──────────────────────────────────────────────────────────────────────────
# _formatear_evento
# ──────────────────────────────────────────────────────────────────────────
# ──────────────────────────────────────────────────────────────────────────
# RGPD — aviso de privacidad y borrado
# ──────────────────────────────────────────────────────────────────────────
def test_texto_privacidad_contenido():
    txt = p._texto_privacidad()
    assert "/borrar_datos" in txt
    assert "@erikenobi" in txt
    # menciona los derechos básicos del RGPD
    assert "borrado" in txt.lower()


def test_markups_borrado():
    assert "borrar:pedir" in _callbacks(p._privacidad_markup())
    assert "borrar:confirm" in _callbacks(p._confirmar_borrado_markup())


def test_formatear_evento():
    row = {
        "created_at": dt.datetime(2026, 6, 8, 10, 30),
        "event": "aprobacion",
        "actor_id": 1, "actor_tipo": "admin",
        "target_user_id": 555, "plan": "corners",
        "fecha_fin": dt.date(2026, 7, 8), "detalle": "via /aprobar",
    }
    linea = p._formatear_evento(row, incluir_target=True)
    assert "aprobacion" in linea
    assert "corners" in linea
    assert "555" in linea


# ──────────────────────────────────────────────────────────────────────────
# parse_start_param (deep-link de /start: intención + UTM + referido)
# ──────────────────────────────────────────────────────────────────────────
def test_parse_start_vacio():
    r = p.parse_start_param("")
    assert r["intent"] == "start"
    assert r["source"] is None
    assert r["referral_code"] is None


def test_parse_start_none():
    r = p.parse_start_param(None)
    assert r["intent"] == "start"


def test_parse_start_solo_intent():
    assert p.parse_start_param("trial_7d")["intent"] == "trial_7d"
    assert p.parse_start_param("free")["intent"] == "free"
    assert p.parse_start_param("goles")["intent"] == "goles"


def test_parse_start_ref_legado():
    r = p.parse_start_param("ref123")
    assert r["referral_code"] == "123"
    assert r["intent"] == "ref"


def test_parse_start_utm_source():
    r = p.parse_start_param("trial_7d__src-instagram")
    assert r["intent"] == "trial_7d"
    assert r["utm_source"] == "instagram"
    assert r["source"] == "instagram"


def test_parse_start_utm_completo():
    r = p.parse_start_param("trial_7d__src-instagram__cmp-julio__cnt-story__med-social")
    assert r["utm_source"] == "instagram"
    assert r["utm_campaign"] == "julio"
    assert r["utm_content"] == "story"
    assert r["utm_medium"] == "social"


def test_parse_start_ref_nuevo():
    r = p.parse_start_param("trial_7d__src-free_channel__ref-456")
    assert r["referral_code"] == "456"
    assert r["utm_source"] == "free_channel"


def test_parse_start_tokens_invalidos_no_rompen():
    r = p.parse_start_param("trial_7d__basura__src-x__otro")
    assert r["utm_source"] == "x"
    assert r["intent"] == "trial_7d"


def test_trial_days_por_defecto_3():
    # El default debe ser 3 (3 días bastan para un servicio con ~90% de
    # acierto), salvo override por entorno.
    import os
    if "TRIAL_DAYS" not in os.environ:
        assert p.TRIAL_DAYS == 3
