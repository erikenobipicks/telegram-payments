# SYSTEM AUDIT — Ecosistema Erikenobi Picks

> Auditoría técnica del estado actual de los tres proyectos antes de implementar
> el funnel de prueba gratuita de 7 días. **No se ha modificado código** durante
> esta auditoría. Fecha: 2026-06-15.

## 0. Resumen ejecutivo

El ecosistema ya está mucho más avanzado de lo que un diseño "desde cero"
asumiría. Tres servicios desplegados en Railway, cada uno con su propia base de
datos PostgreSQL:

| Proyecto | Rol | Estado relevante para el funnel |
|----------|-----|---------------------------------|
| `telegram-payments` (`premium_bot.py`) | **Fuente de verdad de usuarios y acceso premium** | Ya tiene tabla `users`, `trials`, estados, invitación/expulsión de canales, deep-links `/start`, RGPD y job de expiración horario. Trial **= 3 días**. |
| `erikenobi-telegram-bot` | Análisis de picks + estadísticas + publicación Telegram/Instagram | Ya calcula stats reales (strike, profit, ROI), exporta JSON a la landing, genera/publica stories IG (DRY-RUN), promociona el canal FREE. |
| `erikenobi_picks_landing` | Landing/linktree (Flask) | Linktree estático con deep-links al bot. Sin UTM, sin analytics, sin +18, sin mención de "7 días". |

**Conclusión:** el trabajo NO es construir un funnel nuevo, sino **extender,
unificar y medir** lo que ya existe. La mayor parte de la "fuente de verdad" ya
vive en `telegram-payments`.

---

## 1. `telegram-payments` — Bot de pagos / acceso premium

### 1.1 Qué hace
Gestiona suscripciones premium: planes de pago (GOLES, CORNERS, COMBO,
PREPARTIDO), control de acceso a 3 canales de Telegram, prueba gratuita,
referidos, encuestas de satisfacción y auditoría. **Pago manual** (el usuario
envía captura, el admin aprueba con un botón).

### 1.2 Stack
- `python-telegram-bot[job-queue]==21.6`, `psycopg[binary]==3.2.13`, `psycopg-pool==3.2.6`.
- **PostgreSQL** vía `ConnectionPool` (min 1, max 10).
- Arranque por **polling** (`run_polling`), sin Procfile (ejecuta `python premium_bot.py`).
- CI con pytest + ruff (`.github/workflows/ci.yml`).

### 1.3 Variables de entorno
| Var | Uso |
|-----|-----|
| `BOT_TOKEN` | Token Telegram (requerido) |
| `DATABASE_URL` | Postgres de usuarios/pagos (requerido) |
| `PICKS_DATABASE_URL` | DB de solo lectura del bot de picks, para stats reales |
| `BOT_USERNAME` | Username para enlaces de referido (se autoresuelve si falta) |
| `RAILWAY_GIT_COMMIT_SHA/MESSAGE`, `RAILWAY_DEPLOYMENT_ID` | Logging de deploy |

**Hardcodeado en `config.py`** (debería externalizarse): `ADMIN_IDS=[9330181]`,
IDs de canal, `BIZUM="+34660426660"`, URLs de Stripe/PayPal/Revolut,
`PLAN_DAYS`, `TRIAL` (3 días), `INVITE_EXPIRY_HOURS=1`,
`CHECK_EXPIRATIONS_EVERY_SECONDS=3600`, `TIMEZONE=Europe/Madrid`.

### 1.4 Esquema de base de datos (fuente de verdad de usuarios)
| Tabla | Propósito | Campos clave |
|-------|-----------|--------------|
| `users` | Suscriptores | `telegram_user_id` (PK), `username`, `full_name`, `plan`, `fecha_inicio`, `fecha_fin`, `estado` (`activo`/`caducado`/`cancelado`/`reembolsado`), `created_at`, `updated_at`, `acceso_revocado`, `motivo_baja` |
| `trials` | Antiduplicado de prueba | `telegram_user_id` (PK), `plan`, `used_at` |
| `pending_payments` | Pagos por validar | `telegram_user_id` (PK), `plan`, `created_at` |
| `pending_access` | Aprobados pendientes de enlace | `telegram_user_id` (PK), `plan`, `approved_at`, `generaciones`, `ultimos_enlaces` (JSON) |
| `bot_visitors` | Primer contacto | `telegram_user_id` (PK), `first_seen_at` |
| `referrals` | Referidos | `referred_user_id` (PK), `referrer_user_id`, `estado`, `rewarded_at` |
| `encuestas` | Feedback de bajas | `telegram_user_id` (PK), `razon`, `valoracion`, `sugerencia` |
| `audit_log` | **Tabla de eventos** (BIGSERIAL) | `created_at`, `event`, `actor_id`, `actor_tipo`, `target_user_id`, `plan`, `fecha_fin`, `detalle` |

### 1.5 Identificación de usuario
`telegram_user_id` (BIGINT) como PK en todas las tablas. `username`/`full_name`
opcionales. Deep-link de referido: `/start ref<referrer_id>`.

### 1.6 Comandos
- **Usuario:** `/start`, `/help`, `/whoami`, `/referido`, `/privacidad`, `/borrar_datos`.
- **Callbacks:** `info`, `stats`, `guia`, `free`, planes (`goles/corners/combo/pre`), `trial:PLAN`, métodos de pago, `obtener_acceso`, encuesta (`enc:*`), borrado (`borrar:*`).
- **Admin (`ADMIN_IDS`):** `/aprobar`, `/rechazar`, `/estado`, `/historial`, `/auditoria`, `/listar`, `/pendientes`, `/caducan`, `/activos`, `/trials`, `/encuestas`, `/renovar`, `/regalar`, `/link`, `/expulsar`, `/reexpulsar`, `/desbanear`, `/cancelar`, `/reembolsar`, `/promo_referidos`, `/debugpremium`.

### 1.7 Acceso premium
- **Concesión:** `create_chat_invite_link(creates_join_request=True)` (válido 1h) + auto-aprobación en `ChatJoinRequestHandler` validando suscripción.
- **Revocación:** job `check_expirations` (cada 1h) detecta `fecha_fin < hoy`, hace `ban_chat_member`→`unban_chat_member` (expulsa sin ban permanente) y marca `estado='caducado'`, `acceso_revocado=TRUE`.
- **Canales (config.py):** `CANAL_GOLES_ID=-1003895151594`, `CANAL_CORNERS_ID=-1003818905455`, `CANAL_PRE_ID=-1003837149453`.

### 1.8 Trial existente
- Tabla `trials` + `has_used_trial()` + `es_trial_actual()`. **Duración: 3 días.**
- En `trial:PLAN` se bloquea si `has_used_trial()` es verdadero. **Una sola vez por usuario.**

### 1.9 Jobs
- `check_expirations` (1h): avisos de expiración (-3/-2/-1/0 días), expulsión, reintentos, encuestas.
- `limpiar_pending_payments_antiguos` (7 días).

### 1.10 Tests
`tests/test_pure.py`: lógica pura (strike, planes, canales, rate-limit, teclados, RGPD). Sin tests de BD ni async.

---

## 2. `erikenobi-telegram-bot` — Bot de picks y estadísticas

### 2.1 Qué hace
Escucha picks en un canal origen, los filtra (strike, racha, ráfaga,
duplicados, blacklist NG1), los registra, publica en canales por mercado y en el
canal FREE, genera stories de Instagram y resuelve resultados (manual + APIs
SofaScore/API-Football). Calcula estadísticas y publica resúmenes automáticos.

### 2.2 Stack
- `python-telegram-bot[job-queue]==22.7`, `psycopg[binary,pool]>=3.1`, `aiohttp`, `requests`, `Pillow`, `cloudinary`, `openpyxl`.
- **PostgreSQL** (DB independiente de la de pagos). Polling. Sin Procfile.

### 2.3 Variables de entorno
`BOT_TOKEN`, `DATABASE_URL`, `IG_USER_ID`, `IG_ACCESS_TOKEN`, `CLOUDINARY_URL`,
`API_FOOTBALL_KEY`, y overrides opcionales de canales
(`CANAL_APUESTAS_ID`, `CANAL_STORIES_ID`, `CANAL_RECORDATORIO_ID`).
Flags: `STORIES_DRY_RUN=true`, `STORIES_PUBLICAR=false`, `APUESTAS_DRY_RUN=true`.
Config trial/promo: `TRIAL_DIAS=3`, `FREE_CTA_CADA=3`, `FREE_CTA_TRIAL_PROB=0.4`,
`FREE_PROMO_HORAS=[20]`.

> ⚠️ **IDs de canal hardcodeados** en `config.py`. Y **no coinciden** con los de
> `telegram-payments` (ver §4, riesgo crítico).

### 2.4 Esquema de base de datos
- `picks`: tabla principal — `codigo`, `tipo_pick` (`gol`/`corner`), `periodo_codigo`, `modo_codigo`, `linea_codigo`, `liga`, `partido`, `strike_*`, `resultado` (`HIT`/`MISS`/`VOID`/NULL), `odds`, `nivel`, `sistema`, `stake`, `fecha`, `enviado_a_free`, etc. Bien indexada.
- `resumen_control` (clave-valor): antiduplicado de resúmenes.
- `free_state` (clave-valor): contadores diarios del canal FREE.
- Referenciadas pero **no creadas en `init_db`**: `estrategia_config`, `estrategia_liga_stats` (riesgo, §4).
- **No hay tabla de usuarios** — correcto: los usuarios viven en `telegram-payments`.

### 2.5 Estadísticas
- `construir_resumen()`: total, hits, miss, voids, pendientes, strike % = HIT/(HIT+MISS+VOID), profit (cuota fija 1.70 en LIVE; **odds reales en PRE**), ROI.
- Desglose por tipo (gol/corner), por liga, por código/estrategia, por periodo.
- Resúmenes automáticos diario/semanal/mensual/anual + PRE, con compuertas antiduplicado.
- NG1: clasificación dinámica de ligas por Empirical Bayes (ELITE→DESCARTE), proceso nocturno 05:00 UTC.

### 2.6 Telegram
Publica en canales por mercado (goles/corners/general), PRE, y **FREE**
(`CANAL_FREE_ID=-1002973101273`) respetando horario (10–22 Madrid) y cupos.
`promo.py`/`free.py` ya generan CTA de captación y **menciones de trial** en el
canal FREE.

### 2.7 Instagram / Meta
- `instagram_story.py`: genera PNG 1080×1920 con Pillow (marca Erikenobi).
- `instagram_publish.py`: sube a Cloudinary → crea contenedor `STORIES` → `media_publish` (Graph API). **Por defecto DRY-RUN** (solo preview a Telegram). Se activa con `STORIES_PUBLICAR=true` + credenciales.

### 2.8 Export a landing
`export_landing_data.py` genera `landing-data.json` con stats por tipo, PRE_O25FT
(profit/ROI por mes) y últimos picks. `sync_landing_data.ps1` lo commitea/pushea
al repo de landing. **Es el puente datos→landing existente.**

### 2.9 Jobs
~16 jobs (resúmenes live y PRE, promo FREE, recordatorios, auto-resultado,
proceso nocturno NG1, flush de estado). Compuertas por zona horaria + persistencia.

---

## 3. `erikenobi_picks_landing` — Landing

### 3.1 Qué hace
Linktree estático servido por **Flask + gunicorn** (Railway, Procfile).
Rutas: `GET /` (index.html), `GET /<archivo>` (whitelist), `GET /healthz`.
Env: `SECRET_KEY`, `PORT`.

### 3.2 Contenido
6 tarjetas → `t.me/erikenobi_premiumbot?start=free|goles|corners|combo|prepartido`
y `t.me/erikenobi` (contacto). Copy: "Empieza gratis en el bot". SEO básico OK
(title, description, OG, Twitter card, favicon, responsive).

### 3.3 Carencias para el funnel
- ❌ Sin "prueba 7 días gratis" explícita (solo "canal free").
- ❌ Sin analytics ni tracking de clics.
- ❌ Sin captura ni propagación de UTM hacia el deep-link de Telegram.
- ❌ Sin +18 / juego responsable (**riesgo legal**).
- ❌ No consume `landing-data.json` (no muestra stats reales aunque existan).

---

## 4. Riesgos transversales (priorizados)

| # | Riesgo | Severidad | Dónde |
|---|--------|-----------|-------|
| R1 | ~~IDs de canal cruzados entre repos~~ **DESCARTADO** tras verificación directa: ambos repos tienen `GOLES=-1003818905455` y `CORNERS=-1003895151594`. Coinciden; no hay inversión (era un error de lectura de la auditoría inicial). | ✅ Resuelto | `telegram-payments/config.py:21-22` y `erikenobi-telegram-bot/config.py:22-23` |
| R2 | **Pago sin webhook ni verificación de firma**: aprobación manual → se puede conceder premium sin ingreso real; no hay sincronización con pasarela. | 🔴 Alto | `telegram-payments/premium_bot.py` |
| R3 | **Trial = 3 días, no 7** (objetivo del proyecto). Cambiarlo afecta a usuarios vivos y a la copy del canal FREE. | 🟡 Alto | `config.py` (ambos bots) |
| R4 | **Sin tracking de origen (UTM/source)**: no se puede saber si un alta vino de Instagram, canal free o landing. El deep-link solo distingue plan, no campaña. | 🟡 Alto | Los tres repos |
| R5 | **Tablas `estrategia_config`/`estrategia_liga_stats` no creadas en `init_db`**: NG1 cae a fallback silencioso. | 🟡 Medio | `erikenobi-telegram-bot/db.py` |
| R6 | **Secretos/identificadores hardcodeados** (Bizum, ADMIN_IDS, canales, URLs de pago). | 🟡 Medio | `config.py` (ambos bots) |
| R7 | **Sin +18 / juego responsable** en landing y mensajes. | 🟡 Medio (legal) | landing + bots |
| R8 | **Trial repetible si se borra el registro** (`trials` por RGPD) o si el usuario crea otra cuenta Telegram. | 🟢 Bajo | `telegram-payments` |
| R9 | **Profit/ROI LIVE a cuota fija 1.70** puede no reflejar la cuota real. Marketing debe usar las métricas correctas (PRE usa odds reales). | 🟢 Bajo | `erikenobi-telegram-bot/estadisticas.py` |
| R10 | **Dos bots, dos DBs, dos polling loops**: el bot de picks no debe conceder acceso; debe seguir siendo solo lectura sobre usuarios. | 🟢 Bajo (arquitectura) | Ecosistema |

---

## 5. Qué ya existe vs. qué falta (mapa para el funnel de 7 días)

| Requisito del objetivo | Estado actual | Acción |
|------------------------|---------------|--------|
| Fuente de verdad de usuarios | ✅ `telegram-payments` (`users`+`audit_log`) | Extender, no recrear |
| Estados de acceso | 🟡 4 estados, sin `free`/`trial_active`/`trial_expired`/`premium_*` explícitos | Mapear estados (ver ARCHITECTURE) |
| Trial 7 días | 🟡 Existe pero 3 días | Parametrizar a `TRIAL_DAYS=7` |
| Una sola prueba por usuario | ✅ `has_used_trial()` | Mantener |
| Acceso premium automático en trial | ✅ `trial:PLAN` | Verificar y extender a 7d |
| Retirar acceso al expirar | ✅ `check_expirations` | Mantener |
| Landing → bot con tracking | 🟡 Deep-link sí, UTM no | Añadir UTM + start param compuesto |
| Stats reales de picks | ✅ Completo | Reusar para marketing |
| Generador de posts Instagram | 🟡 Stories sí; captions/variantes no | Añadir módulo de texto |
| Promos en canal free | ✅ Existe | Añadir variantes + tracking + dedupe reforzado |
| Tracking/métricas de origen | ❌ | Implementar (ver METRICS) |
| +18 / juego responsable | ❌ | Implementar |

---

## 6. Propuesta técnica (resumen, detalle en `TRIAL_FUNNEL_ARCHITECTURE.md`)

1. **`telegram-payments` sigue siendo la única fuente de verdad.** El bot de
   picks NUNCA concede/retira acceso; solo lee stats.
2. **Parametrizar el trial** vía `TRIAL_DAYS` (default 7) en lugar de constante 3.
3. **Modelo de estados** unificado mapeado sobre la tabla `users` existente
   (sin migración destructiva; columnas nuevas + vista de estado derivado).
4. **Tracking de origen**: nuevas columnas `source/medium/campaign/content/start_param`
   en `users`/`bot_visitors`, alimentadas por el deep-link `/start`.
5. **Landing**: añadir sección "7 días gratis", captura+propagación de UTM al
   deep-link, analytics, +18/juego responsable, y opcionalmente consumir
   `landing-data.json`.
6. **Marketing**: módulo en el bot de picks que produce captions de Instagram y
   mensajes para el canal FREE **solo con datos confirmados**, reusando las
   estadísticas existentes.
7. **Idempotencia y trazabilidad**: todo cambio de estado pasa por `audit_log`.

> La implementación se hará por bloques funcionales en commits separados, en la
> rama de trabajo designada. Antes de tocar comportamiento vivo (trial 3→7, IDs
> de canal, pasarela de pago) se confirmarán las decisiones con el responsable.
