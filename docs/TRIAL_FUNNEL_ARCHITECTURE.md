# TRIAL FUNNEL ARCHITECTURE — Prueba gratuita de 7 días

> Diseño de la sincronización entre los tres proyectos para el funnel de prueba
> gratuita. Parte del estado real auditado en `SYSTEM_AUDIT.md`. **Construye
> sobre lo existente; no reinventa.**

## 1. Principio rector: una única fuente de verdad

```
                          ┌─────────────────────────────────────┐
                          │   telegram-payments (premium_bot)    │
                          │   ►► FUENTE DE VERDAD DE USUARIOS ◄◄  │
                          │   Postgres: users, trials, audit_log │
                          └───────────────▲─────────────────────┘
                                          │ (única que escribe acceso)
   ┌───────────────┐   start=...     ┌────┴─────┐   ban/invite   ┌──────────────┐
   │  Landing      │────deep-link────►│  Bot de  │───────────────►│ Canales TG   │
   │  (Flask)      │   + UTM          │  pagos   │                │ premium      │
   └───────▲───────┘                  └────┬─────┘                └──────────────┘
           │ UTM                            │ lee stats (solo lectura)
           │ stats JSON                     │ PICKS_DATABASE_URL
   ┌───────┴─────────────────────────┐  ┌──▼───────────────────────────────────┐
   │ landing-data.json (export)      │◄─┤ erikenobi-telegram-bot (picks/stats)  │
   │                                 │  │ Postgres: picks, resúmenes, IG, FREE  │
   └─────────────────────────────────┘  └───────────────────────────────────────┘
```

**Reglas de oro:**
- Solo `telegram-payments` **escribe** estado de acceso de usuarios.
- `erikenobi-telegram-bot` es **solo lectura** sobre usuarios (vía
  `PICKS_DATABASE_URL` ya existe el patrón inverso: pagos lee picks). Para
  marketing no necesita escribir usuarios.
- La landing **no tiene backend de usuarios**; delega 100% en el bot vía
  deep-link. Solo añade tracking del lado cliente.

## 2. Modelo de estados

El objetivo pide 7 estados. La tabla `users` actual tiene
`estado ∈ {activo, caducado, cancelado, reembolsado}` + tabla `trials`. En vez
de romper el esquema, **derivamos** el estado canónico del funnel a partir de los
datos existentes + columnas nuevas mínimas.

### 2.1 Estados canónicos
| Estado | Definición derivada |
|--------|---------------------|
| `free` | Visitante registrado (`bot_visitors`) sin fila en `users` o sin trial/sub. |
| `trial_active` | Fila en `users` cuyo periodo vigente es un trial (`es_trial_actual()` y `fecha_fin >= hoy`). |
| `trial_expired` | Usó trial (`has_used_trial()`), trial caducado y nunca pagó. |
| `premium_active` | `estado='activo'` y `fecha_fin >= hoy` por pago (no trial). |
| `premium_expired` | `estado='caducado'` tras haber sido premium de pago. |
| `cancelled` | `estado='cancelado'`. |
| `banned` | Nuevo: `estado='banned'` (abuso / ban manual). |

> Implementación: función `estado_funnel(user_row) -> str` (lógica pura,
> testeable) + opcionalmente columna materializada `status` actualizada en cada
> transición para consultas rápidas. **No se elimina** la columna `estado`
> actual; `status` es un campo derivado adicional.

### 2.2 Diagrama de transiciones
```
free ──/start trial──► trial_active ──paga──► premium_active
  │                         │                      │
  │                    7 días sin pagar         caduca sub
  │                         ▼                      ▼
  └──(nunca trial)──►  trial_expired         premium_expired
                            │                      │
                            └────── paga ──────────┘──► premium_active
   cualquier estado ──cancela──► cancelled
   cualquier estado ──abuso────► banned
```

## 3. Esquema de datos del usuario (campos requeridos)

El objetivo pide un conjunto mínimo de campos. Mapeo sobre lo existente +
columnas nuevas (todas vía `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, no
destructivo):

| Campo objetivo | Origen | Acción |
|----------------|--------|--------|
| `telegram_user_id` | `users.telegram_user_id` | ✅ existe |
| `telegram_username` | `users.username` | ✅ existe |
| `first_name` | `users.full_name` | ✅ existe (full_name) |
| `source` | — | ➕ `ADD COLUMN source TEXT` |
| `created_at` | `users.created_at` | ✅ existe |
| `trial_started_at` | derivable de `trials.used_at` / `fecha_inicio` | ➕ `ADD COLUMN trial_started_at TIMESTAMPTZ` |
| `trial_ends_at` | `fecha_fin` durante trial | ➕ `ADD COLUMN trial_ends_at TIMESTAMPTZ` |
| `subscription_started_at` | `users.fecha_inicio` (pago) | ➕ `ADD COLUMN subscription_started_at TIMESTAMPTZ` |
| `subscription_ends_at` | `users.fecha_fin` | ✅ (fecha_fin) |
| `status` | derivado §2 | ➕ `ADD COLUMN status TEXT` |
| `payment_provider_customer_id` | — | ➕ `ADD COLUMN payment_provider_customer_id TEXT` |
| `last_payment_at` | de `audit_log`/aprobación | ➕ `ADD COLUMN last_payment_at TIMESTAMPTZ` |
| `last_access_check_at` | job de expiración | ➕ `ADD COLUMN last_access_check_at TIMESTAMPTZ` |
| `utm_source/medium/campaign/content` | start param | ➕ 4 columnas TEXT |
| `referral_code` | `referrals.referrer_user_id` | ✅ existe (vía tabla) |

> **Nota:** se conservan `fecha_inicio`/`fecha_fin`/`estado` porque toda la
> lógica viva depende de ellas. Los campos nuevos son aditivos.

## 4. Reglas obligatorias y cómo se cumplen

| Regla | Mecanismo |
|-------|-----------|
| Trial 1 vez por usuario | `trials` (PK `telegram_user_id`) + `has_used_trial()`. ✅ ya existe. |
| Trial dura exactamente 7 días | `TRIAL_DAYS=7` (env) → `trial_ends_at = trial_started_at + TRIAL_DAYS`. Hoy es 3. |
| Acceso premium automático al iniciar trial | Reutiliza la concesión actual (`generar_enlaces_acceso` + auto-aprobación). |
| Perder acceso al terminar trial sin pago | `check_expirations` ya expulsa al caducar. ✅ |
| Pago antes de fin de trial → `premium_active` | Aprobación extiende `fecha_fin` y marca pago. Hay que setear `status` y `subscription_started_at`. |
| Fallo/cancelación de pago → estado correcto | `/cancelar`, `/reembolsar` + (si hay webhook) evento de pasarela. |
| Todo cambio en `audit_log` | ✅ ya se audita; añadir eventos `trial_started`, `status_change`. |
| Idempotencia | `DELETE ... RETURNING` + `ON CONFLICT` ya usados. Toda transición debe ser idempotente y atómica. |
| Ningún bot da acceso sin consultar la fuente de verdad | El bot de picks no concede acceso. La auto-aprobación valida `usuario_activo_para_canal()`. ✅ |

## 5. Deep-link: contrato `/start`

Hoy `/start` soporta `ref<id>` y planes. Extendemos el parámetro para llevar
**plan + campaña** sin romper lo existente:

```
https://t.me/erikenobi_premiumbot?start=trial_7d__src-instagram__cmp-julio
```

Formato propuesto (compatible hacia atrás): tokens separados por `__`, cada uno
`clave-valor`:
- `trial_7d` (o `free`, `goles`, …): intención/plan (1er token, posicional).
- `src-<utm_source>`, `cmp-<utm_campaign>`, `cnt-<utm_content>`, `med-<utm_medium>`.
- `ref-<referrer_id>` (sustituye al `ref<id>` actual, manteniendo ambos parseos).

El parser (`parse_start_param`) es **lógica pura testeable** y rellena
`source/utm_*`/`referral_code` al registrar el visitante/usuario. Límite de
longitud de Telegram (64 chars) respetado usando claves cortas.

## 6. Flujo end-to-end (criterio de éxito)

1. Usuario ve post de Instagram / mensaje del canal FREE → pulsa link.
2. Llega a la landing (con UTM) **o** directo al bot. La landing propaga UTM al
   deep-link de Telegram.
3. Bot recibe `/start trial_7d__src-...`: registra `bot_visitors` + UTM.
4. Si nunca usó trial → explica, pide confirmación, activa trial 7d, concede
   acceso a canal(es), guarda `trial_started_at`/`trial_ends_at`, `status=trial_active`, audita.
5. Si ya usó trial → ofrece pago.
6. Recibe picks premium durante 7 días.
7. Día -1: aviso de fin de trial + CTA pago (ya hay avisos -3/-2/-1/0).
8. Paga → `premium_active`, fechas de sub, acceso garantizado, audita.
9. No paga → al expirar, `check_expirations` expulsa, `status=trial_expired`,
   mensaje de conversión.

## 7. Decisiones (confirmadas con el responsable)

1. **Trial 3→7 días** → ✅ **Solo nuevos trials**. Implementado vía
   `TRIAL_DAYS=int(os.getenv("TRIAL_DAYS","7"))`; los trials activos conservan su
   `fecha_fin`.
2. **Mapeo de IDs de canal** → ✅ **No requiere cambio**: ambos repos ya
   coinciden (`GOLES=-1003818905455`, `CORNERS=-1003895151594`). El "R1" era un
   error de la auditoría inicial.
3. **Pasarela de pago** → ✅ **Se mantiene manual**. Solo se deja preparado el
   hueco (`payment_provider_customer_id`, `PAYMENT_WEBHOOK_SECRET`).
4. **¿Trial sobre qué plan?** → ⏳ **Pendiente**. De momento, `start=trial_7d`
   muestra un banner de bienvenida y el usuario elige plan (flujo `trial:PLAN`
   existente). No se fija un plan por defecto hasta decidirlo.

### Nota de implementación
El tracking de origen (first-touch) se persiste en `bot_visitors`
(`source`, `utm_*`, `start_param`), no en `users`, para no tocar las
transacciones de alta/trial (menor riesgo sobre código vivo). La atribución de
conversiones se obtiene por `JOIN users ↔ bot_visitors`.

## 8. Orden de implementación propuesto (commits separados)

1. `feat(db): columnas de tracking y estado funnel (no destructivo)`
2. `feat(trial): parametrizar duración con TRIAL_DAYS (default 7)`
3. `feat(start): parser de deep-link con UTM/source y referido`
4. `feat(landing): sección 7 días gratis + UTM + +18 + analytics`
5. `feat(stats): API de resúmenes para marketing (solo datos confirmados)`
6. `feat(marketing): generador de captions Instagram (variantes)`
7. `feat(free): promos programadas con tracking y dedupe`
8. `feat(admin): comandos de métricas de funnel (source→trial→pago)`
9. `docs + .env.example`
10. `tests`

Cada bloque se valida con `pytest`/`ruff` antes de continuar.
