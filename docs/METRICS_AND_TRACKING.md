# METRICS & TRACKING — Origen, embudo y conversión

> Cómo saber de dónde viene cada usuario y medir el funnel trial→pago. Parte del
> estado auditado: hoy **no hay tracking de origen** (R4) ni analytics en la
> landing.

## 1. Dimensiones de tracking

Cada alta debe registrar (en `users` / `bot_visitors`, ver ARCHITECTURE §3):

| Campo | Origen | Ejemplo |
|-------|--------|---------|
| `source` | 1er token del start param o `utm_source` | `instagram`, `free_channel`, `landing`, `direct` |
| `medium` | `utm_medium` | `social`, `telegram`, `bio_link` |
| `campaign` | `utm_campaign` | `trial_7d`, `julio_resultados` |
| `content` | `utm_content` | `story_acierto`, `post_educativo` |
| `start_param` | crudo recibido en `/start` | `trial_7d__src-instagram__cmp-julio` |
| `landing_url` | (si pasa por landing) querystring | `https://landing/oferta?utm_...` |
| `created_at` | timestamp | `2026-06-15T10:00:00Z` |

## 2. Cadena de propagación de UTM

```
Instagram bio / post  ─►  Landing (?utm_source=instagram&utm_campaign=trial_7d)
                              │  script.js lee querystring y reescribe el href
                              ▼
                     t.me/erikenobi_premiumbot?start=trial_7d__src-instagram__cmp-trial_7d
                              │  bot parsea start param
                              ▼
                     users.source/utm_* + audit_log(event='signup')
```

- **Landing → bot:** `script.js` captura `window.location.search`, mapea
  `utm_*` a claves cortas y construye el `start=` del enlace de Telegram (límite
  64 chars). Si no hay UTM, usa `DEFAULT_UTM_CAMPAIGN=trial_7d`.
- **Canal FREE → bot:** los CTA del bot de picks (`promo.py`) ya enlazan al bot;
  se les añade `start=trial_7d__src-free_channel__cmp-<promo>`.
- **Instagram directo → bot:** captions usan `start=trial_7d__src-instagram`.

## 3. Eventos del embudo (sobre `audit_log`)

Reutilizamos la tabla `audit_log` existente (no creamos otra). Eventos nuevos:

| event | Cuándo |
|-------|--------|
| `signup` | Primer `/start` con origen (alta en `bot_visitors`). |
| `trial_started` | Activación de prueba 7d. |
| `trial_expired` | Trial caduca sin pago. |
| `payment_pending` | Usuario envía comprobante. |
| `payment_approved` | Admin aprueba / webhook confirma. |
| `subscription_renewed` | Renovación. |
| `access_revoked` | Expulsión por expiración. |
| `cancelled` / `refunded` | Baja. |

## 4. Consultas requeridas (KPIs)

Se exponen como **comandos admin** en el bot de pagos (ver más abajo) y como
SQL documentado:

```sql
-- Altas por origen (últimos 30 días)
SELECT source, COUNT(*) FROM users
WHERE created_at >= NOW() - INTERVAL '30 days' GROUP BY source ORDER BY 2 DESC;

-- Trials activos
SELECT COUNT(*) FROM users WHERE status = 'trial_active';

-- Ratio trial → pago (cohorte)
WITH t AS (SELECT COUNT(*) trials FROM audit_log WHERE event='trial_started'),
     p AS (SELECT COUNT(DISTINCT target_user_id) pagos FROM audit_log WHERE event='payment_approved')
SELECT p.pagos::float / NULLIF(t.trials,0) AS ratio_trial_pago FROM t, p;

-- Trials caducados sin conversión
SELECT COUNT(*) FROM users WHERE status='trial_expired';

-- Premium activos por plan
SELECT plan, COUNT(*) FROM users WHERE status='premium_active' GROUP BY plan;
```

## 5. Comandos admin de métricas (a añadir)

Complementan los existentes (`/trials`, `/activos`, `/listar`):

| Comando | Devuelve |
|---------|----------|
| `/funnel` | Resumen: visitas→trials→pagos→activos + ratios. |
| `/origen [dias]` | Altas agrupadas por `source`. |
| `/origen_instagram` | Usuarios con `source='instagram'`. |
| `/origen_free` | Usuarios con `source='free_channel'`. |
| `/conversiones` | Trials que pasaron a pago + ratio. |

Protegidos por `TELEGRAM_ADMIN_IDS` (env), idempotentes y de solo lectura.

## 6. Privacidad / RGPD en el tracking

- Solo se guardan datos **mínimos y funcionales** (id de Telegram, username,
  origen). No se guardan datos sensibles ni de pago en claro.
- El borrado RGPD (`/borrar_datos`) debe purgar también los campos UTM/source.
- `start_param` se guarda tal cual llega; no debe contener PII (solo claves de
  campaña).

## 7. Qué falta hoy y queda pendiente

- ❌ Analytics en landing (GA4 / Plausible) — opcional, configurable por env
  (`ANALYTICS_ID`), sin exponer en código.
- ❌ Atribución multi-touch (solo se captura el **primer** origen, suficiente
  para empezar).
- ❌ Dashboard visual — de momento, comandos admin + SQL.
