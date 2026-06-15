# TEST PLAN — Funnel de prueba de 7 días

> Estrategia de pruebas para validar el ecosistema. Se apoya en el `pytest`
> existente (`tests/test_pure.py`, CI con ruff+pytest). Prioriza **lógica pura
> testeable** y scripts de validación, sin requerir Telegram real.

## 1. Niveles de prueba

| Nivel | Qué cubre | Herramienta |
|-------|-----------|-------------|
| Unitario (puro) | Parsers, cálculo de estados, fechas de trial, UTM, generadores de texto | `pytest` (sin BD ni red) |
| Integración BD | Transiciones de estado sobre Postgres efímero | `pytest` + Postgres de test (opcional/CI) |
| Validación manual | Flujo real en Telegram de staging | Checklist |

## 2. Casos obligatorios (del objetivo)

| # | Caso | Aserción | Nivel |
|---|------|----------|-------|
| T1 | Usuario nuevo activa trial | `status=trial_active`, `trial_ends_at = inicio + TRIAL_DAYS`, acceso concedido, `audit_log` tiene `trial_started` | Integración |
| T2 | Usuario intenta repetir trial | `has_used_trial()`→ rechazo, **sin** segunda fila en `trials` | Integración |
| T3 | Trial expira sin pago | `check_expirations` → expulsión, `status=trial_expired`, evento `access_revoked` | Integración |
| T4 | Usuario paga durante trial | `status=premium_active`, `subscription_started_at` set, acceso mantenido, `payment_approved` | Integración |
| T5 | Usuario cancela | `status=cancelled`, acceso retirado | Integración |
| T6 | Premium activo conserva acceso | auto-aprobación devuelve OK mientras `fecha_fin>=hoy` | Unitario (`usuario_activo_para_canal`) |
| T7 | Bot de picks genera stats reales | resumen calculado coincide con fixtures de `picks`; nunca inventa | Unitario |
| T8 | Generador Instagram no inventa | si datos insuficientes → devuelve plantilla **educativa**, no de resultados | Unitario |
| T9 | Promo canal free no se duplica | dedupe por clave en `resumen_control`/equivalente; doble llamada = 1 envío | Unitario/Integración |
| T10 | UTM se guarda correctamente | `parse_start_param('trial_7d__src-instagram__cmp-x')` → source=instagram, campaign=x | Unitario |

## 3. Casos del parser de deep-link (T10 ampliado)

```
parse_start_param("free")                         -> intent=free,  source=direct
parse_start_param("trial_7d")                     -> intent=trial_7d
parse_start_param("trial_7d__src-instagram")      -> source=instagram
parse_start_param("trial_7d__src-x__cmp-y__cnt-z")-> source=x, campaign=y, content=z
parse_start_param("ref-123")                      -> referral_code=123  (compat ref123)
parse_start_param("")                             -> intent=start, source=direct
parse_start_param(<65+ chars basura>)             -> no rompe, ignora tokens inválidos
```

## 4. Casos del modelo de estados (lógica pura)

`estado_funnel(row)` con filas sintéticas:
- sin `users` → `free`
- trial vigente → `trial_active`
- trial caducado, sin pago → `trial_expired`
- pago vigente → `premium_active`
- pago caducado → `premium_expired`
- `estado='cancelado'` → `cancelled`
- `estado='banned'` → `banned`

## 5. Casos del generador de marketing (T7/T8)

- `resumen_marketing(periodo='7d')` con fixtures → strike/profit correctos.
- `tiene_datos_suficientes(stats, min_picks=N)` → bool; si `False`, el generador
  produce variante educativa.
- Las variantes (agresiva/elegante/educativa) **no contienen** promesas de
  ganancias (test de blacklist de frases: "dinero seguro", "ganancia garantizada").
- Toda salida de resultados incluye disclaimer +18 cuando corresponde.

## 6. Validación manual (staging)

Checklist previo a producción:
- [ ] Deep-link desde landing llega al bot con UTM correcto.
- [ ] `/start trial_7d` activa 7 días y concede acceso al canal correcto (R1).
- [ ] Aviso de fin de trial llega el día -1.
- [ ] Tras expirar, el usuario es expulsado y recibe CTA de pago.
- [ ] Promo del canal FREE no se repite el mismo día.
- [ ] Story/caption de Instagram usa stats reales del periodo.
- [ ] `/funnel` y `/origen` devuelven números coherentes.

## 7. Comandos de ejecución

```bash
# telegram-payments
cd telegram-payments && python -m pytest -q && ruff check .

# erikenobi-telegram-bot (añadir pytest si no existe)
cd erikenobi-telegram-bot && python -m pytest -q
```

## 8. Pendiente / fuera de alcance inicial

- Mocking completo de la API de Telegram (handlers async) — alto coste, se
  cubre con validación manual de staging.
- Tests E2E reales contra Instagram Graph API — se mantienen en DRY-RUN.
