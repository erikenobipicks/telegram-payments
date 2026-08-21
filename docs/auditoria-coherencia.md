# Auditoría de coherencia — valores de producto

> **Estado:** fase 0 cerrada y resuelta. El bloque
> [Resoluciones](#resoluciones-de-la-fase-0) recoge la decisión tomada para cada
> contradicción; el inventario de abajo se conserva tal cual como registro.
>
> **Fase 0** del trabajo "fuente única de verdad para precios, límites y textos
> de producto". **No se ha modificado ningún valor de producto** al elaborar
> este informe: es exclusivamente un inventario.
>
> Alcance: los tres repositorios del ecosistema.
> Fecha: 2026-08-20. Rama: `claude/product-coherence-audit-f1na8t`.

## Cómo leer este documento

- **Repos:** `bot` = `erikenobi-telegram-bot`, `pagos` = `telegram-payments`,
  `landing` = `erikenobi_picks_landing`.
- Cada tabla lista **fichero:línea**, **valor actual** y **categoría**.
- Las referencias en `docs/` de cada repo se listan aparte cuando son solo
  documentación (no llegan al usuario), pero se incluyen porque también
  contradicen al código y despistan a quien las lee.
- La sección final, **[Contradicciones detectadas](#contradicciones-detectadas)**,
  recoge cada valor con dos o más versiones distintas en el ecosistema.

## Nota previa sobre la topología

El brief asume un solo repositorio (`shared/` "en la raíz"). En realidad son
**tres repos independientes**, desplegados por separado en Railway, sin
submódulos ni paquete compartido. Resuelto en D1: el repo canónico es
**`telegram-payments`** y los otros dos llevan una copia sincronizada por CI, con
un test de guardia que falla si alguna diverge.

---

## 1. Límites del plan gratuito

Hay **dos productos free distintos** y conviene no mezclarlos: el free de
**fútbol** (goles/córners, `CANALES_FREE`) y el free de **ping pong**
(`CANALES_PINPON_FREE`).

### 1.1 Free de fútbol — límites reales (código)

| Fichero:línea | Valor actual | Nota |
|---|---|---|
| `bot/config.py:270` | `MAX_FREE_GOLES = 2` | picks de goles/día |
| `bot/config.py:271` | `MAX_FREE_CORNERS = 2` | picks de córners/día |
| `bot/config.py:272` | `MAX_FREE_TOTAL = 4` | tope combinado/día |
| `bot/config.py:277` | `MAX_FREE_PRE_O25FTOK = 1` | cupo aparte, **no** consume del total |
| `bot/config.py:279` | `FREE_TIMEZONE = "Europe/Madrid"` | |
| `bot/config.py:280` | `FREE_HORA_INICIO = 10` | ventana de publicación |
| `bot/config.py:281` | `FREE_HORA_FIN = 22` | ventana de publicación |
| `bot/free.py:87` | usa `MAX_FREE_TOTAL` | tope diario efectivo |
| `bot/free.py:92-105` | "1 por hora" por tipo | goles y córners con slot horario propio |

**Total real publicable al día en el free de fútbol: hasta 4 picks live + 1 PRE = 5.**

### 1.2 Free de fútbol — lo que dicen los textos

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `bot/promo.py:60` | `"Este es <b>1 pick gratuito</b>."` | límite free |
| `bot/promo.py:62` | `"Esto es solo la muestra..."` | límite free (sin número) |
| `bot/promo.py:108` | `"...sin límite diario."` | frecuencia premium |
| `bot/promo.py:132` | `"Al instante, sin límite diario y antes que nadie"` | frecuencia premium |
| `bot/promo.py:139` | `"llegando <b>sin tope diario</b>"` | frecuencia premium |
| `bot/config.py:303` | `FREE_CTA_CADA = 3` | 1 de cada 3 picks lleva CTA |
| `bot/config.py:307` | `FREE_CTA_TRIAL_PROB = 0.4` | prob. de incluir recordatorio de trial |

### 1.3 Free de ping pong

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `bot/config.py:70` | comentario: "2 picks gratis/día" | límite free |
| `bot/config.py:174` | comentario: "parrilla oculta (hora+liga) + 2 picks gratis/día (desde el 10/8)" | límite free |
| `bot/config.py:177` | `PINPON_FREE_MODO = "completo"` (default) | modo de publicación free |
| `bot/config.py:184` | `PINPON_FREE_TARDE_AUTO = true` | auto-revelado del 2º pick |
| `bot/config.py:167` | `PINPON_OCULTAR_PARTIDOS = false` | anti-copia (oculta emparejamientos) |
| `bot/config.py:191` | `PINPON_FREE_AVISO_FECHA = "2026-08-09"` | fecha del aviso "último día gratis" |
| `bot/config.py:192` | `PINPON_FREE_AVISO_HORA = 12` | |
| `bot/config.py:205` | `PINPON_FREE_HYPE = true` | avisos de "¡Ganada!" en el free |
| `bot/config.py:208` | `PINPON_FREE_HYPE_MIN = 20` | cooldown (min) del bloque balance+CTA |
| `bot/config.py:213` | `PINPON_FREE_EN_GENERAL = true` | replica picks free de PP en el free de fútbol |
| `bot/handlers.py:1362` | docstring: "los 2 picks gratis del día (mañana + tarde)" | límite free |
| `bot/handlers.py:2333` | `"Seguirás teniendo <b>2 picks gratis cada día</b>..."` | límite free |
| `bot/handlers.py:3083` | `"En el free ves solo <b>2 picks al día</b>."` | límite free |
| `bot/handlers.py:3378` | docstring: "comparativo free (2 picks) vs premium" | límite free |
| `bot/handlers.py:3389` | `"🆓 <b>Free (2 picks):</b>"` | límite free |
| `bot/pinpon.py:393` | `"🎁 {hora} — <b>PICK GRATIS</b> · <i>se desvela 5 min antes</i>"` | retardo de revelación |
| `bot/pinpon.py:400` | `"🔒 {hora} — <i>se desvela 5 min antes</i>"` | retardo de revelación |
| `landing/index.html:317` | `"2 picks gratis al día y resultados"` | límite free |

### 1.4 Qué se muestra y qué no (anti-copia)

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `bot/config.py:163-167` | calendario free: hora + liga, **sin nombre del jugador** | qué se muestra |
| `bot/pinpon.py:342` | docstring del mismo comportamiento | qué se muestra |
| `bot/pinpon.py:389-400` | render de la vista free (ocultos vs gratis) | qué se muestra |
| `bot/config.py:174` | "parrilla oculta (hora+liga)" | qué se muestra |

**No existe hoy ningún valor de "retardo de publicación en horas"** en el free:
el único retardo implementado es el "se desvela 5 min antes" del ping pong.
El campo `delayed_publication_hours` del esquema propuesto es, por tanto,
**nuevo** (ver la nota del brief sobre el cambio de estrategia).

---

## 2. Precios

**Cinco planes**: goles, córners, combo, prepartido (Over 2.5 FT), ping pong.
El importe de cada uno está escrito a mano en **cinco sitios distintos**.

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `pagos/config.py:39` | `PRECIO_GOLES = "20€"` | precio |
| `pagos/config.py:40` | `PRECIO_CORNERS = "20€"` | precio |
| `pagos/config.py:41` | `PRECIO_COMBO = "30€"` | precio |
| `pagos/config.py:42` | `PRECIO_PRE = "20€"` | precio |
| `pagos/config.py:43` | `PRECIO_PINPON = "50€"` | precio |
| `pagos/config.py:62` | `PLAN_DAYS = 30` | periodicidad |
| `pagos/keyboards.py:32` | botón `"⚽ GOLES — 20€"` | precio (copia 2) |
| `pagos/keyboards.py:33` | botón `"🚩 CORNERS — 20€"` | precio (copia 2) |
| `pagos/keyboards.py:35` | botón `"🔥 GOLES + CORNERS — 30€"` | precio (copia 2) |
| `pagos/keyboards.py:36` | botón `"📊 PREPARTIDO — 20€"` | precio (copia 2) |
| `pagos/keyboards.py:43` | botón `"🏓 PING PONG — 50€"` | precio (copia 2) |
| `pagos/keyboards.py:61` | `precios = {"goles":"20","corners":"20","combo":"30","pre":"20","pinpon":"50"}` | precio (copia 3, para el link de PayPal) |
| `pagos/premium_bot.py:2752` | `"⚽ *GOLES — 20€/mes*"` | precio (copia 4) |
| `pagos/premium_bot.py:2754` | `"🚩 *CORNERS — 20€/mes*"` | precio (copia 4) |
| `pagos/premium_bot.py:2756` | `"📊 *PREPARTIDO — 20€/mes*"` | precio (copia 4) |
| `pagos/premium_bot.py:2758` | `"🔥 *COMBO — 30€/mes*"` | precio (copia 4) |
| `pagos/premium_bot.py:2760` | `"🏓 *PING PONG — 50€/mes*"` | precio (copia 4) |
| `pagos/premium_bot.py:2927` | `"💰 Precio: *20€/mes*"` (plan PRE) | precio hardcodeado pese a existir `PRECIO_PRE` |
| `pagos/premium_bot.py:5317-5322` | `precios = {...}` + `precios.get(plan, "20€")` | precio (usa config; el default `"20€"` es literal) |
| `landing/app.py:34-35` | `_PRECIOS_PLAN = {"goles":20,"corners":20,"combo":30,"pre":20,"prepartido":20,"pinpon":50,"total":90}` | precio (copia 5, para estimar ingresos del panel) |
| `landing/index.html:268` | `20€/mes` (Goles) | precio (copia 6) |
| `landing/index.html:276` | `20€/mes` (Córners) | precio (copia 6) |
| `landing/index.html:284` | `30€/mes` (Combo) | precio (copia 6) |
| `landing/index.html:292` | `20€/mes` (Over 2.5 FT) | precio (copia 6) |
| `landing/index.html:300` | `50€/mes` (Ping Pong) | precio (copia 6) |
| `landing/index.html:308` | `0€` (Canal free) | precio |
| `landing/index.html:316` | `0€` (Ping Pong free) | precio |
| `bot/landing-ventas/index.html:214` | `20€ / 30 días` (Goles) | precio (copia 7) |
| `bot/landing-ventas/index.html:234` | `20€ / 30 días` (Prepartido) | precio (copia 7) |
| `bot/landing-ventas/index.html:254` | `20€ / 30 días` (Córners) | precio (copia 7) |
| `bot/landing-ventas/index.html:274` | `30€ / 30 días` (Combo) | precio (copia 7) |
| `bot/landing-ventas/index.html:294` | `50€ / 30 días` (Ping Pong) | precio (copia 7) |
| `bot/landing-ventas/index.html:437,443,449` | `20€` (tabla comparativa) | precio (copia 8) |
| `bot/landing-ventas/index.html:455` | `30€` (tabla comparativa) | precio (copia 8) |
| `bot/landing-ventas/index.html:461` | `50€` (tabla comparativa) | precio (copia 8) |

### 2.1 Moneda y periodicidad

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| todos los anteriores | `€` incrustado en la cadena | moneda (nunca separada del importe) |
| `pagos/config.py:62` | `PLAN_DAYS = 30` | periodicidad real |
| `landing/index.html:268-300` | `"/mes"` | periodicidad mostrada |
| `bot/landing-ventas/index.html:214-294` | `"/ 30 días"` | periodicidad mostrada |
| `pagos/premium_bot.py:2752-2760,5328` | `"/mes"` | periodicidad mostrada |

### 2.2 IDs de precio del proveedor (Stripe)

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `pagos/config.py:53` | `STRIPE_GOLES = "https://buy.stripe.com/aFa8wObuQ9MbdgA00x08g01"` | payment link |
| `pagos/config.py:54` | `STRIPE_CORNERS = ".../bJe3cugPaf6vdgA5kR08g02"` | payment link |
| `pagos/config.py:55` | `STRIPE_COMBO = ".../4gM7sK8iE0bBgsMfZv08g03"` | payment link |
| `pagos/config.py:56` | `STRIPE_PRE = ".../aFafZg9mI6zZccw00x08g04"` | payment link |
| `pagos/config.py:59` | `STRIPE_PINPON = env o ".../6oUcN4gPa2jJb8s8x308g05"` | payment link |
| `bot/landing-ventas/index.html:222,242,262,282,302` | los mismos 5 enlaces, duplicados a mano | payment link (copia 2) |
| `bot/landing-ventas/index.html:225,245,265,285,305` | `paypal.me/erikenobi/{20,20,20,30,50}` | importe incrustado en la URL |
| `pagos/keyboards.py:79` | `f"{PAYPAL_LINK}/{importe}"` | importe incrustado en la URL |

> **Importante para la fase 2:** son *Payment Links* de Stripe, no `price_...`.
> No hay `provider_price_id` en el repo, ni ninguna llamada a la API de Stripe:
> el pago es **manual** (el usuario manda captura, el admin aprueba). Ver
> [Decisiones que necesito de ti](#decisiones-que-necesito-de-ti).

### 2.3 Otros métodos de pago

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `pagos/config.py:49` | `BIZUM = "+34660426660"` | identidad/pago |
| `pagos/config.py:50` | `PAYPAL_LINK = "https://paypal.me/erikenobi"` | identidad/pago |
| `pagos/config.py:51` | `REVOLUT_LINK = "https://revolut.me/ericblasco9"` | identidad/pago |
| `pagos/.env.example:31` | `BIZUM_PHONE=+34660426660` | identidad/pago (duplicado) |
| `bot/landing-ventas/index.html:601` | `https://revolut.me/ericblasco9` | identidad/pago (duplicado) |

---

## 3. Prueba gratuita

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `pagos/config.py:66` | `TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "7"))` | duración (fuente de verdad de facto) |
| `pagos/config.py:67` | `PINPON_TRIAL_DAYS = int(os.getenv("PINPON_TRIAL_DAYS", "3"))` | duración ping pong |
| `pagos/config.py:47` | `TRIAL_PLANS = ("goles","corners","combo","pre","pinpon")` | qué planes tienen prueba |
| `pagos/premium_bot.py:880` | `PINPON_TRIAL_DAYS if plan=="pinpon" else TRIAL_DAYS` | resolución por plan |
| `pagos/premium_bot.py:2450` | `f"🎁 *Prueba gratis {TRIAL_DAYS} días* ({PINPON_TRIAL_DAYS} en Ping Pong)"` | copy (parametrizado ✅) |
| `pagos/premium_bot.py:2803` | `"Pruébalo gratis (7 días fútbol/Over 2.5, 3 días Ping Pong)"` | copy (**hardcodeado**) |
| `pagos/keyboards.py:71-73` | `f"🎁 Probar gratis {_dias} días"` | copy (parametrizado ✅) |
| `pagos/premium_bot.py:1363,1477,4338,4400` | cálculo de `fecha_fin` con `TRIAL_DAYS` | lógica |
| `bot/config.py:297` | `TRIAL_DIAS = int(os.getenv("TRIAL_DIAS") or os.getenv("TRIAL_DAYS") or "7")` | duración (copia 2) |
| `bot/marketing.py:26` | `TRIAL_DIAS = int(os.getenv("TRIAL_DIAS") or os.getenv("TRIAL_DAYS") or "7")` | duración (copia 3) |
| `bot/.env.example:88` | `TRIAL_DIAS=7` | duración (copia 4) |
| `pagos/.env.example:15` | `TRIAL_DAYS=7` | duración (copia 5) |
| `landing/.env.example:12` | `TRIAL_DAYS=7` (comentado como "no es fuente de verdad") | duración (copia 6) |
| `bot/promo.py:35,87,116,158,182` | `{TRIAL_DIAS} días` | copy (parametrizado ✅) |
| `bot/marketing.py:130,176,230,254,260` | `{TRIAL_DIAS} días` | copy (parametrizado ✅) |
| `bot/estadisticas.py:1116,1118` | `{TRIAL_DIAS} días` | copy (parametrizado ✅) |
| `bot/promo.py:5` | docstring: "recordatorio recurrente del trial de **3 días**" | copy (**contradice** el valor 7) |
| `bot/promo.py:25` | comentario: "orienta al trial de **7 días**" | comentario |
| `bot/promo.py:73` | docstring: "recordatorio explícito de los **3 días** gratis" | copy (**contradice**) |
| `bot/config.py:306` | comentario: "recordatorio explícito del trial de **3 días**" | comentario (**contradice**) |
| `bot/handlers.py:3076` | `"🎁 <b>3 días de Ping Pong Premium GRATIS</b>"` | copy (**hardcodeado**, cross-repo) |
| `landing/index.html:6,9,14,25,42` | "7 días" en `<title>` y metas | copy (hardcodeado) |
| `landing/index.html:59,62` | FAQ JSON-LD: "7 días (3 días en Ping Pong)" | copy (hardcodeado) |
| `landing/index.html:127,144,270,278,286,294` | "7 días" | copy (hardcodeado) |
| `landing/index.html:302` | "Probar 3 días gratis" (Ping Pong) | copy (hardcodeado) |
| `landing/index.html:250,260` | "7 días fútbol/Over 2.5, 3 días Ping Pong" | copy (hardcodeado) |
| `landing/index.html:358,360,362` | "Pruébalo gratis 7 días" / "Activar mi prueba de 7 días" | copy (hardcodeado) |
| `landing/index.html:134` | "una prueba por persona" | condición (no repetible) |
| `pagos/premium_bot.py:1476-1477` | "No extiende suscripciones existentes" + tabla `trials` (PK por usuario) | condición: **no repetible** |
| `bot/landing-ventas/index.html` | **ninguna mención de prueba gratuita** | ausencia |

### 3.1 Valores relacionados con el acceso

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `pagos/config.py:70` | `INVITE_EXPIRY_HOURS = int(os.getenv(..., "24"))` | validez del enlace de invitación |
| `pagos/.env.example:35` | `INVITE_EXPIRY_HOURS=1` | **contradice** el default del código |
| `bot/landing-ventas/index.html:106` | `Invitación: <strong>1 hora</strong>` | **contradice** |
| `pagos/config.py:75` | `REFERIDOR_DIAS = 15` | días de regalo por referido |
| `pagos/config.py:76` | `REFERIDO_MULTIPLICADOR = 1` | el recomendado no gana días |
| `pagos/config.py:89` | `MAX_GENERACIONES_ACCESO = 6` | enlaces auto-generables por periodo |
| `pagos/config.py:81` | `CHECK_EXPIRATIONS_EVERY_SECONDS = 3600` | barrido de caducidades |
| `pagos/config.py:84` | `REEXPULSION_RETRY_DAYS = 7` | ventana de reintento de expulsión |
| `pagos/config.py:95-98` | `NPS_DELAY_DAYS=12`, `NPS_LOTE=10`, `NPS_HORA_MIN=11`, `NPS_HORA_MAX=21` | encuesta CSAT |
| `pagos/premium_bot.py:5386` | `"caduca en 3 días"` | aviso de caducidad (hardcodeado) |

---

## 4. Textos legales y de juego responsable

El aviso legal existe en **ocho redacciones distintas**, ninguna de ellas
centralizada, más una ausencia total en una de las dos landings.

| Fichero:línea | Valor actual | Variante |
|---|---|---|
| `bot/marketing.py:33` | `DISCLAIMER = "+18. Juega con responsabilidad. Sin promesas de beneficios."` | **A** |
| `bot/instagram_tutorial.py:210` | `"+18. Juega con responsabilidad. Sin promesas de beneficios."` | A (literal duplicado) |
| `bot/instagram_summary.py:297` | idem | A (literal duplicado) |
| `bot/instagram_summary.py:492` | idem | A (literal duplicado) |
| `bot/instagram_premium.py:336` | idem | A (literal duplicado) |
| `bot/estadisticas.py:1119` | `"<i>+18. Juega con responsabilidad. Sin promesas de beneficios.</i>"` | A (con cursiva) |
| `bot/handlers.py:6077` | `"+18. Juega con responsabilidad."` | **B** (truncada) |
| `bot/pinpon.py:439` | `"⚠️ +18 · juego responsable"` | **C** |
| `bot/pinpon.py:741` | `"⚠️ +18 · juego responsable"` | C |
| `bot/handlers.py:2371` | `"Nos vemos dentro. 🙌  🔞 +18 · juego responsable."` | **D** |
| `bot/handlers.py:2407` | `"🔞 +18 · juego responsable"` | D |
| `bot/handlers.py:2968` | `"🔞 +18 · juego responsable"` | D |
| `bot/handlers.py:3041` | `"🔞 +18 · juego responsable"` | D |
| `bot/handlers.py:3059` | `"🔞 +18 · juego responsable · sin promesas de ganancia"` | **E** |
| `bot/handlers.py:3241` | idem | E |
| `bot/handlers.py:3482` | `"🔞 +18 · juego responsable · sin promesas de ganancia."` | E (con punto) |
| `pagos/premium_bot.py:2805` | `"🔞 \+18 · juego responsable · sin promesas de ganancia\."` | E (escapado MarkdownV2) |
| `landing/index.html:251` | `"🔞 +18 · juego responsable · sin promesas de ganancia."` | E |
| `bot/handlers.py:1495` | `"Método ping pong · +18, juego responsable."` | **F** |
| `bot/handlers.py:2337` | `"Gracias por estar aquí. 🙌 +18, juego responsable."` | F |
| `bot/handlers.py:3394` | `"+18 · juego responsable."` | F |
| `landing/index.html:9,14,42,218` | `"+18, juego responsable."` | F |
| `landing/index.html:25` | `"+18."` | **G** (solo edad) |
| `landing/index.html:118` | `"Picks por Telegram · +18"` | G |
| `landing/index.html:372` | `<p class="legal-badge">+18</p>` | G |
| `pagos/premium_bot.py:5622` | `"🔞 +18"` | G |
| `landing/index.html:373-377` | párrafo largo: mayores de 18, riesgo de pérdida, adicción, apuesta solo lo que puedas permitirte perder, no garantiza ganancias | **H** (versión completa) |
| `landing/index.html:379-381` | enlace de ayuda a `https://www.jugarbien.es/` | **único enlace de ayuda del ecosistema** |
| `bot/landing-ventas/index.html` | **sin +18, sin juego responsable, sin enlace de ayuda** | ausencia |

### 4.1 Disclaimers de no garantía de rentabilidad

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `pagos/premium_bot.py:794-796` | `"Porcentajes calculados sobre picks con resultado ya conocido. El rendimiento pasado no garantiza resultados futuros. Este servicio es únicamente informativo."` | no-garantía |
| `pagos/premium_bot.py:2768-2770` | `"Servicio únicamente informativo. Cada usuario es responsable de sus propias decisiones."` | no-garantía |
| `landing/index.html:100-102` | FAQ JSON-LD: "No. Es un servicio de análisis e información..." | no-garantía |
| `landing/index.html:350` | misma respuesta en el `<details>` visible (1ª persona: "Comparto") | no-garantía (**divergencia de voz** con el JSON-LD, que dice "Compartimos") |
| `landing/index.html:217-218` | "Los datos pasados no garantizan resultados futuros." | no-garantía |
| `landing/index.html:364` | "No prometo ganancias — nadie serio lo hace." | no-garantía |
| `bot/landing-ventas/index.html:404` | "Resultados pasados no garantizan..." | no-garantía |
| `bot/landing-ventas/index.html:689` | "el rendimiento pasado no garantiza resultados futuros." | no-garantía |
| `bot/landing-ventas/index.html:714` | "Servicio informativo. El rendimiento pasado no garantiza resultados futuros." | no-garantía |
| `bot/marketing.py:43-45` | lista de frases prohibidas ("beneficio garantizado", "sin riesgo", "100% seguro"…) | control de copy |

### 4.2 Edad mínima

`18` aparece siempre incrustado en la cadena (`+18`, `🔞 +18`, `mayores de 18
años`). **No existe ninguna constante** `min_age` en ningún repo.

---

## 5. Identidad

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `bot/config.py:19` | `CANAL_ORIGEN_ID = -1003876204382` (origen premium) | canal |
| `bot/config.py:22` | `CANAL_CORNERS_ID = -1003895151594` | canal |
| `bot/config.py:23` | `CANAL_GOLES_ID = -1003818905455` | canal |
| `bot/config.py:25` | `CANAL_FREE_ID = -1002973101273` | canal free principal |
| `bot/config.py:26` | `CANAL_PRE_ID = -1003774898516` (Carlos Mollar) | canal |
| `bot/config.py:27` | `CANAL_PRE_GENERAL_ID = -1003959149689` | canal |
| `bot/config.py:29` | `CANAL_PRE_25_ID = -1003837149453` | canal |
| `bot/config.py:35` | `CANAL_FREE_SECUNDARIO_ID = -1002037791209` | canal free 2 |
| `bot/config.py:40` | `CANALES_FREE = [FREE, FREE_SECUNDARIO]` | canal |
| `bot/config.py:54-55` | `CANAL_PINPON_RAW_ID = -1004444255144`, `CANAL_PINPON_OFICIAL_ID = -1004259041662` | canal |
| `bot/config.py:60` | `CANAL_PINPON_ESPEJO_ID = -1004457922573` | canal |
| `bot/config.py:73` | `CANAL_PINPON_FREE_ID = -1004474092446` | canal free ping pong |
| `bot/config.py:79` | `CANAL_PINPON_TTCUP_ID = -1004298922749` | canal |
| `bot/config.py:85` | `CANAL_PINPON_ANALISIS_ID = -1003707155404` | canal |
| `bot/config.py:877` (`CANAL_PRE_15HT_ID`) | `-1004244147250` | canal |
| `bot/config.py:628` | `CANAL_APUESTAS_ID = -1003520868755` | canal interno |
| `bot/config.py:641` | `CANAL_STORIES_ID = -1003520868755` | canal interno |
| `bot/config.py:13` | `ADMIN_IDS = [9330181]` | identidad |
| `pagos/config.py:18` | `ADMIN_IDS = [9330181]` | identidad (duplicado) |
| `pagos/config.py:21` | `CANAL_CORNERS_ID = -1003895151594` | canal (duplicado) |
| `pagos/config.py:22` | `CANAL_GOLES_ID = -1003818905455` | canal (duplicado) |
| `pagos/config.py:23` | `CANAL_PRE_ID = -1003837149453` | canal (**mismo nombre, otro valor** que `bot`) |
| `pagos/config.py:30` | `CANAL_PINPON_ID = -1004259041662` | canal (duplicado del oficial de PP) |
| `pagos/config.py:34` | `PINPON_FREE_URL = "https://t.me/+PIREb9LtUqM5ZWZk"` | enlace free PP |
| `landing/app.py:25` | `PINPON_FREE_URL = "https://t.me/+PIREb9LtUqM5ZWZk"` | enlace free PP (duplicado) |
| `pagos/config.py:36` | `LINK_FREE = "https://t.me/+WhIkP2PstS1kMDVk"` | enlace free fútbol |
| `bot/landing-ventas/index.html:66,630,720` | `https://t.me/+WhIkP2PstS1kMDVk` | enlace free fútbol (duplicado ×3) |
| `bot/config.py:292` | `BOT_PREMIUM_URL = "https://t.me/erikenobi_premiumbot?start=free"` | bot username |
| `bot/marketing.py:24` | `BOT_PREMIUM_BASE = "https://t.me/erikenobi_premiumbot"` | bot username (copia 2) |
| `bot/.env.example:92` | `BOT_PREMIUM_BASE=https://t.me/erikenobi_premiumbot` | bot username (copia 3) |
| `pagos/.env.example:12` | `BOT_USERNAME=erikenobi_premiumbot` | bot username (copia 4) |
| `landing/.env.example:10` | `TELEGRAM_BOT_USERNAME=erikenobi_premiumbot` | bot username (copia 5) |
| `bot/handlers.py:1357,2336,2406,2967,3040,3053,3393,3481` | `https://t.me/erikenobi_premiumbot?start=pinpon` literal ×8 | bot username (copias 6-13) |
| `pagos/premium_bot.py:5622` | `https://t.me/erikenobi_premiumbot?start=pinpon` | bot username (copia 14) |
| `landing/index.html:126,129,270,278,286,294,302,310,362` | `t.me/erikenobi_premiumbot?start=...` ×9 | bot username (copias 15-23) |
| `bot/marketing.py:23` | `LANDING_BASE_URL = "https://erikenobipicks.com"` | web |
| `bot/.env.example:91` | `LANDING_BASE_URL=https://erikenobipicks.com` | web (copia 2) |
| `pagos/.env.example:16` | `LANDING_BASE_URL=https://erikenobipicks.com` | web (copia 3) |
| `landing/app.py:50` | `_SITE = "https://www.erikenobipicks.com"` | web (**con `www`**) |
| `landing/index.html:12,16,19,26,40,47,48` | `https://www.erikenobipicks.com/` | web (**con `www`**) |
| `bot/config.py:696` | `STORY_FREE_LINK = "t.me/erikenobi"` | soporte / canal personal |
| `bot/config.py:697` | `STORY_IG_HANDLE = "@erikenobipicks"` | Instagram |
| `bot/instagram_summary.py:169` | `"t.me/erikenobi  ·  @erikenobipicks"` | literal duplicado |
| `bot/instagram_premium.py:311` | `"t.me/erikenobi   ·   @erikenobipicks"` | literal duplicado (espaciado distinto) |
| `bot/instagram_story.py:270,311` | `"t.me/erikenobi  ·  @erikenobipicks"` | literal duplicado |
| `bot/instagram_story.py:469` | fallback `"t.me/erikenobi", "@erikenobipicks"` | literal duplicado |
| `bot/instagram_story.py:472,749` | `"t.me/erikenobi_premiumbot   ·   @erikenobipicks"` | literal duplicado |
| `bot/instagram_tutorial.py:49` | `"@erikenobipicks"` | literal duplicado |
| `bot/instagram_brand.py:192` | `"@erikenobipicks"` | literal duplicado |
| `pagos/keyboards.py:48` | botón Contacto → `https://t.me/erikenobi` | soporte |
| `pagos/premium_bot.py:2744` | `"❓ ¿Algún problema? Escríbeme: @erikenobi"` | soporte |
| `pagos/premium_bot.py:5314` | `"Habla con @erikenobi si quieres renovarlo."` | soporte |
| `landing/index.html:50,371` | `https://t.me/erikenobi` | soporte |
| `bot/landing-ventas/index.html:46,520,608,609,705` | `t.me/erikenobi` / `@erikenobi` | soporte |
| `bot/landing-ventas/script.js:143` | `https://t.me/erikenobi?text=...` | soporte |
| `landing/app.py:114-119` | ruta `/pinpon-free` → redirección | URL de producto |

### 5.1 Nombre de marca

| Variante | Apariciones | Ejemplos |
|---|---|---|
| `Erikenobi Picks` | 17 | `landing/index.html:39`, `bot/README.md:3` |
| `Erikenobi Picks Premium` | 8 | `bot/landing-ventas/index.html:6,34`, `pagos/premium_bot.py:2454` |
| `ERIKENOBI PREMIUM` | 10 | `bot/instagram_tutorial.py:169,207` |
| `ERIKENOBI PICKS` | 6 | plantillas de Instagram |
| `erikenobipicks` | 22 | dominio y handle |

### 5.2 URL de estadísticas públicas

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `landing/app.py:447` | ruta `/api/stats` (JSON) | stats |
| `landing/index.html:205-222` | sección `#estadisticas` | stats |
| `bot/landing-ventas/data/landing-data.json` | export estático (hoy **vacío**: `generated_at: null`) | stats |
| `bot/export_landing_data.py` | genera ese JSON | stats |
| `bot/.env.example:96` | `LANDING_DATA_OUTPUT=landing-ventas/data/landing-data.json` | stats |

**No existe una `stats_url` pública canónica** (la sección vive dentro de la
home). El campo `stats_url` del esquema propuesto quedaría vacío o apuntando a
`https://www.erikenobipicks.com/#estadisticas`.

---

## 6. Ligas cubiertas y promesas de frecuencia / horario

### 6.1 Ligas

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `bot/config.py:233-256` | `PINPON_GRUPOS`: `Czech Liga Pro` 🇨🇿, `TT Elite Series` 🇵🇱, `TT Cup` 🏓 | ligas de ping pong |
| `bot/config.py:262` | `PINPON_OMITIR_FINDE = []` | ligas sin picks en finde |
| `bot/config.py:385-394` | `BLACKLIST_NG1`: 8 ligas de fútbol excluidas | filtro (⚠️ **no tocar**: lógica de picks) |
| `bot/config.py:439-443` | `NG1_GATE_REGION_BLOCKLIST=["Finland"]`, `NG1_GATE_LIGA_ALLOWLIST=[3 ligas]` | filtro (⚠️ **no tocar**) |
| `landing/index.html:185` | `"Fútbol de las principales ligas."` | promesa de cobertura (vaga) |
| `landing/index.html:199-201` | ping pong descrito sin nombrar ligas | promesa de cobertura |
| `bot/config.py:197-199` | `PINPON_CASAS = "Retabet · Bwin · Bet365 · 1xBet (solo Rep. Checa) · Winamax"` | casas donde encontrar los partidos |

> Las listas de `BLACKLIST_NG1` y los gates NG1/UGM son **lógica de filtrado de
> picks**, no configuración de producto. El brief excluye explícitamente tocar
> los filtros: quedan inventariadas pero **fuera** del alcance de la migración.

### 6.2 Frecuencia y horario

| Fichero:línea | Valor actual | Categoría |
|---|---|---|
| `bot/config.py:280-281` | free de fútbol publica de 10:00 a 22:00 (Madrid) | horario |
| `bot/config.py:312` | `FREE_PROMO_HORAS = [20]` | horario de promo |
| `bot/config.py:322-325` | `PROMO_POSTING_SCHEDULE = [13, 18]` (default) | horario de promo |
| `bot/.env.example:90` | `PROMO_POSTING_SCHEDULE=10,16,20` | horario de promo (**contradice**) |
| `bot/config.py:223-226` | `PINPON_PROMO_HORAS = [20]` | horario de promo PP |
| `bot/config.py:93` | `PINPON_AVISO_MIN_ANTES = 5` | aviso previo al partido |
| `bot/config.py:99` | `PINPON_PROG_PUBLICAR_HORA = 7` | hora de publicación del calendario |
| `bot/config.py:106` | `PINPON_PROG_ANTICIPACION_MIN = 90` | antelación mínima del calendario |
| `bot/config.py:111-112` | `PINPON_RESUMEN_DIA_HORA = 23`, `..._MIN = 30` | hora del resumen del día |
| `bot/config.py:117` | `PINPON_RACHA_MIN = 3` | racha mínima para presumir |
| `bot/pinpon.py:64` | `"🇪🇸 Horarios en hora española (peninsular)"` | horario |
| `pagos/config.py:100` | `TIMEZONE = "Europe/Madrid"` | zona horaria (duplicada) |
| `pagos/.env.example:34` | `TIMEZONE=Europe/Madrid` | zona horaria (duplicada) |
| `landing/index.html:196` | Over 2.5 "con un aviso 5 minutos antes" | promesa de horario |
| `landing/index.html:238` | Over 2.5 "el pick con valor **hasta 12h antes**" | promesa de horario |
| `pagos/premium_bot.py:2796` | "Recibes el pick con valor **hasta 12h antes**" | promesa de horario (duplicada) |
| `bot/config.py:819` | `PRE_O25_RECORDATORIO_MIN_ANTES = 5` | aviso previo Over 2.5 |
| `landing/index.html:121` | "**Cada día** comparto mis picks" | promesa de frecuencia |
| `bot/promo.py:108,132,139` | premium "sin límite diario" / "sin tope diario" | promesa de frecuencia |
| `bot/landing-ventas/index.html:60` | "La idea no es mandar volumen por mandar" | promesa de frecuencia (opuesta) |

### 6.3 Claims de rendimiento incrustados en copy

No son "límites" ni "precios", pero son **números dentro de un texto** que
prometen algo al usuario, así que entran en el mismo problema.

| Fichero:línea | Valor actual |
|---|---|
| `bot/landing-ventas/index.html:89,215` | Goles `+70%` estimado |
| `bot/landing-ventas/index.html:94,255` | Córners `+80%` estimado |
| `bot/landing-ventas/index.html:275` | Combo `+75%` estimado |
| `bot/landing-ventas/index.html:295` | Ping Pong `~90% de acierto` |
| `bot/landing-ventas/index.html:235,332` | Prepartido `71.3% acierto` · `ROI +19.4%` (ene-mar 2026) |
| `bot/landing-ventas/index.html:364,376` | `ROI +18.0%`, `ROI +16.82%` |
| `bot/landing-ventas/index.html:382-384` | "bank de 500€ (1u = 5€)… Total 3 meses: +533.3€" |
| `pagos/premium_bot.py:2823,2826,2829` | fallback de stats: `+70%` / `+80%` / `+75%` |
| `pagos/config.py:45-46` | comentario: "3 días son más que suficientes para un método con ~90% de acierto" |
| `pagos/config.py:73-74` | comentario: "un servicio con ~90% de acierto" |
| `bot/config.py:860-866` | `PRE_O25DEF_SEED_MENSUAL` (datos semilla reales, en código) |
| `bot/config.py:827-834` | `PRE_O25DEF_SEED_DIARIO` (datos semilla reales, en código) |

> `landing/` (la web viva) **no** repite estos porcentajes: los lee en vivo de
> `/api/stats`. `bot/landing-ventas/` sí los tiene escritos a mano y desfasados.

---

## 7. Documentación desactualizada

Estos ficheros no llegan al usuario final, pero contradicen al código y son la
fuente más probable de un error futuro.

| Fichero:línea | Dice | Realidad |
|---|---|---|
| `pagos/docs/SYSTEM_AUDIT.md:15` | "Trial **= 3 días**" | `TRIAL_DAYS = 7` |
| `pagos/docs/SYSTEM_AUDIT.md:54` | "`TRIAL` (3 días), `INVITE_EXPIRY_HOURS=1`" | 7 días / 24 h |
| `pagos/docs/SYSTEM_AUDIT.md:17` | landing "sin +18, sin mención de 7 días" | la landing ya tiene ambos |
| `pagos/docs/TRIAL_FUNNEL_ARCHITECTURE.md:104` | "Trial dura exactamente 7 días … **Hoy es 3**" | ya son 7 |
| `pagos/docs/SYSTEM_AUDIT.md:33` | planes: GOLES, CORNERS, COMBO, PREPARTIDO | faltan PING PONG y TOTAL |
| `landing/docs/FUNNEL_INTEGRATION.md:21` | "+18 / juego responsable (**hoy ausente** — riesgo legal)" | ya presente en `landing/index.html` |
| `bot/README.md:7` | carpeta `landing-instagram/` | no existe en ese repo |
| `bot/README.md:10` | `premium_bot.py` "bot premium/comercial" | vive en `telegram-payments`, no aquí |

---

## Resoluciones de la fase 0

**Principio rector:** el refactor no cambia ningún comportamiento en producción.
Donde el código y el copy discrepen, **gana el código**: el copy se corrige para
describir lo que el sistema hace hoy. Los cambios de producto van en un PR
posterior y separado. Las dos únicas excepciones que sí cambian producción son
**C10** (aviso legal único) y **C4** (IDs cruzados en el `.env.example`).

**Estado: las doce están cerradas.** C1, C3, C4, C8 y C10 se decidieron una por
una; las siete restantes se resolvieron por el principio rector (gana el código)
y quedaron confirmadas en bloque el 21/08/2026. Ninguna de esas siete cambió
comportamiento: confirmarlas ratificó lo que el sistema ya hacía.

| # | Resolución | Estado |
|---|---|---|
| C1 | No hay valor único: bloques `free_tier` **por producto**. Fútbol **4/día** (código). Ping pong **modo `completo`** (default real), `picks_per_day: null` = sin tope. El pie de `promo.py:60` deja de ser cadena fija. `PINPON_FREE_MODO` sale del entorno y pasa a `product.json`. | Decidida |
| C2 | Trial **7 días** (fútbol) y **3 días** (ping pong), por código. | Confirmada (21/08/2026) |
| C3 | **24 h**, por código. `.env.example` corregido. | Decidida |
| C4 | `.env.example` corregido para coincidir con los dos `config.py`, más test de guardia que compara ejemplo y configuración real. | Decidida (cambia producción) |
| C5 | Nombres canónicos de canal fijados en `product.json` (`premium_pre_over25`, `premium_pre_carlos_mollar`, `premium_pre_general`). | Confirmada (21/08/2026) |
| C6 | **30 días** (`PLAN_DAYS`), por código. El copy muestra `{plan_interval_days} días`, no "/mes". | Confirmada (21/08/2026) |
| C7 | **4 métodos** (Stripe, PayPal, Bizum, Revolut), por código. | Confirmada (21/08/2026) |
| C8 | Dominio canónico **con `www`**. El esquema rechaza cualquier otro. Prioridad alta: el redirect estaba descartando los UTM. | Decidida |
| C9 | `[13, 18]`, el default de `config.py`, por código. | Confirmada (21/08/2026) |
| C10 | Redacción **única** en `copy.es.json` (`legal.full` y `legal.short`), aplicada en todas partes. | Decidida (cambia producción) |
| C11 | Los porcentajes se leen en vivo; ningún claim de rendimiento entra en `copy.es.json`. | Confirmada (21/08/2026) |
| C12 | Marca **"Erikenobi Picks"**; el servicio de pago es **"Erikenobi Picks Premium"** (`brand.premium_name`). | Confirmada (21/08/2026) |

### Decisiones estructurales

| # | Resolución |
|---|---|
| D1 | Repo canónico **`telegram-payments`**, copia sincronizada por CI y test de guardia que falla si una copia diverge. |
| D2 | Solo se comprueba que el Payment Link responde. El procedimiento manual de cambio de precio (**Stripe primero, `product.json` después, el mismo día**) queda documentado en `shared/README.md`. |
| D3 | `landing-ventas` **borrada** (PR #222, −2882 líneas). Era la landing de abril, reemplazada el 9 de abril por el árbol de enlaces que creció hasta la actual; nunca se desplegó por separado. Resuelta por arqueología de git — ver abajo. |

### D3 — resuelta: `landing-ventas` era una versión anterior, no un sitio aparte

**Estado final: borrada.** El bloqueo se levantó por el historial de git, no por
la analítica: `landing-ventas` es la landing de abril de 2026, sustituida el 9 de
abril por el árbol de enlaces que fue creciendo hasta la landing actual. Comparten
el `<title>` y un 95 % del texto visible. Nunca tuvo despliegue propio, así que no
había ninguna URL viva que redirigir ni tráfico que preservar.

Se documenta abajo la verificación original porque explica por qué hizo falta la
arqueología: dos de las tres comprobaciones no se pueden hacer desde el código.

Antes del 301 había que verificar tres cosas. Ninguna salía positiva, pero **dos no
se podían verificar desde el código**:

| # | Comprobación | Resultado |
|---|---|---|
| 1 | Ningún Payment Link de Stripe apunta ahí | ⚠️ **No verificable.** No hay integración con la API de Stripe en ningún repo: son *Payment Links* configurados en el panel. La URL de redirección posterior al pago solo se ve desde el panel de Stripe. |
| 2 | Ningún mensaje fijado, bio o mensaje automático la enlaza | ✅ **Negativo dentro de los repos.** Los 23 usos del bot username apuntan todos a `t.me/erikenobi_premiumbot?start=...`; los únicos hosts web referenciados en los tres repos son `erikenobipicks.com` y `www.erikenobipicks.com`. ⚠️ Mensajes fijados a mano, la descripción del bot en BotFather y las bios de Instagram/Threads **viven fuera del repo** y no se pueden comprobar desde aquí. |
| 3 | No recibe visitas residuales de búsqueda | ⚠️ **No verificable.** No hay analítica cargada: `script.js` solo dispara eventos si existen `window.gtag` o `window.plausible`, y `ANALYTICS_ID` no se inyecta en ningún sitio. Haría falta Search Console o los logs del servidor. |

**Hallazgo adicional:** `landing-ventas/` **no tiene ninguna configuración de
despliegue** en el repo — sin `Procfile`, sin config de sitio estático, sin
workflow, sin `CNAME`. Su propio `README.md` describe "crear un repo o despliegue
específico" como un paso **futuro**. Es decir: desde el repositorio no consta ni
que esté publicada ni en qué URL, así que tampoco hay rutas concretas que redirigir.

---

## Contradicciones detectadas

Cada bloque es una decisión que necesito de ti antes de continuar con la fase 1.

### C1 — Picks gratis al día: **1 vs 2 vs 4**

Esta es la contradicción que ya conocías, y resulta que tiene **tres** valores,
no dos, porque hay **dos productos free** que el copy mezcla.

| Dónde | Qué dice | Producto |
|---|---|---|
| `bot/config.py:270-272` | 2 goles + 2 córners = **4/día** (+1 PRE aparte) | free fútbol (**código real**) |
| `bot/promo.py:60` | "Este es **1 pick gratuito**" | free fútbol (**copy**) |
| `bot/config.py:70,174` | "**2 picks gratis**/día" | free ping pong (comentarios) |
| `bot/handlers.py:2333,3083,3389` | "**2 picks gratis** cada día" / "solo 2 picks al día" | free ping pong (copy) |
| `landing/index.html:317` | "**2 picks gratis al día**" | free ping pong (copy) |

El CTA de `promo.py:60` se adjunta a los picks del canal free de **fútbol**
(`handlers.py:6872,7079`), donde el tope real es 4. Es decir: **el usuario ve
hasta 4 picks al día y el pie del mensaje le dice que ese es "1 pick gratuito"**.

Los "2 picks" del ping pong sí son correctos hoy (mañana + tarde), pero solo en
`PINPON_FREE_MODO=reducido`; el default en código es `completo` (el free recibe
**todo**), así que hoy en producción ese copy tampoco describe lo que pasa.

**Necesito que decidas:**
1. ¿El esquema modela **un** `free_tier.picks_per_day` o **uno por producto**
   (`free_tier.football.picks_per_day` / `free_tier.pinpon.picks_per_day`)?
   Mi recomendación: **uno por producto**, porque hoy son dos canales, dos
   límites y dos modos, y colapsarlos volvería a crear la mentira que estamos
   arreglando.
2. Valor correcto para fútbol: ¿4 (lo que hace el código) o bajarlo a 1-2 (lo
   que dice el copy)?
3. Valor correcto para ping pong: ¿2, y `PINPON_FREE_MODO` pasa a `reducido`?

### C2 — Duración de la prueba: **7 vs 3 días** (fútbol) y **3** (ping pong)

| Dónde | Qué dice |
|---|---|
| `pagos/config.py:66` | `TRIAL_DAYS = 7` ← comportamiento real |
| `pagos/config.py:67` | `PINPON_TRIAL_DAYS = 3` ← comportamiento real |
| `bot/promo.py:5` | "trial de **3 días**" |
| `bot/promo.py:73` | "recordatorio explícito de los **3 días** gratis" |
| `bot/config.py:306` | "recordatorio explícito del trial de **3 días**" |
| `bot/promo.py:25` | "orienta al trial de **7 días**" |

Los tres "3 días" son restos de cuando el trial duraba 3. **Hoy solo aparecen
en comentarios y docstrings**, no en texto que vea el usuario (el copy visible
usa `{TRIAL_DIAS}`), pero son exactamente el tipo de resto que reaparece en un
mensaje la próxima vez que alguien copie y pegue.

**Necesito que confirmes:** el trial es 7 días (fútbol/Over 2.5) y 3 días (ping
pong), y los comentarios que dicen "3 días" en contexto de fútbol son
simplemente obsoletos. Si es así, ¿el esquema lleva `trial.duration_days` por
producto, como en C1?

### C3 — Validez del enlace de invitación: **24 h vs 1 h**

| Dónde | Qué dice |
|---|---|
| `pagos/config.py:70` | `INVITE_EXPIRY_HOURS` default **24** (comentario: "antes 1h — demasiado corto") |
| `pagos/.env.example:35` | `INVITE_EXPIRY_HOURS=1` |
| `bot/landing-ventas/index.html:106` | "Invitación: **1 hora**" |
| `pagos/docs/SYSTEM_AUDIT.md:54` | "`INVITE_EXPIRY_HOURS=1`" |

El código dice 24, pero **el `.env.example` dice 1**: quien despliegue copiando
ese fichero reintroduce el bug que el comentario dice haber arreglado.

**Necesito que confirmes:** 24 h es el valor bueno.

### C4 — IDs de canal GOLES y CORNERS **intercambiados** en `pagos/.env.example`

| Dónde | GOLES | CORNERS |
|---|---|---|
| `bot/config.py:22-23` | `-1003818905455` | `-1003895151594` |
| `pagos/config.py:21-22` | `-1003818905455` | `-1003895151594` |
| `pagos/.env.example:24-25` | `-1003895151594` ❌ | `-1003818905455` ❌ |

Los dos `config.py` coinciden; **el `.env.example` los tiene cruzados**. Hoy no
rompe nada porque `pagos/config.py` no lee esas variables (los valores están
hardcodeados), pero si alguien externaliza la configuración siguiendo el
ejemplo, **los suscriptores de goles acabarán en el canal de córners**.

**Necesito que confirmes** que el mapeo bueno es el de los dos `config.py`.
(Formalmente esto entra en "fallo evidente de formato", pero como afecta a
quién entra en qué canal prefiero no tocarlo sin tu visto bueno.)

### C5 — `CANAL_PRE_ID`: mismo nombre, dos canales distintos

| Dónde | Valor | Significado |
|---|---|---|
| `bot/config.py:26` | `-1003774898516` | canal prepartido "Carlos Mollar" |
| `bot/config.py:29` (`CANAL_PRE_25_ID`) | `-1003837149453` | canal prepartido Over 2.5 |
| `pagos/config.py:23` (`CANAL_PRE_ID`) | `-1003837149453` | el plan "PREPARTIDO" que se vende |

El plan que se vende como "PREPARTIDO" en el bot de pagos apunta al canal que en
el bot de picks se llama `CANAL_PRE_25_ID`. No es un bug (el mapeo funciona),
pero **el mismo identificador significa dos cosas distintas** según el repo, y
eso hará que la fase 3 se equivoque si no lo fijamos ahora.

**Necesito que decidas** el nombre canónico de cada canal en el esquema
compartido. Propuesta: `pre_carlos_mollar`, `pre_over25`, `pre_general`.

### C6 — Periodicidad mostrada: **"/mes" vs "/ 30 días"**

| Dónde | Qué dice |
|---|---|
| `pagos/config.py:62` | `PLAN_DAYS = 30` ← comportamiento real |
| `landing/index.html:268-300`, `pagos/premium_bot.py:2752-2760,5328` | "**/mes**" |
| `bot/landing-ventas/index.html:214-294` | "**/ 30 días**" |
| `bot/landing-ventas/index.html:663` | "acceso … durante **30 días**" |

30 días no es un mes en 7 de los 12 meses. Con renovación manual la diferencia
es cosmética, pero es una promesa distinta.

**Necesito que decidas** qué se muestra: "/mes" o "/30 días". (El esquema
propuesto tiene `interval: "month"`, que es la primera opción; si eliges 30
días, el esquema necesita `interval_days` en su lugar.)

### C7 — Métodos de pago: **4 vs 3**

| Dónde | Qué dice |
|---|---|
| `pagos/config.py:49-51`, `pagos/keyboards.py:78-81` | Stripe, PayPal, **Bizum**, Revolut |
| `pagos/premium_bot.py:2742,2763` | "Stripe · PayPal · Bizum · Revolut" |
| `landing/index.html:70,261,334` | "Stripe, PayPal, Bizum y Revolut" |
| `bot/landing-ventas/index.html:69` | "Stripe, PayPal o **Revolut**" (sin Bizum) |
| `bot/landing-ventas/index.html:191,547,588` | idem, sin Bizum |
| `bot/landing-ventas/index.html:595-597` | "Bizum privado … el detalle aparece dentro del bot" |

`landing-ventas` lista 3 métodos en el flujo y menciona Bizum solo como nota al
pie. **Necesito que confirmes** que son 4 y que Bizum se anuncia como los demás.

### C8 — Dominio con y sin `www`

| Dónde | Qué dice |
|---|---|
| `bot/marketing.py:23`, `bot/.env.example:91`, `pagos/.env.example:16` | `https://erikenobipicks.com` |
| `landing/app.py:50`, `landing/index.html:12,16,19,26,40,47,48` | `https://www.erikenobipicks.com` |

El `canonical` de la web es **con `www`**; todos los deep-links de tracking que
generan los bots apuntan **sin `www`**. Si el servidor redirige, se pierden los
parámetros UTM en el salto y la atribución del funnel se rompe.

**Necesito que decidas** cuál es el dominio canónico.

### C9 — Horario de promos: **[13,18] vs [10,16,20]**

| Dónde | Qué dice |
|---|---|
| `bot/config.py:322-325` | `PROMO_POSTING_SCHEDULE` default `13,18` |
| `bot/.env.example:90` | `PROMO_POSTING_SCHEDULE=10,16,20` |

**Necesito que confirmes** cuál es el bueno (2 o 3 promos al día).

### C10 — El aviso legal tiene 8 redacciones y una landing sin ninguna

Ocho variantes (A-H, ver [§4](#4-textos-legales-y-de-juego-responsable)) del
mismo aviso, y `bot/landing-ventas/index.html` **no tiene +18, ni mención de
juego responsable, ni enlace de ayuda** — solo "Servicio informativo. El
rendimiento pasado no garantiza resultados futuros." (línea 714).

El enlace a `jugarbien.es` aparece **una sola vez en todo el ecosistema**
(`landing/index.html:381`) y en ningún bot.

**Necesito que decidas:**
1. La redacción canónica (corta para pies de pick, larga para pies de web).
2. Si `landing-ventas` debe llevar el aviso completo. Mi recomendación: **sí**,
   y es lo más urgente de todo el informe — es la única página del ecosistema
   que vende sin mencionar la edad mínima.
3. Si el enlace de ayuda debe ir también en los bots.

### C11 — Claims de rendimiento desfasados en `landing-ventas`

`bot/landing-ventas/index.html` promete `+70%` / `+80%` / `+75%` / `~90%` a
mano; `landing/` los lee en vivo de la DB. `pagos/premium_bot.py:2823-2829`
repite los mismos tres porcentajes como *fallback* cuando no hay DB.

Son cifras "estimadas" escritas a mano en una página de venta, en un producto
cuyo argumento es la transparencia. No es estrictamente un valor de
configuración, pero **necesito que decidas** si se eliminan, se marcan
explícitamente como estimación, o se pasan a leerse de `/api/stats`.

### C12 — Nombre de marca: 5 variantes

`Erikenobi Picks` / `Erikenobi Picks Premium` / `ERIKENOBI PREMIUM` /
`ERIKENOBI PICKS` / `erikenobipicks`. Algunas son legítimas (mayúsculas en
plantillas de Instagram, handle en minúsculas), pero "Erikenobi Picks" vs
"Erikenobi Picks Premium" se usan indistintamente para el mismo servicio.

**Necesito que decidas** el nombre canónico y si "Premium" forma parte de él.

---

## Decisiones que necesito de ti

> **Resueltas.** Ver [Resoluciones de la fase 0](#resoluciones-de-la-fase-0).
> Se conserva el planteamiento original como registro de por qué se decidió así.

Además de C1-C12, hay tres decisiones estructurales que condicionan la fase 1:

### D1 — ¿Dónde vive `shared/`, siendo tres repos?

El brief dice "un directorio `shared/` en la raíz". Con tres repos
independientes hay tres opciones:

| Opción | Cómo | Coste |
|---|---|---|
| **A. Repo canónico + copia sincronizada** | `shared/` vive en un repo; los otros dos reciben una copia por CI y un test que falla si difiere | Simple, sin infraestructura nueva. Hay una ventana en la que las copias divergen. |
| **B. Submódulo git** | `shared/` como repo propio, montado en los tres | Coherencia real. Los submódulos complican cada deploy en Railway. |
| **C. Paquete publicado** | `shared/` como paquete instalable | Lo más limpio a largo plazo. Es el que más trabajo añade ahora. |

**Mi recomendación: A**, con `telegram-payments` como repo canónico (ya aloja
los docs del ecosistema) y un test de guardia que compara los ficheros. Es
reversible: si más adelante quieres B o C, los JSON ya están escritos.

### D2 — El proveedor de pago no tiene API que verificar

La fase 2 pide que el arranque compruebe contra la API del proveedor que el
importe de `product.json` coincide con `provider_price_id`. Hoy eso **no es
posible**: no hay `price_...`, solo *Payment Links* (`buy.stripe.com/...`), no
hay clave de API de Stripe en el repo (`PAYMENT_PROVIDER_API_KEY` está en el
`.env.example` pero vacío y sin usar), y **el cobro es manual** — el usuario
paga y manda una captura que un admin aprueba a mano.

Opciones:
1. Dejar `provider_price_id` como el **payment link** y verificar solo que el
   enlace existe y responde 200 (sin API key).
2. Crear precios reales en Stripe y una clave de solo lectura, y hacer la
   verificación de importes de verdad.
3. Aplazar la verificación externa y dejar el hueco en el esquema.

**Mi recomendación: 1 ahora, 2 cuando automatices el cobro.** La verificación
del importe real solo tiene sentido cuando Stripe cobre por sí mismo; hoy el
importe que se cobra lo teclea una persona.

### D3 — ¿`landing-ventas` sigue viva?

`bot/landing-ventas/` es una landing comercial larga dentro del repo del bot,
con precios, claims y enlaces de Stripe duplicados, sin aviso +18, sin mención
de la prueba gratuita, y con datos desfasados. `bot/README.md` la describe, pero
`landing/README.md` dice que la landing comercial larga debería vivir "en un
repo o proyecto aparte".

Es donde se concentran la mitad de las contradicciones de este informe.

**Necesito saber si está publicada.** Si no lo está, borrarla resuelve C3, C7,
C11 y buena parte de C10 de golpe, y reduce mucho el trabajo de la fase 3. Si sí
lo está, entra en la migración como un consumidor más.

---

## Resumen

| Categoría | Apariciones | Sitios distintos con el mismo valor |
|---|---|---|
| Límites del plan gratuito | 31 | 4 |
| Precios | 45 | 8 |
| Prueba gratuita | 38 | 6 |
| Textos legales | 33 | 8 redacciones |
| Identidad | 70+ | 5 (marca) · 23 (bot username) |
| Ligas / frecuencia / horario | 25 | — |

**12 contradicciones** y **3 decisiones estructurales**, todas resueltas y
cerradas en [Resoluciones de la fase 0](#resoluciones-de-la-fase-0). Las siete
que se resolvieron por el principio rector quedaron confirmadas el 21/08/2026.

**Las cinco fases están hechas.** Los tres servicios leen los mismos valores del
mismo sitio y lo validan al arrancar: si `product.json` no cuadra con su esquema,
el proceso no arranca. `shared/` viaja sellado y cada repo comprueba su copia en
CI, así que una divergencia como la que ya ocurrió entre el canónico y la landing
ahora sale en rojo en vez de pasar desapercibida.

Solo dos cosas cambiaron comportamiento en producción, ambas por decisión
explícita: **C4** (IDs de canal cruzados en `.env.example`) y **C10** (aviso legal
único), más el fallback de estadísticas del bot de pagos, que inventaba
porcentajes justo cuando la base de datos fallaba.

### Lo que sigue fuera de este trabajo

- **Cambiar un precio exige tocar Stripe y `product.json` el mismo día**, en ese
  orden. No hay API que preguntar: son Payment Links y el arranque solo comprueba
  que responden, no que el importe coincida. Procedimiento en `shared/README.md`.
- **`test_panel_catchup_publica_ayer_si_falta`** falla en `main` del bot de picks
  desde antes de este trabajo y está excluido por nombre en su CI. Arreglarlo toca
  lógica de picks.
- **54 hallazgos de ruff** en el resto del repo del bot de picks. Por eso su lint
  está acotado a `shared/`.
