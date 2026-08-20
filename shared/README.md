# `shared/` — fuente única de verdad

Cualquier cambio de **precio, límite, duración de prueba, canal, identidad o texto
legal** se hace **aquí y en ningún otro sitio**. Si un número o una promesa aparece
escrita a mano en un `.py`, un `.html` o un `.env`, es un bug.

- **`product.json`** — valores estructurados: importes en céntimos, límites, días,
  IDs de canal, URLs. Un valor, un sitio.
- **`copy.es.json`** — textos. **Nunca escribas un número dentro de un texto:** usa
  un marcador (`{trial_days}`, `{free_picks_per_day}`) que se resuelve en tiempo de
  ejecución desde `product.json`. De ahí venía la contradicción "1 vs 2 picks".
- **`product.schema.json`** — valida `product.json` al arrancar. Si falla, el
  proceso **no arranca**: mejor caer ruidosamente que servir un precio incorrecto.

**Sin secretos.** Aquí solo van valores públicos; este directorio debe poder
publicarse sin riesgo. Tokens, claves y credenciales siguen en variables de entorno.

**Repo canónico: `telegram-payments`.** Los otros dos repos llevan una copia
sincronizada por CI; un test de guardia falla si alguna diverge del canónico.
Edita siempre el canónico.

---

## Cambiar un precio (procedimiento manual)

Hoy el cobro **no** está automatizado: son *Payment Links* de Stripe y la
aprobación es manual, así que el importe vive en **dos** sitios — el panel de
Stripe y `product.json` — y nada los reconcilia solo. El arranque únicamente
comprueba que el Payment Link responde, no que el importe coincida.

Por eso, **el mismo día y en este orden**:

1. **Stripe primero.** Cambia el importe del Payment Link en el panel de Stripe.
2. **`product.json` después.** Actualiza `amount_cents` del plan correspondiente
   (en céntimos: `20€` → `2000`).
3. Despliega. Verifica que el botón de pago del bot muestra el importe nuevo y
   que el enlace lleva al importe nuevo en Stripe.

Si los dos pasos no ocurren el mismo día, hay una ventana en la que el bot anuncia
un precio y Stripe cobra otro. El día que Stripe cobre por sí mismo (precios reales
con `price_id` y clave de API), esta comprobación pasa a ser automática y este
procedimiento desaparece.

---

## Estructura

Una marca, dos verticales. `brand`, `compliance`, `currency`, `payment_methods`,
`access` y `tracking` son **globales**; todo lo que describa límites, mercados,
canales, planes o prueba vive **dentro de su producto** (`products.futbol`,
`products.pinpon`). No hay un `picks_per_day` global, y no debe haberlo: son dos
productos con dos límites distintos, y colapsarlos fue justo el origen del problema.

`copy.es.json` sigue la misma división: `legal` y `brand` compartidos, un bloque
por producto para todo lo que describa límites o mercados. Está preparado para
añadir `copy.ca.json` y `copy.en.json` sin refactorizar: misma estructura de claves,
otro fichero.

### Qué NO vive aquí

Fuera de alcance a propósito, para no mezclar producto con operación:

- **Lógica de picks:** filtros, gates NG1/UGM, listas negras de liga, stakes de
  estrategia y umbrales de clasificación. Siguen en `config.py` de cada repo.
- **Canales internos:** RAW de ping pong, canal de origen de alertas, previews de
  stories, avisos de apuestas en dry-run y recordatorios de admin. No son producto.
- **Secretos y endpoints:** tokens, `DATABASE_URL`, claves de API. Variables de
  entorno.

### Valores derivados

Nunca se almacenan; se calculan siempre:

| Marcador | Se calcula desde |
|---|---|
| `plan_price` | `amount_cents` + `currency` |
| `responsible_gambling_domain` | `compliance.responsible_gambling_url` |
| `free_hours_range` | `free_tier.publication_hours` |
| `leagues_list` | `products.pinpon.leagues` |

### Deep links

`tracking.start_param_format` exige **origen y vertical**:
`{intent}__src-{source}__vrt-{vertical}__cmp-{campaign}__cnt-{content}`, con tope de
64 caracteres. El orden de `start_param_token_order` es el orden de truncado: `src`
y `vrt` van primero porque son los que deben sobrevivir. Los enlaces apuntan siempre
a `brand.website_url` **con `www`**: el redirect www/no-www descartaba los parámetros
UTM y con ellos la atribución de origen.
