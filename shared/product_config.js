/**
 * Cargador de la fuente única de verdad para el navegador.
 *
 * Contrato equivalente al de `product_config.py`: la config se lee una sola vez
 * y se cachea, `text()` resuelve los marcadores y lanza error si queda alguno
 * sin resolver, y el precio mostrable se deriva siempre de `amount_cents`.
 *
 * DIFERENCIA DELIBERADA con el cargador de Python: aquí NO se valida contra
 * `product.schema.json`. El navegador recibe una config que el servidor ya
 * validó al arrancar; embarcar el esquema y un validador solo añadiría peso sin
 * detectar nada nuevo. Lo que sí se hace es fallar ruidosamente si la config no
 * llega, llega incompleta, o un texto deja un marcador sin resolver.
 *
 * Uso: el servidor embebe la config en la página
 *
 *   <script type="application/json" id="product-config">{"product":…,"copy":…}</script>
 *
 * y este módulo la lee la primera vez que se le pide algo.
 */
(function (global) {
  "use strict";

  var MARCADOR = /\{([^{}]+)\}/g;
  var SIMBOLO_MONEDA = { EUR: "€" };
  var cache = null;

  function ConfigError(mensaje) {
    var e = new Error(mensaje);
    e.name = "ConfigError";
    return e;
  }

  function cargar() {
    if (cache) return cache;
    var nodo = document.getElementById("product-config");
    if (!nodo) {
      throw ConfigError(
        'falta <script type="application/json" id="product-config"> en la página'
      );
    }
    var datos;
    try {
      datos = JSON.parse(nodo.textContent);
    } catch (e) {
      throw ConfigError("product-config no es JSON válido: " + e.message);
    }
    if (!datos || !datos.product || !datos.copy) {
      throw ConfigError("product-config debe traer las claves 'product' y 'copy'");
    }
    cache = datos;
    return cache;
  }

  /** Permite inyectar la config a mano (tests, o render sin <script>). */
  function init(datos) {
    if (!datos || !datos.product || !datos.copy) {
      throw ConfigError("init() necesita {product, copy}");
    }
    cache = datos;
    return cache;
  }

  function config() {
    return cargar().product;
  }

  function copy() {
    return cargar().copy;
  }

  function producto(vertical) {
    var p = config().products[vertical];
    if (!p) {
      throw ConfigError(
        "producto desconocido '" + vertical + "'; hay: " +
          Object.keys(config().products).join(", ")
      );
    }
    return p;
  }

  function plan(planId) {
    var productos = config().products;
    for (var vertical in productos) {
      var lista = productos[vertical].plans || [];
      for (var i = 0; i < lista.length; i++) {
        if (lista[i].id === planId) {
          var copia = Object.assign({}, lista[i]);
          copia.vertical = vertical;
          return copia;
        }
      }
    }
    var bundles = config().bundles || [];
    for (var j = 0; j < bundles.length; j++) {
      if (bundles[j].id === planId) return Object.assign({ vertical: null }, bundles[j]);
    }
    throw ConfigError("plan desconocido '" + planId + "'");
  }

  function mercado(vertical, marketId) {
    var lista = producto(vertical).markets || [];
    for (var i = 0; i < lista.length; i++) {
      if (lista[i].id === marketId) return lista[i];
    }
    throw ConfigError("mercado desconocido '" + marketId + "' en '" + vertical + "'");
  }

  /** Picks gratis al día según el modo activo. Sin tope => error. */
  function freePicksPerDay(vertical) {
    var libre = producto(vertical).free_tier;
    var valor = libre.mode ? libre.modes[libre.mode].picks_per_day : libre.picks_per_day;
    if (valor === null || valor === undefined) {
      throw ConfigError(
        vertical + ": el modo free '" + libre.mode + "' no tiene tope de picks; " +
          "no uses un texto que anuncie un número"
      );
    }
    return valor;
  }

  /** 2000 -> '20€' · 1999 -> '19,99€' (convención es-ES). */
  function formatPrice(amountCents, currency) {
    var moneda = currency || config().currency;
    var simbolo = SIMBOLO_MONEDA[moneda];
    if (!simbolo) throw ConfigError("moneda sin símbolo definido: '" + moneda + "'");
    if (!Number.isInteger(amountCents) || amountCents < 0) {
      throw ConfigError("importe inválido: " + amountCents);
    }
    var enteros = Math.floor(amountCents / 100);
    var centimos = amountCents % 100;
    if (!centimos) return enteros + simbolo;
    return enteros + "," + String(centimos).padStart(2, "0") + simbolo;
  }

  function price(planId) {
    return formatPrice(plan(planId).amount_cents);
  }

  function dominio(url) {
    return String(url).replace(/^https?:\/\//, "").replace(/^www\./, "").split("/")[0];
  }

  function sanear(valor) {
    return String(valor === null || valor === undefined ? "" : valor)
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 24);
  }

  /** Start param con origen Y vertical, respetando el tope de Telegram. */
  function startParam(intent, source, vertical, campaign, content) {
    var t = config().tracking;
    var valores = {
      src: sanear(source),
      vrt: sanear(vertical),
      cmp: sanear(campaign || t.default_utm_campaign),
      cnt: sanear(content),
    };
    var resultado = sanear(intent) || "start";
    var orden = t.start_param_token_order;
    for (var i = 0; i < orden.length; i++) {
      var token = orden[i];
      if (!valores[token]) continue;
      var candidato = resultado + "__" + token + "-" + valores[token];
      if (candidato.length > t.start_param_max_length) break;
      resultado = candidato;
    }
    return resultado;
  }

  function botDeepLink(intent, source, vertical, campaign, content) {
    return config().brand.bot_url + "?start=" +
      startParam(intent, source, vertical, campaign, content);
  }

  function claveCopy(clave) {
    var nodo = copy();
    var partes = clave.split(".");
    for (var i = 0; i < partes.length; i++) {
      if (!nodo || typeof nodo !== "object" || !(partes[i] in nodo)) {
        throw ConfigError("clave de texto desconocida: '" + clave + "'");
      }
      nodo = nodo[partes[i]];
    }
    return nodo;
  }

  function contexto(opts) {
    var c = config();
    var ctx = {
      brand_premium_name: function () { return c.brand.premium_name; },
      brand_name: function () { return c.brand.name; },
      website_url: function () { return c.brand.website_url; },
      support_contact: function () { return c.brand.support_contact; },
      support_url: function () { return c.brand.support_url; },
      bot_url: function () { return c.brand.bot_url; },
      instagram_handle: function () { return c.brand.instagram_handle; },
      min_age: function () { return c.compliance.min_age; },
      responsible_gambling_url: function () { return c.compliance.responsible_gambling_url; },
      responsible_gambling_domain: function () { return dominio(c.compliance.responsible_gambling_url); },
      plan_interval_days: function () { return c.access.plan_interval_days; },
      invite_expiry_hours: function () { return c.access.invite_expiry_hours; },
      referrer_bonus_days: function () { return c.access.referral.referrer_bonus_days; },
    };

    if (opts.product) {
      var v = opts.product;
      var p = producto(v);
      ctx.product_display_name = function () { return p.display_name; };
      ctx.stats_url = function () { return p.stats_url; };
      ctx.channel_free_url = function () { return p.telegram_channel_free; };
      ctx.trial_days = function () { return p.trial.duration_days; };
      ctx.free_picks_per_day = function () { return freePicksPerDay(v); };
      ctx.free_hours_range = function () {
        var h = p.free_tier.publication_hours;
        if (!h) throw ConfigError(v + ": no tiene publication_hours");
        return h.start + ":00–" + h.end + ":00";
      };
      ctx.reveal_minutes_before = function () { return p.free_tier.reveal_minutes_before; };
      ctx.alert_minutes_before = function () { return p.method.alert_minutes_before; };
      ctx.bookmakers = function () { return p.method.bookmakers; };
      ctx.stake_game_2 = function () { return p.method.stake_game_2; };
      ctx.stake_game_3 = function () { return p.method.stake_game_3; };
      ctx.leagues_list = function () {
        return p.leagues.map(function (x) { return x.name; }).join(", ");
      };
    }

    if (opts.plan) {
      var datosPlan = plan(opts.plan);
      ctx.plan_display_name = function () { return datosPlan.display_name; };
      ctx.plan_price = function () { return formatPrice(datosPlan.amount_cents); };
    }

    if (opts.product && opts.market) {
      var m = mercado(opts.product, opts.market);
      ctx.market_display_name = function () { return m.display_name; };
      ctx.advance_hours_max = function () { return m.advance_hours_max; };
      ctx.reminder_minutes_before = function () { return m.reminder_minutes_before; };
    }

    Object.keys(opts).forEach(function (k) {
      if (k === "product" || k === "plan" || k === "market") return;
      ctx[k] = function () { return opts[k]; };
    });
    return ctx;
  }

  function resolver(plantilla, ctx, visitadas) {
    return String(plantilla).replace(MARCADOR, function (_, nombre) {
      if (nombre.indexOf(".") !== -1) {
        if (visitadas.indexOf(nombre) !== -1) {
          throw ConfigError("referencia circular en textos: '" + nombre + "'");
        }
        var destino = claveCopy(nombre);
        if (typeof destino !== "string") {
          throw ConfigError("la referencia '" + nombre + "' no apunta a un texto");
        }
        return resolver(destino, ctx, visitadas.concat([nombre]));
      }
      if (!(nombre in ctx)) {
        throw ConfigError(
          "marcador {" + nombre + "} sin valor en este contexto; " +
            "¿falta pasar product/plan/market o un valor de runtime?"
        );
      }
      var valor = ctx[nombre]();
      if (valor === null || valor === undefined) {
        throw ConfigError("marcador {" + nombre + "} resuelve a vacío");
      }
      return String(valor);
    });
  }

  function resolverPlantilla(plantilla, clave, opts) {
    var resuelto = resolver(plantilla, contexto(opts || {}), [clave]);
    MARCADOR.lastIndex = 0;
    if (MARCADOR.test(resuelto)) {
      MARCADOR.lastIndex = 0;
      throw ConfigError("'" + clave + "' deja marcadores sin resolver");
    }
    return resuelto;
  }

  function text(clave, opts) {
    var plantilla = claveCopy(clave);
    if (typeof plantilla !== "string") {
      throw ConfigError("'" + clave + "' no es un texto");
    }
    return resolverPlantilla(plantilla, clave, opts);
  }

  function texts(clave, opts) {
    var valores = claveCopy(clave);
    if (!Array.isArray(valores)) {
      throw ConfigError("'" + clave + "' no es una lista de textos");
    }
    return valores.map(function (v) { return resolverPlantilla(v, clave, opts); });
  }

  function legal(variante) {
    return text("legal." + (variante || "short"));
  }

  var api = {
    ConfigError: ConfigError,
    init: init, config: config, copy: copy,
    product: producto, plan: plan, market: mercado,
    freePicksPerDay: freePicksPerDay,
    formatPrice: formatPrice, price: price,
    startParam: startParam, botDeepLink: botDeepLink,
    text: text, texts: texts, legal: legal,
  };

  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else global.ProductConfig = api;
})(typeof globalThis !== "undefined" ? globalThis : this);
