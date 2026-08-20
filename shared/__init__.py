"""
Fuente única de verdad de precios, límites, identidad y textos.

Uso:

    from shared import product_config as cfg

    cfg.verify_startup()            # al arrancar el servicio
    cfg.price("goles")              # '20€'
    cfg.legal("short")              # aviso legal canónico
    cfg.text("trial.cta_button", product="futbol")

Ver `shared/README.md`. Cualquier cambio de precio, límite o texto legal se
hace en estos ficheros y en ningún otro sitio.
"""
from . import product_config  # noqa: F401
