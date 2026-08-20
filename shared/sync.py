"""
Sincroniza `shared/` desde el repo canónico a los repos que lo consumen (D1).

El repo canónico es `telegram-payments`. Los otros dos llevan una copia literal;
nadie edita la copia. Este script la propaga y comprueba que no ha divergido.

    python -m shared.sync --check ../erikenobi-telegram-bot
    python -m shared.sync --to    ../erikenobi-telegram-bot

`--check` devuelve código de salida 1 si la copia difiere, para poder usarlo
como guarda en CI. Cada repo que reciba una copia debe añadir un test que
ejecute la misma comparación (ver `plantilla_test()`).
"""
from __future__ import annotations

import argparse
import filecmp
import shutil
from pathlib import Path

_DIR = Path(__file__).resolve().parent

# Lo que se copia. `sync.py` viaja con el resto para que la copia pueda
# comprobarse a sí misma.
FICHEROS = (
    "__init__.py",
    "product.json",
    "product.schema.json",
    "copy.es.json",
    "product_config.py",
    "product_config.js",
    "sync.py",
    "README.md",
)


def _destino(repo: Path) -> Path:
    return Path(repo).resolve() / "shared"


def comparar(repo: Path) -> list[str]:
    """Devuelve la lista de ficheros que difieren o faltan en la copia."""
    destino = _destino(repo)
    if not destino.is_dir():
        return [f"(falta el directorio {destino})"]
    diferencias = []
    for nombre in FICHEROS:
        origen, copia = _DIR / nombre, destino / nombre
        if not copia.exists():
            diferencias.append(f"{nombre}: falta en la copia")
        elif not filecmp.cmp(origen, copia, shallow=False):
            diferencias.append(f"{nombre}: difiere del canónico")
    sobrantes = {
        p.name for p in destino.iterdir()
        if p.is_file() and p.name not in FICHEROS and not p.name.startswith(".")
    }
    diferencias += [f"{n}: sobra en la copia" for n in sorted(sobrantes)]
    return diferencias


def copiar(repo: Path) -> list[str]:
    destino = _destino(repo)
    destino.mkdir(parents=True, exist_ok=True)
    copiados = []
    for nombre in FICHEROS:
        shutil.copy2(_DIR / nombre, destino / nombre)
        copiados.append(nombre)
    return copiados


def plantilla_test() -> str:
    """Test de guardia que debe llevar cada repo que reciba una copia."""
    return '''
def test_shared_no_ha_divergido_del_canonico():
    """
    `shared/` es una copia literal del repo canónico (telegram-payments).
    Si difiere, alguien ha editado la copia en vez del original y los precios
    o los textos van a contradecirse entre servicios.
    """
    from shared import sync
    canonico = Path(os.environ.get("SHARED_CANONICO", ""))
    if not canonico.is_dir():
        pytest.skip("SHARED_CANONICO no apunta al repo canónico")
    diferencias = sync.comparar(canonico)
    assert not diferencias, "la copia ha divergido:\\n  " + "\\n  ".join(diferencias)
'''.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--check", metavar="REPO", help="comprueba la copia y no escribe")
    grupo.add_argument("--to", metavar="REPO", help="copia shared/ al repo destino")
    grupo.add_argument("--print-test", action="store_true",
                       help="imprime el test de guardia para el repo copia")
    args = parser.parse_args(argv)

    if args.print_test:
        print(plantilla_test())
        return 0

    if args.check:
        diferencias = comparar(Path(args.check))
        if diferencias:
            print(f"la copia en {args.check} ha divergido del canónico:")
            for d in diferencias:
                print(f"  {d}")
            return 1
        print(f"la copia en {args.check} está al día")
        return 0

    copiados = copiar(Path(args.to))
    print(f"copiados {len(copiados)} ficheros a {_destino(Path(args.to))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
