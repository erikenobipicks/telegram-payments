"""
Sincroniza `shared/` desde el repo canónico a los repos que lo consumen (D1).

El repo canónico es `telegram-payments`. Los otros dos llevan una copia literal;
nadie edita la copia. Este script la propaga y comprueba que no ha divergido.

    python -m shared.sync --check ../erikenobi-telegram-bot
    python -m shared.sync --to    ../erikenobi-telegram-bot

`--check` devuelve código de salida 1 si la copia difiere, para poder usarlo
como guarda en CI. Pero CI no ve los tres repos a la vez: cada uno se construye
solo. Por eso `shared/` viaja **sellado** — `CHECKSUMS.txt` lleva el sha256 de
cada fichero — y cada repo comprueba su propia copia contra el sello sin
necesitar el canónico delante (ver `plantilla_test()`).

El sello lo reescribe `--stamp`, y `--to` lo reescribe antes de copiar. Editar
`shared/` sin volver a sellar deja el sello obsoleto y el test falla: es el
recordatorio de que el cambio todavía no se ha propagado.
"""
from __future__ import annotations

import argparse
import filecmp
import hashlib
import shutil
from pathlib import Path

_DIR = Path(__file__).resolve().parent

_CABECERA_SELLO = (
    "# Sello de shared/. Lo genera `python -m shared.sync --stamp` en el repo\n"
    "# canónico (telegram-payments). No lo edites a mano: si no coincide con los\n"
    "# ficheros, alguien ha editado una copia en vez del original.\n"
)


def _sha256(ruta: Path) -> str:
    return hashlib.sha256(ruta.read_bytes()).hexdigest()

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

# El sello no se hashea a sí mismo, pero sí se copia.
SELLO = "CHECKSUMS.txt"


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
        if p.is_file() and p.name not in FICHEROS and p.name != SELLO
        and not p.name.startswith(".")
    }
    diferencias += [f"{n}: sobra en la copia" for n in sorted(sobrantes)]
    return diferencias


def sellar() -> str:
    """Reescribe `CHECKSUMS.txt` con el sha256 de cada fichero. Devuelve el sello."""
    sello = "".join(f"{_sha256(_DIR / n)}  {n}\n" for n in sorted(FICHEROS))
    (_DIR / SELLO).write_text(_CABECERA_SELLO + sello, encoding="utf-8")
    return sello


def verificar_sello(directorio: Path | None = None) -> list[str]:
    """
    Comprueba un `shared/` contra el sello que viaja dentro de él.

    No necesita el repo canónico delante: por eso puede correr en el CI de
    cualquiera de los tres repos. Devuelve la lista de discrepancias.
    """
    base = Path(directorio) if directorio else _DIR
    fichero = base / SELLO
    if not fichero.exists():
        return [f"falta {SELLO}: shared/ viaja sin sello"]

    esperado = {}
    for linea in fichero.read_text(encoding="utf-8").splitlines():
        if not linea.strip() or linea.startswith("#"):
            continue
        digest, _, nombre = linea.partition("  ")
        esperado[nombre] = digest

    problemas = []
    for nombre in sorted(FICHEROS):
        ruta = base / nombre
        if nombre not in esperado:
            problemas.append(f"{nombre}: no está en el sello")
        elif not ruta.exists():
            problemas.append(f"{nombre}: falta")
        elif _sha256(ruta) != esperado[nombre]:
            problemas.append(f"{nombre}: no coincide con el sello")
    problemas += [f"{n}: está en el sello pero ya no existe en shared/"
                  for n in sorted(set(esperado) - set(FICHEROS))]
    return problemas


def copiar(repo: Path) -> list[str]:
    sellar()
    destino = _destino(repo)
    destino.mkdir(parents=True, exist_ok=True)
    copiados = []
    for nombre in (*FICHEROS, SELLO):
        shutil.copy2(_DIR / nombre, destino / nombre)
        copiados.append(nombre)
    return copiados


def plantilla_test() -> str:
    """Test de guardia que debe llevar todo repo con una copia de `shared/`."""
    return '''
def test_shared_no_ha_divergido_del_canonico():
    """
    `shared/` es una copia literal del repo canónico (telegram-payments) y viaja
    sellada. Si un fichero no coincide con su sello, alguien ha editado la copia
    en vez del original, y los precios o los textos van a contradecirse entre
    servicios: justo lo que este directorio existe para impedir.

    Se arregla editando el canónico y propagando:
        python -m shared.sync --to <este repo>
    """
    from shared import sync
    problemas = sync.verificar_sello()
    assert not problemas, (
        "shared/ no coincide con su sello:\\n  " + "\\n  ".join(problemas)
        + "\\n\\nNo edites la copia. Edita telegram-payments/shared/ y propaga."
    )
'''.strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--check", metavar="REPO", help="comprueba la copia y no escribe")
    grupo.add_argument("--to", metavar="REPO", help="copia shared/ al repo destino")
    grupo.add_argument("--stamp", action="store_true",
                       help="reescribe CHECKSUMS.txt desde los ficheros actuales")
    grupo.add_argument("--print-test", action="store_true",
                       help="imprime el test de guardia para el repo copia")
    args = parser.parse_args(argv)

    if args.stamp:
        sellar()
        print(f"sellado {_DIR / SELLO} ({len(FICHEROS)} ficheros)")
        return 0

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
