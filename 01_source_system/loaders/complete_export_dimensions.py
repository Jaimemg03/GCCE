"""Completa las dimensiones geograficas que el CSV de origen deja vacias.

Dos exportaciones del ISTAC solo escriben el nombre de la geografia en la
primera fila de cada bloque; el resto de filas del bloque heredan esa
geografia de forma implicita. La capa Raw conserva el CSV tal cual, por lo que
esas celdas quedan con la cadena vacia y rompen la lectura de la dimension.

Este script rellena **unicamente** las celdas vacias de ``geography`` en:

* ``src.istac_employment_cnae_raw``: 9 bloques de 110 filas (actividades CNAE).
* ``src.istac_employment_status_raw``: 105 bloques de 3 filas (situaciones).

Reglas de seguridad:

* Solo se actualizan filas con ``record_origin = 'real'``.
* Solo se actualizan celdas ``NULL`` o ``''``; ningun valor existente cambia.
* El relleno se hace *forward-fill* dentro de cada bloque, ordenando por
  ``row_id``. Los bloques son posicionales y de tamano fijo, tal como los
  exporta el ISTAC; asi el script sigue siendo idempotente cuando todas las
  celdas ya estan completas.
* Antes de escribir se valida la estructura de los bloques (tamano, numero y
  secuencia de la clave de serie) y se calcula una huella de todas las demas
  columnas. Si la huella cambia tras el UPDATE, se hace ``ROLLBACK``.
* El script es idempotente: una segunda ejecucion no encuentra celdas vacias.

Uso:
    python complete_export_dimensions.py              # dry-run (por defecto)
    python complete_export_dimensions.py --apply      # escribe los cambios
    python complete_export_dimensions.py --dataset istac_employment_cnae --apply
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "source_talento")
DB_USER = os.getenv("SRC_WRITER_USER", os.getenv("POSTGRES_USER", "src_writer"))
DB_PASS = os.getenv("SRC_WRITER_PASSWORD", os.getenv("POSTGRES_PASSWORD", ""))

DEFAULT_SAMPLE_SIZE = 5


@dataclass(frozen=True)
class FillRule:
    """Regla de relleno de una dimension geografica exportada por bloques."""

    dataset_name: str
    table_name: str
    series_key: str
    expected_blocks: int
    expected_block_size: int


FILL_RULES = [
    FillRule(
        dataset_name="istac_employment_cnae",
        table_name="src.istac_employment_cnae_raw",
        series_key="activity",
        expected_blocks=9,
        expected_block_size=110,
    ),
    FillRule(
        dataset_name="istac_employment_status",
        table_name="src.istac_employment_status_raw",
        series_key="employment_status",
        expected_blocks=105,
        expected_block_size=3,
    ),
]


@dataclass
class Block:
    """Bloque de filas que comparten una misma geografia."""

    geography: str
    start_row_id: int
    # [(row_id, series_key, geography_vacia), ...]
    rows: list = field(default_factory=list)

    @property
    def empty_row_ids(self):
        return [
            row_id
            for row_id, _, is_empty_geography in self.rows
            if is_empty_geography
        ]


@dataclass
class TablePlan:
    """Resultado de validar una tabla antes de escribir."""

    rule: FillRule
    blocks: list
    updates: list  # [(row_id, geography), ...]
    synthetic_rows: int
    total_rows: int
    fingerprint_before: str


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def is_empty(value):
    return value is None or value == ""


def get_total_rows(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def get_synthetic_rows(cur, table_name):
    cur.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE record_origin = 'synthetic'"
    )
    return cur.fetchone()[0]


def get_fingerprint(cur, table_name):
    """Huella de todas las columnas salvo ``geography``.

    Se calcula sobre todas las filas (reales y sinteticas) para demostrar que
    el relleno no altera ninguna otra columna ni ninguna otra fila.
    """
    cur.execute(
        f"""
        SELECT md5(
            string_agg(md5(payload::text), '' ORDER BY row_id)
        )
        FROM (
            SELECT row_id, to_jsonb(t) - 'geography' AS payload
            FROM {table_name} AS t
        ) AS rows
        """
    )
    return cur.fetchone()[0]


def check_geography_column(cur, table_name):
    cur.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'src'
          AND table_name = %s
          AND column_name = 'geography'
        """,
        (table_name.split(".", 1)[1],),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"{table_name}: no existe la columna geography")
    if row[0] != "text":
        raise RuntimeError(
            f"{table_name}: geography deberia ser TEXT y es {row[0]}"
        )


def check_whitespace_geographies(cur, table_name):
    """Detecta geografias con solo espacios, que no son celdas vacias."""
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE record_origin = 'real'
          AND geography IS NOT NULL
          AND geography <> ''
          AND btrim(geography) = ''
        """
    )
    return cur.fetchone()[0]


def fetch_real_rows(cur, rule):
    cur.execute(
        f"""
        SELECT row_id, geography, {rule.series_key}
        FROM {rule.table_name}
        WHERE record_origin = 'real'
        ORDER BY row_id
        """
    )
    return cur.fetchall()


def build_blocks(rule, rows):
    """Agrupa las filas reales en bloques posicionales de tamano fijo.

    La exportacion del ISTAC escribe ``expected_block_size`` filas por
    geografia. Se agrupa por posicion (no por celdas no vacias) para que el
    script siga siendo idempotente una vez rellenadas todas las celdas.
    """
    if not rows:
        raise RuntimeError(f"{rule.table_name}: no hay filas reales")

    if len(rows) % rule.expected_block_size != 0:
        raise RuntimeError(
            f"{rule.table_name}: {len(rows)} filas reales no son multiplo de "
            f"{rule.expected_block_size}; la estructura de bloques ha cambiado"
        )

    blocks = []
    for offset in range(0, len(rows), rule.expected_block_size):
        chunk = rows[offset : offset + rule.expected_block_size]

        if is_empty(chunk[0][1]):
            raise RuntimeError(
                f"{rule.table_name}: la primera fila del bloque que empieza en "
                f"row_id={chunk[0][0]} no tiene geografia; no se puede "
                "identificar el bloque"
            )

        geographies = {
            geography
            for _, geography, _ in chunk
            if not is_empty(geography)
        }

        if not geographies:
            raise RuntimeError(
                f"{rule.table_name}: el bloque que empieza en row_id="
                f"{chunk[0][0]} no tiene ninguna geografia"
            )
        if len(geographies) > 1:
            raise RuntimeError(
                f"{rule.table_name}: el bloque que empieza en row_id="
                f"{chunk[0][0]} mezcla varias geografias: "
                f"{sorted(geographies)}"
            )

        geography = next(iter(geographies))
        if geography != geography.strip():
            raise RuntimeError(
                f"{rule.table_name}: geografia con espacios sobrantes en el "
                f"bloque que empieza en row_id={chunk[0][0]}: {geography!r}"
            )

        block = Block(geography=geography, start_row_id=chunk[0][0])
        for row_id, row_geography, series_key in chunk:
            block.rows.append((row_id, series_key, is_empty(row_geography)))
        blocks.append(block)

    return blocks


def validate_blocks(rule, blocks):
    """Valida numero, tamano y secuencia de claves de los bloques."""
    if len(blocks) != rule.expected_blocks:
        raise RuntimeError(
            f"{rule.table_name}: se esperaban {rule.expected_blocks} bloques "
            f"y se han encontrado {len(blocks)}"
        )

    geographies = [block.geography for block in blocks]
    if len(set(geographies)) != len(geographies):
        duplicated = sorted(
            {geo for geo in geographies if geographies.count(geo) > 1}
        )
        raise RuntimeError(
            f"{rule.table_name}: geografias duplicadas entre bloques: {duplicated}"
        )

    reference_keys = None
    for block in blocks:
        keys = [series_key for _, series_key, _ in block.rows]
        if len(block.rows) != rule.expected_block_size:
            raise RuntimeError(
                f"{rule.table_name}: el bloque '{block.geography}' "
                f"(row_id={block.start_row_id}) tiene {len(block.rows)} filas "
                f"y se esperaban {rule.expected_block_size}"
            )
        if reference_keys is None:
            reference_keys = keys
        elif keys != reference_keys:
            raise RuntimeError(
                f"{rule.table_name}: el bloque '{block.geography}' "
                f"(row_id={block.start_row_id}) no repite la secuencia de "
                f"'{rule.series_key}' del primer bloque"
            )


def build_plan(conn, rule):
    with conn.cursor() as cur:
        check_geography_column(cur, rule.table_name)

        whitespace = check_whitespace_geographies(cur, rule.table_name)
        if whitespace:
            raise RuntimeError(
                f"{rule.table_name}: {whitespace} geografias contienen solo "
                "espacios; revisalas antes de rellenar"
            )

        rows = fetch_real_rows(cur, rule)
        blocks = build_blocks(rule, rows)
        validate_blocks(rule, blocks)

        updates = []
        for block in blocks:
            for row_id in block.empty_row_ids:
                updates.append((row_id, block.geography))

        total_rows = get_total_rows(cur, rule.table_name)
        synthetic_rows = get_synthetic_rows(cur, rule.table_name)
        fingerprint_before = get_fingerprint(cur, rule.table_name)

    return TablePlan(
        rule=rule,
        blocks=blocks,
        updates=updates,
        synthetic_rows=synthetic_rows,
        total_rows=total_rows,
        fingerprint_before=fingerprint_before,
    )


def print_plan(plan, sample_size):
    rule = plan.rule
    print(f"\n[{rule.dataset_name}] {rule.table_name}")
    print(
        f"  filas totales: {plan.total_rows} "
        f"(reales: {plan.total_rows - plan.synthetic_rows}, "
        f"sinteticas ignoradas: {plan.synthetic_rows})"
    )
    print(
        f"  bloques validados: {len(plan.blocks)} "
        f"x {rule.expected_block_size} filas"
    )
    print(f"  celdas geography vacias a rellenar: {len(plan.updates)}")
    print(f"  huella de columnas no objetivo: {plan.fingerprint_before}")

    if not plan.updates:
        print("  [SKIP] No hay celdas vacias; nada que hacer.")
        return

    print(f"  ejemplo (primeras {sample_size} actualizaciones):")
    for row_id, geography in plan.updates[:sample_size]:
        print(f"    row_id={row_id:<6} -> {geography}")


def apply_plan(conn, plan):
    """Aplica el relleno en una transaccion y verifica que solo cambia geography."""
    rule = plan.rule
    if not plan.updates:
        return 0

    values_sql = ", ".join(["(%s::bigint, %s::text)"] * len(plan.updates))
    params = []
    for row_id, geography in plan.updates:
        params.extend([row_id, geography])

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {rule.table_name} AS t
            SET geography = v.geography
            FROM (VALUES {values_sql}) AS v(row_id, geography)
            WHERE t.row_id = v.row_id
              AND t.record_origin = 'real'
              AND (t.geography IS NULL OR t.geography = '')
            """,
            params,
        )
        updated = cur.rowcount

        if updated != len(plan.updates):
            raise RuntimeError(
                f"{rule.table_name}: se esperaban {len(plan.updates)} "
                f"actualizaciones y se han aplicado {updated}"
            )

        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM {rule.table_name}
            WHERE record_origin = 'real'
              AND (geography IS NULL OR geography = '')
            """
        )
        remaining = cur.fetchone()[0]
        if remaining:
            raise RuntimeError(
                f"{rule.table_name}: quedan {remaining} geografias vacias "
                "tras el relleno"
            )

        cur.execute(
            f"""
            SELECT COUNT(DISTINCT geography)
            FROM {rule.table_name}
            WHERE record_origin = 'real'
              AND geography IS NOT NULL
              AND geography <> ''
            """
        )
        distinct_geographies = cur.fetchone()[0]
        if distinct_geographies != rule.expected_blocks:
            raise RuntimeError(
                f"{rule.table_name}: se esperaban {rule.expected_blocks} "
                f"geografias distintas y hay {distinct_geographies}"
            )

        total_rows = get_total_rows(cur, rule.table_name)
        if total_rows != plan.total_rows:
            raise RuntimeError(
                f"{rule.table_name}: el numero de filas ha cambiado "
                f"({plan.total_rows} -> {total_rows})"
            )

        fingerprint_after = get_fingerprint(cur, rule.table_name)
        if fingerprint_after != plan.fingerprint_before:
            raise RuntimeError(
                f"{rule.table_name}: la huella de columnas no objetivo ha "
                "cambiado; se cancela la transaccion"
            )

    return updated


def run_rule(conn, rule, apply, sample_size):
    plan = build_plan(conn, rule)
    print_plan(plan, sample_size)

    if not apply:
        if plan.updates:
            print("  [DRY-RUN] No se ha escrito nada. Usa --apply para aplicar.")
        return 0

    try:
        updated = apply_plan(conn, plan)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    if updated:
        print(f"  [OK] {updated} celdas geography completadas y verificadas.")
    else:
        print("  [OK] Nada que actualizar; la tabla ya estaba completa.")
    return updated


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Completa las geografias vacias de las exportaciones ISTAC por "
            "bloques. Por defecto solo simula (dry-run)."
        )
    )
    parser.add_argument(
        "--dataset",
        choices=[rule.dataset_name for rule in FILL_RULES],
        help="Procesa solo el dataset indicado.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Escribe los cambios. Sin esta opcion el script es un dry-run.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Numero de actualizaciones de ejemplo a mostrar (por defecto 5).",
    )
    args = parser.parse_args()

    selected = [
        rule
        for rule in FILL_RULES
        if args.dataset in (None, rule.dataset_name)
    ]

    conn = get_connection()
    failures = []
    total_updated = 0
    try:
        for rule in selected:
            try:
                total_updated += run_rule(
                    conn,
                    rule,
                    apply=args.apply,
                    sample_size=max(args.sample_size, 0),
                )
            except Exception as exc:
                conn.rollback()
                failures.append((rule.dataset_name, exc))
                print(
                    f"[ERROR] {rule.dataset_name}: {exc}",
                    file=sys.stderr,
                )
    finally:
        conn.close()

    print("\nResumen:")
    print(f"  modo: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"  celdas actualizadas: {total_updated}")
    if failures:
        print("  errores:", file=sys.stderr)
        for dataset_name, exc in failures:
            print(f"    - {dataset_name}: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
