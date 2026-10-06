"""
Capa PLATA del pipeline (bronce -> plata).

Recibe los DataFrames que entrega src/extract/extract_excel.py (columnas ya en
español), los limpia y los guarda en data/silver/:

    data/silver/ventas_silver.csv
    data/silver/benchmark_silver.csv

Decisiones tomadas a partir del EDA (notebooks/eda.ipynb):
    - Fechas en tres formatos (8/23/2022, 11/20/2022 y 07-03-2022). Las de barra
      son mes/día; el orden de las de guion se detecta automáticamente.
    - tipo_pago tiene ~10 % de nulos: se etiquetan como "Desconocido" en vez de
      borrar filas, porque el tipo de pago no interviene en los KPIs.
    - Los montos altos (hasta 900 = 60 x 15) son legítimos: no se eliminan.
    - monto_transaccion debe ser precio_unitario x cantidad: si no cuadra, se
      recalcula y queda registrado en el log.
    - genero_cajero no aporta al problema de mermas: se elimina.
    - Se agregan columnas de calendario (día de la semana, fin de semana,
      quincena) que usará la capa oro.

Uso independiente (desde la raíz del proyecto, con el venv activado):
    python -m src.transform.clean_excel
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RAIZ = Path(__file__).resolve().parents[2]
silver_dir = RAIZ / "data" / "silver"

# --------------------------------------------------------------------------- #
# Constantes
# --------------------------------------------------------------------------- #
COLUMNAS_VENTAS = [
    "id_orden", "fecha", "nombre_item", "tipo_item", "precio_unitario",
    "cantidad", "monto_transaccion", "tipo_pago", "genero_cajero", "franja_horaria",
]
COLUMNAS_A_ELIMINAR_VENTAS = ["genero_cajero"]
FRANJAS_VALIDAS = ["Morning", "Afternoon", "Evening", "Night", "Midnight"]
DIAS_SEMANA = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
VALOR_PAGO_DESCONOCIDO = "Desconocido"

# Días posteriores al pago que también se marcan como quincena.
# En Colombia el pago suele ser el 15 y el último día del mes.
DIAS_DESPUES_DE_PAGO = 1

COLUMNAS_BENCHMARK_MIN = ["pais", "servicio_alimentos_kg_percapita_anio", "confianza_estimacion"]
NIVEL_CONFIANZA = {
    "Very Low Confidence": 1,
    "Low Confidence": 2,
    "Medium Confidence": 3,
    "High Confidence": 4,
}

PATRON_BARRA = r"^\d{1,2}/\d{1,2}/\d{4}$"
PATRON_GUION = r"^\d{1,2}-\d{1,2}-\d{4}$"
FORMATO_MES_DIA = "%m-%d-%Y"
FORMATO_DIA_MES = "%d-%m-%Y"


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def _validar_columnas(df: pd.DataFrame, requeridas: list[str], fuente: str) -> None:
    faltantes = [c for c in requeridas if c not in df.columns]
    if faltantes:
        raise KeyError(
            f"A la fuente '{fuente}' le faltan columnas: {faltantes}. "
            f"Columnas recibidas: {list(df.columns)}"
        )


def _limpiar_textos(df: pd.DataFrame) -> pd.DataFrame:
    """Quita espacios al inicio y al final, y convierte textos vacíos en nulos."""
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
            texto = df[col].astype("string").str.strip()
            df[col] = texto.mask(texto == "")
    return df


def _formato_fechas_guion(texto: pd.Series, referencia: pd.Series) -> str:
    """
    Decide si las fechas con guion son mes-día o día-mes.

    1. Si algún primer número es > 12, no puede ser mes: es día-mes.
    2. Si algún segundo número es > 12, no puede ser mes: es mes-día.
    3. Si no hay forma de saberlo, se elige el formato que deja más fechas
       dentro del rango de las fechas con barra (que no son ambiguas).
    """
    partes = texto.str.extract(r"^(\d{1,2})-(\d{1,2})-(\d{4})$").astype(int)
    primero_mayor = int((partes[0] > 12).sum())
    segundo_mayor = int((partes[1] > 12).sum())
    logger.info(
        "Fechas con guion: %d con primer número > 12, %d con segundo número > 12",
        primero_mayor, segundo_mayor,
    )

    if segundo_mayor and not primero_mayor:
        return FORMATO_MES_DIA
    if primero_mayor and not segundo_mayor:
        return FORMATO_DIA_MES
    if primero_mayor and segundo_mayor:
        logger.warning(
            "Las fechas con guion mezclan mes-día y día-mes. Se usa mes-día y las "
            "filas que no encajen se intentan con día-mes."
        )
        return FORMATO_MES_DIA

    referencia = referencia.dropna()
    if referencia.empty:
        logger.warning("Fechas con guion ambiguas y sin referencia: se asume mes-día.")
        return FORMATO_MES_DIA

    minimo, maximo = referencia.min(), referencia.max()
    dentro = {
        fmt: int(pd.to_datetime(texto, format=fmt, errors="coerce").between(minimo, maximo).sum())
        for fmt in (FORMATO_MES_DIA, FORMATO_DIA_MES)
    }
    elegido = max(dentro, key=dentro.get)
    logger.info("Fechas con guion ambiguas. Dentro del rango de referencia: %s", dentro)
    return elegido


def parsear_fechas(serie: pd.Series) -> pd.Series:
    """Convierte la columna fecha (con formatos mezclados) a datetime."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie

    texto = serie.astype("string").str.strip()
    resultado = pd.Series(pd.NaT, index=serie.index, dtype="datetime64[ns]")

    es_barra = texto.str.match(PATRON_BARRA).fillna(False).astype(bool)
    es_guion = texto.str.match(PATRON_GUION).fillna(False).astype(bool)
    otros = ~es_barra & ~es_guion & texto.notna()

    if es_barra.any():
        resultado.loc[es_barra] = pd.to_datetime(texto[es_barra], format="%m/%d/%Y", errors="coerce")

    if es_guion.any():
        formato = _formato_fechas_guion(texto[es_guion], resultado[es_barra])
        logger.info("Formato elegido para fechas con guion: %s", formato)
        convertidas = pd.to_datetime(texto[es_guion], format=formato, errors="coerce")
        alterno = FORMATO_DIA_MES if formato == FORMATO_MES_DIA else FORMATO_MES_DIA
        fallidas = convertidas.isna()
        if fallidas.any():
            convertidas.loc[fallidas] = pd.to_datetime(
                texto[es_guion][fallidas], format=alterno, errors="coerce"
            )
        resultado.loc[es_guion] = convertidas

    if otros.any():
        logger.warning("%d fechas con un formato no esperado; se intenta conversión flexible.", otros.sum())
        resultado.loc[otros] = pd.to_datetime(texto[otros], format="mixed", errors="coerce")

    logger.info(
        "Fechas: %d con barra, %d con guion, %d otras, %d sin poder convertir (NaT)",
        es_barra.sum(), es_guion.sum(), otros.sum(), resultado.isna().sum(),
    )
    return resultado


def _es_quincena(fechas: pd.Series, dias_despues: int = DIAS_DESPUES_DE_PAGO) -> pd.Series:
    """True el día 15, el último día del mes y los `dias_despues` días siguientes a cada uno."""
    dia = fechas.dt.day
    ultimo_dia = fechas.dt.days_in_month
    cerca_del_15 = dia.between(15, 15 + dias_despues)
    fin_de_mes = dia == ultimo_dia
    inicio_de_mes = dia <= dias_despues
    return (cerca_del_15 | fin_de_mes | inicio_de_mes).fillna(False).astype(bool)


# --------------------------------------------------------------------------- #
# Limpieza de ventas
# --------------------------------------------------------------------------- #
def limpiar_ventas(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia las transacciones de ventas y agrega columnas de calendario."""
    _validar_columnas(df, COLUMNAS_VENTAS, "ventas")
    df = df.copy()
    filas_iniciales = len(df)

    # 1. Textos
    df = _limpiar_textos(df)

    # 2. Duplicados
    duplicados_exactos = int(df.duplicated().sum())
    df = df.drop_duplicates()
    duplicados_id = int(df.duplicated(subset="id_orden").sum())
    df = df.drop_duplicates(subset="id_orden", keep="first")
    logger.info("Duplicados eliminados: %d exactos, %d por id_orden", duplicados_exactos, duplicados_id)

    # 3. Tipos numéricos
    df["id_orden"] = pd.to_numeric(df["id_orden"], errors="coerce").astype("Int64")
    for col in ["precio_unitario", "cantidad", "monto_transaccion"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Fechas
    df["fecha"] = parsear_fechas(df["fecha"])
    sin_fecha = int(df["fecha"].isna().sum())
    if sin_fecha:
        logger.warning("Se eliminan %d filas sin fecha válida.", sin_fecha)
    df = df.dropna(subset=["fecha"])

    # 5. Valores inválidos (precio o cantidad nulos, cero o negativos)
    invalidas = (
        df["precio_unitario"].isna() | df["cantidad"].isna()
        | (df["precio_unitario"] <= 0) | (df["cantidad"] <= 0)
    )
    if invalidas.any():
        logger.warning("Se eliminan %d filas con precio o cantidad inválidos.", int(invalidas.sum()))
    df = df[~invalidas].copy()
    df["cantidad"] = df["cantidad"].astype("int64")

    # 6. Coherencia: monto = precio x cantidad
    calculado = df["precio_unitario"] * df["cantidad"]
    incoherentes = df["monto_transaccion"].isna() | ((df["monto_transaccion"] - calculado).abs() > 0.01)
    if incoherentes.any():
        logger.warning(
            "%d filas con monto distinto de precio x cantidad: se recalcula el monto.",
            int(incoherentes.sum()),
        )
        df.loc[incoherentes, "monto_transaccion"] = calculado[incoherentes]
    else:
        logger.info("Coherencia de montos: todas las filas cuadran con precio x cantidad.")

    # 7. Nulos de tipo_pago
    nulos_pago = int(df["tipo_pago"].isna().sum())
    df["tipo_pago"] = df["tipo_pago"].fillna(VALOR_PAGO_DESCONOCIDO)
    logger.info("tipo_pago: %d nulos etiquetados como '%s'", nulos_pago, VALOR_PAGO_DESCONOCIDO)

    # 8. Franja horaria fuera de catálogo (solo se reporta)
    fuera_de_catalogo = ~df["franja_horaria"].isin(FRANJAS_VALIDAS)
    if fuera_de_catalogo.any():
        logger.warning(
            "%d filas con franja_horaria no reconocida: %s",
            int(fuera_de_catalogo.sum()),
            sorted(df.loc[fuera_de_catalogo, "franja_horaria"].dropna().unique().tolist()),
        )

    # 9. Columnas que no aportan al análisis
    df = df.drop(columns=[c for c in COLUMNAS_A_ELIMINAR_VENTAS if c in df.columns])

    # 10. Columnas de calendario para la capa oro
    df["anio"] = df["fecha"].dt.year
    df["mes"] = df["fecha"].dt.month
    df["dia_semana"] = df["fecha"].dt.dayofweek.map(dict(enumerate(DIAS_SEMANA)))
    df["es_fin_de_semana"] = df["fecha"].dt.dayofweek >= 5
    df["es_quincena"] = _es_quincena(df["fecha"])

    df = df.sort_values(["fecha", "id_orden"]).reset_index(drop=True)
    logger.info(
        "Ventas plata: %d filas (de %d), rango %s a %s",
        len(df), filas_iniciales, df["fecha"].min().date(), df["fecha"].max().date(),
    )
    return df


# --------------------------------------------------------------------------- #
# Limpieza del benchmark por país
# --------------------------------------------------------------------------- #
def limpiar_benchmark(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia el benchmark de desperdicio por país y agrega un nivel de confianza numérico."""
    _validar_columnas(df, COLUMNAS_BENCHMARK_MIN, "benchmark")
    df = df.copy()
    filas_iniciales = len(df)

    df = _limpiar_textos(df)

    columnas_numericas = [
        c for c in df.columns if c.endswith("_kg_percapita_anio") or c.endswith("_toneladas_anio")
    ]
    for col in columnas_numericas:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "codigo_m49" in df.columns:
        df["codigo_m49"] = pd.to_numeric(df["codigo_m49"], errors="coerce").astype("Int64")

    df = df.dropna(subset=["pais"])
    duplicados = int(df.duplicated(subset="pais").sum())
    df = df.drop_duplicates(subset="pais", keep="first")
    if duplicados:
        logger.info("Benchmark: %d países duplicados eliminados", duplicados)

    df["nivel_confianza"] = df["confianza_estimacion"].map(NIVEL_CONFIANZA).astype("Int64")
    sin_nivel = df["nivel_confianza"].isna() & df["confianza_estimacion"].notna()
    if sin_nivel.any():
        logger.warning(
            "Valores de confianza_estimacion no reconocidos: %s",
            sorted(df.loc[sin_nivel, "confianza_estimacion"].unique().tolist()),
        )

    df = df.sort_values("pais").reset_index(drop=True)
    logger.info("Benchmark plata: %d países (de %d filas)", len(df), filas_iniciales)
    return df


# --------------------------------------------------------------------------- #
# Guardado y ejecución
# --------------------------------------------------------------------------- #
def guardar_silver(df: pd.DataFrame, nombre: str, carpeta: Path = silver_dir) -> Path:
    """Guarda un DataFrame en data/silver/<nombre>.csv y devuelve la ruta."""
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{nombre}.csv"
    df.to_csv(ruta, index=False, date_format="%Y-%m-%d", encoding="utf-8")
    logger.info("Guardado %s (%d filas)", ruta.relative_to(RAIZ) if ruta.is_relative_to(RAIZ) else ruta, len(df))
    return ruta


def ejecutar_limpieza(
    ventas: pd.DataFrame | None = None,
    benchmark: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Paso completo bronce -> plata. Si no se pasan DataFrames, los obtiene del
    módulo de extracción. Devuelve (ventas_silver, benchmark_silver).
    """
    if ventas is None or benchmark is None:
        from src.extract.extract_excel import extraer_benchmark_pais, extraer_ventas_referencia

        if ventas is None:
            ventas = extraer_ventas_referencia()
        if benchmark is None:
            benchmark = extraer_benchmark_pais()

    ventas_silver = limpiar_ventas(ventas)
    benchmark_silver = limpiar_benchmark(benchmark)

    guardar_silver(ventas_silver, "ventas_silver")
    guardar_silver(benchmark_silver, "benchmark_silver")
    return ventas_silver, benchmark_silver


if __name__ == "__main__":
    archivo_log = RAIZ / "logs" / "logs.txt"
    archivo_log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(archivo_log, encoding="utf-8")],
    )
    ventas_silver, benchmark_silver = ejecutar_limpieza()
    print("\nVentas (plata):")
    print(ventas_silver.head())
    print("\nBenchmark (plata):")
    print(benchmark_silver.head())
