"""
extract_excel.py
Capa Extract del pipeline ETL - Mermas en franquicias de comida rápida.
"""

from pathlib import Path
import logging
import re
import pandas as pd

log = logging.getLogger(__name__)

bronze_dir = Path("data/bronze")

# Columnas del dataset "Fast Food Sales Report" (Kaggle,
# rajatsurana979/fast-food-sales-report), usado como fuente de
# referencia mientras se consigue el dato real de la franquicia.
MAPA_COLUMNAS_VENTAS_REF = {
    "order_id": "id_orden",
    "date": "fecha",
    "item_name": "nombre_item",
    "item_type": "tipo_item",          # Fastfood / Beverages
    "item_price": "precio_unitario",
    "quantity": "cantidad",
    "transaction_amount": "monto_transaccion",
    "transaction_type": "tipo_pago",    # cash / online / other
    "received_by": "genero_cajero",
    "time_of_sale": "franja_horaria",   # Morning/Afternoon/Evening/Night/Midnight
}


def _snake(nombre) -> str:
    """'Food service estimate (kg/capita/year)' -> 'food_service_estimate_kg_capita_year'."""
    return re.sub(r"[^0-9a-z]+", "_", str(nombre).strip().lower()).strip("_")


def _normalizar_ventas_ref(df: pd.DataFrame) -> pd.DataFrame:
    # Se estandarizan los nombres primero, así no importan mayúsculas ni espacios.
    df = df.rename(columns=_snake)
    faltantes = set(MAPA_COLUMNAS_VENTAS_REF) - set(df.columns)
    if faltantes:
        log.warning(
            "El archivo no trae las columnas esperadas: %s. "
            "Verifica si Kaggle cambió el esquema.", faltantes,
        )
    return df.rename(columns=MAPA_COLUMNAS_VENTAS_REF)


def leer_archivo(ruta: Path) -> pd.DataFrame:
    """Lee un CSV o Excel de Bronze según su extensión."""
    if not ruta.exists():
        raise FileNotFoundError(
            f"No se encontró '{ruta}'. Descárgalo y colócalo en {bronze_dir}/"
        )
    if ruta.suffix == ".csv":
        return pd.read_csv(ruta)
    if ruta.suffix in (".xlsx", ".xls"):
        return pd.read_excel(ruta)
    raise ValueError(f"Formato no soportado: {ruta.suffix}")


def extraer_ventas_referencia_local(
    nombre_archivo: str = "fast_food_sales_report.csv",
) -> pd.DataFrame:
    """Opción 2: lee el CSV ya descargado manualmente desde data/bronze/."""
    df = leer_archivo(bronze_dir / nombre_archivo)
    df = _normalizar_ventas_ref(df)
    log.info("Extraídas %d filas (archivo local)", len(df))
    return df


def extraer_ventas_referencia_kagglehub() -> pd.DataFrame:
    """
    Opción 1: descarga el dataset con kagglehub.

    Requiere:
      pip install kagglehub
      Tener ~/.kaggle/kaggle.json (API key personal, se genera en
      kaggle.com/settings -> API -> Create New Token).
    """
    import kagglehub
    from kagglehub import KaggleDatasetAdapter

    handle = "rajatsurana979/fast-food-sales-report"

    # El dataset trae un único CSV; si Kaggle le cambia el nombre,
    # se descarga la carpeta completa y se detecta el archivo.
    carpeta = kagglehub.dataset_download(handle)
    archivos_csv = list(Path(carpeta).glob("*.csv"))
    if not archivos_csv:
        raise FileNotFoundError(f"No se halló ningún CSV en {carpeta}")

    df = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS, handle, archivos_csv[0].name
    )
    df = _normalizar_ventas_ref(df)
    log.info("Extraídas %d filas (kagglehub)", len(df))
    return df


def extraer_ventas_referencia() -> pd.DataFrame:
    """Intenta kagglehub primero; si falla (sin API key, sin red), usa el CSV local."""
    try:
        return extraer_ventas_referencia_kagglehub()
    except Exception as e:
        log.warning("kagglehub no disponible (%s); usando archivo local.", e)
        return extraer_ventas_referencia_local()


# --- Benchmark de desperdicio de alimentos por país (contexto, no operativo) ---
# Dataset: joebeachcapital/food-waste ("Food Waste Data And Research By Country")
MAPA_COLUMNAS_BENCHMARK = {
    "country": "pais",
    "combined_figures_kg_capita_year": "total_kg_percapita_anio",
    "household_estimate_kg_capita_year": "hogar_kg_percapita_anio",
    "household_estimate_tonnes_year": "hogar_toneladas_anio",
    "retail_estimate_kg_capita_year": "comercio_kg_percapita_anio",
    "retail_estimate_tonnes_year": "comercio_toneladas_anio",
    "food_service_estimate_kg_capita_year": "servicio_alimentos_kg_percapita_anio",
    "food_service_estimate_tonnes_year": "servicio_alimentos_toneladas_anio",
    "confidence_in_estimate": "confianza_estimacion",
    "m49_code": "codigo_m49",
    "region": "region",
    "source": "fuente",
}


def _normalizar_benchmark(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=_snake)
    faltantes = set(MAPA_COLUMNAS_BENCHMARK) - set(df.columns)
    if faltantes:
        log.warning(
            "El archivo de benchmark no trae las columnas esperadas: %s.",
            faltantes,
        )
    return df.rename(columns=MAPA_COLUMNAS_BENCHMARK)


def extraer_benchmark_pais_local(
    nombre_archivo: str = "food_waste_by_country.csv",
) -> pd.DataFrame:
    """Lee el CSV de benchmark ya descargado manualmente desde data/bronze/."""
    df = leer_archivo(bronze_dir / nombre_archivo)
    df = _normalizar_benchmark(df)
    log.info("Extraídas %d filas de benchmark (archivo local)", len(df))
    return df


def extraer_benchmark_pais_kagglehub() -> pd.DataFrame:
    """Descarga con kagglehub el dataset de desperdicio de alimentos por país."""
    import kagglehub
    from kagglehub import KaggleDatasetAdapter

    handle = "joebeachcapital/food-waste"
    carpeta = kagglehub.dataset_download(handle)
    archivos_csv = list(Path(carpeta).glob("*.csv"))
    if not archivos_csv:
        raise FileNotFoundError(f"No se halló ningún CSV en {carpeta}")

    df = kagglehub.dataset_load(
        KaggleDatasetAdapter.PANDAS, handle, archivos_csv[0].name
    )
    df = _normalizar_benchmark(df)
    log.info("Extraídas %d filas de benchmark (kagglehub)", len(df))
    return df


def extraer_benchmark_pais() -> pd.DataFrame:
    """Intenta kagglehub primero; si falla, usa el CSV local en data/bronze/."""
    try:
        return extraer_benchmark_pais_kagglehub()
    except Exception as e:
        log.warning("kagglehub no disponible (%s); usando archivo local.", e)
        return extraer_benchmark_pais_local()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    print(extraer_ventas_referencia().head())
    print(extraer_benchmark_pais().head())