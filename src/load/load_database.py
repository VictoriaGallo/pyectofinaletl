from __future__ import annotations
 
import logging
import os
import sys
from pathlib import Path
 
import pandas as pd
import yaml
from dotenv import load_dotenv
from sqlalchemy import Date, create_engine, text
from sqlalchemy.engine import URL, Engine
 
RAIZ = Path(__file__).resolve().parents[2]
# Permite ejecutar el archivo con el botón de Play de VS Code
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))
 
load_dotenv(RAIZ / ".env")
logger = logging.getLogger(__name__)
 
RUTA_CONFIG = RAIZ / "config" / "config.yaml"
DRIVER_POR_DEFECTO = "postgresql+psycopg2"
 
# Clave primaria de cada tabla (al crearla se valida que no haya duplicados)
CLAVES_PRIMARIAS = {
    "ventas": ["id_orden"],
    "benchmark": ["pais"],
    "hechos_demanda_diaria": ["fecha", "nombre_item"],
    "demanda_dia_franja": ["dia_semana", "franja_horaria"],
    "variabilidad_item": ["nombre_item"],
    "ventas_mensuales": ["periodo"],
    "efecto_calendario": ["variable", "grupo"],
    "benchmark_contexto": ["referencia"],
    "merma_simulada_diaria": ["fecha", "nombre_item"],
    "kpis_simulados_resumen": ["nivel", "clave"],
    "kpis_simulados_mensual": ["periodo"],
}
 
 
# --------------------------------------------------------------------------- #
# Configuración y conexión
# --------------------------------------------------------------------------- #
def leer_config(ruta: Path = RUTA_CONFIG) -> dict:
    """Lee config/config.yaml."""
    if not ruta.exists():
        raise FileNotFoundError(f"No existe el archivo de configuración: {ruta}")
    with open(ruta, encoding="utf-8") as archivo:
        return yaml.safe_load(archivo) or {}
 
 
def _url_conexion(driver: str, base_de_datos: str | None = None) -> URL:
    faltantes = [v for v in ("DB_NAME", "DB_USER", "DB_PASSWORD") if not os.getenv(v)]
    if faltantes:
        raise EnvironmentError(f"Faltan estas variables en el archivo .env: {faltantes}")
    return URL.create(
        drivername=driver,
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=base_de_datos or os.getenv("DB_NAME"),
    )
 
 
def crear_base_si_no_existe(driver: str = DRIVER_POR_DEFECTO) -> None:
    """Crea la base de datos de DB_NAME si todavía no existe."""
    nombre = os.getenv("DB_NAME")
    motor = create_engine(_url_conexion(driver, "postgres"), isolation_level="AUTOCOMMIT")
    try:
        with motor.connect() as conexion:
            existe = conexion.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :nombre"), {"nombre": nombre}
            ).scalar()
            if existe:
                logger.info("La base de datos '%s' ya existe", nombre)
            else:
                nombre_seguro = motor.dialect.identifier_preparer.quote(nombre)
                conexion.execute(text(f"CREATE DATABASE {nombre_seguro}"))
                logger.info("Base de datos '%s' creada", nombre)
    finally:
        motor.dispose()
 
 
def crear_motor(driver: str = DRIVER_POR_DEFECTO) -> Engine:
    """Crea la conexión a PostgreSQL y verifica que responda."""
    motor = create_engine(_url_conexion(driver), pool_pre_ping=True)
    with motor.connect() as conexion:
        version = conexion.execute(text("SHOW server_version")).scalar()
    logger.info(
        "Conectado a PostgreSQL %s en %s:%s, base '%s'",
        version, os.getenv("DB_HOST", "localhost"), os.getenv("DB_PORT", "5432"), os.getenv("DB_NAME"),
    )
    return motor
 
 
# --------------------------------------------------------------------------- #
# Carga
# --------------------------------------------------------------------------- #
def _leer_csv(ruta: Path) -> pd.DataFrame:
    columnas = pd.read_csv(ruta, nrows=0).columns
    return pd.read_csv(ruta, parse_dates=[c for c in columnas if c == "fecha"])
 
 
def _nombre_tabla(ruta: Path, capa: str) -> str:
    """ventas_silver.csv -> ventas (el esquema ya indica la capa)."""
    return ruta.stem.removesuffix(f"_{capa}")
 
 
def crear_esquema(motor: Engine, esquema: str) -> None:
    nombre_seguro = motor.dialect.identifier_preparer.quote(esquema)
    with motor.begin() as conexion:
        conexion.execute(text(f"CREATE SCHEMA IF NOT EXISTS {nombre_seguro}"))
 
 
def cargar_tabla(df: pd.DataFrame, tabla: str, motor: Engine, esquema: str, modo: str = "replace") -> int:
    """Carga un DataFrame como tabla y, si se recreó, le agrega su clave primaria."""
    df = df.copy()
    df["fecha_carga"] = pd.Timestamp.now().floor("s")
    tipos = {"fecha": Date()} if "fecha" in df.columns else None
    q = motor.dialect.identifier_preparer.quote
 
    with motor.begin() as conexion:
        df.to_sql(
            tabla, conexion, schema=esquema, if_exists=modo, index=False,
            chunksize=1000, method="multi", dtype=tipos,
        )
        claves = CLAVES_PRIMARIAS.get(tabla)
        if claves and modo == "replace":
            columnas = ", ".join(q(c) for c in claves)
            try:
                with conexion.begin_nested():
                    conexion.execute(text(f"ALTER TABLE {q(esquema)}.{q(tabla)} ADD PRIMARY KEY ({columnas})"))
            except Exception as error:  # la tabla se carga igual, pero queda el aviso
                logger.warning("No se pudo crear la clave primaria de %s.%s: %s", esquema, tabla, error)
 
    logger.info("Cargada %s.%s (%d filas)", esquema, tabla, len(df))
    return len(df)
 
 
def cargar_capa(motor: Engine, carpeta: Path, capa: str, esquema: str, modo: str = "replace") -> dict[str, int]:
    """Carga todos los CSV de una carpeta (data/silver o data/gold) en un esquema."""
    archivos = sorted(carpeta.glob("*.csv"))
    if not archivos:
        logger.warning("No hay archivos CSV en %s; se omite la capa %s", carpeta, capa)
        return {}
    crear_esquema(motor, esquema)
    cargadas = {}
    for ruta in archivos:
        tabla = _nombre_tabla(ruta, capa)
        cargadas[f"{esquema}.{tabla}"] = cargar_tabla(_leer_csv(ruta), tabla, motor, esquema, modo)
    return cargadas
 
 
def verificar_carga(motor: Engine, esperadas: dict[str, int], modo: str = "replace") -> bool:
    """Compara las filas de cada tabla en la base con las filas que se cargaron."""
    q = motor.dialect.identifier_preparer.quote
    todo_bien = True
    with motor.connect() as conexion:
        for nombre, filas_cargadas in esperadas.items():
            esquema, tabla = nombre.split(".", 1)
            filas_en_base = conexion.execute(text(f"SELECT COUNT(*) FROM {q(esquema)}.{q(tabla)}")).scalar()
            coincide = filas_en_base == filas_cargadas if modo == "replace" else filas_en_base >= filas_cargadas
            if not coincide:
                todo_bien = False
                logger.error("%s: se cargaron %d filas pero la base tiene %d", nombre, filas_cargadas, filas_en_base)
    if todo_bien:
        logger.info("Verificación OK: %d tablas con el número de filas esperado", len(esperadas))
    return todo_bien
 
 
def ejecutar_carga(config: dict | None = None) -> dict[str, int]:
    """Paso completo de carga. Devuelve {esquema.tabla: filas cargadas}."""
    config = config or leer_config()
    opciones = config.get("base_de_datos", {})
    rutas = config.get("rutas", {})
    driver = opciones.get("driver", DRIVER_POR_DEFECTO)
    modo = opciones.get("modo_carga", "replace")
    capas = opciones.get("capas", {"silver": "silver", "gold": "gold"})
 
    if modo not in ("replace", "append"):
        raise ValueError(f"modo_carga debe ser 'replace' o 'append', no '{modo}'")
    if opciones.get("crear_base_si_no_existe", True):
        crear_base_si_no_existe(driver)
 
    motor = crear_motor(driver)
    try:
        cargadas: dict[str, int] = {}
        for capa, esquema in capas.items():
            carpeta = RAIZ / rutas.get(capa, f"data/{capa}")
            cargadas.update(cargar_capa(motor, carpeta, capa, esquema, modo))
        verificar_carga(motor, cargadas, modo)
    finally:
        motor.dispose()
    return cargadas
 
 
if __name__ == "__main__":
    configuracion = leer_config()
    archivo_log = RAIZ / configuracion.get("rutas", {}).get("logs", "logs/logs.txt")
    archivo_log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(archivo_log, encoding="utf-8")],
    )
    ejecutar_carga(configuracion)