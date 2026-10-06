from __future__ import annotations
 
import argparse
import logging
import sys
import time
from pathlib import Path
 
RAIZ = Path(__file__).resolve().parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))
 
from src.extract.extract_excel import extraer_benchmark_pais, extraer_ventas_referencia  # noqa: E402
from src.load.load_database import ejecutar_carga, leer_config  # noqa: E402
from src.transform.clean_excel import ejecutar_limpieza  # noqa: E402
from src.transform.gold_data import ejecutar_gold  # noqa: E402
 
logger = logging.getLogger("pipeline")
 
 
def configurar_logging(archivo_log: Path) -> None:
    archivo_log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(archivo_log, encoding="utf-8")],
    )
 
 
def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline ETL de mermas en franquicias de comida rápida")
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument("--sin-carga", action="store_true", help="no cargar a PostgreSQL")
    grupo.add_argument("--solo-carga", action="store_true", help="solo cargar a PostgreSQL")
    args = parser.parse_args()
 
    config = leer_config()
    configurar_logging(RAIZ / config.get("rutas", {}).get("logs", "logs/logs.txt"))
    logger.info("========== Inicio del pipeline ==========")
    inicio = time.perf_counter()
 
    try:
        if not args.solo_carga:
            logger.info("---- 1. EXTRACT (bronce) ----")
            ventas = extraer_ventas_referencia()
            benchmark = extraer_benchmark_pais()
 
            logger.info("---- 2. TRANSFORM (plata) ----")
            ventas_silver, benchmark_silver = ejecutar_limpieza(ventas, benchmark)
 
            logger.info("---- 3. TRANSFORM (oro) ----")
            ejecutar_gold(ventas_silver, benchmark_silver, parametros=config.get("simulacion"))
 
        if not args.sin_carga:
            logger.info("---- 4. LOAD (PostgreSQL) ----")
            ejecutar_carga(config)
 
    except Exception:
        logger.exception("El pipeline se detuvo por un error")
        return 1
 
    logger.info("========== Pipeline terminado en %.1f s ==========", time.perf_counter() - inicio)
    return 0
 
 
if __name__ == "__main__":
    sys.exit(main())