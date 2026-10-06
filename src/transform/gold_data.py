from __future__ import annotations
 
import logging
import sys
from pathlib import Path
 
import numpy as np
import pandas as pd
 
RAIZ = Path(__file__).resolve().parents[2]
# Permite ejecutar el archivo con el botón de Play de VS Code
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))
 
from src.transform.clean_excel import DIAS_SEMANA, FRANJAS_VALIDAS, _es_quincena  # noqa: E402
 
logger = logging.getLogger(__name__)
 
silver_dir = RAIZ / "data" / "silver"
gold_dir = RAIZ / "data" / "gold"
 
PARAMETROS_SIMULACION = {
    "porcentaje_costo_insumo": 0.35,  # costo del insumo como fracción del precio de venta
    "colchon_seguridad": 0.10,        # se prepara un 10 % por encima del pronóstico
    "semanas_historia": 4,            # semanas usadas para el pronóstico
}
PAIS_REFERENCIA = "Colombia"
NIVEL_CONFIANZA_MINIMO = 3  # 3 = Medium Confidence, 4 = High Confidence
 
 
# --------------------------------------------------------------------------- #
# Carga
# --------------------------------------------------------------------------- #
def cargar_silver() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Lee las tablas de la capa plata."""
    ruta_ventas = silver_dir / "ventas_silver.csv"
    ruta_benchmark = silver_dir / "benchmark_silver.csv"
    faltantes = [r.name for r in (ruta_ventas, ruta_benchmark) if not r.exists()]
    if faltantes:
        raise FileNotFoundError(
            f"No existen {faltantes} en data/silver. "
            "Corre primero: python -m src.transform.clean_excel"
        )
    ventas = pd.read_csv(ruta_ventas, parse_dates=["fecha"])
    benchmark = pd.read_csv(ruta_benchmark)
    logger.info("Plata cargada: %d ventas, %d países", len(ventas), len(benchmark))
    return ventas, benchmark
 
 
# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def _agregar_calendario(df: pd.DataFrame) -> pd.DataFrame:
    df["periodo"] = df["fecha"].dt.to_period("M").astype(str)
    df["dia_semana"] = df["fecha"].dt.dayofweek.map(dict(enumerate(DIAS_SEMANA)))
    df["es_fin_de_semana"] = df["fecha"].dt.dayofweek >= 5
    df["es_quincena"] = _es_quincena(df["fecha"])
    return df
 
 
def _porcentaje(numerador: pd.Series, denominador: pd.Series) -> pd.Series:
    return (numerador / denominador.replace(0, np.nan) * 100).round(2)
 
 
# --------------------------------------------------------------------------- #
# Tablas con datos reales
# --------------------------------------------------------------------------- #
def construir_demanda_diaria(ventas: pd.DataFrame) -> pd.DataFrame:
    """
    Tabla de hechos: una fila por día e ítem para TODO el periodo.
    Los días sin ventas quedan con 0, que es clave para medir la variabilidad
    real y para simular la merma (un día sin ventas es merma total).
    """
    fechas = pd.date_range(ventas["fecha"].min(), ventas["fecha"].max(), freq="D")
    catalogo = (
        ventas.groupby("nombre_item")
        .agg(tipo_item=("tipo_item", "first"), precio_unitario=("precio_unitario", "median"))
        .reset_index()
    )
    agregado = (
        ventas.groupby(["fecha", "nombre_item"])
        .agg(ordenes=("id_orden", "count"), unidades=("cantidad", "sum"), monto=("monto_transaccion", "sum"))
        .reset_index()
    )
    grilla = pd.MultiIndex.from_product(
        [fechas, catalogo["nombre_item"]], names=["fecha", "nombre_item"]
    ).to_frame(index=False)
 
    df = grilla.merge(agregado, on=["fecha", "nombre_item"], how="left")
    df[["ordenes", "unidades"]] = df[["ordenes", "unidades"]].fillna(0).astype("int64")
    df["monto"] = df["monto"].fillna(0)
    df = df.merge(catalogo, on="nombre_item", how="left")
    df = _agregar_calendario(df)
 
    logger.info(
        "Demanda diaria: %d días x %d ítems = %d filas (%.1f %% con ventas)",
        len(fechas), len(catalogo), len(df), (df["unidades"] > 0).mean() * 100,
    )
    return df.sort_values(["fecha", "nombre_item"]).reset_index(drop=True)
 
 
def construir_demanda_dia_franja(ventas: pd.DataFrame, demanda_diaria: pd.DataFrame) -> pd.DataFrame:
    """Demanda por día de la semana y franja horaria, con promedio por día."""
    dias_en_periodo = demanda_diaria.drop_duplicates("fecha")["dia_semana"].value_counts()
    tabla = (
        ventas.groupby(["dia_semana", "franja_horaria"])
        .agg(ordenes=("id_orden", "count"), unidades=("cantidad", "sum"), monto=("monto_transaccion", "sum"))
        .reset_index()
    )
    tabla["dias_en_periodo"] = tabla["dia_semana"].map(dias_en_periodo)
    tabla["promedio_unidades_por_dia"] = (tabla["unidades"] / tabla["dias_en_periodo"]).round(2)
 
    orden_dia = {d: i for i, d in enumerate(DIAS_SEMANA)}
    orden_franja = {f: i for i, f in enumerate(FRANJAS_VALIDAS)}
    tabla = (
        tabla.assign(
            _d=tabla["dia_semana"].map(orden_dia),
            _f=tabla["franja_horaria"].map(orden_franja).fillna(len(FRANJAS_VALIDAS)),
        )
        .sort_values(["_d", "_f"])
        .drop(columns=["_d", "_f"])
        .reset_index(drop=True)
    )
    return tabla
 
 
def construir_variabilidad_item(demanda_diaria: pd.DataFrame) -> pd.DataFrame:
    """Variabilidad de la demanda diaria por ítem (incluye los días con 0 ventas)."""
    tabla = (
        demanda_diaria.groupby(["nombre_item", "tipo_item"])
        .agg(
            precio_unitario=("precio_unitario", "first"),
            dias_periodo=("unidades", "size"),
            dias_con_venta=("unidades", lambda s: int((s > 0).sum())),
            unidades_totales=("unidades", "sum"),
            monto_total=("monto", "sum"),
            media_diaria=("unidades", "mean"),
            desv_diaria=("unidades", "std"),
        )
        .reset_index()
    )
    tabla["pct_dias_con_venta"] = _porcentaje(tabla["dias_con_venta"], tabla["dias_periodo"])
    tabla["cv"] = (tabla["desv_diaria"] / tabla["media_diaria"].replace(0, np.nan)).round(2)
    tabla["participacion_monto_pct"] = _porcentaje(tabla["monto_total"], pd.Series(tabla["monto_total"].sum(), index=tabla.index))
    tabla["ranking_variabilidad"] = tabla["cv"].rank(ascending=False, method="min").astype("Int64")
    tabla[["media_diaria", "desv_diaria"]] = tabla[["media_diaria", "desv_diaria"]].round(2)
    return tabla.sort_values("ranking_variabilidad").reset_index(drop=True)
 
 
def construir_ventas_mensuales(ventas: pd.DataFrame) -> pd.DataFrame:
    tabla = (
        ventas.assign(periodo=ventas["fecha"].dt.to_period("M").astype(str))
        .groupby("periodo")
        .agg(ordenes=("id_orden", "count"), unidades=("cantidad", "sum"), monto=("monto_transaccion", "sum"))
        .reset_index()
    )
    tabla["ticket_promedio"] = (tabla["monto"] / tabla["ordenes"]).round(2)
    return tabla
 
 
def construir_efecto_calendario(demanda_diaria: pd.DataFrame) -> pd.DataFrame:
    """Compara la demanda diaria promedio en quincena y fin de semana contra el resto de días."""
    diario = (
        demanda_diaria.groupby(["fecha", "es_quincena", "es_fin_de_semana"], as_index=False)
        .agg(unidades=("unidades", "sum"), monto=("monto", "sum"))
    )
    filas = []
    for variable in ["es_quincena", "es_fin_de_semana"]:
        resumen = diario.groupby(variable).agg(
            dias=("fecha", "size"),
            promedio_unidades_diarias=("unidades", "mean"),
            promedio_monto_diario=("monto", "mean"),
        )
        base = resumen["promedio_unidades_diarias"].get(False, np.nan)
        for valor, fila in resumen.iterrows():
            filas.append({
                "variable": variable,
                "grupo": "Sí" if valor else "No",
                "dias": int(fila["dias"]),
                "promedio_unidades_diarias": round(fila["promedio_unidades_diarias"], 2),
                "promedio_monto_diario": round(fila["promedio_monto_diario"], 2),
                "diferencia_vs_resto_pct": round((fila["promedio_unidades_diarias"] / base - 1) * 100, 2) if valor else 0.0,
            })
    return pd.DataFrame(filas)
 
 
def construir_benchmark_contexto(benchmark: pd.DataFrame, pais: str = PAIS_REFERENCIA) -> pd.DataFrame:
    """Ubica al país de referencia frente a su región y al mundo (kg por persona al año)."""
    columnas = [c for c in benchmark.columns if c.endswith("_kg_percapita_anio")]
    fila_pais = benchmark[benchmark["pais"].str.casefold() == pais.casefold()]
    if fila_pais.empty:
        logger.warning("El país '%s' no está en el benchmark.", pais)
        return pd.DataFrame(columns=["referencia", "n_paises", "nivel_confianza", *columnas])
 
    region = fila_pais["region"].iloc[0]
    de_la_region = benchmark[benchmark["region"] == region]
    confiables = benchmark[benchmark["nivel_confianza"] >= NIVEL_CONFIANZA_MINIMO]
 
    filas = [
        {"referencia": pais, "n_paises": 1,
         "nivel_confianza": fila_pais["nivel_confianza"].iloc[0], **fila_pais[columnas].iloc[0].to_dict()},
        {"referencia": f"Promedio {region}", "n_paises": len(de_la_region),
         "nivel_confianza": np.nan, **de_la_region[columnas].mean().round(1).to_dict()},
        {"referencia": "Mediana mundial", "n_paises": len(benchmark),
         "nivel_confianza": np.nan, **benchmark[columnas].median().to_dict()},
        {"referencia": "Mediana mundial (confianza media o alta)", "n_paises": len(confiables),
         "nivel_confianza": np.nan, **confiables[columnas].median().to_dict()},
    ]
    return pd.DataFrame(filas)
 
 
# --------------------------------------------------------------------------- #
# Merma simulada y KPIs
# --------------------------------------------------------------------------- #
def simular_merma(demanda_diaria: pd.DataFrame, parametros: dict | None = None) -> pd.DataFrame:
    """
    Simula cuánto se habría preparado cada día y la merma resultante.
    Reemplazar esta función cuando haya datos reales de producción y costos.
    """
    p = {**PARAMETROS_SIMULACION, **(parametros or {})}
    df = demanda_diaria.sort_values(["nombre_item", "fecha"]).copy()
    df["_dow"] = df["fecha"].dt.dayofweek
 
    df["pronostico"] = df.groupby(["nombre_item", "_dow"])["unidades"].transform(
        lambda s: s.shift(1).rolling(p["semanas_historia"], min_periods=1).mean()
    )
    sin_historia = int(df["pronostico"].isna().sum())
    df = df.dropna(subset=["pronostico"]).copy()
    logger.info("Simulación: se excluyen %d filas de la primera semana (sin historia para pronosticar)", sin_historia)
 
    # round(6) evita que errores de punto flotante (p. ej. 11.000000000000002) suban una unidad
    df["preparado"] = np.ceil((df["pronostico"] * (1 + p["colchon_seguridad"])).round(6)).astype("int64")
    df = df.rename(columns={"unidades": "demanda"})
    df["vendido"] = np.minimum(df["demanda"], df["preparado"])
    df["merma_unidades"] = df["preparado"] - df["vendido"]
    df["demanda_insatisfecha"] = df["demanda"] - df["vendido"]
    df["desviacion_abs"] = (df["preparado"] - df["demanda"]).abs()
 
    df["costo_unitario_insumo"] = df["precio_unitario"] * p["porcentaje_costo_insumo"]
    df["venta_simulada"] = df["vendido"] * df["precio_unitario"]
    df["costo_merma"] = df["merma_unidades"] * df["costo_unitario_insumo"]
    df["venta_perdida"] = df["demanda_insatisfecha"] * df["precio_unitario"]
    df["pronostico"] = df["pronostico"].round(2)
 
    columnas = [
        "fecha", "periodo", "dia_semana", "es_fin_de_semana", "es_quincena",
        "nombre_item", "tipo_item", "precio_unitario", "costo_unitario_insumo",
        "demanda", "pronostico", "preparado", "vendido", "merma_unidades",
        "demanda_insatisfecha", "desviacion_abs", "venta_simulada", "costo_merma", "venta_perdida",
    ]
    return df[columnas].sort_values(["fecha", "nombre_item"]).reset_index(drop=True)
 
 
def _resumir_kpis(merma: pd.DataFrame, claves: list[str]) -> pd.DataFrame:
    tabla = (
        merma.groupby(claves)
        .agg(
            demanda_unidades=("demanda", "sum"),
            preparado_unidades=("preparado", "sum"),
            merma_unidades=("merma_unidades", "sum"),
            demanda_insatisfecha_unidades=("demanda_insatisfecha", "sum"),
            desviacion_abs=("desviacion_abs", "sum"),
            venta_simulada=("venta_simulada", "sum"),
            costo_merma=("costo_merma", "sum"),
            venta_perdida=("venta_perdida", "sum"),
        )
        .reset_index()
    )
    # KPI 1: Costo de Merma sobre Venta
    tabla["cmv_pct"] = _porcentaje(tabla["costo_merma"], tabla["venta_simulada"])
    # KPI 2: Tasa de Desviación de Demanda = suma |preparado - demanda| / suma demanda
    tabla["tasa_desviacion_pct"] = _porcentaje(tabla["desviacion_abs"], tabla["demanda_unidades"])
    tabla["tasa_merma_pct"] = _porcentaje(tabla["merma_unidades"], tabla["preparado_unidades"])
    return tabla.drop(columns="desviacion_abs")
 
 
def construir_kpis_resumen(merma: pd.DataFrame) -> pd.DataFrame:
    """KPIs totales, por categoría (tipo_item) y por ítem."""
    partes = []
    for nivel, columna in [("Total", None), ("Categoría", "tipo_item"), ("Ítem", "nombre_item")]:
        datos = merma.assign(clave="Total") if columna is None else merma.assign(clave=merma[columna])
        tabla = _resumir_kpis(datos, ["clave"])
        tabla.insert(0, "nivel", nivel)
        # KPI 3: Costo por Categoría de Insumo Desperdiciado (participación dentro del nivel)
        tabla["participacion_costo_merma_pct"] = _porcentaje(
            tabla["costo_merma"], pd.Series(tabla["costo_merma"].sum(), index=tabla.index)
        )
        partes.append(tabla.sort_values("costo_merma", ascending=False))
    return pd.concat(partes, ignore_index=True)
 
 
def construir_kpis_mensual(merma: pd.DataFrame) -> pd.DataFrame:
    return _resumir_kpis(merma, ["periodo"])
 
 
# --------------------------------------------------------------------------- #
# Guardado y ejecución
# --------------------------------------------------------------------------- #
def guardar_gold(df: pd.DataFrame, nombre: str, carpeta: Path = gold_dir) -> Path:
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{nombre}.csv"
    df = df.copy()
    numericas = df.select_dtypes(include="number").columns
    df[numericas] = df[numericas].round(2)
    df.to_csv(ruta, index=False, date_format="%Y-%m-%d", encoding="utf-8")
    logger.info("Guardado %s (%d filas)", ruta.relative_to(RAIZ) if ruta.is_relative_to(RAIZ) else ruta, len(df))
    return ruta
 
 
def ejecutar_gold(
    ventas: pd.DataFrame | None = None,
    benchmark: pd.DataFrame | None = None,
    parametros: dict | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Paso completo plata -> oro. Si no se pasan DataFrames, los lee de data/silver.
    Devuelve un diccionario {nombre_tabla: DataFrame}.
    """
    if ventas is None or benchmark is None:
        ventas_csv, benchmark_csv = cargar_silver()
        ventas = ventas if ventas is not None else ventas_csv
        benchmark = benchmark if benchmark is not None else benchmark_csv
 
    demanda = construir_demanda_diaria(ventas)
    merma = simular_merma(demanda, parametros)
 
    tablas = {
        "hechos_demanda_diaria": demanda,
        "demanda_dia_franja": construir_demanda_dia_franja(ventas, demanda),
        "variabilidad_item": construir_variabilidad_item(demanda),
        "ventas_mensuales": construir_ventas_mensuales(ventas),
        "efecto_calendario": construir_efecto_calendario(demanda),
        "benchmark_contexto": construir_benchmark_contexto(benchmark),
        "merma_simulada_diaria": merma,
        "kpis_simulados_resumen": construir_kpis_resumen(merma),
        "kpis_simulados_mensual": construir_kpis_mensual(merma),
    }
    for nombre, df in tablas.items():
        guardar_gold(df, nombre)
 
    p = {**PARAMETROS_SIMULACION, **(parametros or {})}
    total = tablas["kpis_simulados_resumen"].iloc[0]
    logger.info(
        "KPIs SIMULADOS (costo insumo %.0f %%, colchón %.0f %%, historia %d semanas): "
        "CMV %.2f %% | Desviación de demanda %.2f %% | Merma %.2f %% de lo preparado",
        p["porcentaje_costo_insumo"] * 100, p["colchon_seguridad"] * 100, p["semanas_historia"],
        total["cmv_pct"], total["tasa_desviacion_pct"], total["tasa_merma_pct"],
    )
    return tablas
 
 
if __name__ == "__main__":
    archivo_log = RAIZ / "logs" / "logs.txt"
    archivo_log.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(archivo_log, encoding="utf-8")],
    )
    pd.set_option("display.width", 160)
    pd.set_option("display.max_columns", 12)
    tablas = ejecutar_gold()
    print("\nKPIs simulados:")
    print(tablas["kpis_simulados_resumen"][
        ["nivel", "clave", "venta_simulada", "costo_merma", "cmv_pct",
         "tasa_desviacion_pct", "participacion_costo_merma_pct"]
    ].to_string(index=False))
    print("\nVariabilidad por ítem (incluye días sin ventas):")
    print(tablas["variabilidad_item"][
        ["nombre_item", "pct_dias_con_venta", "media_diaria", "cv", "participacion_monto_pct"]
    ].to_string(index=False))