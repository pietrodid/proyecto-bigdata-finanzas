"""
BRONZE -> SILVER: Limpieza y Transformacion de Datos
RDDs 
"""

import pyspark


def ejecutar_bronze_to_silver():

    print("\n" + "=" * 60)
    print("   PIPELINE BRONZE -> SILVER")
    print("=" * 60)

    # ═══════════════════════════════════════════════
    # Configuracion de Spark 
    # ═══════════════════════════════════════════════
    sc = pyspark.SparkContext.getOrCreate(
        pyspark.SparkConf().setMaster("local[*]").setAppName("Bronze_to_Silver")
    )

    # ═══════════════════════════════════════════════
    # CAPA BRONZE - Cargar datos crudos 
    # ═══════════════════════════════════════════════
    print("\n--- Cargando datos Bronze ---")
    rdd = sc.textFile("data/bronze/datos_raw.csv")

    # Visualizacion inicial 
    print("\nPrimeras 5 filas del CSV crudo:")
    for fila in rdd.take(5):
        print(f"  {fila}")

    total_lineas = rdd.count()
    print(f"\nTotal de lineas (incluyendo cabecera): {total_lineas}")

    # ═══════════════════════════════════════════════
    # Separar cabecera y campos 
    # ═══════════════════════════════════════════════
    header = rdd.first()
    print(f"\nCabecera: {header}")

    rdd_sin_header = rdd.filter(lambda linea: linea != header)
    rdd_campos = rdd_sin_header.map(lambda linea: linea.split(','))

    print(f"\nEjemplo de campos separados:")
    for fila in rdd_campos.take(3):
        print(f"  {fila}")

    # ═══════════════════════════════════════════════
    # Tipado de datos 
    # CSV: date,accion,open,high,low,close,volume
    # ═══════════════════════════════════════════════
    def tipar(campos):
        return (
            campos[0],           # date (str)
            campos[1],           # accion (str)
            float(campos[2]),    # open
            float(campos[3]),    # high
            float(campos[4]),    # low
            float(campos[5]),    # close
            int(campos[6])       # volume
        )

    rdd_tipado = rdd_campos.map(tipar)

    # Verificar tipos
    tipos = rdd_tipado.map(lambda row: tuple(type(c).__name__ for c in row)).distinct().collect()
    print(f"\nTipos de datos: {tipos}")

    # ═══════════════════════════════════════════════
    # Filtrar registros invalidos
    # ═══════════════════════════════════════════════
    def registro_valido(x):
        try:
            date, accion, open_, high, low, close, volume = x
            if low > high:
                return False
            if volume < 0:
                return False
            if close <= 0 or open_ <= 0 or high <= 0 or low <= 0:
                return False
            if not date or len(date) < 10:
                return False
            return True
        except:
            return False

    total_antes = rdd_tipado.count()
    rdd_filtrado = rdd_tipado.filter(registro_valido)
    total_despues = rdd_filtrado.count()

    print(f"\n--- Filtrado de registros ---")
    print(f"  Registros antes:     {total_antes}")
    print(f"  Registros despues:   {total_despues}")
    print(f"  Registros eliminados: {total_antes - total_despues}")

    # ═══════════════════════════════════════════════
    # Calcular spread y return 
    # ═══════════════════════════════════════════════
    def calcular_metricas(registro):
        date, accion, open_, high, low, close, volume = registro
        spread = high - low
        return_pct = (close - open_) / open_ if open_ > 0 else 0.0

        # Clasificar tipo de activo
        if accion.endswith(".MC"):
            tipo = "IBEX35"
        elif "-USD" in accion:
            tipo = "CRYPTO"
        elif accion.endswith(".SS") or accion.endswith(".SZ") or accion.endswith(".HK"):
            tipo = "CHINA"
        else:
            tipo = "SP500"

        return (date, accion, open_, high, low, close, volume, spread, return_pct, tipo)

    rdd_silver = rdd_filtrado.map(calcular_metricas)

    print(f"\nEjemplo de datos Silver (con metricas):")
    for fila in rdd_silver.take(3):
        print(f"  {fila}")

    # ═══════════════════════════════════════════════
    # Diagnostico de calidad
    # ═══════════════════════════════════════════════
    print(f"\n--- Diagnostico de calidad ---")

    # Tickers unicos
    tickers = rdd_silver.map(lambda x: x[1]).distinct().collect()
    print(f"  Tickers unicos: {len(tickers)}")

    # Distribucion por tipo de activo
    print(f"\n  Distribucion por tipo de activo:")
    distribucion = rdd_silver.map(
        lambda x: (x[9], 1)
    ).reduceByKey(
        lambda a, b: a + b
    ).collect()

    for tipo, count in sorted(distribucion):
        print(f"    {tipo:>10s}: {count:>6} registros")

    # Rango de fechas
    fechas = rdd_silver.map(lambda x: x[0])
    print(f"\n  Fecha minima: {fechas.min()}")
    print(f"  Fecha maxima: {fechas.max()}")

    # ═══════════════════════════════════════════════
    # Guardar capa Silver 
    # ═══════════════════════════════════════════════
    import shutil
    import os
    ruta_silver = "data/silver/stock_silver"
    if os.path.exists(ruta_silver):
        shutil.rmtree(ruta_silver)

    rdd_silver.saveAsTextFile(ruta_silver)

    print(f"\n  Datos Silver guardados en: {ruta_silver}")

    # ═══════════════════════════════════════════════
    # Resumen
    # ═══════════════════════════════════════════════
    print(f"\n{'=' * 60}")
    print(f"  RESUMEN BRONZE -> SILVER")
    print(f"{'=' * 60}")
    print(f"  Filas originales: {total_antes}")
    print(f"  Filas limpias:    {total_despues}")
    print(f"  Filas eliminadas: {total_antes - total_despues}")
    print(f"  Tickers:          {len(tickers)}")
    print(f"  Periodo:          {fechas.min()} a {fechas.max()}")

    print("\n" + "=" * 60)
    print("   BRONZE -> SILVER COMPLETADO")
    print("=" * 60)

    return sc, rdd_silver


if __name__ == "__main__":
    sc, rdd = ejecutar_bronze_to_silver()
    sc.stop()