"""
BACKTESTING: Validacion Historica de la Estrategia
RDDs
"""

import pyspark


def ejecutar_backtesting_completo():

    print("\n" + "=" * 60)
    print("   BACKTESTING")
    print("=" * 60)

    # ═══════════════════════════════════════════════
    # Configuracion de Spark
    # ═══════════════════════════════════════════════
    sc = pyspark.SparkContext.getOrCreate(
        pyspark.SparkConf().setMaster("local[*]").setAppName("Backtesting")
    )

    # ═══════════════════════════════════════════════
    # Cargar datos Gold
    # ═══════════════════════════════════════════════
    print("\n--- Cargando datos Gold ---")
    rdd_raw = sc.textFile("data/gold/stock_gold")

    def parsear_gold(linea):
        linea = linea.strip("()")
        partes = linea.split(", ")

        date = partes[0].strip("'")
        accion = partes[1].strip("'")
        close = float(partes[2])

        # SMA y RSI pueden ser None
        sma_20 = float(partes[3]) if partes[3] != "None" else None
        sma_50 = float(partes[4]) if partes[4] != "None" else None
        rsi = float(partes[5]) if partes[5] != "None" else None

        senal = partes[6].strip("'")
        tipo = partes[7].strip("'")

        return (date, accion, close, sma_20, sma_50, rsi, senal, tipo)

    rdd_gold = rdd_raw.map(parsear_gold)

    total = rdd_gold.count()
    print(f"  Registros Gold cargados: {total}")

    # Filtrar solo registros con indicadores validos
    rdd_valido = rdd_gold.filter(
        lambda x: x[3] is not None and x[4] is not None and x[5] is not None
    )
    print(f"  Registros con indicadores validos: {rdd_valido.count()}")

    # ═══════════════════════════════════════════════
    # Filtrar senales BUY y SELL
    # ═══════════════════════════════════════════════
    rdd_senales = rdd_valido.filter(lambda x: x[6] in ("BUY", "SELL"))
    print(f"  Total senales BUY/SELL: {rdd_senales.count()}")

    # ═══════════════════════════════════════════════
    # Simular operaciones (agrupar por ticker)
    # ═══════════════════════════════════════════════
    print("\n--- Ejecutando backtesting ---")

    rdd_por_ticker = rdd_senales.map(
        lambda x: (x[1], x)
    ).groupByKey().mapValues(
        lambda regs: sorted(list(regs), key=lambda r: r[0])
    )

    def backtesting(ticker_data):
        ticker, registros = ticker_data
        registros = list(registros)
        operaciones = []
        posicion = None

        for reg in registros:
            date, accion, close, sma_20, sma_50, rsi, senal, tipo = reg

            if senal == "BUY" and posicion is None:
                posicion = {"fecha": date, "precio": close}

            elif senal == "SELL" and posicion is not None:
                ganancia = (close - posicion["precio"]) / posicion["precio"] * 100
                operaciones.append(
                    (accion, posicion["fecha"], posicion["precio"],
                     date, close, round(ganancia, 2), tipo)
                )
                posicion = None

        return operaciones

    rdd_operaciones = rdd_por_ticker.flatMap(backtesting)
    resultados = rdd_operaciones.collect()

    # ═══════════════════════════════════════════════
    # Mostrar resultados
    # ═══════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("RESULTADOS DEL BACKTESTING")
    print("=" * 60)

    total_ops = len(resultados)

    if total_ops == 0:
        print("\n  No se generaron operaciones completas (BUY -> SELL)")
        print("  Los criterios pueden ser muy estrictos.")
        sc.stop()
        return []

    ganadoras = [r for r in resultados if r[5] > 0]
    perdedoras = [r for r in resultados if r[5] <= 0]
    ganancias = [r[5] for r in resultados]

    print(f"\n  Total operaciones:      {total_ops}")
    print(f"  Operaciones ganadoras:  {len(ganadoras)} ({len(ganadoras)/total_ops*100:.1f}%)")
    print(f"  Operaciones perdedoras: {len(perdedoras)} ({len(perdedoras)/total_ops*100:.1f}%)")
    print(f"  Ganancia media:         {sum(ganancias)/total_ops:+.2f}%")
    print(f"  Mejor operacion:        {max(ganancias):+.2f}%")
    print(f"  Peor operacion:         {min(ganancias):+.2f}%")

    # Top 5 mejores (como ejercicio 11 - sortBy)
    mejores = sorted(resultados, key=lambda x: x[5], reverse=True)[:5]
    print(f"\n  TOP 5 MEJORES OPERACIONES:")
    print("  " + "-" * 75)
    for op in mejores:
        print(f"  {op[0]:>10s} | Compra: {op[1]} a {op[2]:>10.2f} "
              f"| Venta: {op[3]} a {op[4]:>10.2f} | {op[5]:>+7.2f}%")

    # Top 5 peores
    peores = sorted(resultados, key=lambda x: x[5])[:5]
    print(f"\n  TOP 5 PEORES OPERACIONES:")
    print("  " + "-" * 75)
    for op in peores:
        print(f"  {op[0]:>10s} | Compra: {op[1]} a {op[2]:>10.2f} "
              f"| Venta: {op[3]} a {op[4]:>10.2f} | {op[5]:>+7.2f}%")

    # Resultados por tipo de activo (como ejercicio 10 - reduceByKey)
    print(f"\n  RESULTADOS POR TIPO DE ACTIVO:")
    print("  " + "-" * 55)

    rdd_resultados = sc.parallelize(resultados)
    por_tipo = rdd_resultados.map(
        lambda x: (x[6], (x[5], 1, 1 if x[5] > 0 else 0))
    ).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2])
    ).collect()

    for tipo, (gan_sum, total_t, ganad_t) in sorted(por_tipo):
        print(f"  {tipo:>10s} | Ops: {total_t:>4} "
              f"| Ganadoras: {ganad_t/total_t*100:>5.1f}% "
              f"| Ganancia media: {gan_sum/total_t:>+7.2f}%")

    # ═══════════════════════════════════════════════
    # Guardar resultados
    # ═══════════════════════════════════════════════
    import os, shutil
    ruta = "data/gold/backtesting_resultados"
    if os.path.exists(ruta):
        shutil.rmtree(ruta)

    rdd_resultados.saveAsTextFile(ruta)
    print(f"\n  Resultados guardados en: {ruta}")

    print("\n" + "=" * 60)
    print("   BACKTESTING COMPLETADO")
    print("=" * 60)

    return resultados


if __name__ == "__main__":
    resultados = ejecutar_backtesting_completo()
    pyspark.SparkContext.getOrCreate().stop()