"""
SILVER -> GOLD: Indicadores Tecnicos y señales
RDDs 
"""

import pyspark
import os
import shutil


def ejecutar_silver_to_gold():

    print("\n" + "=" * 60)
    print("   PIPELINE SILVER -> GOLD")
    print("=" * 60)

    # ═══════════════════════════════════════════════
    # Configuracion de Spark
    # ═══════════════════════════════════════════════
    sc = pyspark.SparkContext.getOrCreate(
        pyspark.SparkConf().setMaster("local[*]").setAppName("Silver_to_Gold")
    )

    # ═══════════════════════════════════════════════
    # Cargar datos Silver
    # ═══════════════════════════════════════════════
    print("\n--- Cargando datos Silver ---")
    rdd_raw = sc.textFile("data/silver/stock_silver")

    # Parsear las tuplas guardadas como texto
    def parsear_silver(linea):
        # Quitar parentesis y comillas
        linea = linea.strip("()")
        partes = linea.split(", ")

        date = partes[0].strip("'")
        accion = partes[1].strip("'")
        open_ = float(partes[2])
        high = float(partes[3])
        low = float(partes[4])
        close = float(partes[5])
        volume = int(partes[6])
        spread = float(partes[7])
        return_pct = float(partes[8])
        tipo = partes[9].strip("'")

        return (date, accion, open_, high, low, close, volume, spread, return_pct, tipo)

    rdd_silver = rdd_raw.map(parsear_silver)

    total = rdd_silver.count()
    print(f"  Registros cargados: {total}")

    # ═══════════════════════════════════════════════
    # Agrupar por accion 
    # ═══════════════════════════════════════════════
    print("\n--- Agrupando por accion ---")
    rdd_por_accion = rdd_silver.map(
        lambda x: (x[1], x)
    ).groupByKey().mapValues(
        lambda registros: sorted(list(registros), key=lambda r: r[0])
    )

    n_acciones = rdd_por_accion.count()
    print(f"  Acciones agrupadas: {n_acciones}")

    # ═══════════════════════════════════════════════
    # Calcular indicadores y generar señales
    # ═══════════════════════════════════════════════
    print("\n--- Calculando indicadores tecnicos ---")

    def calcular_indicadores_accion(ticker_data):
        ticker, registros = ticker_data
        registros = list(registros)
        resultados = []

        for i, reg in enumerate(registros):
            date = reg[0]
            accion = reg[1]
            close = reg[5]
            tipo = reg[9]

            # SMA 20 (media ultimos 20 dias)
            if i >= 19:
                closes_20 = [registros[j][5] for j in range(i - 19, i + 1)]
                sma_20 = sum(closes_20) / 20
            else:
                sma_20 = None

            # SMA 50 (media ultimos 50 dias)
            if i >= 49:
                closes_50 = [registros[j][5] for j in range(i - 49, i + 1)]
                sma_50 = sum(closes_50) / 50
            else:
                sma_50 = None

            # RSI (14 dias)
            if i >= 14:
                ganancias = []
                perdidas = []
                for j in range(i - 13, i + 1):
                    if j > 0:
                        cambio = registros[j][5] - registros[j - 1][5]
                        if cambio > 0:
                            ganancias.append(cambio)
                        else:
                            perdidas.append(abs(cambio))
                avg_gain = sum(ganancias) / 14 if ganancias else 0
                avg_loss = sum(perdidas) / 14 if perdidas else 0

                if avg_loss == 0:
                    rsi = 100.0
                elif avg_gain == 0:
                    rsi = 0.0
                else:
                    rs = avg_gain / avg_loss
                    rsi = round(100 - (100 / (1 + rs)), 2)
            else:
                rsi = None

            # Generar senal
            if sma_20 and sma_50 and rsi:
                if sma_20 > sma_50 and rsi < 30:
                    senal = "BUY"
                elif sma_20 < sma_50 and rsi > 70:
                    senal = "SELL"
                else:
                    senal = "HOLD"
            else:
                senal = "HOLD"

            sma_20_r = round(sma_20, 4) if sma_20 else None
            sma_50_r = round(sma_50, 4) if sma_50 else None

            resultados.append((date, accion, close, sma_20_r, sma_50_r, rsi, senal, tipo))

        return resultados

    rdd_gold = rdd_por_accion.flatMap(calcular_indicadores_accion)

    print("  SMA 20 calculado")
    print("  SMA 50 calculado")
    print("  RSI calculado")

    # ═══════════════════════════════════════════════
    # Distribucion de señales 
    # ═══════════════════════════════════════════════
    print("\n--- Distribucion de señales ---")
    distribucion = rdd_gold.map(
        lambda x: (x[6], 1)
    ).reduceByKey(
        lambda a, b: a + b
    ).collect()

    for senal, count in sorted(distribucion):
        print(f"  {senal:>6s}: {count:>6} registros")

    # Ultimas señales de compra
    print("\n--- Ultimas señales de COMPRA ---")
    señales = rdd_gold.filter(lambda x: x[6] == "BUY").collect()
    for s in sorted(señales, key=lambda x: x[0], reverse=True)[:10]:
        print(f"  {s[0]} | {s[1]:>10s} | Close: {s[2]:>10.2f} | RSI: {s[5]:>5.1f} | BUY")

    # Ultimas señales de venta
    print("\n--- Ultimas señales de VENTA ---")
    señales_sell = rdd_gold.filter(lambda x: x[6] == "SELL").collect()
    for s in sorted(señales_sell, key=lambda x: x[0], reverse=True)[:10]:
        print(f"  {s[0]} | {s[1]:>10s} | Close: {s[2]:>10.2f} | RSI: {s[5]:>5.1f} | SELL")

    # ═══════════════════════════════════════════════
    # Guardar capa Gold 
    # ═══════════════════════════════════════════════
    ruta_gold = "data/gold/stock_gold"
    if os.path.exists(ruta_gold):
        shutil.rmtree(ruta_gold)

    rdd_gold.saveAsTextFile(ruta_gold)
    print(f"\n  Datos Gold guardados en: {ruta_gold}")

    print("\n" + "=" * 60)
    print("   SILVER -> GOLD COMPLETADO")
    print("=" * 60)

    return sc, rdd_gold


if __name__ == "__main__":
    sc, rdd = ejecutar_silver_to_gold()
    sc.stop()