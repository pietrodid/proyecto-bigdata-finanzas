"""
PROYECTO BIG DATA - ANALISIS AUTOMATIZADO DE MERCADOS FINANCIEROS
Pipeline completo: Bronze -> Silver -> Gold -> Backtesting

Ejecutar: python main.py
"""

import time


def main():
    inicio = time.time()

    print("\n" + "=" * 60)
    print("   PROYECTO BIG DATA - MERCADOS FINANCIEROS")
    print("   Pipeline Completo")
    print("=" * 60)

    # ═══════════════════════════════════════
    # FASE 1: Bronze -> Silver (Limpieza)
    # ═══════════════════════════════════════
    print("\n>>> FASE 1: LIMPIEZA DE DATOS (Bronze -> Silver)")
    from src.etl.bronze_to_silver import ejecutar_bronze_to_silver
    sc, rdd_silver = ejecutar_bronze_to_silver()
    sc.stop()

    # ═══════════════════════════════════════
    # FASE 2: Silver -> Gold (Indicadores)
    # ═══════════════════════════════════════
    print("\n>>> FASE 2: INDICADORES Y SENALES (Silver -> Gold)")
    from src.etl.silver_to_gold import ejecutar_silver_to_gold
    sc, rdd_gold = ejecutar_silver_to_gold()
    sc.stop()

    # ═══════════════════════════════════════
    # FASE 3: Backtesting
    # ═══════════════════════════════════════
    print("\n>>> FASE 3: BACKTESTING")
    from src.backtesting.backtester import ejecutar_backtesting_completo
    resultados = ejecutar_backtesting_completo()

    # ═══════════════════════════════════════
    # RESUMEN FINAL
    # ═══════════════════════════════════════
    duracion = time.time() - inicio

    print("\n" + "=" * 60)
    print("   PIPELINE COMPLETADO")
    print("=" * 60)
    print(f"\n  Tiempo total: {duracion:.1f} segundos ({duracion/60:.1f} minutos)")
    print(f"\n  Archivos generados:")
    print(f"    data/silver/stock_silver         (datos limpios)")
    print(f"    data/gold/stock_gold             (indicadores + senales)")
    print(f"    data/gold/backtesting_resultados (resultados simulacion)")

    import pyspark
    pyspark.SparkContext.getOrCreate().stop()


if __name__ == "__main__":
    main()