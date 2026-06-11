# Proyecto Big Data — Analisis Automatizado de Mercados Financieros

## Descripcion

Sistema automatizado de analisis de mercados financieros mediante tecnologias Big Data.
El proyecto implementa un pipeline completo que descarga datos historicos de acciones y
criptomonedas, calcula indicadores tecnicos, genera señales de compra/venta y valida
la estrategia mediante backtesting sobre 10 anios de datos reales.

Asignatura: Big Data
Equipo: Ines Alguacil Molto, Ines Alguacil Molto, Pietro Dichiara
Fecha: Junio 2026


## Resultados Principales

Datos procesados: 197,642 registros
Activos analizados: 81 (IBEX35 + SP500 + China + Crypto)
Periodo: 10 años (2016-2026)
Operaciones simuladas: 1,339
Ratio de acierto: 64.8%
Ganancia media: +6.22% por operacion
Mejor operacion: +1,098.07% (BNB-USD)


## Resultados por Mercado

S&P 500:  753 operaciones | 68.5% acierto | +6.92% ganancia media
IBEX 35:  271 operaciones | 64.9% acierto | +3.77% ganancia media
China:    178 operaciones | 59.0% acierto | +1.43% ganancia media
Crypto:   137 operaciones | 51.8% acierto | +13.47% ganancia media


## Arquitectura Medallion

El proyecto implementa una Arquitectura Medallion con tres capas:

BRONZE (Crudo)        SILVER (Limpio)        GOLD (Analitica)
CSV original    --->  Datos limpios    --->  Indicadores
197,642 filas   --->  Tipados          --->  señales
Sin validar     --->  Validados        --->  Backtesting
                      Clasificados           Resultados


## Tecnologias

- PySpark (RDDs): Procesamiento distribuido de datos
- AWS S3: Almacenamiento del data lake
- AWS Lambda: Ingesta automatizada de datos
- Python: Lenguaje de programacion principal
- yfinance: Descarga de datos financieros


## Estructura del Proyecto

proyecto-bigdata-finanzas/
|
|-- config/
|   |-- tickers.yml                 # Configuracion de parametros
|
|-- data/
|   |-- bronze/
|   |   |-- datos_raw.csv           # Datos crudos (197,642 filas)
|   |-- silver/                     # Datos limpios (generado automaticamente)
|   |-- gold/                       # Indicadores y resultados (generado automaticamente)
|
|-- docs/                           # Informes del proyecto
|
|-- src/
|   |-- etl/
|   |   |-- bronze_to_silver.py     # Limpieza y transformacion
|   |   |-- silver_to_gold.py       # Indicadores tecnicos y señales
|   |-- backtesting/
|   |   |-- backtester.py           # Simulacion historica
|   |-- ingesta/                    # Descarga de datos
|   |-- signals/                    # Generacion de señales
|   |-- visualization/              # Visualizacion de resultados
|
|-- main.py                         # Ejecuta el pipeline completo
|-- requirements.txt                # Dependencias del proyecto
|-- README.md                       # Este archivo


## Instalacion y Ejecucion

### Requisitos Previos

- Python 3.10+
- Java JDK 17

### 1. Clonar el repositorio

git clone https://github.com/pietrodid/proyecto-bigdata-finanzas.git
cd proyecto-bigdata-finanzas

### 2. Instalar Java (si no lo tienes)

sudo apt update
sudo apt install openjdk-17-jdk -y
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc

### 3. Crear entorno virtual e instalar dependencias

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

### 4. Ejecutar el pipeline completo

python main.py

### 5. O ejecutar paso a paso

python -m src.etl.bronze_to_silver
python -m src.etl.silver_to_gold
python -m src.backtesting.backtester


## Pipeline de Datos

### Fase 1: Bronze -> Silver (Limpieza)

- Carga del CSV crudo con sc.textFile()
- Separacion de cabecera y campos con split(',')
- Tipado de datos con funcion tipar() (str, float, int)
- Filtrado de registros invalidos con funcion registro_valido()
- Calculo de spread (high - low) y return porcentual
- Clasificacion por tipo de activo (IBEX35, SP500, China, Crypto)
- Guardado con saveAsTextFile()

### Fase 2: Silver -> Gold (Indicadores + señales)

Indicadores tecnicos calculados:
- SMA 20: Media movil simple de 20 dias (tendencia corto plazo)
- SMA 50: Media movil simple de 50 dias (tendencia largo plazo)
- RSI 14: Indice de fuerza relativa de 14 dias (sobrecompra/sobreventa)

Reglas de señales:
- BUY: SMA20 > SMA50 (tendencia alcista) + RSI < 30 (sobrevendida)
- SELL: SMA20 < SMA50 (tendencia bajista) + RSI > 70 (sobrecomprada)
- HOLD: Cualquier otra combinacion

Operaciones con RDDs:
- Agrupacion por accion con groupByKey()
- Calculo de indicadores con map() y flatMap()
- Conteo de señales con reduceByKey()

### Fase 3: Backtesting

Simulacion de operaciones sobre 10 anios de datos historicos:
- Compra al precio de cierre cuando aparece senal BUY
- Venta al precio de cierre cuando aparece senal SELL
- Calculo de ganancia/perdida por operacion
- Agregacion de resultados por tipo de activo con reduceByKey()
- Ordenacion de mejores operaciones con sortBy()


## Datos

### Fuentes
- Yahoo Finance: Precios historicos OHLCV mediante libreria yfinance
- Alpha Vantage: Precios ajustados por dividendos y splits

### Composicion del dataset
- IBEX 35: 35 acciones espanolas (40,912 registros)
- S&P 500: 36 acciones estadounidenses (111,724 registros)
- China: 10 acciones Shanghai/Shenzhen/Hong Kong (24,435 registros)
- Crypto: 10 criptomonedas (20,571 registros)
- Total: 81 activos, 197,642 registros



## Licencia

Proyecto academico - Uso exclusivamente educativo.