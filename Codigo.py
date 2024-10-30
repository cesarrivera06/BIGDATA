# Importamos las librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('BeneficiariosAnalisis').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/row.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema para verificar la estructura del DataFrame
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadísticas básicas
df.summary().show()

# Consulta: Filtrar por "CantidadDeBeneficiarios" mayor a 100 y seleccionar columnas relevantes
print("Beneficiarios con cantidad mayor a 100\n")
beneficiarios_mayor_100 = df.filter(F.col('CantidadDeBeneficiarios') > 100).select(
    'Bancarizado', 'CodigoDepartamentoAtencion', 'CodigoMunicipioAtencion', 'Discapacidad', 
    'EstadoBeneficiario', 'Etnia', 'FechaInscripcionBeneficiario', 'Genero', 
    'NivelEscolaridad', 'NombreDepartamentoAtencion', 'NombreMunicipioAtencion', 'Pais', 
    'TipoAsignacionBeneficio', 'TipoBeneficio', 'TipoDocumento', 'TipoPoblacion', 
    'RangoBeneficioConsolidadoAsignado', 'RangoUltimoBeneficioAsignado', 'FechaUltimoBeneficioAsignado', 
    'RangoEdad', 'Titular', 'CantidadDeBeneficiarios'
)
beneficiarios_mayor_100.show()

# Ordenar filas por los valores en la columna "CantidadDeBeneficiarios" en orden descendente
print("Beneficiarios ordenados por cantidad de mayor a menor\n")
sorted_df = df.sort(F.col("CantidadDeBeneficiarios").desc())
sorted_df.show()













