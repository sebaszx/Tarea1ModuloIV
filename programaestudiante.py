
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)
import sys
from functions import join_dfs,aggregate,top_n

#inputs
input_1 = sys.argv[1].lower() #ciclista
input_2 = sys.argv[2].lower() #ruta
input_3 = sys.argv[3].lower() #actividad

#spark session

spark = SparkSession.builder \
    .master("local") \
    .appName("tarea1")  \
    .getOrCreate()

#Schema creation

ciclistas_schema= StructType([
    StructField('Cedula',IntegerType()),
    StructField('Nombre',StringType()),
    StructField('Provincia',StringType())])


ruta_schema= StructType([
    StructField('Codigo Ruta',IntegerType()),
    StructField('Nombre Ruta',StringType()),
    StructField('Kilometros',IntegerType())])

actividad_schema= StructType([
    StructField('Codigo Ruta',IntegerType()),
    StructField('Cedula',IntegerType()),
    StructField('Fecha',DateType())])

#DataFrame Creation


df_ciclista= spark.read.csv(input_1,schema=ciclistas_schema,header=False)
df_ruta=spark.read.csv(input_2,schema=ruta_schema,header=False)
df_actividad=spark.read.csv(input_3,schema=actividad_schema,header=False)

df_ciclista.show()

df_ruta.show()

df_actividad.show()


#Join 3 dataframes
FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
FinalDataframe.show(10)

#Join 3 dataframes left join 
FinalDataframe_left=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta","left","left")
FinalDataframe_left.show(100)


#Aggregations 
persona=aggregate("Persona",FinalDataframe)
persona.show(50) # se puede hacer un isinstance de string o pyspark dataframe
ruta=aggregate("ruta",FinalDataframe)
ruta.show(50)
provincia=aggregate("provincia",FinalDataframe).show(50)
dia=aggregate("dia",FinalDataframe).show(50)
