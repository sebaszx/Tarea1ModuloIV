from conftest import spark_session
import pytest
import datetime
#Importar las funciones creadas
from functions import join_dfs, aggregate, top_n
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)

#Unit tests para aggregate
#Test 1
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




def test_join_pass(spark_session):
    print('Unit test: aggregate')
   
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    schema = StructType([ \
    StructField("Cedula",IntegerType(),True), \
    StructField("Nombre",StringType(),True), \
    StructField("Provincia",StringType(),True), \
    StructField("Fecha", DateType(), True), \
    StructField("Codigo Ruta", IntegerType(), True), \
    StructField("Nombre Ruta", StringType(), True), \
    StructField("Kilometros",IntegerType(),True) 
  ])

    correct_df = spark_session.createDataFrame([(1, "Malakai Howard",  "San José", datetime.date(2022,12,7),  5,  "Ruta 5", 112),
                                                (1, "Malakai Howard",  "San José", datetime.date(2022,1,9),  3,  "Ruta 3",   94),
                                                (2, "Gemma Andersen","Puntarenas" ,datetime.date(2022,1,9),  4,  "Ruta 4",  162),
                                                (2, "Gemma Andersen","Puntarenas", datetime.date(2021,5,27),  4,  "Ruta 4",  162),
                                                (2, "Gemma Andersen","Puntarenas", datetime.date(2022,1,9),  2,  "Ruta 2",   66),
                                                (4, "Damaris Grimes","Alajuela", datetime.date(2021,1,16),  5,  "Ruta 5",  112),
                                                (5, "Kristian Keller",  "Alajuela",datetime.date(2020,2,26),  1,  "Ruta 1",  182),
                                                (6, "Nevaeh Goodman",   "Heredia", datetime.date(2020,2,26),  3,  "Ruta 3",   94) ]
                                                ,schema=schema)
    correct_df.printSchema()
    FinalDataframe.printSchema()
    assert FinalDataframe.collect() == correct_df.collect()


#Test 2
def test_aggregate_ruta(spark_session):
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    ruta=aggregate("ruta",FinalDataframe)
    schema = StructType([ \
        StructField("Codigo Ruta", IntegerType(), True), \
        StructField("Kilometros",IntegerType(),True) 
  ])
    correct_df=spark_session.createDataFrame([(1,       182),
                                              (3,       188),
                                              (5,       224),
                                              (4,       324),
                                              (2,        66)],schema=schema)
    assert ruta.collect() == correct_df.collect()
#Test 3
def test_aggregate_provincia(spark_session):
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    provincia=aggregate("provincia",FinalDataframe)
    schema = StructType([ \
        StructField("Provincia",StringType(),True),
        StructField("Kilometros",IntegerType(),True) 
  ])
    correct_df=spark_session.createDataFrame([(  "San José",       206),
                                            (  "Puntarenas",       390),
                                            (  "Alajuela",       294),
                                            (  "Heredia",        94)],schema=schema)
    assert provincia.collect() == correct_df.collect()
#Test 4
def test_aggregate_Cedula(spark_session):
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    Cedula=aggregate("persona",FinalDataframe)
    schema = StructType([ \
       StructField("Cedula",IntegerType(),True), \
        StructField("Kilometros",IntegerType(),True) 
  ])
    correct_df=spark_session.createDataFrame([(1,       206),
                                              (6,        94),
                                              (5,       182),
                                              (4,       112),
                                              (2,       390)],schema=schema)
    assert Cedula.collect() == correct_df.collect()

#Test 5
def test_aggregate_fecha(spark_session):
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    fecha=aggregate("dia",FinalDataframe)
    schema = StructType([ \
       StructField("Fecha", DateType(), True), \
        StructField("Kilometros",IntegerType(),True) 
  ])
    correct_df=spark_session.createDataFrame([(datetime.date(2022,1,9),       322),
                                              (datetime.date(2021,5,27),       162),
                                              (datetime.date(2022,12,7),       112),
                                              (datetime.date(2021,1,16),       112),
                                              (datetime.date(2020,2,26),       276)],schema=schema)
    assert fecha.collect() == correct_df.collect()
#Test 6
def test_topn_sum(spark_session):
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    TotalKilometros=top_n(5, FinalDataframe,"sum")
    schema = StructType([ \
    StructField("Cedula",IntegerType(),True), \
    StructField("Provincia",StringType(),True), \
    StructField("Fecha", DateType(), True), \
    StructField("Sum Kilometros",IntegerType(),True)  ])
    correct_df=spark_session.createDataFrame([(2,"Puntarenas",datetime.date(2022,1,9), 228),
                                            (5,  "Alajuela",datetime.date(2020,2,26), 182),
                                            (2,"Puntarenas",datetime.date(2021,5,27), 162),
                                            (1,  "San José",datetime.date(2022,12,7), 112),
                                            (4,  "Alajuela",datetime.date(2021,1,16), 112)],schema=schema)

    assert TotalKilometros.collect() == correct_df.collect()
def test_topn_avg(spark_session):
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    AvgKilometros=top_n(3, FinalDataframe,"avg")
    schema = StructType([ \
    StructField("Cedula",IntegerType(),True), \
    StructField("Provincia",StringType(),True), \
    StructField("Fecha", DateType(), True), \
    StructField("Avg Kilometros",FloatType(),True)  ])
    correct_df=spark_session.createDataFrame([
                                            (5,  "Alajuela",datetime.date(2020,2,26), 182.0),
                                            (2,"Puntarenas",datetime.date(2021,5,27), 162.0),
                                            (2,  "Puntarenas",datetime.date(2022,1,9), 114.0)],schema=schema)

    assert AvgKilometros.collect() == correct_df.collect()
"""def test_aggregate(spark_session):
    print('Unit test 2: aggregate')
    
    FinalDataframe=spark_session.createDataFrame([('Ruta 3', 558), ('Ruta 3', 100),
                                            ('Ruta 1', 990), ('Ruta 2', 230), ('Ruta 2', 100),
                                            ('Ruta 4', 1134), ('Ruta 5', 460), ('Ruta 5', 100)],
                                            ['ruta', 'Kilometros'])
    ruta2=aggregate("ruta",FinalDataframe)

    correct_df2 = spark_session.createDataFrame([('Ruta 3', 658), ('Ruta 1', 910),
                                            ('Ruta 2', 330), ('Ruta 4', 1134), ('Ruta 5', 560)],
                                            ['Nombre Ruta', 'sum(Kilometros)'])
    assert ruta2.collect() == correct_df2.collect()"""




