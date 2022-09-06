from conftest import spark_session
import pytest
import datetime

#Importar las funciones creadas
from functions import join_dfs, aggregate, top_n
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)

#Definición de schemas
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

#Unit tests para 'join_dfs'
#Test 1
def test_join_pass(spark_session):
    print('Unit test 1: join_dfs')
    
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

    correct_df = spark_session.createDataFrame([(2, "Gemma Andersen","Puntarenas", datetime.date(2022,1,9),  2,  "Ruta 2",   66),
                                                (6, "Nevaeh Goodman",   "Alajuela", datetime.date(2020,2,26),  3,  "Ruta 3",   94),
                                                (4, "Damaris Grimes","Alajuela", datetime.date(2021,1,16),  5,  "Ruta 5",  112),
                                                (2, "Gemma Andersen","Puntarenas", datetime.date(2021,5,27),  4,  "Ruta 4",  162),
                                                (1, "Malakai Howard",  "San José", datetime.date(2022,1,9),  3,  "Ruta 3",   94),
                                                (2, "Gemma Andersen","Puntarenas" ,datetime.date(2022,1,9),  4,  "Ruta 4",  162),
                                                (1, "Malakai Howard",  "San José", datetime.date(2022,12,7),  5,  "Ruta 5", 112),
                                                (5, "Kristian Keller",  "Alajuela",datetime.date(2020,2,26),  1,  "Ruta 1",  182),
                                                (6, "Nevaeh Goodman",   "Alajuela", datetime.date(2022,6,18),  1,  "Ruta 1",   182),
                                                (3, "Julianna Hebert", "San José", datetime.date(2022,12,7), 5, "Ruta 5", 112),
                                                (3, "Julianna Hebert", "San José", datetime.date(2021,1,16), 5, "Ruta 5", 112),
                                                (7, "Christopher Blanco", "Puntarenas", datetime.date(2022,1,9), 4, "Ruta 4", 162),
                                                (7, "Christopher Blanco", "Puntarenas", datetime.date(2022,12,7), 5, "Ruta 5", 112),
                                                (7, "Christopher Blanco", "Puntarenas", datetime.date(2020,2,26), 1, "Ruta 1", 182)]
                                                ,schema=schema)
    correct_df.printSchema()
    FinalDataframe.printSchema()
    assert FinalDataframe.collect() == correct_df.collect()


#Test 2
def test_join_pass_left(spark_session):
    print('Unit test 2: join_dfs, left join')
    
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)

    FinalDataframe_left=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta","left","left")

    schema = StructType([ \
    StructField("Cedula",IntegerType(),True), \
    StructField("Nombre",StringType(),True), \
    StructField("Provincia",StringType(),True), \
    StructField("Fecha", DateType(), True), \
    StructField("Codigo Ruta", IntegerType(), True), \
    StructField("Nombre Ruta", StringType(), True), \
    StructField("Kilometros",IntegerType(),True) 
  ])

    correct_df_left = spark_session.createDataFrame([(1, "Malakai Howard",  "San José", datetime.date(2022,12,7),  5,  "Ruta 5", 112),
                                                (1, "Malakai Howard",  "San José", datetime.date(2022,1,9),  3,  "Ruta 3",   94),
                                                (2, "Gemma Andersen","Puntarenas" ,datetime.date(2022,1,9),  4,  "Ruta 4",  162),
                                                (2, "Gemma Andersen","Puntarenas", datetime.date(2021,5,27),  4,  "Ruta 4",  162),
                                                (2, "Gemma Andersen","Puntarenas", datetime.date(2022,1,9),  2,  "Ruta 2",   66),
                                                (3, "Julianna Hebert", "San José", datetime.date(2021,1,16), 5, "Ruta 5", 112),
                                                (3, "Julianna Hebert", "San José", datetime.date(2022,12,7), 5, "Ruta 5", 112),
                                                (4, "Damaris Grimes","Alajuela", datetime.date(2021,1,16),  5,  "Ruta 5",  112),
                                                (5, "Kristian Keller",  "Alajuela",datetime.date(2020,2,26),  1,  "Ruta 1",  182),
                                                (6, "Nevaeh Goodman",   "Alajuela", datetime.date(2022,6,18),  1,  "Ruta 1",  182),
                                                (6, "Nevaeh Goodman",   "Alajuela", datetime.date(2020,2,26),  3,  "Ruta 3",   94),
                                                (7, "Christopher Blanco", "Puntarenas", datetime.date(2020,2,26), 1, "Ruta 1", 182),
                                                (7, "Christopher Blanco", "Puntarenas", datetime.date(2022,12,7), 5, "Ruta 5", 112),
                                                (7, "Christopher Blanco", "Puntarenas", datetime.date(2022,1,9), 4, "Ruta 4", 162)]                                                
                                                ,schema=schema)
    correct_df_left.printSchema()
    FinalDataframe_left.printSchema()
    assert FinalDataframe_left.collect() == correct_df_left.collect()


#Unit tests para 'aggreagate'
#Test 3
def test_aggregate_ruta(spark_session):
    print('Unit test 3: aggregate: ruta')

    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    
    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    ruta=aggregate("ruta",FinalDataframe)
    schema = StructType([ \
        StructField("Codigo Ruta", IntegerType(), True), \
        StructField("Kilometros",IntegerType(),True) 
  ])
    
    correct_df=spark_session.createDataFrame([(1, 546),
                                            (3, 188),
                                            (5, 560),
                                            (4, 486),
                                            (2, 66)],schema=schema)
    assert ruta.collect() == correct_df.collect()

    
#Test 4
def test_aggregate_provincia(spark_session):
    print('Unit test 4: aggregate: provincia')

    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    
    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    provincia=aggregate("provincia",FinalDataframe)
    schema = StructType([ \
        StructField("Provincia",StringType(),True),
        StructField("Kilometros",IntegerType(),True) 
  ])
    
    correct_df=spark_session.createDataFrame([(  "San José", 430),
                                            (  "Puntarenas", 846),
                                            (  "Alajuela", 570)],schema=schema)
    assert provincia.collect() == correct_df.collect()


#Test 5
def test_aggregate_Cedula(spark_session):
    print('Unit test 5: aggregate: cedula')

    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    
    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    Cedula=aggregate("persona",FinalDataframe)
    schema = StructType([ \
       StructField("Cedula",IntegerType(),True), \
        StructField("Kilometros",IntegerType(),True) 
  ])
    
    correct_df=spark_session.createDataFrame([(1, 206),
                                              (6, 276),
                                              (3, 224),
                                              (5, 182),
                                              (4, 112),
                                              (7, 456),
                                              (2, 390)],schema=schema)
    assert Cedula.collect() == correct_df.collect()

#Test 6
def test_aggregate_fecha(spark_session):
    print('Unit test 6: aggregate: fecha')

    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    fecha=aggregate("dia",FinalDataframe)
    schema = StructType([ \
       StructField("Fecha", DateType(), True), \
        StructField("Kilometros",IntegerType(),True) 
  ])
    
    correct_df=spark_session.createDataFrame([(datetime.date(2022,6,18),    182),
                                              (datetime.date(2022,1,9),     484),
                                              (datetime.date(2021,5,27),    162),
                                              (datetime.date(2022,12,7),    336),
                                              (datetime.date(2021,1,16),    224),
                                              (datetime.date(2020,2,26),    458)],schema=schema)
    assert fecha.collect() == correct_df.collect()


#Test 7
def test_topn_sum(spark_session):
    
    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    TotalKilometros=top_n(2, FinalDataframe,"sum")
    
    schema = StructType([ \
    StructField("Provincia",StringType(),True), \
    StructField("Nombre",StringType(),True), \
    StructField("Sum Kilometros",IntegerType(),True)  ])
    
    correct_df=spark_session.createDataFrame([("Alajuela", "Nevaeh Goodman",     276),
                                            ("Alajuela", "Kristian Keller",      182),
                                            ("Puntarenas", "Christopher Blanco", 456),
                                            ("Puntarenas", "Gemma Andersen",     390),
                                            ("San José","Julianna Hebert",       224),
                                            ("San José","Malakai Howard",        206)],schema=schema)

    assert TotalKilometros.collect() == correct_df.collect()


#Test 8
def test_topn_avg(spark_session):

    df_ciclista= spark_session.read.csv("ciclista.csv",schema=ciclistas_schema,header=False)
    df_ruta=spark_session.read.csv("ruta.csv",schema=ruta_schema,header=False)
    df_actividad=spark_session.read.csv("actividad.csv",schema=actividad_schema,header=False)
    

    FinalDataframe=join_dfs(df_ciclista,df_actividad,df_ruta,"Cedula","Codigo Ruta")
    AvgKilometros=top_n(2, FinalDataframe,"avg")

    schema = StructType([ \
    StructField("Provincia",StringType(),True), \
    StructField("Nombre",StringType(),True), \
    StructField("Avg Kilometros",FloatType(),True)  ])

    correct_df=spark_session.createDataFrame([("Alajuela", "Kristian Keller",     182.0),
                                            ("Alajuela", "Nevaeh Goodman",        138.0),
                                            ("Puntarenas", "Gemma Andersen",      195.0),
                                            ("Puntarenas", "Christopher Blanco",  152.0),
                                            ("San José","Julianna Hebert",        112.0),
                                            ("San José","Malakai Howard",         103.0)],schema=schema)

    assert AvgKilometros.collect() == correct_df.collect()
