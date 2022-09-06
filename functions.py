from pyspark.sql.functions import col,dense_rank, sum,mean,avg, row_number
from pyspark.sql.window import Window

def join_dfs(df1,df2,df3,matchColumn1,matchColumn2, type1='inner',type2='inner'):
    print(type1)
    final_df=df1.join(df2,df1[matchColumn1]==df2[matchColumn1],how=type1).join(df3,df2[matchColumn2]==df3[matchColumn2],how=type2)
    return final_df.select(df1.Cedula,df1.Nombre,df1.Provincia,df2.Fecha,df3['Codigo Ruta'],df3['Nombre Ruta'],df3['Kilometros'])

def aggregate(groupby,dataframe):
    groupby=groupby.lower()
    if groupby=="persona":
        return dataframe.groupby("Cedula").agg(sum("Kilometros").alias("Kilometros")) #

    if groupby=="ruta":
        return dataframe.groupby("Codigo Ruta").agg(sum("Kilometros").alias("Kilometros")) #

    if groupby=="provincia":

        return  dataframe.groupby("Provincia").agg(sum("Kilometros").alias("Kilometros")) #
    if groupby=="dia":
        return dataframe.groupby("Fecha").agg(sum("Kilometros").alias("Kilometros")) #
    return dataframe.groupby("Nombre","Nombre Ruta","Provincia","Fecha").agg(sum("Kilometros").alias("Kilometros")) #
    #return de cuando se manda algo que no esta en las opciones del group by

def top_n(N, dataframe,tipo):
    print('Top', N, 'ciclistas por provincia')
    #total kilometros
    if tipo =="sum":
       print('Total de kilometros recorridos')
       df_sum = dataframe.groupby("Provincia","Nombre").agg(sum("Kilometros").alias("Sum Kilometros"))
       windowProvincia = Window.partitionBy("Provincia").orderBy(col("Provincia").asc(),col("Sum Kilometros").desc())
       df_topN = df_sum.withColumn("row",row_number().over(windowProvincia))
       df_topN = df_topN.filter(col("row") <= N)
       return df_topN.drop("row")

    #promedio kilometros por día
    if tipo =="avg":
       print('Promedio de kilometros recorridos por dia')
       df_avg = dataframe.groupby("Provincia","Nombre", "Fecha").agg(sum("Kilometros").alias("Sum Kilometros"))
       df_avg = df_avg.groupby("Provincia", "Nombre").agg(avg("Sum Kilometros").alias("Avg Kilometros"))#
       windowProvincia = Window.partitionBy("Provincia").orderBy(col("Provincia").asc(),col("Avg Kilometros").desc())
       df_topN = df_avg.withColumn("row",row_number().over(windowProvincia))
       df_topN = df_topN.filter(col("row") <= N)
       return df_topN.drop("row")


