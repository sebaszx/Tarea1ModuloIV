from pyspark.sql.functions import col,dense_rank, sum,mean,avg
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
    #total kilometros
    if tipo =="sum": return dataframe.groupby("Cedula","Provincia","Fecha").agg(sum("Kilometros").alias("Sum Kilometros")).orderBy(col("Sum Kilometros").desc()).limit(N)
    if tipo =="avg": return dataframe.groupby("Cedula","Provincia","Fecha").agg(avg("Kilometros").alias("Avg Kilometros")).orderBy(col("Avg Kilometros").desc()).limit(N)
    #promedio kilometros por día


     
