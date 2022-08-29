from pyspark.sql.functions import col,dense_rank, sum,mean
from pyspark.sql.window import Window

def join_dfs(df1,df2,df3,matchColumn1,matchColumn2, type1='inner',type2='inner'):
    print(type1)
    final_df=df1.join(df2,df1[matchColumn1]==df2[matchColumn1],how=type1).join(df3,df2[matchColumn2]==df3[matchColumn2],how=type2)
    return final_df

def aggregate(groupby,dataframe):
    groupby=groupby.lower()
    if groupby=="persona":
        return dataframe.groupby("Nombre").sum("Kilometros") #
    
    if groupby=="ruta":
        return dataframe.groupby("Nombre Ruta").sum("Kilometros") #

    if groupby=="provincia":

        return dataframe.groupby("Provincia").sum("Kilometros") 
    if groupby=="dia":
        return dataframe.groupby("Fecha").sum("Kilometros") 
    #return de cuando se manda algo que no esta en las opciones del group by
    
def top_n(N,):
    pass
