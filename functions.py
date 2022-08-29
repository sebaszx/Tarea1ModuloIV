from pyspark.sql.functions import col,dense_rank, sum,mean
from pyspark.sql.window import Window

def join_dfs(df1,df2,df3,matchColumn1,matchColumn2, type1='inner',type2='inner'):
    final_df=df1.join(df2,how=type1).where(df1[matchColumn1]==df2[matchColumn1]).join(df3,how=type2).where(df2[matchColumn2]==df3[matchColumn2])
    return final_df

def aggregate():
    pass
def top_n():
    pass
