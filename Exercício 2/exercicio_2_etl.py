# Exercício 2 - ETL PySpark (pipeline simplificado)

from pyspark.sql import functions as F, Window

# Leitura dos dados
df_purchase = spark.read.parquet("/raw/purchase")
df_product_item = spark.read.parquet("/raw/product_item")
df_extra = spark.read.parquet("/raw/purchase_extra_info")

#Junta todass as tabelas pelo ID da compra
df_joined = df_purchase.join(df_product_item, "purchase_id", "outer") \
                       .join(df_extra, "purchase_id", "outer")

#Marca se a compra foi paga
df_joined = df_joined.withColumn(
    "is_paid",
    F.when(F.col("release_date").isNotNull(), F.lit(True)).otherwise(F.lit(False))
)

#Marca qual é a versão mais atual por compraa
w = Window.partitionBy("purchase_id").orderBy(F.col("transaction_datetime").desc())
df_joined = df_joined.withColumn("rn", F.row_number().over(w))
df_joined = df_joined.withColumn("is_current", F.when(F.col("rn") == 1, True).otherwise(False))

#Escreve histórico
df_joined.write.mode("append").partitionBy("transaction_date").format("delta").save("/delta/fact_gmv_history")

