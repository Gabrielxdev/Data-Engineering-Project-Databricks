# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable 

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmgc", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-dp-gabriel/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)

#define the tables 
bronze_table = f"{catalog}.{bronze}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"


# COMMAND ----------

bronze_table, silver_table, gold_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

# DBTITLE 1,Read CSV files from landing (with validation)
df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

# DBTITLE 1,Write Delta Table
# Verifica se a tabela bronze existe
if not spark.catalog.tableExists(bronze_table):
    # Primeira execução: cria a tabela
    print(f"✨ Criando tabela {bronze_table}...")
    df.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .mode("overwrite") \
        .saveAsTable(bronze_table)
    print(f"✅ Tabela criada com {df.count()} registros")
else:
    # Execuções seguintes: faz MERGE (upsert) para evitar duplicatas
    print(f"🔄 Fazendo MERGE na tabela {bronze_table}...")
    bronze_delta = DeltaTable.forName(spark, bronze_table)
    
    bronze_delta.alias("target").merge(
        df.alias("source"),
        # Condição de match: identifica registros únicos
        """target.order_id = source.order_id 
        AND target.product_id = source.product_id 
        AND target.customer_id = source.customer_id 
        AND target.order_placement_date = source.order_placement_date"""
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print(f"✅ MERGE concluído - duplicatas prevenidas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just the arrived incremenal data

# COMMAND ----------

# DBTITLE 1,Write Delta Table
df.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving files from source to processed directory

# COMMAND ----------

# Lista todos os arquivos e diretórios presentes no caminho 'landing_path' no sistema de arquivos do Databricks.
files = dbutils.fs.ls(landing_path)

# Para cada arquivo encontrado, move o arquivo do diretório de origem ('landing_path') para o diretório de processados ('processed_path').
for file_info in files: 
    dbutils.fs.mv(
        file_info.path,                # Caminho completo do arquivo de origem
        f"{processed_path}/{file_info.name}",  # Novo caminho de destino do arquivo
        True                          # Sobrescreve o arquivo de destino se já existir
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_orders = spark.sql(f"select * from {catalog}.{bronze}.staging_{data_source};")
df_orders.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

df_orders = df_orders.filter(F.col("order_qty").isNotNull())

df_orders = df_orders.withColumn(
    "customer_id",
    # Verifica se 'customer_id' contém apenas números
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
    .otherwise("999999")
    .cast("string")
)


# Regex para limpar o formato da data:
# Problema: Algumas datas vêm como "Monday, December 01, 2025" e outras como "01-12-2025"
# Objetivo: Remover o dia da semana (ex: "Monday, ") do início da string
# Explicação do regex pattern: r"^[A-Za-z]+,\s*"
#   ^           → Indica o INÍCIO da string (garante que só remove se estiver no começo)
#   [A-Za-z]+   → Uma ou mais letras (maiúsculas ou minúsculas) = captura o dia da semana (Monday, Tuesday, etc)
#   ,           → Vírgula literal que vem após o dia da semana
#   \s*         → Zero ou mais espaços em branco após a vírgula
#   ""          → Substitui tudo que foi encontrado por vazio (remove)


df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)


df_orders = df_orders.withColumn(
    "order_placement_date", 
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

df_orders = df_orders.withColumn(
    'product_id', 
    F.col('product_id').cast('string')
)

# COMMAND ----------

df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Join with products**

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")

# Join e seleciona todas as colunas de df_orders + product_code de df_products
# Sintaxe: df_orders["*"] seleciona todas as colunas (string "*" entre aspas)
df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])

df_joined.show(5)

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date and silver.order_id = bronze.order_id and silver.product_code = bronze.product_code and silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just the arrived incremenal data

# COMMAND ----------

# stagging for incremental data

df_joined.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_gold = spark.sql(f"select order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity from {catalog}.{silver_schema}.staging_{data_source};")

df_gold.show(2)

# COMMAND ----------

df_gold.count()

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating new table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date and source.order_id = gold.order_id and source.product_code = gold.product_code and source.customer_code = gold.customer_code"
                                     ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging with Parent company

# COMMAND ----------

# MAGIC %md
# MAGIC - Note: We want data for monthly level but child data is on daily level

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental Load**

# COMMAND ----------

df_child = spark.sql(f"select order_placement_date as date from {catalog}.{silver_schema}.staging_{data_source}")

incremental_month_df = df_child.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()

incremental_month_df.show()

incremental_month_df.createOrReplaceTempView("incremental_month")

# COMMAND ----------

monthly_table = spark.sql(f"""
         select 
            date, 
            product_code, 
            customer_code, 
            sold_quantity
         from {catalog}.{gold_schema}.sb_fact_orders sbf 
         inner join incremental_month m 
            on trunc(sbf.date, 'MM') = m.start_month                 
                          
""")

print("Total Rows: ", monthly_table.count())
monthly_table.show(10)

# COMMAND ----------

monthly_table.select('date').distinct().orderBy('date').show()

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date") #month_start -> date = first of month
)

df_monthly_recalc.show(10, truncate=False)

# COMMAND ----------

df_monthly_recalc.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly_recalc.alias("child_gold"), "parent_gold.date = child_gold.date and parent_gold.product_code = child_gold.product_code and parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table fmcg.silver.staging_orders;