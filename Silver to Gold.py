# Databricks notebook source
# MAGIC %md
# MAGIC #Tranformações GOLD

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fazendo um exemplo de transformacao para uma tabela

# COMMAND ----------

# dbutils.fs.ls('/mnt/silver/SalesLT/')

# COMMAND ----------

# dbutils.fs.ls('/mnt/gold/')

# COMMAND ----------

# input_path = '/mnt/silver/SalesLT/Address/'

# COMMAND ----------

# df = spark.read.format('delta').load(input_path)

# COMMAND ----------

# display(df)

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, regexp_replace

# column_names = df.columns

# for old_col_name in column_names:
#     new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

#     df = df.withColumnRenamed(old_col_name, new_col_name)

# COMMAND ----------

# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fazendo um exemplo de transformacao para TODAS tabelas

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls('mnt/silver/SalesLT/'):
    table_name.append(i.name.split('/')[0])


# COMMAND ----------

table_name

# COMMAND ----------

output_paths_list = []
for name in table_name:
    path = '/mnt/silver/SalesLT/' + name 
    print(path)
    df = spark.read.format('delta').load(path)

    column_names = df.columns

    for old_col_name in column_names:
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

        df = df.withColumnRenamed(old_col_name, new_col_name)
    
    output_path = '/mnt/gold/SalesLT/' + name + '/'
    df.write.format('delta').mode("overwrite").save(output_path)
    output_paths_list.append(output_path)

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/silver/SalesLT/Address')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# %sql
# drop table Address

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criacao de tabela no catalog (executar uma unica vez)

# COMMAND ----------

# gold_mount_path = "/mnt/gold/SalesLT/"
# gold_directories = dbutils.fs.ls(gold_mount_path)


# for directory in gold_directories:
#     directory_path = directory.path
#     folder_name = directory.name 
#     table_name = folder_name.replace("/", "")
#     df = spark.read.format("delta").load(directory_path)  

#     df.write.saveAsTable(table_name)


# COMMAND ----------

# %sql
# SELECT count(*) FROM hive_metastore.default.customer;
