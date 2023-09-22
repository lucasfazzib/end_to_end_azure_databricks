# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

existing_mounts = dbutils.fs.mounts()
target_mount_point = "/mnt/bronze"

if any(mount.mountPoint == target_mount_point for mount in existing_mounts):
    print(f"The directory '{target_mount_point}' is already mounted.")
else:
    configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
    }

    dbutils.fs.mount(
    source = "abfss://bronze@storageaccountmetastore.dfs.core.windows.net/",
    mount_point = target_mount_point,
    extra_configs = configs)
    print(f"Mounted the directory '{target_mount_point}'.")


# COMMAND ----------

existing_mounts = dbutils.fs.mounts()
target_mount_point = "/mnt/silver"

if any(mount.mountPoint == target_mount_point for mount in existing_mounts):
    print(f"The directory '{target_mount_point}' is already mounted.")
else:
    configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
    }

    dbutils.fs.mount(
    source = "abfss://silver@storageaccountmetastore.dfs.core.windows.net/",
    mount_point = target_mount_point,
    extra_configs = configs)
    print(f"Mounted the directory '{target_mount_point}'.")


# COMMAND ----------

existing_mounts = dbutils.fs.mounts()
target_mount_point = "/mnt/gold"

if any(mount.mountPoint == target_mount_point for mount in existing_mounts):
    print(f"The directory '{target_mount_point}' is already mounted.")
else:
    configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
    }

    dbutils.fs.mount(
    source = "abfss://gold@storageaccountmetastore.dfs.core.windows.net/",
    mount_point = target_mount_point,
    extra_configs = configs)
    print(f"Mounted the directory '{target_mount_point}'.")


# COMMAND ----------

dbutils.fs.ls("/mnt/bronze/SalesLT/")

# COMMAND ----------

dbutils.fs.ls("/mnt/silver/SalesLT/")

# COMMAND ----------

dbutils.fs.ls("/mnt/gold/SalesLT/")
