# Databricks notebook source
from pyspark.sql import functions as F
from common.audit import AuditLogger

dbutils.widgets.text("catalog", "olist")
dbutils.widgets.text("run_id", "manual")
CATALOG = dbutils.widgets.get("catalog")
RUN_ID  = dbutils.widgets.get("run_id")

B, S = f"{CATALOG}.bronze", f"{CATALOG}.silver"
audit = AuditLogger(spark, CATALOG, "silver", RUN_ID)

def ts(c):   return F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss")
def zip5(c): return F.lpad(F.col(c), 5, "0")
def dec(c):  return F.col(c).cast("decimal(10,2)")

def save(df, name):
    (df.withColumn("_processed_at", F.current_timestamp())
       .withColumn("_run_id", F.lit(RUN_ID))
       .write.mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{S}.{name}"))

# COMMAND ----------
# ORDERS: a tabela central e onde nasce o target

def build_orders():
    df = (spark.table(f"{B}.orders")
          .select("order_id", "customer_id", "order_status",
                  ts("order_purchase_timestamp").alias("order_purchase_timestamp"),
                  ts("order_approved_at").alias("order_approved_at"),
                  ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
                  ts("order_delivered_customer_date").alias("order_delivered_customer_date"),
                  ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"))
          .dropDuplicates(["order_id"]))

    delivered = (F.col("order_status") == "delivered") & \
                F.col("order_delivered_customer_date").isNotNull()

    df = (df
        # compara DATAS: a estimativa vem sempre às 00:00:00, então comparar
        # timestamps marcaria como atraso uma entrega no próprio dia prometido
        .withColumn("is_late", F.when(delivered,
            (F.to_date("order_delivered_customer_date") >
             F.to_date("order_estimated_delivery_date")).cast("int")))
        .withColumn("promised_days", F.datediff(
            F.to_date("order_estimated_delivery_date"),
            F.to_date("order_purchase_timestamp")))
        .withColumn("actual_days", F.when(delivered, F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_purchase_timestamp")))))
    save(df, "orders")

audit.run("orders", f"{B}.orders", build_orders)

# COMMAND ----------
# ITENS, PAGAMENTOS, REVIEWS

def build_order_items():
    df = (spark.table(f"{B}.order_items")
          .select("order_id",
                  F.col("order_item_id").cast("int").alias("order_item_id"),
                  "product_id", "seller_id",
                  ts("shipping_limit_date").alias("shipping_limit_date"),
                  dec("price").alias("price"),
                  dec("freight_value").alias("freight_value"))
          .dropDuplicates(["order_id", "order_item_id"]))
    save(df, "order_items")

def build_order_payments():
    df = (spark.table(f"{B}.order_payments")
          .select("order_id",
                  F.col("payment_sequential").cast("int").alias("payment_sequential"),
                  "payment_type",
                  F.col("payment_installments").cast("int").alias("payment_installments"),
                  dec("payment_value").alias("payment_value")))
    save(df, "order_payments")

def build_order_reviews():
    df = (spark.table(f"{B}.order_reviews")
          .select("review_id", "order_id",
                  F.col("review_score").cast("int").alias("review_score"),
                  "review_comment_title", "review_comment_message",
                  ts("review_creation_date").alias("review_creation_date"),
                  ts("review_answer_timestamp").alias("review_answer_timestamp")))
    save(df, "order_reviews")

audit.run("order_items",    f"{B}.order_items",    build_order_items)
audit.run("order_payments", f"{B}.order_payments", build_order_payments)
audit.run("order_reviews",  f"{B}.order_reviews",  build_order_reviews)

# COMMAND ----------
# CLIENTES E VENDEDORES

def build_customers():
    df = (spark.table(f"{B}.customers")
          .select("customer_id", "customer_unique_id",
                  zip5("customer_zip_code_prefix").alias("customer_zip_code_prefix"),
                  F.initcap("customer_city").alias("customer_city"),
                  F.upper("customer_state").alias("customer_state"))
          .dropDuplicates(["customer_id"]))
    save(df, "customers")

def build_sellers():
    df = (spark.table(f"{B}.sellers")
          .select("seller_id",
                  zip5("seller_zip_code_prefix").alias("seller_zip_code_prefix"),
                  F.initcap("seller_city").alias("seller_city"),
                  F.upper("seller_state").alias("seller_state"))
          .dropDuplicates(["seller_id"]))
    save(df, "sellers")

audit.run("customers", f"{B}.customers", build_customers)
audit.run("sellers",   f"{B}.sellers",   build_sellers)

# COMMAND ----------
# PRODUTOS (corrige os nomes com erro de digitação e traz a categoria em inglês)

def build_products():
    tr = spark.table(f"{B}.category_translation")
    df = (spark.table(f"{B}.products").alias("p")
          .join(tr.alias("t"), "product_category_name", "left")
          .select("product_id", "product_category_name",
                  F.coalesce("product_category_name_english",
                             "product_category_name").alias("product_category_name_english"),
                  F.col("product_name_lenght").cast("int").alias("product_name_length"),
                  F.col("product_description_lenght").cast("int").alias("product_description_length"),
                  F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
                  F.col("product_weight_g").cast("double").alias("product_weight_g"),
                  F.col("product_length_cm").cast("double").alias("product_length_cm"),
                  F.col("product_height_cm").cast("double").alias("product_height_cm"),
                  F.col("product_width_cm").cast("double").alias("product_width_cm"))
          .withColumn("product_volume_cm3",
                      F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
          .dropDuplicates(["product_id"]))
    save(df, "products")

audit.run("products", f"{B}.products", build_products)

# COMMAND ----------
# GEOLOCALIZAÇÃO: remove pontos fora do Brasil e agrega por prefixo de CEP

def build_geolocation():
    df = (spark.table(f"{B}.geolocation")
          .select(zip5("geolocation_zip_code_prefix").alias("zip_code_prefix"),
                  F.col("geolocation_lat").cast("double").alias("lat"),
                  F.col("geolocation_lng").cast("double").alias("lng"))
          # bounding box aproximado do Brasil: existem pontos espúrios fora do país
          .filter(F.col("lat").between(-34.0, 5.5) & F.col("lng").between(-74.0, -34.0))
          .groupBy("zip_code_prefix")
          .agg(F.avg("lat").alias("lat"), F.avg("lng").alias("lng"),
               F.count("*").alias("n_points")))
    save(df, "geolocation")

audit.run("geolocation", f"{B}.geolocation", build_geolocation)

# COMMAND ----------

audit.flush()