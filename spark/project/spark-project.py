import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType, IntegerType
from user_agents import parse

from util.config import Config
from util.logger import Log4j

import time
def _to_dim_product(df):
    dim_product_df = df.select(col("value.product_id").alias("product_id"))
    dim_product_prev_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_product") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_product_df = dim_product_df.select(col("product_id")) \
        .subtract(dim_product_prev_df.select("product_id"))
    dim_product_df \
        .select(col("product_id")) \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_product") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def _to_dim_country(df):
    tld2country = spark.read.csv("/data/project/country-codes-tlds.csv", header="true", inferSchema="true")
    tld2country = tld2country.withColumn("domain", f.split(col(" tld"), '[.]', -1).getItem(1))

    dim_country_prev_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_country") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_country_df = df.select(col("value.current_url").alias("current_url"))
    dim_country_df = dim_country_df.withColumn(" tld", f.split(col("current_url"), "/").getItem(2))

    dim_country_df = dim_country_df.withColumn("domain", f.split(col(" tld"), '[.]', -1))
    dim_country_df = dim_country_df.withColumn("domain", f.element_at(col("domain"), -1))
    dim_country_df = dim_country_df.distinct().select(col("domain"))
    dim_country_df = dim_country_df.join(tld2country, "domain")
    dim_country_df = dim_country_df.select(dim_country_df["domain"].alias("domain"), col("country").alias("country_name")) \
        .subtract(dim_country_prev_df.select(col("domain"), col("country_name")))

    dim_country_df \
        .select(col("domain"), col("country_name")) \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_country") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def _string_to_date(date_string):
    date_time = date_string.split(" ")

    date = date_time[0].split("-")
    year = int(date[0])
    month = int(date[1])
    day = int(date[2])

    hour = str(date_time[1]).split(":")
    hour = int(hour[0])
    return [year,month,day,hour]

def _to_dim_date(df):
    dim_date_prev_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_date") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_date_prev_df = dim_date_prev_df.select("Year","Month","Day").distinct()

    _string_to_date_udf = f.udf(_string_to_date, returnType=ArrayType(IntegerType()))

    dim_date_df = df.select(col("value.local_time").alias("local_time"))
    dim_date_df = dim_date_df.withColumn("Year", _string_to_date_udf("local_time")[0])
    dim_date_df = dim_date_df.withColumn("Month", _string_to_date_udf("local_time")[1])
    dim_date_df = dim_date_df.withColumn("Day", _string_to_date_udf("local_time")[2])
    dim_date_df = dim_date_df.select("Year", "Month", "Day")
    dim_date_df = dim_date_df.subtract(dim_date_prev_df)
    dim_date_df = dim_date_df.withColumn("Hour", f.lit([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]))
    dim_date_df = dim_date_df.select("Year", "Month", "Day", f.explode("Hour").alias("Hour"))
    dim_date_df \
        .select("Year", "Month", "Day","Hour") \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_date") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def parse_browser(ua):
    user_agent = parse(ua)
    return [user_agent.browser.family, user_agent.os.family]

def _to_dim_user_agent(df):
    dim_ua_prev_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_user_agent") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_ua_prev_df = dim_ua_prev_df.select("os","browser")

    dim_ua_df = df.select(col("value.user_agent").alias("user_agent"))
    parse_browser_udf = f.udf(parse_browser, returnType=ArrayType(StringType()))
    dim_ua_df = dim_ua_df.withColumn("browser", parse_browser_udf("user_agent")[0])
    dim_ua_df = dim_ua_df.withColumn("os", parse_browser_udf("user_agent")[1])
    dim_ua_df = dim_ua_df.select("os","browser") \
        .subtract(dim_ua_prev_df.select("os","browser"))

    dim_ua_df \
        .select("os","browser").distinct() \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_user_agent") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def _to_dim_referrer_url(df):
    dim_ref_url_prev_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_referrer_url") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_ref_url_prev_df = dim_ref_url_prev_df.select("referrer_url")
    dim_referrer_url = df.select(col("value.referrer_url").alias("referrer_url"))
    dim_referrer_url = dim_referrer_url.withColumn("referrer_url", f.split(col("referrer_url"), '/').getItem(2))
    dim_referrer_url = dim_referrer_url.select("referrer_url").distinct()
    dim_referrer_url = dim_referrer_url.select(col("referrer_url")) \
        .subtract(dim_ref_url_prev_df.select(col("referrer_url")))

    dim_referrer_url \
        .select("referrer_url") \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_referrer_url") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def _to_dim_store(df):
    dim_store_df = df.select(col("value.store_id").alias("store_id"))
    dim_store_prev_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_store") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()

    dim_store_df = dim_store_df.select(col("store_id")) \
        .subtract(dim_store_prev_df.select("store_id"))
    dim_store_df \
        .select(col("store_id")) \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_store") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def _to_fact_each_country(df):
    fact_each_country_df = df.select(col("value.current_url").alias("current_url"),
                                     col("value.store_id").alias("store_id"))

    fact_each_country_df = fact_each_country_df.withColumn(" tld", f.split(col("current_url"), "/").getItem(2))
    fact_each_country_df = fact_each_country_df.withColumn("domain", f.split(col(" tld"), '[.]', -1))
    fact_each_country_df = fact_each_country_df.withColumn("domain", f.element_at(col("domain"), -1))
    fact_each_country_df = fact_each_country_df.select("domain", "store_id")

    dim_store_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_store") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_store_df = dim_store_df.select(col("id").alias("dim_store_id"),
                                       col("store_id").alias("store_id"))

    dim_country_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_country") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_country_df = dim_country_df.select(col("id").alias("dim_country_id"),
                                           col("domain").alias("domain"))

    fact_each_country_df = fact_each_country_df.join(dim_country_df, "domain", "leftouter")
    fact_each_country_df = fact_each_country_df.join(dim_store_df, "store_id", "leftouter")

    fact_each_country_df.show()
    time.sleep(10)

    fact_each_country_df = fact_each_country_df.select(col("dim_country_id").alias("country_id"),
                                                       col("dim_store_id").alias("store_id"))
    fact_each_country_df \
        .select("store_id", "country_id") \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "fact_each_country") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def _to_fact_every_hour(df):
    dim_ua_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_user_agent") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_ua_df = dim_ua_df.select(col("id").alias("dim_user_agent_id"),col("os"),col("browser"))

    dim_date_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_date") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_date_df = dim_date_df.select(col("id").alias("dim_date_id"), col("Year"), col("Month"), col("Day"), col("Hour"))

    dim_product_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_product") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_product_df = dim_product_df.select(col("id").alias("dim_product_id"), col("product_id"))

    fact_every_hour_df = df.select(col("value.product_id").alias("product_id"),
                                   col("value.user_agent").alias("user_agent"),
                                   col("value.local_time").alias("local_time"))

    parse_browser_udf = f.udf(parse_browser, returnType=ArrayType(StringType()))
    fact_every_hour_df = fact_every_hour_df.withColumn("browser", parse_browser_udf("user_agent")[0])
    fact_every_hour_df = fact_every_hour_df.withColumn("os", parse_browser_udf("user_agent")[1])

    _string_to_date_udf = f.udf(_string_to_date, returnType=ArrayType(IntegerType()))
    fact_every_hour_df = fact_every_hour_df.withColumn("Year", _string_to_date_udf("local_time")[0])
    fact_every_hour_df = fact_every_hour_df.withColumn("Month", _string_to_date_udf("local_time")[1])
    fact_every_hour_df = fact_every_hour_df.withColumn("Day", _string_to_date_udf("local_time")[2])
    fact_every_hour_df = fact_every_hour_df.withColumn("Hour", _string_to_date_udf("local_time")[3])



    fact_every_hour_df = fact_every_hour_df.join(dim_product_df, "product_id", "leftouter") \
        .join(dim_ua_df, ["os", "browser"], "leftouter") \
        .join(dim_date_df, ["Year", "Month", "Day", "Hour"], "leftouter")

    fact_every_hour_df.select(col("dim_product_id").alias("product_id"),
                              col("dim_user_agent_id").alias("user_agent_id"),
                              col("dim_date_id").alias("date_id")) \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "fact_every_hour") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()

def _to_fact_every_day(df):
    fact_every_day_df = df.select(col("value.product_id").alias("product_id"),
                               col("value.local_time").alias("local_time"),
                               col("value.referrer_url").alias("referrer_url"),
                               col("value.current_url").alias("current_url"))

    dim_product_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_product") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_product_df = dim_product_df.select(col("id").alias("dim_product_id"),
                                           col("product_id"))

    dim_date_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_date") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_date_df = dim_date_df.select(col("id").alias("dim_date_id"),
                                     col("Year"),
                                     col("Month"),
                                     col("Day"),
                                     col("Hour")) \
        .where((col("Hour") == 24))

    dim_country_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_country") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_country_df = dim_country_df.select(col("id").alias("dim_country_id"),
                                           col("domain").alias("domain"))

    dim_ref_url_df = spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "dim_referrer_url") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .load()
    dim_ref_url_df = dim_ref_url_df.select(col("id").alias("dim_ref_url_id"),
                                           col("referrer_url").alias("referrer_url"))

    _string_to_date_udf = f.udf(_string_to_date, returnType=ArrayType(IntegerType()))
    fact_every_day_df = fact_every_day_df.withColumn("Year", _string_to_date_udf("local_time")[0])
    fact_every_day_df = fact_every_day_df.withColumn("Month", _string_to_date_udf("local_time")[1])
    fact_every_day_df = fact_every_day_df.withColumn("Day", _string_to_date_udf("local_time")[2])

    fact_every_day_df = fact_every_day_df.withColumn(" tld", f.split(col("current_url"), "/").getItem(2))
    fact_every_day_df = fact_every_day_df.withColumn("domain", f.split(col(" tld"), '[.]', -1))
    fact_every_day_df = fact_every_day_df.withColumn("domain", f.element_at(col("domain"), -1))

    fact_every_day_df = fact_every_day_df.withColumn("referrer_url", f.split(col("referrer_url"), '/').getItem(2))

    fact_every_day_df = fact_every_day_df.join(dim_product_df,"product_id", "leftouter") \
        .join(dim_country_df, "domain", "leftouter") \
        .join(dim_ref_url_df, "referrer_url", "leftouter") \
        .join(dim_date_df, ["Year", "Month", "Day"], "leftouter")

    fact_every_day_df.show()
    time.sleep(10)

    fact_every_day_df.select(col("dim_country_id").alias("country_id"),
                             col("dim_product_id").alias("product_id"),
                             col("dim_ref_url_id").alias("referrer_url_id"),
                             col("dim_date_id").alias("date_id")) \
        .write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/spark_streaming") \
        .option("dbtable", "fact_every_day") \
        .option("user", "postgres") \
        .option("password", "UnigapPostgres@123") \
        .mode("append") \
        .save()


def _write_stream(df, epoch_id):
    _to_dim_product(df) #ok
    _to_dim_country(df) #ok
    _to_dim_user_agent(df) #ok
    _to_dim_date(df) #ok
    _to_dim_referrer_url(df) #ok
    _to_dim_store(df) #ok

    _to_fact_each_country(df) #ok
    _to_fact_every_hour(df) #ok
    _to_fact_every_day(df) #debugging


if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    df.printSchema()
    raw_schema = StructType() \
        .add("id", StringType()) \
        .add("time_stamp", StringType()) \
        .add("ip", StringType()) \
        .add("user_agent", StringType()) \
        .add("resolution", StringType()) \
        .add("device_id", StringType()) \
        .add("api_version", StringType()) \
        .add("store_id", IntegerType()) \
        .add("local_time", StringType()) \
        .add("show_recommendation", StringType()) \
        .add("current_url", StringType()) \
        .add("referrer_url", StringType()) \
        .add("email_address", StringType()) \
        .add("collection", StringType()) \
        .add("product_id", StringType()) \
        .add("option", ArrayType(StructType().add("option_label", StringType()) \
                                 .add("option_id", StringType()) \
                                 .add("value_label",StringType()) \
                                 .add("value_id",StringType())))

    raw_df = df.withColumn("value", from_json(col("value").cast(StringType()),raw_schema))

    # query = raw_df.select(col("value.*")) \
    #     .writeStream \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .trigger(processingTime="30 seconds") \
    #     .start()

    raw_df.writeStream \
        .foreachBatch(_write_stream) \
        .option("truncate", False) \
        .trigger(processingTime="60 seconds") \
        .start()

    spark.streams.awaitAnyTermination()
