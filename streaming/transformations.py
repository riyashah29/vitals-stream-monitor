from pyspark.sql.functions import col, to_timestamp, when

def parse_and_clean(df):
    """
    Cleans the raw stream:
    - Converts timestamp string to proper timestamp type
    - Drops rows missing patient_id or timestamp
    - Caps obviously impossible values (data quality check)
    """
    return df \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .filter(col("patient_id").isNotNull()) \
        .filter(col("event_time").isNotNull()) \
        .withColumn("heart_rate",
            when(col("heart_rate").between(0, 300), col("heart_rate"))
            .otherwise(None)
        ) \
        .withColumn("glucose",
            when(col("glucose").between(0, 1000), col("glucose"))
            .otherwise(None)
        ) \
        .withColumn("bp_systolic",
            when(col("bp_systolic").between(0, 300), col("bp_systolic"))
            .otherwise(None)
        )