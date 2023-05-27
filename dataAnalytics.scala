import spark.sql

// Read the file from the ADLSGen2 Storage Account

val csvPath = "abfs://xx@xxxx.dfs.core.windows.net/generatedData/transactions/transactions.csv"
val dfTransactions = spark.read.format("csv").option("header","true").option("inferSchema","true").load(csvPath)

// Outputs the schema

dfTransactions.printSchema

// First aggregation to see Total and Average operations by Year and Month

display(
    dfTransactions
        .withColumn("yearmonth", date_format($"transaction_dt", "y-MM"))
        .groupBy($"yearmonth", $"operation")
        .agg(sum($"value").as("total_value"), avg($"value").as("average_value"))
        .orderBy($"yearmonth")
)

// Second aggregation to see top Sellers in the dataset

display(
    dfTransactions
        .filter($"operation" === "SELL")
        .groupBy($"name")
        .agg(sum($"value").as("total_value"), count($"transaction_id").as("total_sell_operations"))
        .orderBy($"total_value".desc)
        .limit(5)
)