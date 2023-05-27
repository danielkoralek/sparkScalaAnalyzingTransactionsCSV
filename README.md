# Spark Scala analyzing the Transactions CSV file

This is a very simple repo containing the basic Scala powered by `Spark` on an `Azure Synapse Spark Pool`, used to quickly retrieve the 2MM file stored in the ADLSGen2 Storage Account and perform a schema infer, and also two analytic aggregations. Note that when running the spark command in the Synapse environment, no further integration configuration was required. It naturally integrated with the `ADLSGen2 Storage Account`.

#### First, the result of the query option to infer the schema during the spark.read operation:

![schema](1_schemaInfer.png)

#### The result of the first aggregation performed over the entire file, with a small transformation to generate the Y-M column for the aggregation:

![Aggregation 1](2_aggregation1.png)

#### The result of the ranking aggregation to display the top sellers after applying a quick filter on the data:

![Aggregation 2](3_aggregation2.png)
