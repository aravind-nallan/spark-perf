# spark-perf
Spark performance analyzer and optimizer

Includes a SparkListener designed to collect statistics about a Spark Application so we can analyze them and suggest performance optimizations. Includes a sample ReadOptimizer which iterates over a couple of parameters that control Cassandra read performance and runs a simple Spark count function so we do a Full Table Scan over a Cassandra table, and selects the top three performing parameter combinations.

# Usage
Building this code requires use of maven. Run 'mvn clean install' to generate the spark-perf-<version>.jar.

ReadOptimizer is a Spark Application that can be sumitted using Spark Submit like so

```dse spark-submit --class com.datastax.spark.perf.ReadOptimizer target/spark-perf-0.1.jar killr_video videos_by_two_actors```

If you want to use the StatsCollector directly to run your own optimizations, just add the StatsCollector as an "extraListener" as shown in ReadOptimizer.scala. It is important to add it as such and not use SparkContect.addSparkListener so we can get the SparkListenerApplicationStart event where the application metrics object is initialized. Once your application ends you can access the metrics using StatsCollector.currentApp.
