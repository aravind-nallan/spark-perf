package com.datastax.spark.perf

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.param._

import com.datastax.spark.perf.metrics._

object WriteOptimizer extends App {

  var appMetrics: AppMetrics = null

  override def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: WriteOptimizer <keyspace> <table>")
      sys.exit(0)
    }

    val keyspace = args(0)
    val table = args(1)

    val throughput = Array(0.1, 0.5, 1.0)
    val batchSizes = Array(1, 5, 10, 20, 50)
    //val throughput = Array(0.1)
    //val batchSizes = Array(1)

    var gridBuilder = new ParamGridBuilder
    val throughputParam = new DoubleParam("throughput", "spark.cassandra.output.throughput_mb_per_sec", "Throughput mbps")
    val batchSizeParam = new IntParam("batchsize", "spark.cassandra.output.batch.size.rows", "Batch size in rows")
    gridBuilder.addGrid(throughputParam, throughput)
    gridBuilder.addGrid(batchSizeParam, batchSizes)
    val paramGrid = gridBuilder.build

    val conf = new SparkConf()
      .setAppName("C* Table Write Optimizer")
      .set("spark.extraListeners", "com.datastax.spark.perf.StatsCollector")
      .set("spark.cassandra.input.split.size_in_mb", "8")
      .set("spark.scheduler.minRegisteredResourcesRatio", "1.0") // Wait for all requested resources

    val sc = new SparkContext(conf)

    print("Reading and repartitioning...")
    // Read and cache the table
    val readRdd = sc.cassandraTable(keyspace, table)
    // Repartition to separate read stage from write stage and also setup a defacto cache so we dont have to read the same data over and over
    // Also makes sure data read is randomly distributed across partitions which might more accurately reflect the state of data before it is written in actual app code
    val repartitionedRdd = readRdd.repartition(readRdd.partitions.size)
    repartitionedRdd.count
    println("Reading and repartitioning...done")

    val metrics = paramGrid.map(paramMap => {
      paramMap.toSeq.foreach(paramPair => { println(s"Setting ${paramPair.param.name} to ${paramPair.value}"); })
      val testWriteConf = WriteConf(ignoreNulls=true, throughputMiBPS=paramMap.get(throughputParam).get, batchSize=RowsInBatch(paramMap.get(batchSizeParam).get))
      repartitionedRdd.saveToCassandra(keyspace, table, writeConf=testWriteConf)
      // Some stats take a little time to materialize in the listener, so wait for them
      Thread sleep 1000
      (paramMap, StatsCollector.getAppMetrics.jobMetrics.last.endTime - StatsCollector.getAppMetrics.jobMetrics.last.startTime)
    })

    //StatsCollector.getAppMetrics.print
    println("Top 3")
    metrics.sortBy(_._2).take(3).foreach(println)
    println("Bottom 3")
    metrics.sortBy(- _._2).take(3).foreach(println)

    sc.stop()
  }

}
