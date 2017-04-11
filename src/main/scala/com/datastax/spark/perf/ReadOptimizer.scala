package com.datastax.spark.perf

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.param._

import com.datastax.spark.perf.metrics._

//TODO:
// 1) Create an analyzer framework where once the data is collected from a single app, it can be analyzed
//    by a list of analyzers to figure out what happened and suggest fixes.
// 2) For optimizers need to look at implementing strategies where we can select the next option to try
//    when one fails or based on other objectives.
// 3) How to get info about cores/memory available across cluster and assigned to application.
//    ExecutorAdded has core info

object ReadOptimizer extends App {

  var appMetrics: AppMetrics = null

  override def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: ReadOptimizer <keyspace> <table>")
      sys.exit(0)
    }

    val keyspace = args(0)
    val table = args(1)

    val fetchSizes = Array(500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 500000, 1000000)
    val splitSizes = Array(2, 4, 8, 16, 32, 64, 128)
    //val fetchSizes = Array(100000)
    //val splitSizes = Array(128)

    var gridBuilder = new ParamGridBuilder
    gridBuilder.addGrid(new IntParam("fetchsize", "spark.cassandra.input.fetch.size_in_rows", "Fetch size in rows"), fetchSizes)
    gridBuilder.addGrid(new IntParam("splitsize", "spark.cassandra.input.split.size_in_mb", "Split size in MB"), splitSizes)
    val paramGrid = gridBuilder.build

    paramGrid.map(paramMap => {
      cassandraCount(keyspace, table, paramMap)
      (paramMap, StatsCollector.currentApp.jobMetrics(0).endTime - StatsCollector.currentApp.jobMetrics(0).startTime)
    }).sortBy(_._2).take(3).foreach(println)

  }

  def cassandraCount(keyspace: String, table: String, paramMap: ParamMap) = {

    val conf = new SparkConf()
      .setAppName("C* Table Read Optimizer")
      .set("spark.extraListeners", "com.datastax.spark.perf.StatsCollector")
    paramMap.toSeq.foreach(paramPair => { println(s"Setting ${paramPair.param.name} to ${paramPair.value}"); conf.set(paramPair.param.name, s"${paramPair.value}") })

    val sc = new SparkContext(conf)

    // Count the table rows for FTS
    sc.cassandraTable(keyspace, table).count

    // Some stats take a little time to materialize in the listener, so wait for them
    Thread sleep 1000

    sc.stop()

  }

}
