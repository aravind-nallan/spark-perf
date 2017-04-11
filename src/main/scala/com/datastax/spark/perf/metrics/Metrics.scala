package com.datastax.spark.perf.metrics

import org.apache.spark.TaskEndReason
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.status.api.v1.TaskMetricDistributions

class AppMetrics {

  var name: String = ""
  var startTime: Long = 0
  var endTime: Long = 0
  var numExecutors: Int = 0
  var executorRemovedReason: Seq[String] = Nil
  var jobMetrics: Seq[JobMetrics] = Nil

  def print = {
    println(s"Application: [name=$name, numExecutors=$numExecutors, duration=${endTime-startTime}]")
    if (executorRemovedReason.length > 0) {
      println("Some executors were removed due to the following reasons:")
      executorRemovedReason.foreach(println)
    }
    for( job <- jobMetrics ) job.print
  }
}

class JobMetrics {
  var id: Int = 0
  var startTime: Long = 0
  var endTime: Long = 0
  var success: Boolean = false
  var stageMetrics: scala.collection.mutable.Map[Int, StageMetrics] = scala.collection.mutable.Map[Int, StageMetrics]()

  def print = {
    println(s"\tJob: [duration=${endTime-startTime}, success=$success]")
    for( stage <- stageMetrics ) stage._2.print
  }
}

class StageMetrics {
  var id: Int = 0
  var  startTime: Long = 0
  var  endTime: Long = 0
  var  parentStages: Seq[Int] = Nil
  var  taskMetrics: scala.collection.mutable.Map[Long, TaskMetrics] = scala.collection.mutable.Map[Long, TaskMetrics]()
  var  taskMetricDistros: TaskMetricDistributions = null
  var  rddMetrics: Seq[RDDMetrics] = Nil

  def print = {
    println(s"\t\tStage: [duration=${endTime-startTime}, number of tasks=${taskMetrics.size}]")
    println("Executor Run Time:")
    taskMetricDistros.executorRunTime.foreach(println)
    for( task <- taskMetrics ) task._2.print
  }
}

class TaskMetrics {
  var id: Long = 0
  var startTime: Long = 0
  var endTime: Long = 0
  var success: Boolean = false
  var locality: TaskLocality.TaskLocality = null
  var endReason: TaskEndReason = null
  var taskMetrics: org.apache.spark.status.api.v1.TaskMetrics = null

  def print = {
    println(s"\t\t\tTask: [duration=${endTime-startTime}, success=$success, locality=$locality, end reason=$endReason]")
  }
}

// Only for cached RDDs?
class RDDMetrics {
  var id: Int = 0
  var numPartitions: Int = 0
  var parents: Seq[Int] = Nil
  var diskSize: Long = 0
  var memSize: Long = 0
  var cached: Boolean = false
}
