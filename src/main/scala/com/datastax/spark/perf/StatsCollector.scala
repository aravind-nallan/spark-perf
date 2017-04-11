package com.datastax.spark.perf

import org.apache.spark._
import org.apache.spark.scheduler._
import com.datastax.spark.perf.metrics._

class StatsCollector extends SparkFirehoseListener {

  var currentApp : AppMetrics = new AppMetrics
  var currentJob : JobMetrics = null
  import StatsCollector._
  setCurrentApp(currentApp)

  override def onEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerApplicationStart => {
        currentApp.name = e.appName
        currentApp.startTime = e.time
      }
      case e: SparkListenerApplicationEnd => {
        currentApp.endTime = e.time
      }
      case e: SparkListenerJobStart => {
        currentJob = new JobMetrics
        currentJob.startTime = e.time
      }
      case e: SparkListenerJobEnd => {
        currentJob.endTime = e.time
        currentJob.success = (e.jobResult == JobSucceeded)
        currentApp.jobMetrics = currentApp.jobMetrics :+ currentJob
        currentJob = null
      }
      case e: SparkListenerStageSubmitted => {
        var stageMetrics = new StageMetrics
        stageMetrics.id = e.stageInfo.stageId
        stageMetrics.parentStages = e.stageInfo.parentIds
        currentJob.stageMetrics += (e.stageInfo.stageId -> stageMetrics)
      }
      case e: SparkListenerStageCompleted => {
        var stageMetrics = currentJob.stageMetrics(e.stageInfo.stageId)
        stageMetrics.startTime = e.stageInfo.submissionTime.get
        stageMetrics.endTime = e.stageInfo.completionTime.get
      }
      case e: SparkListenerTaskStart => {
        var stageMetrics = currentJob.stageMetrics(e.stageId)
        var taskMetrics = new com.datastax.spark.perf.metrics.TaskMetrics
        taskMetrics.startTime = e.taskInfo.launchTime
        taskMetrics.locality = e.taskInfo.taskLocality
        stageMetrics.taskMetrics += (e.taskInfo.taskId -> taskMetrics)
      }
      case e: SparkListenerTaskEnd => {
        var stageMetrics = currentJob.stageMetrics(e.stageId)
        var taskMetrics = stageMetrics.taskMetrics(e.taskInfo.taskId)
        taskMetrics.endTime = e.taskInfo.finishTime
        taskMetrics.success = e.taskInfo.successful
        taskMetrics.endReason = e.reason
      }
      case e: SparkListenerExecutorAdded => currentApp.numExecutors += 1
      case e: SparkListenerExecutorRemoved => currentApp.executorRemovedReason = currentApp.executorRemovedReason :+ e.reason
      case _ => 
    }
  }
}

object StatsCollector {

  var instance : AppMetrics = null

  def setCurrentApp(app: AppMetrics): Unit = { instance = app }
  def getAppMetrics : AppMetrics = return instance

}
