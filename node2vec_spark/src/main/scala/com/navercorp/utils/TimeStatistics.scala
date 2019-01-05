package com.navercorp.utils

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.Instant

class TimeStatistics {
  var timeRecord1: Long = 0
  var timeRecord2: Long = 0

  var timeRecordBeginTimestamp: String = ""

  def initRecordTime = {
    timeRecord1 = System.nanoTime
    timeRecordBeginTimestamp = Instant.now().toString
  }

  def endRecordTime = {
    timeRecord2 = System.nanoTime
  }

  def consumeTime(scale: String = "s") = scale match {
    case "s" => (timeRecord2 - timeRecord1) / 1e9d
    case "m" => (timeRecord2 - timeRecord1) / 1e9d / 60
    case "h" => (timeRecord2 - timeRecord1) / 1e9d / 60 / 60
    case _ => (timeRecord2 - timeRecord1) / 1e6d
  }

  def writeResult(filePath: String, timeScale: String, desc: String = "") = {
    val writer: PrintWriter = new PrintWriter(new FileOutputStream(new File(filePath), true))
    writer.write("Program began at " + desc + " " + timeRecordBeginTimestamp + "\n")
    writer.write("Total Consume " + desc + " " + consumeTime(timeScale).toString + " " + timeScale + "\n")
    writer.close()
  }
}
