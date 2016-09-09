package edu.umd.cs.hcil.spark.analytics.utils

object TimeScale extends Enumeration {
  type TimeScale = Value
  val MINUTE, HOURLY, DAILY = Value
}