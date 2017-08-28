package edu.umd.cs.hcil.spark.analytics.utils

import twitter4j.Status
import twitter4j.TwitterObjectFactory

object JsonUtils {
  def jsonToStatus(line : String) : Status = {
    try {
      val status = TwitterObjectFactory.createStatus(line)
      return status
    } catch {
      case npe : NullPointerException => return null
      case e : Exception => return null
    }
  }
}