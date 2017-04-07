/*
 * Copyright (C) 2016-2017 Gannett Corp. All rights reserved.
 *
 */
package com.gannett

import org.joda.time.DateTime
import org.rogach.scallop.ScallopConf

/**
  * Created by Dineshkumar Murugan on 02/20/17.
  */
class ProcessorConfiguration(arguments: Seq[String])  extends ScallopConf(arguments) with Serializable {

  val forDate = opt[String](required = false, default = Some(new DateTime().toString("yyyy-MM-dd")) , descr = "Date to process in the format YYYY-mm-dd such as 2016-07-05, default is today ")
  val filePath = opt[String](required = true, descr = "Path to the properties file ")
  val partitionValue = opt[String](required = false, descr = "Partition value to fetch if the table is paritioned")

  banner("""Usage: GetSummary
           | Options:
           |""".stripMargin)
  verify()
}

object CommandLineArgumentProcessor {
  def processCommandLineForParser(args: Array[String]): ProcessorConfiguration = {
    val conf = new ProcessorConfiguration(args)
    conf
  }


}
