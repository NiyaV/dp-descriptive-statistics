/*
 * Copyright (C) 2016-2017 Gannett Corp. All rights reserved.
 *
 */

package com.gannett

import net.minidev.json.JSONObject
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by Dineshkumar Murugan on 02/20/17.
  * The purpose of this class is to create a Spark Session
  * This will be used everywhere whenever we SparkSession
  */
object GannettContext extends Serializable {

  var spark: Option[SparkSession] = None

  /**
    * creates the Spark Session
    * @return
    */
  def createSparkSession(configuration: ProcessorConfiguration): SparkSession = {

    //if it was already intialized just pick it up from the singleton instance.
    if (GannettContext.spark.isDefined) {
      GannettContext.spark.get
    } else {
      val spark  = SparkSession.builder
        .appName("Data Platform Column Summary Utill")
        .enableHiveSupport()
        .getOrCreate()

      spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
      spark.conf.set("spark.reducer.maxSizeInFlight", "1024m")
      spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      spark.sparkContext.getConf.registerKryoClasses(Array(classOf[JSONObject],classOf[JSONObject]))
      spark.sparkContext.setCheckpointDir("/tmp/checkpoint")

      //create the context
      GannettContext.spark = Option(spark)


      spark
    }

  }

  /**
    * creates the sqlContext from sparkContext.
    * @return
    */
  def createSqlContext(configuration: ProcessorConfiguration): SparkSession = {
    createSparkSession(configuration)
  }

}
