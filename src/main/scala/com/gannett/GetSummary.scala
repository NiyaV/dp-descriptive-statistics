/*
 * Copyright (C) 2016-2017 Gannett Corp. All rights reserved.
 *
 */
package com.gannett


import java.io.{FileInputStream, InputStream}
import java.util.Properties

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.StatCounter
import com.gannett.udf.UDF.WeightedAverage
import com.gannett.udf.Functions.{findOutlier, getBinaryDiscreteVariableStats, getContinuousVariableStats, getDiscreteVariableStats}
import net.minidev.json.JSONObject
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaRDD
import org.slf4j.{Logger, LoggerFactory}



/**
  * Created by dmurugan on 2/13/17.
  */
object GetSummary extends Serializable{
  val logger: Logger = LoggerFactory.getLogger("Summary Util")

  def loadTConfig(configuration: ProcessorConfiguration) ={
    val filePath:String = configuration.filePath.get.get
    val(tableName,continuousVar,discreteVar,discreteBinaryVar,destinationPath,s3Bucket,partitionColumn) =
    try {
      val prop = new Properties()
      //prop.load(getClass().getResourceAsStream("/sample.properties"))
      prop.load(new FileInputStream(filePath))
      (
        prop.getProperty("TableName"),
        prop.getProperty("ContinuousVariable"),
        prop.getProperty("DiscreteVariable"),
        prop.getProperty("DiscreteBinaryVariable"),
        prop.getProperty("DestinationPath"),
        prop.getProperty("S3Bucket"),
        prop.getProperty("PartitionColumn")

      )
    } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)

    }

    (tableName,continuousVar,discreteVar,discreteBinaryVar,destinationPath,s3Bucket,partitionColumn)
  }



  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()


    val configuration = CommandLineArgumentProcessor.processCommandLineForParser(args)
    var (tableName,continuousVar,discreteVar,discreteBinaryVar,destinationPath,s3Bucket,partitionColumn) = loadTConfig(configuration)
    logger.info(configuration.summary)
    logger.info("Creating spark context")
    val spark = GannettContext.createSparkSession(configuration)
    val df = if(partitionColumn.length > 1) {
      val parValue = configuration.partitionValue.get.get
      destinationPath = destinationPath+"/"+partitionColumn+"="+parValue
      spark.sql("select * from "+tableName+" where "+partitionColumn+"="+parValue)
    }else {
      spark.sql("select * from "+tableName)
    }

    val json  = new JSONObject

    json.put(tableName,process(tableName,continuousVar,discreteVar,discreteBinaryVar,destinationPath,s3Bucket,configuration,df,spark))

    //println(json)
    logger.info("Final Json::"+json.toJSONString)
    val endTime = System.currentTimeMillis()
    logger.info("Processing complete. TOTAL TIME FOR Summary =>" + (endTime - startTime) / 1000 )

    spark.stop()
  }


  def process(tableName:String,continuousVar:String,discreteVar:String,discreteBinaryVar:String,destinationPath:String,s3Bucket:String,configuration: ProcessorConfiguration, df:Dataset[Row], spark:SparkSession): JSONObject = {

    val json = new JSONObject
    val df = spark.sql("select * from "+tableName).cache()
    df.rdd.checkpoint()

    json.put("ContinuousVariable",continuousVariableStat(tableName,continuousVar,configuration,df,spark))
    json.put("DiscreteVariable",discreteVariableStat(tableName,discreteVar,configuration,df,spark))
    json.put("DiscreteBinaryVariable",discreteBinaryVariableStat(tableName,discreteBinaryVar,configuration,df,spark))

    val stringRDD = spark.sparkContext.parallelize(Seq(json.toJSONString))
    val destTable = spark.sqlContext.jsonRDD(stringRDD)
    //destTable.registerTempTable("finalTable")
    FileSystem.get(new URI(s3Bucket), spark.sparkContext.hadoopConfiguration).delete(new Path(destinationPath), true)
    destTable.coalesce(1)
    destTable.write.json(destinationPath)

    json

  }


  def continuousVariableStat(tableName:String,continuousVar:String,configuration: ProcessorConfiguration, df:Dataset[Row], spark:SparkSession): JSONObject = {
    val json = new JSONObject()
    if(continuousVar.length<1) {
        return json
    }
    val arrC = continuousVar.split(",")

    for(v <- arrC) {
      val tempjson = new JSONObject
      tempjson.put("Con", getContinuousVariableStats(spark,tableName,v,df))
      tempjson.put("Outlier",findOutlier(spark,tableName,v,df))
      json.put(v,tempjson)
    }
    json
  }

  def discreteVariableStat(tableName:String,discreteVar: String,configuration: ProcessorConfiguration, df:Dataset[Row], spark:SparkSession): JSONObject = {


    val json = new JSONObject()
    if(discreteVar.length<1) {
      return json
    }
    val arrD = discreteVar.split(",")

    for(v <- arrD) {
      val tempjson = new JSONObject

      tempjson.put(v, getDiscreteVariableStats(spark,tableName,v,df))
      json.put(v,tempjson)
    }

    json
  }

  def discreteBinaryVariableStat(tableName:String,discreteBinaryVar: String,configuration: ProcessorConfiguration, df:Dataset[Row], spark:SparkSession): JSONObject = {


    val json = new JSONObject()

    if(discreteBinaryVar.length<1) {
      return json
    }

    val arrD = discreteBinaryVar.split(",")
    for(v <- arrD) {
      val tempjson = new JSONObject

      tempjson.put(v, getBinaryDiscreteVariableStats(spark,tableName,v,df))
      json.put(v,tempjson)
    }

    json
  }
}





