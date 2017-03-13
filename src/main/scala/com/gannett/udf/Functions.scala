/*
 * Copyright (C) 2016-2017 Gannett Corp. All rights reserved.
 *
 */
package com.gannett.udf

import com.esotericsoftware.kryo.Kryo
import net.minidev.json.{JSONObject, JSONValue}
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Dineshkumar Murugan on 2/14/17.
  */

object Functions extends Serializable {

  import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
  import org.apache.spark.mllib.linalg.Vectors
  val logger: Logger = LoggerFactory.getLogger("Summary Util Functions")

  def getContinuousVariableStats(spark: SparkSession, tableName: String, columnName: String,df:Dataset[Row]):JSONObject= {
    //Creating JSON
    val json = new JSONObject()

    df.registerTempTable("TempTableCon")
    val dftemp = spark.sql("select " + columnName + " from TempTableCon")//.cache()

    dftemp.cache()

    val nda = dftemp.describe()

    val stddev = nda.rdd.map {
      case r: Row => (r.getAs[String]("summary"), r.get(1))
    }.filter(_._1 == "stddev").map(_._2).collect


    val sd = stddev.reduce((a, b) => if (a == b) a else a)

    val cou = nda.rdd.map {
      case r: Row => (r.getAs[String]("summary"), r.get(1))
    }.filter(_._1 == "count").map(_._2).collect


    val c = cou.reduce((a, b) => if (a == b) a else a)

    val mean = nda.rdd.map {
      case r: Row => (r.getAs[String]("summary"), r.get(1))
    }.filter(_._1 == "mean").map(_._2).collect


    val me = mean.reduce((a, b) => if (a == b) a else a)

    val min = nda.rdd.map {
      case r: Row => (r.getAs[String]("summary"), r.get(1))
    }.filter(_._1 == "min").map(_._2).collect


    val mi = min.reduce((a, b) => if (a == b) a else a)

    val max = nda.rdd.map {
      case r: Row => (r.getAs[String]("summary"), r.get(1))
    }.filter(_._1 == "max").map(_._2).collect


    val ma = max.reduce((a, b) => if (a == b) a else a)

    //nda.rdd.map(x => (x.get(0).toString,x.get(1))).filter(_._1 == "stddev").map(_._2).collect()

    import org.apache.spark.sql.functions._

    val av = dftemp.select(avg(columnName)).collect()

    json.put("avg", av.reduce((a, b) => Row(a.get(0).toString + "," + b.get(0).toString)).toString())

    val hi = dftemp.rdd.map(x => if (x.get(0) == null) 0 else x.get(0).toString.toDouble)
    val hin = hi.histogram(10)



    val discretizer = new QuantileDiscretizer()
      .setInputCol(columnName)
      .setOutputCol("result")
      .setRelativeError(0.01)
      .setNumBuckets(10)


    val toDouble = udf[Double, String]((x: String) => {

      try {
        val y = x.toDouble
        if (y.isInfinity)
          0
        else if (y.isNegInfinity)
          0
        else if (y.isPosInfinity)
          0
        else
          y
      }
      catch {
        case e: Exception => 0
      }
    })


    val ndf = dftemp.withColumn(columnName, toDouble(dftemp(columnName)))

    val filtereddf = ndf.filter(!ndf.col(columnName).isNaN)


    val result = discretizer.fit(filtereddf).transform(filtereddf)



    val invertedResult: RDD[(String, String)] = result.rdd.map(f = x => {
      (x.get(1).toString
        , x.get(0).toString)
    }).cache()

    val countMap = invertedResult.map(_._1).countByValue()


    val countRdd = spark.sparkContext.parallelize(countMap.toList)


    val minrdd = invertedResult.reduceByKey((a, b) => {

      if (a.toString.toDouble < b.toString.toDouble) a else b
    })

    val maxrdd = invertedResult.reduceByKey((a, b) => {
      if (a.toString.toDouble > b.toString.toDouble) a else b
    })


    val joinedrdd = minrdd.join(maxrdd)

    val valueAndCount = joinedrdd.join(countRdd)


    /*
     * RDD of type JSONObject is created - Each Stat is put as key and its value is put in Value
     */
    val jsonrdd = valueAndCount.map(x => {
      val jsontemp1 = new JSONObject()
      val ww = x._2._1._1


      jsontemp1.put(x._2._1._1 + " - " + x._2._1._2, x._2._2.toString)
      jsontemp1

    })

    val red = jsonrdd.reduce((x, y) => {
      val ti = x.entrySet().iterator()


      while (ti.hasNext) {
        val nx = ti.next()
        y.put(nx.getKey, nx.getValue)

      }

      val tj = y.entrySet().iterator()
      while (tj.hasNext) {
        val nx = tj.next()
        y.put(nx.getKey, nx.getValue)

      }


      y

    })


    json.put("Binning Value and count", red)

    val jsontemp = new JSONObject()



    for (i <- 0 to hin._1.length - 2) {

      jsontemp.put(hin._1(i) + "-" + hin._1(i + 1), hin._2(i).toString)
    }


    json.put("Histogram Range and Count: ", jsontemp)

    json.put("stddev", sd.toString)
    json.put("count", c.toString)
    json.put("mean", me.toString)
    json.put("min", mi.toString)
    json.put("max", ma.toString)


    json
  }




  def findOutlier(spark: SparkSession,tableName: String, columnName: String,df:Dataset[Row]):JSONObject= {

    val json = new JSONObject()

    df.registerTempTable("TempTableOut")

    val dftemp = spark.sql("select "+columnName+",count(*) as cou from TempTableOut group by "+columnName).cache()

    dftemp.registerTempTable("Oinitialtemp")

    val parseddf = dftemp.rdd.map(x => Vectors.dense(x.getLong(1).toDouble)).cache()

    //Running Kmeans to get the Outlier - Stores in CLuster 1
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parseddf, numClusters, numIterations)

    val WSSSE = clusters.computeCost(parseddf)


    val newdf = dftemp.rdd.map(x => (x.getLong(1),clusters.predict(Vectors.dense(x.getLong(1).toDouble))))


    import spark.implicits._

    val newNames = Seq("id", "x1")
    val dfRenamed = newdf.toDF(newNames: _*).cache()

    dfRenamed.toDF().registerTempTable("Ofinaltemp")



    json.put("no_of_outlier",dfRenamed.select("x1").filter($"x1" === 1).count().toString)

    val nrow:Row = Row("")

    val getID = spark.sql("select case when "+columnName+" is NULL then \"NULL\" else "+columnName+" end as "+columnName+",f.x1 from Oinitialtemp i join Ofinaltemp f on i.cou = f.id and f.x1 = 1 ")


    val filrdd = getID.filter($"x1"===1).select(columnName)

    val finalrdd = filrdd.rdd.reduce((a,b) => Row(a.get(0).toString +","+b.get(0).toString ))

    json.put("outliers",finalrdd.getString(0))

    json.put("WSSSE - Within Set Sum of Squared Errors",WSSSE.toString)


    json


  }

  def getDiscreteVariableStats(spark: SparkSession, tableName: String, columnName: String, df:Dataset[Row]):JSONObject= {
    val json = new JSONObject


    val cou = df.count()

    val couN = df.where(df(columnName).isNull).count()


    json.put("Total Count",cou.toString)
    json.put("Null Values count",couN.toString)

    json
  }

  def getBinaryDiscreteVariableStats(spark: SparkSession, tableName: String, columnName: String,df:Dataset[Row]):JSONObject= {
    val json = new JSONObject

    df.registerTempTable("TempTableBin")

    val keyVal = df.select(columnName).rdd.groupBy(x => x.get(0))

    val jsonrdd = keyVal.map(x => {
      val tempjson = new JSONObject()
      var key = x._1
      if (key == null) {
        key = "NULL"
      }
      var i =0
      for( r <- x._2) {
        i = i+1
      }
      tempjson.put(key.toString,i.toString)
      tempjson

    })

    json.put("Unique Values",keyVal.count().toString())


    val fVal = jsonrdd.reduce((x, y) => {
      val ti = x.entrySet().iterator()


      while (ti.hasNext) {
        val nx = ti.next()
        y.put(nx.getKey, nx.getValue)

      }

      val tj = y.entrySet().iterator()
      while (tj.hasNext) {
        val nx = tj.next()
        y.put(nx.getKey, nx.getValue)

      }


      y})

    json.put("value and count",fVal)


    json
  }

}