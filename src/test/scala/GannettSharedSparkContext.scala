package com.gannett
import java.util.Date

import com.holdenkarau.spark.testing.{LocalSparkContext, SparkContextProvider}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end. */
trait GannettSharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>


  var spark: Option[SparkSession] = None


  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString



  override def beforeAll() {
    val spark  = SparkSession.builder
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    spark.conf.set("spark.ui.enabled", "false")
    spark.conf.set("spark.app.id", appID)
    spark.conf.set("spark.local.ip","127.0.0.1")
    spark.conf.set("spark.driver.host","127.0.0.1")

    //spark.sparkContext.getConf.setExecutorEnv("spark.debug.maxToStringFields","11111")

    //_sc = GannettContext.createSparkContext(conf)
    this.spark = Option(spark)

    super.beforeAll()
  }

  override def afterAll() {
    try {
      spark.get.stop()

    } finally {

      super.afterAll()
    }
  }
}