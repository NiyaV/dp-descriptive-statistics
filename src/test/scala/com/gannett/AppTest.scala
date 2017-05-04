package com.gannett

import com.gannett.udf.Functions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{ShouldMatchers, FunSuite}
import net.minidev.json.JSONObject


class AppTest extends FunSuite with GannettSharedSparkContext with ShouldMatchers{

    test("test App") {

        val spark = this.spark.get
        var json  = new JSONObject
        var json1  = new JSONObject


        import spark.implicits._

        val Column : String = "gender"
        val tableName : String = "tempTable"

        val customSchema = StructType(Array(
            StructField("name", StringType, nullable = true),
            StructField("gender", StringType, nullable = true),
            StructField("IsAmerican", StringType, nullable = true)
        ))

        val df = spark.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .schema(customSchema)
          .load("src/test/resources/input.csv")

        json = Functions.getDiscreteVariableStats(spark, tableName, Column, df)
        json1 = Functions.getBinaryDiscreteVariableStats(spark, tableName, Column, df)
        println(json.toJSONString())
        val tc = json.get("Total Count")
        val nvc = json.get("Null Values count")
        assert(tc == "6")
        assert(nvc == "0")
        println(json1.toJSONString())
        val uv = json1.get("Unique Values")
        assert(uv == "2")

    }

}