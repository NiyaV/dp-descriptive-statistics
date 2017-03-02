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
object UDF extends Serializable {
  import org.apache.spark.sql.functions.udf


  def udfScoreToCategory=udf((score: Int) => {
    score match {
      case t if t >= 80 => "A"
      case t if t >= 60 => "B"
      case t if t >= 35 => "C"
      case _ => "D"
    }})

  def WeightedAverage=udf( (x: Seq[Double]) => {
    class State {
      var sum_value: Double =0
      var sum_weight: Double = 0
    }

    val state = new State

    state.sum_value = 0
    state.sum_weight = 0

    val weight = 1 / x.size

    x.foreach(iterate(_,weight))

    def iterate(value: Double, weight: Double): Unit = {
      if (value == 0 || weight == 0) {
        return true
      }
      if (state.sum_value == 0) {
        state.sum_value = value * weight
      } else {
        state.sum_value += value * weight
      }

      if (state.sum_weight == 0) {
        state.sum_weight = weight.doubleValue()
      } else {
        state.sum_weight += weight
      }
      return true

    }

    def terminatePartial(): State = {
      return state
    }

    def merge(other: State): Unit = {
      if (other == null) {
        return true
      }

      if (other.sum_value != 0) {
        if (state.sum_value == 0) {
          state.sum_value = other.sum_value.doubleValue()
        } else {
          state.sum_value += other.sum_value
        }
      }

      if (other.sum_weight != 0) {
        if (state.sum_weight == 0) {
          state.sum_weight = other.sum_weight.doubleValue()
        } else {
          state.sum_weight += other.sum_weight
        }
      }
      return true
    }

    def terminate(): Unit = {
      if (state.sum_value == 0 || state.sum_weight == 0) {
        return null
      }
      return state.sum_value / state.sum_weight
    }

    terminate()

  })


}


