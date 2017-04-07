/*
 * Copyright (C) 2016-2017 Gannett Corp. All rights reserved.
 *
 */
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Cast, Coalesce, Expression, ExpressionDescription, If, ImplicitCastInputTypes, IsNull, Literal}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, DataType, _}

import org.apache.spark.sql.catalyst.expressions.{WeightedAverage}
/**
  * Created by dmurugan on 2/14/17.
  *
  * Kept for Future Purpose  - when DataScience Team asks for Weighted Average
  *
  */


//Next CLass
@ExpressionDescription(
usage = "_FUNC_(expr) - Returns the Weighted Average value of `expr`.")
case class CalWeightedAverage(value: Expression,weight: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {


  private lazy val resultType = value.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _ => DoubleType
  }


  private lazy val valueDataType = value.dataType match {
    case _ @ DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _ => DoubleType
  }
  private lazy val weightDataType = weight.dataType match {
    case _ @ DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _ => DoubleType
  }

 // private lazy val sum = AttributeReference("sum", sumDataType)()
  private lazy val count = AttributeReference("count", LongType)()

  private lazy val avg = AttributeReference("avg", value.dataType)()

  override val initialValues=  Seq(

  )



  override lazy val updateExpressions = Seq(
    /* sum = */
//    WeightedAverage(
  //    Coalesce(Cast(value, valueDataType) :: Cast(Literal(0), valueDataType) :: Nil),
  //    Coalesce(Cast(weight, weightDataType) :: Cast(Literal(0), weightDataType) :: Nil))

  )



  override lazy val mergeExpressions = Seq(

  )

  override val evaluateExpression: AttributeReference = avg

  override def aggBufferAttributes: Seq[AttributeReference] = ???

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def nullable: Boolean = {
    false
  }

  override def dataType: DataType = value.dataType

  override def children: Seq[Expression] =  Seq(value)
}
