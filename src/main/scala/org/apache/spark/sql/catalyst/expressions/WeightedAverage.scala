/*
 * Copyright (C) 2016-2017 Gannett Corp. All rights reserved.
 *
 */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
  * Created by dmurugan on 2/14/17.
  *
  * Kept for Future Purpose  - when DataScience Team asks for Weighted Average
  *
  *
  */


@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`+`expr2`.",
  extended = """
    Examples:
      > SELECT 1 _FUNC_ 2;
       3
  """)
case class WeightedAverage(left: Expression, right: Expression) extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val value =  input1.asInstanceOf[Double]
    val weight = input1.asInstanceOf[Double]


  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {

    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}
