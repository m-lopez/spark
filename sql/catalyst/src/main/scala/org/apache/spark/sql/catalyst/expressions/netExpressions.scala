/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines expressions for network type operations.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Defines utility functions for Ipv6 address expressions.
 */
object Ipv6AddressExpressionUtils {
  /** The shape of an IPv6 address type. */
  def ipv6DataType: DataType =
    StructType(
      Array(
        StructField("hiBits", LongType, nullable = false),
        StructField("loBits", LongType, nullable = false)
      )
    )
}

case class Ipv6AddressEqExpression(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      Ipv6AddressExpressionUtils.ipv6DataType
    )
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = "eq"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    (xRow.getLong(0) == yRow.getLong(0)) && (xRow.getLong(1) == yRow.getLong(1))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        ${ev.value} = ($x.getLong(0) == $y.getLong(0)) && ($x.getLong(1) == $y.getLong(1));
      """
    )
}

case class Ipv6AddressNeqExpression(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      Ipv6AddressExpressionUtils.ipv6DataType
    )
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = "neq"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    (xRow.getLong(0) != yRow.getLong(0)) || (xRow.getLong(1) != yRow.getLong(1))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        ${ev.value} = ($x.getLong(0) != $y.getLong(0)) || ($x.getLong(1) != $y.getLong(1));
      """
    )
}

case class Ipv6AddressLtExpression(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      Ipv6AddressExpressionUtils.ipv6DataType
    )
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = "<"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    val xHi = xRow.getLong(0)
    val yHi = yRow.getLong(0)
    if (xHi == yHi) {
      java.lang.Long.compareUnsigned(xRow.getLong(1), yRow.getLong(1)) < 0
    } else {
      java.lang.Long.compareUnsigned(xHi, yHi) < 0
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        long xHi = $x.getLong(0);
        long yHi = $y.getLong(0);
        if (xHi == yHi) {
          ${ev.value} = java.lang.Long.compareUnsigned($x.getLong(1), $y.getLong(1)) < 0;
        } else {
          ${ev.value} = java.lang.Long.compareUnsigned(xHi, yHi) < 0;
        }
      """
    )
}

case class Ipv6AddressLteqExpression(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      Ipv6AddressExpressionUtils.ipv6DataType
    )
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = "<="
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    val xHi = xRow.getLong(0)
    val yHi = yRow.getLong(0)
    if (xHi == yHi) {
      java.lang.Long.compareUnsigned(xRow.getLong(1), yRow.getLong(1)) <= 0
    } else {
      java.lang.Long.compareUnsigned(xHi, yHi) <= 0
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        long xHi = $x.getLong(0);
        long yHi = $y.getLong(0);
        if (xHi == yHi) {
          ${ev.value} = java.lang.Long.compareUnsigned($x.getLong(1), $y.getLong(1)) <= 0;
        } else {
          ${ev.value} = java.lang.Long.compareUnsigned(xHi, yHi) <= 0;
        }
      """
    )
}

case class Ipv6AddressGtExpression(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      Ipv6AddressExpressionUtils.ipv6DataType
    )
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = ">"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    val xHi = xRow.getLong(0)
    val yHi = yRow.getLong(0)
    if (xHi == yHi) {
      java.lang.Long.compareUnsigned(xRow.getLong(1), yRow.getLong(1)) > 0
    } else {
      java.lang.Long.compareUnsigned(xHi, yHi) > 0
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        long xHi = $x.getLong(0);
        long yHi = $y.getLong(0);
        if (xHi == yHi) {
          ${ev.value} = java.lang.Long.compareUnsigned($x.getLong(1), $y.getLong(1)) > 0;
        } else {
          ${ev.value} = java.lang.Long.compareUnsigned(xHi, yHi) > 0;
        }
      """
    )
}

case class Ipv6AddressGteqExpression(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      Ipv6AddressExpressionUtils.ipv6DataType
    )
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = ">="
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    val xHi = xRow.getLong(0)
    val yHi = yRow.getLong(0)
    if (xHi == yHi) {
      java.lang.Long.compareUnsigned(xRow.getLong(1), yRow.getLong(1)) >= 0
    } else {
      java.lang.Long.compareUnsigned(xHi, yHi) >= 0
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        long xHi = $x.getLong(0);
        long yHi = $y.getLong(0);
        if (xHi == yHi) {
          ${ev.value} = java.lang.Long.compareUnsigned($x.getLong(1), $y.getLong(1)) >= 0;
        } else {
          ${ev.value} = java.lang.Long.compareUnsigned(xHi, yHi) >= 0;
        }
      """
    )
}

case class Ipv6AddressJump(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      LongType
    )
  override def dataType: DataType = Ipv6AddressExpressionUtils.ipv6DataType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = "jump"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yLong = y.asInstanceOf[Long]
    // FIXME: This is wrong, but I don't really care right now.
    //        It should consider the carry-bit.
    InternalRow(xRow.getLong(0), xRow.getLong(1) + yLong)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    ctx.addMutableState("Object[]", values, s"$values = null;")
    val code = s"""
      ${leftGen.code}
      ${rightGen.code}
      if (${leftGen.isNull} || ${rightGen.isNull}) {
        ${ev.isNull} = true;
      } else {
        // Initialize the object.
        $values = new Object[2];
        $values[0] = ${leftGen.value};
        $values[1] = ${rightGen.value};
        // Build the object.
        final InternalRow ${ev.value} = new $rowClass($values);
      }
      $values = null;
    """
    ev.copy(code = code)

  }
}

case class Ipv6AddressPrefix(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      Ipv6AddressExpressionUtils.ipv6DataType,
      IntegerType
    )
  override def dataType: DataType = Ipv6AddressExpressionUtils.ipv6DataType

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def toString: String = "prefix"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yInt = y.asInstanceOf[Int]
    // FIXME: This is wrong, but I don't really care right now.
    //        It should consider the carry-bit.
    val xHi = xRow.getLong(0)
    val xLo = xRow.getLong(1)
    if (yInt == 0) {
      InternalRow(0L, 0L)
    } else if (yInt == 64) {
      InternalRow(xRow.getLong(0), 0L)
    } else if (yInt < 64) {
      InternalRow(xRow.getLong(0) & (-1L << (64 - yInt)), 0L)
    } else {
      InternalRow(xRow.getLong(0), xRow.getLong(1) & (-1L << (128 - yInt)))
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val rowClass = classOf[GenericInternalRow].getName
    val values = ctx.freshName("values")
    ctx.addMutableState("Object[]", values, s"$values = null;")
    val code = s"""
      ${leftGen.code}
      ${rightGen.code}
      if (${leftGen.isNull} || ${rightGen.isNull}) {
        ${ev.isNull} = true;
      } else {
        $values = new Object[2];
        if (${rightGen.value} == 0) {
          $values[0] = 0;
          $values[1] = 0;
        } else if (${rightGen.value} == 64) {
          $values[0] = ${leftGen.value}.getLong(0);
          $values[1] = 0;
        } else if (${rightGen.value} < 64)
          $values[0] = ${leftGen.value}.getLong(0) & (-1L << (64 - ${rightGen.value}));
          $values[1] = 0;
        } else {
          $values[0] = ${leftGen.value}.getLong(0);
          $values[1] = ${rightGen.value}.getLong(1) & (-1L << (128 - ${rightGen.value}));
        }
        // Build the object.
        final InternalRow ${ev.value} = new $rowClass($values);
        $values = null;
      }
    """
    ev.copy(code = code)
  }
}
