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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

case class IPv4BitOr(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(Ipv4AddressType, Ipv4AddressType)

  override def dataType: DataType = Ipv4AddressType

  protected override def nullSafeEval(x: Any, y: Any): Any = {
    x.asInstanceOf[Ipv4AddressType.InternalType] | y.asInstanceOf[Ipv4AddressType.InternalType]
  }

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode
  ): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) => { s"${ev.value} = $x | $y;" })
}

case class IPv4BitAnd(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[DataType] = Seq(Ipv4AddressType, Ipv4AddressType)

  override def dataType: DataType = Ipv4AddressType

  protected override def nullSafeEval(x: Any, y: Any): Any = {
    x.asInstanceOf[Ipv4AddressType.InternalType] & y.asInstanceOf[Ipv4AddressType.InternalType]
  }

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode
  ): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) => { s"${ev.value} = $x & $y;" })
}

case class IPv4Distance(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(Ipv4AddressType, Ipv4AddressType)

  override def dataType: DataType = LongType

  protected override def nullSafeEval(x: Any, y: Any): Any = {
    val xLong = x.asInstanceOf[Ipv4AddressType.InternalType].toLong
    val yLong = y.asInstanceOf[Ipv4AddressType.InternalType].toLong
    yLong - xLong
  }

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode
  ): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) => {
      s"${ev.value} = (long)($y) - (long)($x);"
    })
}

case class IPv4Jump(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(Ipv4AddressType, IntegerType)

  override def dataType: DataType = Ipv4AddressType

  protected override def nullSafeEval(x: Any, y: Any): Any = {
    x.asInstanceOf[Ipv4AddressType.InternalType] +
      y.asInstanceOf[IntegerType.InternalType]
  }

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode
  ): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) => { s"${ev.value} = $x + $y;" })
}

/** @todo What is the proper way to restrict the right operand to [0, 32]?
 */
case class Ipv4MaskByPrefixLength(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(Ipv4AddressType, IntegerType)

  override def dataType: DataType = Ipv4AddressType

  protected override def nullSafeEval(x: Any, y: Any): Any = {
    val yInt = y.asInstanceOf[IntegerType.InternalType]
    if (yInt == 0) {
      0
    } else if (0 < yInt && yInt <= 32) {
      x.asInstanceOf[Ipv4AddressType.InternalType] & (-1 << (32 - yInt))
    } else {
      null
    }
  }

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode
  ): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) => {
      s"""
        if ($y == 0) {
          ${ev.value} = 0;
        } else if (0 < $y && $y <= 32) {
          ${ev.value} = (-1 << (32 - $y));
        } else {
          ${ev.isNull} = true;
        }
      """
    })
}
