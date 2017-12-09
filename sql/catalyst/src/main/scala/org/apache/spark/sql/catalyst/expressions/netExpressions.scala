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

object Ipv4Utils {
  def Ipv4VlsnDataType: DataType =
    StructType(
      Array(
        StructField("address", IntegerType, nullable = false),
        StructField("prefix_length", ByteType, nullable = false)
      )
    )

  def toMask(prefix_length: Byte): Int =
    if (prefix_length == 32) {
      -1
    } else if (prefix_length == 0) {
      0
    } else {
      -1 << (32 - prefix_length)
    }

  def isContainedIn(xIp: Int, xPre: Byte, yIp: Int, yPre: Byte): Boolean = {
    if (xPre >= yPre) {
      (xIp & toMask(yPre)) == yIp
    } else {
      false
    }
  }

  def isStrictlyContainedIn(xIp: Int, xPre: Byte, yIp: Int, yPre: Byte): Boolean = {
    val notSame = !((xIp == yIp) && (xPre == yPre))
    notSame && isContainedIn(xIp, xPre, yIp, yPre)
  }
}

case class Ipv4SubnetEq(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(Ipv4Utils.Ipv4VlsnDataType, Ipv4Utils.Ipv4VlsnDataType)
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def toString: String = "eq"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    (xRow.getInt(0) == yRow.getInt(0)) && (xRow.getByte(1) == yRow.getByte(1))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        ${ev.value} = ($x.getInt(0) == $y.getInt(0)) && ($x.getByte(1) == $y.getByte(1));
      """
    )
}

case class Ipv4SubnetNeq(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(Ipv4Utils.Ipv4VlsnDataType, Ipv4Utils.Ipv4VlsnDataType)
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def toString: String = "neq"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    (xRow.getInt(0) != yRow.getInt(0)) || (xRow.getByte(1) != yRow.getByte(1))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        ${ev.value} = ($x.getInt(0) != $y.getInt(0)) || ($x.getByte(1) != $y.getByte(1));
      """
    )
}

case class Ipv4SubnetIsContainedIn(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(Ipv4Utils.Ipv4VlsnDataType, Ipv4Utils.Ipv4VlsnDataType)
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def toString: String = "contains"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    Ipv4Utils.isContainedIn(xRow.getInt(0), xRow.getByte(1), yRow.getInt(0), yRow.getByte(1))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        byte xPre = $x.getByte(1);
        byte yPre = $y.getByte(1);
        if (xPre >= yPre) {
          int mask = -1;  // The case where yPre is 32.
          if (yPre == 0) {
            mask = 0;
          } else {
            mask = -1 << (32 - yPre);
          }
          int xIp = $x.getInt(0);
          int yIp = $y.getInt(0);
          ${ev.value} = (xIp & mask) == yIp;
        } else {
          ${ev.value} = false;
        }
      """
    )
}

case class Ipv4SubnetIsStrictlyContainedIn(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(Ipv4Utils.Ipv4VlsnDataType, Ipv4Utils.Ipv4VlsnDataType)
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def toString: String = "strictly_contains"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[InternalRow]
    val yRow = y.asInstanceOf[InternalRow]
    Ipv4Utils.isStrictlyContainedIn(
      xRow.getInt(0), xRow.getByte(1),
      yRow.getInt(0), yRow.getByte(1)
    )
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        byte xPre = $x.getByte(1);
        byte yPre = $y.getByte(1);
        if (xPre >= yPre) {
          int mask = -1;  // The case where yPre is 32.
          if (yPre == 0) {
            mask = 0;
          } else {
            mask = -1 << (32 - yPre);
          }
          int xIp = $x.getInt(0);
          int yIp = $y.getInt(0);
          bool notSame = !(xPre == yPre && xIp == yIp);
          ${ev.value} = notSame && ((xIp & mask) == yIp);
        } else {
          ${ev.value} = false;
        }
      """
    )
}

case class Ipv4SubnetAddressIsContainedIn(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(Ipv4AddressType, Ipv4Utils.Ipv4VlsnDataType)
  override def dataType: DataType = BooleanType

  override def foldable: Boolean = false
  override def toString: String = "address_is_contained_in"
  override def prettyName: String = toString

  override def nullSafeEval(x: Any, y: Any): Any = {
    val xRow = x.asInstanceOf[Ipv4AddressType.InternalType]
    val yRow = y.asInstanceOf[InternalRow]
    Ipv4Utils.isContainedIn(
      xRow, 32.toByte,
      yRow.getInt(0), yRow.getByte(1)
    )
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    nullSafeCodeGen(ctx, ev, (x, y) =>
      s"""
        byte yPre = $y.getByte(1);
        int mask = -1;  // The case where yPre is 32.
        if (yPre == 0) {
          mask = 0;
        } else {
          mask = -1 << (32 - yPre);
        }
        int yIp = $y.getInt(0);
        ${ev.value} = ($x & mask) == yIp;
      """
    )
}
