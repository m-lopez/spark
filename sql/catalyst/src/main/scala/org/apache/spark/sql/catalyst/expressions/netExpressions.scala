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
