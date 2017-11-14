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
import org.apache.spark.sql.types.{DataType, Ipv4AddressType}

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
  ): ExprCode = {
    nullSafeCodeGen(ctx, ev, (x, y) => {
      s"""
        ${ev.value} = $x | $y;
        ${ev.isNull} = ${ev.value} == null;
       """
    })
  }
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
  ): ExprCode = {
    nullSafeCodeGen(ctx, ev, (x, y) => {
      s"""
        ${ev.value} = $x & $y;
        ${ev.isNull} = ${ev.value} == null;
       """
    })
  }
}
