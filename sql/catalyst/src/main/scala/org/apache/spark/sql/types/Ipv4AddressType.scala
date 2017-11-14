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

package org.apache.spark.sql.types

import scala.math.{Integral, Numeric, Ordering}
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.Ipv4Address

/**
 * The data type representing `Int` values. Please use the singleton `DataTypes.IntegerType`.
 *
 * @since 2.4.0
 */
class Ipv4AddressType private() extends IntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "IntegerType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Int
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val numeric = implicitly[Numeric[Int]]
  private[sql] val integral = implicitly[Integral[Int]]
  // private[sql] val ordering = implicitly[Ordering[InternalType]]
  private[sql] val ordering = Ipv4AddressUtils.unsignedOrder

  /**
   * The default size of a value of the IntegerType is 4 bytes.
   */
  override def defaultSize: Int = 4

  override def simpleString: String = "ipv4 address"

  private[spark] override def asNullable: Ipv4AddressType = this
}

/**
 * @since 1.3.0
 */
case object Ipv4AddressType extends Ipv4AddressType

object Ipv4AddressUtils {

  val unsignedOrder: Ordering[Int] = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int =
      java.lang.Integer.compareUnsigned(x, y)
  }

  // An explicit constructor from [[Int]].
  def fromInt(n: Int): Ipv4Address = new Ipv4Address(n)

  // An explicit destructor to [[Int]].
  def toInt(n: Ipv4Address): Int = n.address
}
