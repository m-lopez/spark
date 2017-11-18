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

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.Ipv6

/**
 * The dataType representing IPv6 addresses.
 *
 * @since 2.4.0
 */
@InterfaceStability.Stable
class Ipv6AddressType private() extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "LongType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  private[sql] type InternalType = Ipv6
  @transient private[sql] lazy val tag = typeTag[InternalType]
  private[sql] val ordering = Ipv6AddressUtils.ipv6Ordering

  /**
   * The default size of a value of the Ipv6AddressType is 16 bytes.
   */
  override def defaultSize: Int = 16

  override def simpleString: String = "ipv6-address"

  private[spark] override def asNullable: Ipv6AddressType = this
}

/**
 * @since 2.4.0
 */
@InterfaceStability.Stable
case object Ipv6AddressType extends Ipv6AddressType

object Ipv6AddressUtils {

  val ipv6Ordering = new Ordering[Ipv6] {
    override def compare(x: Ipv6, y: Ipv6): Int = {
      if (x.hi == y.hi) {
        java.lang.Long.compareUnsigned(x.lo, y.lo)
      } else {
        java.lang.Long.compareUnsigned(x.hi, y.hi)
      }
    }
  }
}
