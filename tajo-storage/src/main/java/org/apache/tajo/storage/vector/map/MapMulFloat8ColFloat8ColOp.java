/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.vector.map;

import org.apache.tajo.storage.vector.SizeOf;
import org.apache.tajo.storage.vector.UnsafeUtil;
import sun.misc.Unsafe;

public class MapMulFloat8ColFloat8ColOp {
  static Unsafe unsafe = UnsafeUtil.unsafe;

  public static void map(int vecNum, long result, long lhsPtr, long rhsPtr, int [] selVec) {

    if (selVec == null) {
      for (int i = 0; i < vecNum; i++) {
        double lhsValue = unsafe.getDouble(lhsPtr);
        double rhsValue = unsafe.getDouble(rhsPtr);
        unsafe.putDouble(result, lhsValue - rhsValue);

        result += SizeOf.SIZE_OF_LONG;
        lhsPtr += SizeOf.SIZE_OF_LONG;
        rhsPtr += SizeOf.SIZE_OF_LONG;
      }
    } else {
      long offset;

      for (int i = 0; i < vecNum; i++) {
        offset = (selVec[i] * SizeOf.SIZE_OF_LONG);
        double lhsValue = unsafe.getDouble(lhsPtr + offset);
        double rhsValue = unsafe.getDouble(rhsPtr + offset);
        unsafe.putDouble(result, lhsValue * rhsValue);
        result += SizeOf.SIZE_OF_LONG;
      }
    }
  }
}
