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

package org.apache.tajo.storage.map;

import org.apache.tajo.storage.vector.SizeOf;

public class MapAddInt8ColInt8ColOp extends MapBinaryOp {
  private static final int LOOP_UNROLLOING_NUM = 4;

  public void map(int vecNum, long resVec, long lhsVec, long rhsVec, long nullVec, long selVec) {

    if (selVec == 0) {
      for (int i = 0; i < vecNum; i++) {
        long lval1 = unsafe.getLong(lhsVec);
        long rval1 = unsafe.getLong(rhsVec);
        unsafe.putLong(resVec, lval1 + rval1);

        resVec += SizeOf.SIZE_OF_LONG;
        rhsVec += SizeOf.SIZE_OF_LONG;
        lhsVec += SizeOf.SIZE_OF_LONG;
      }
    } else {
      for (int rid = 0; rid < vecNum; rid++) {
        int selId = unsafe.getInt(selVec);

        int offset = SizeOf.SIZE_OF_LONG * selId;
        long lval1 = unsafe.getLong(lhsVec + offset);
        long rval1 = unsafe.getLong(rhsVec + offset);

        unsafe.putLong(resVec, lval1 + rval1);

        selVec += SizeOf.SIZE_OF_INT;
        resVec += SizeOf.SIZE_OF_LONG;
      }
    }
  }
}
