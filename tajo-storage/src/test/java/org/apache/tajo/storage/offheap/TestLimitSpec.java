/*
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

package org.apache.tajo.storage.offheap;

import org.apache.tajo.unit.StorageUnit;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLimitSpec {

  @Test
  public void testLimitSpec() {
    ResizableSpec limit = new ResizableSpec(10 * StorageUnit.MB, 1000 * StorageUnit.MB, 0.5f);

    assertEquals(limit.limit(), 1000 * StorageUnit.MB);

    assertTrue(limit.increasedSize(1000) == 1500);

    assertEquals(limit.increasedSize(1600 * StorageUnit.MB), 1000 * StorageUnit.MB);

    assertTrue(0.98f == limit.remainRatio(980 * StorageUnit.MB));

    assertFalse(limit.canIncrease(limit.limit()));
  }
}