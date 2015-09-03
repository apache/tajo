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

package org.apache.tajo.tuple.memory;

import org.apache.tajo.unit.StorageUnit;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestResizableSpec {

  @Test
  public void testResizableLimit() {
    ResizableLimitSpec limit = new ResizableLimitSpec(10 * StorageUnit.MB, 1000 * StorageUnit.MB, 0.1f, 1.0f);

    long expectedMaxSize = (long) (1000 * StorageUnit.MB + (1000 * StorageUnit.MB * 0.1f));

    assertTrue(limit.limit() == 1000 * StorageUnit.MB + (1000 * StorageUnit.MB * 0.1f));

    assertEquals(20971520, limit.increasedSize(10 * StorageUnit.MB));

    assertEquals(expectedMaxSize, limit.increasedSize(1600 * StorageUnit.MB));

    assertEquals(0.98f, limit.remainRatio(980 * StorageUnit.MB), 0.1);

    assertFalse(limit.canIncrease(limit.limit()));
  }

  @Test
  public void testFixedLimit() {
    FixedSizeLimitSpec limit = new FixedSizeLimitSpec(100 * StorageUnit.MB, 0.0f);

    assertEquals(limit.limit(), 100 * StorageUnit.MB);

    assertEquals(100 * StorageUnit.MB, limit.increasedSize(1000));

    assertEquals(100 * StorageUnit.MB, limit.increasedSize(1600 * StorageUnit.MB));

    assertTrue(0.98f == limit.remainRatio(98 * StorageUnit.MB));

    assertFalse(limit.canIncrease(limit.limit()));
  }
}