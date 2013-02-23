/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.storage.hcfile;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexTest {

  @Test
  public void test() {
    Index<Integer> index = new Index<Integer>();
    index.add(new IndexItem(3, 400));
    index.add(new IndexItem(0, 100));
    index.add(new IndexItem(6, 700));
    index.add(new IndexItem(1, 200));
    index.sort();

    IndexItem<Integer> result = index.get(0);
    assertEquals(0, result.getRid());
    result = index.get(1);
    assertEquals(1, result.getRid());
    result = index.get(2);
    assertEquals(3, result.getRid());
    result = index.get(3);
    assertEquals(6, result.getRid());

    result = index.searchExact(0);
    assertNotNull(result);
    assertEquals(0, result.getRid());
    assertEquals(100, result.getValue().intValue());

    result = index.searchLargestSmallerThan(5);
    assertNotNull(result);
    assertEquals(3, result.getRid());
    assertEquals(400, result.getValue().intValue());

    result = index.searchLargestSmallerThan(3);
    assertNotNull(result);
    assertEquals(3, result.getRid());
    assertEquals(400, result.getValue().intValue());

    result = index.searchSmallestLargerThan(5);
    assertNotNull(result);
    assertEquals(6, result.getRid());
    assertEquals(700, result.getValue().intValue());
  }
}
