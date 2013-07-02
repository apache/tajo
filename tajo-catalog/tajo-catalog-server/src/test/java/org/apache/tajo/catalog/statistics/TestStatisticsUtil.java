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

package org.apache.tajo.catalog.statistics;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.apache.tajo.catalog.proto.CatalogProtos.StatType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestStatisticsUtil {
  @Test
  public void testAggregate() throws CloneNotSupportedException {
    Stat stat = new Stat(StatType.TABLE_NUM_ROWS);
    stat.incrementBy(100); // 100
    assertEquals(100, stat.getValue());
    
    Stat stat2 = (Stat) stat.clone();
    stat2.incrementBy(100); // 200
    assertEquals(200, stat2.getValue());
    
    Stat stat3 = new Stat(StatType.TABLE_NUM_BLOCKS);
    stat3.incrementBy(50); // 50
    assertEquals(50, stat3.getValue());
    
    StatSet group = new StatSet();
    group.putStat(stat); // num of rows - 100 
    group.putStat(stat2); // num of rows - 200
    group.putStat(stat3); // num of blocks - 50
    
    // One group has 300 rows and 50 blocks, and it is cloned.
    StatSet group2 = (StatSet) group.clone();
    group2.getStat(StatType.TABLE_NUM_ROWS).incrementBy(100); // plus 100
    
    // expected that num of rows = 200 * 2 + 100, num of blocks = 50 * 2 
    StatSet agg = StatisticsUtil.aggregateStatSet(
        Lists.newArrayList(group, group2));
    assertEquals(500, agg.getStat(StatType.TABLE_NUM_ROWS).getValue());
    assertEquals(100, agg.getStat(StatType.TABLE_NUM_BLOCKS).getValue());
  }

  @Test
  public void testEmptyAggregate() {
    TableStat stat1 = new TableStat();
    TableStat stat2 = new TableStat();
    TableStat stat3 = new TableStat();

    assertNotNull(StatisticsUtil.aggregateTableStat(
        Lists.newArrayList(stat1, stat2, stat3)));
  }
}
