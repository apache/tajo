/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.catalog.statistics;

import org.junit.Test;
import tajo.catalog.proto.CatalogProtos.StatType;

import static org.junit.Assert.assertEquals;

public class TestStatSet {
  @Test
  public final void testStatGroup() throws CloneNotSupportedException {
    Stat stat = new Stat(StatType.TABLE_NUM_ROWS);
    stat.increment();
    stat.incrementBy(100);
    assertEquals(101, stat.getValue());
    
    Stat stat2 = (Stat) stat.clone();
    assertEquals(stat, stat2);
    
    Stat stat3 = new Stat(StatType.TABLE_NUM_BLOCKS);
    stat3.increment();
    stat3.increment();
    stat3.increment();
    stat3.subtract();
    stat3.subtractBy(2);
    stat3.increment();
    assertEquals(1, stat3.getValue());
    
    StatSet group = new StatSet();
    group.putStat(stat);
    group.putStat(stat3);
    
    assertEquals(2, group.getAllStats().size());
    assertEquals(stat, group.getStat(StatType.TABLE_NUM_ROWS));
    assertEquals(101, group.getStat(StatType.TABLE_NUM_ROWS).getValue());
    assertEquals(1, group.getStat(StatType.TABLE_NUM_BLOCKS).getValue());
    
    StatSet group2 = new StatSet(group.getProto());
    assertEquals(2, group2.getAllStats().size());
    assertEquals(stat, group2.getStat(StatType.TABLE_NUM_ROWS));
    assertEquals(101, group2.getStat(StatType.TABLE_NUM_ROWS).getValue());
    assertEquals(1, group2.getStat(StatType.TABLE_NUM_BLOCKS).getValue());
    
    StatSet group3 = (StatSet) group.clone();
    assertEquals(2, group3.getAllStats().size());
    assertEquals(stat, group3.getStat(StatType.TABLE_NUM_ROWS));
    assertEquals(101, group3.getStat(StatType.TABLE_NUM_ROWS).getValue());
    assertEquals(1, group3.getStat(StatType.TABLE_NUM_BLOCKS).getValue());
  }
}
