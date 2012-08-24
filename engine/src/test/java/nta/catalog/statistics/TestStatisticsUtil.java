package nta.catalog.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import nta.engine.TCommonProtos.StatType;

import org.junit.Test;

import com.google.common.collect.Lists;

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
