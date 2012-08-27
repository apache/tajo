package tajo.catalog.statistics;

import org.junit.Test;
import tajo.engine.TCommonProtos.StatType;

import static org.junit.Assert.assertEquals;

/**
 * @author Hyunsik Choi
 */
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
