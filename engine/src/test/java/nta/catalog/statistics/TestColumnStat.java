package nta.catalog.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestColumnStat {

  @Test
  public final void testColumnStat() {
    ColumnStat stat = new ColumnStat();
    stat.setNumDistVals(1000);
    stat.setNumNulls(999);
    
    assertTrue(1000 == stat.getNumDistValues());
    assertTrue(999 == stat.getNumNulls());
    
    ColumnStat stat2 = new ColumnStat(stat.getProto());
    assertTrue(1000 == stat2.getNumDistValues());
    assertTrue(999 == stat2.getNumNulls());
  }

  @Test
  public final void testEqualsObject() {
    ColumnStat stat = new ColumnStat();
    stat.setNumDistVals(1000);
    stat.setNumNulls(999);
    
    ColumnStat stat2 = new ColumnStat(stat.getProto());
    assertEquals(stat, stat2);
  }

  @Test
  public final void testClone() throws CloneNotSupportedException {
    ColumnStat stat = new ColumnStat();
    stat.setNumDistVals(1000);
    stat.setNumNulls(999);
    
    ColumnStat stat2 = (ColumnStat) stat.clone();
    assertEquals(stat, stat2);
  }
}
