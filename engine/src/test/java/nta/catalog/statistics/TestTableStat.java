package nta.catalog.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 */
public class TestTableStat {
  @Test
  public final void testTableStat() throws CloneNotSupportedException {
    TableStat stat = new TableStat();
    stat.setNumRows(957685);
    stat.setNumBytes(1023234);
    stat.setNumBlocks(3123);
    stat.setNumPartitions(5);
    stat.setAvgRows(80000);
        
    int numCols = 3;
    ColumnStat [] cols = new ColumnStat[numCols];
    for (int i = 0; i < numCols; i++) {
      cols[i] = new ColumnStat(new Column("col_" + i, CatalogProtos.DataType.LONG));
      cols[i].setNumDistVals(1024 * i);
      cols[i].setNumNulls(100 * i);
      stat.addColumnStat(cols[i]);
    }
    
    assertTrue(957685 == stat.getNumRows());
    assertTrue(1023234 == stat.getNumBytes());
    assertTrue(3123 == stat.getNumBlocks());
    assertTrue(5 == stat.getNumPartitions());
    assertTrue(80000 == stat.getAvgRows());
    assertEquals(3, stat.getColumnStats().size());
    for (int i = 0; i < numCols; i++) {
      assertEquals(cols[i], stat.getColumnStats().get(i));
    }
    
    TableStat stat2 = new TableStat(stat.getProto());
    tableStatEquals(stat, stat2);
    
    TableStat stat3 = (TableStat) stat.clone();
    tableStatEquals(stat, stat3);    
  }
  
  public void tableStatEquals(TableStat s1, TableStat s2) {
    assertEquals(s1.getNumRows(), s2.getNumRows());
    assertEquals(s1.getNumBlocks(), s2.getNumBlocks());
    assertEquals(s1.getNumPartitions(), s2.getNumPartitions());
    assertEquals(s1.getAvgRows(), s2.getAvgRows());
    assertEquals(s1.getColumnStats().size(), s2.getColumnStats().size());
    for (int i = 0; i < s1.getColumnStats().size(); i++) {
      assertEquals(s1.getColumnStats().get(i), s2.getColumnStats().get(i));
    }
  }
}
