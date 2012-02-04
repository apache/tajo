package nta.engine.ipc.protocolrecords;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestFragment {
  private Schema schema1;
  private TableMeta meta1;
  
  @Before
  public final void setUp() throws Exception {
    schema1 = new Schema();
    schema1.addColumn("id", DataType.INT);
    schema1.addColumn("name", DataType.STRING);
    meta1 = new TableMetaImpl(schema1, StoreType.CSV);
  }

  @Test
  public final void testGetAndSetFields() {    
    Fragment fragment1 = new Fragment("table1_1", new Path("/table0"), 
        meta1, 0, 500);

    assertEquals("table1_1", fragment1.getId());
    assertEquals(new Path("/table0"), fragment1.getPath());
    assertEquals(meta1.getStoreType(), fragment1.getMeta().getStoreType());
    assertEquals(meta1.getSchema().getColumnNum(), 
        fragment1.getMeta().getSchema().getColumnNum());
    for(int i=0; i < meta1.getSchema().getColumnNum(); i++) {
      assertEquals(meta1.getSchema().getColumn(i).getColumnName(), 
          fragment1.getMeta().getSchema().getColumn(i).getColumnName());
      assertEquals(meta1.getSchema().getColumn(i).getDataType(), 
          fragment1.getMeta().getSchema().getColumn(i).getDataType());
    }
    assertEquals(0, fragment1.getStartOffset());
    assertEquals(500, fragment1.getLength());
  }

  @Test
  public final void testTabletTabletProto() {
    Fragment fragment0 = new Fragment("table1_1", new Path("/table0"), meta1, 0, 500);
    
    Fragment fragment1 = new Fragment(fragment0.getProto());
    assertEquals("table1_1", fragment1.getId());
    assertEquals(new Path("/table0"), fragment1.getPath());
    assertEquals(meta1.getStoreType(), fragment1.getMeta().getStoreType());
    assertEquals(meta1.getSchema().getColumnNum(), 
        fragment1.getMeta().getSchema().getColumnNum());
    for(int i=0; i < meta1.getSchema().getColumnNum(); i++) {
      assertEquals(meta1.getSchema().getColumn(i).getColumnName(), 
          fragment1.getMeta().getSchema().getColumn(i).getColumnName());
      assertEquals(meta1.getSchema().getColumn(i).getDataType(), 
          fragment1.getMeta().getSchema().getColumn(i).getDataType());
    }
    assertEquals(0, fragment1.getStartOffset());
    assertEquals(500, fragment1.getLength());
  }

  @Test
  public final void testCompareTo() {
    final int num = 10;
    Fragment [] tablets = new Fragment[num];
    for (int i = num - 1; i >= 0; i--) {
      tablets[i]
          = new Fragment("tablet1_"+i, new Path("tablet0"), meta1, i * 500, 
              (i+1) * 500);
    }
    
    Arrays.sort(tablets);

    for(int i = 0; i < num; i++) {
      assertEquals("tablet1_"+i, tablets[i].getId());
    }
  }
}
