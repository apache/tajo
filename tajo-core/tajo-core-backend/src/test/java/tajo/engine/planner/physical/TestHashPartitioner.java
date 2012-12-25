package tajo.engine.planner.physical;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import static org.junit.Assert.assertEquals;

public class TestHashPartitioner {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public final void testGetPartition() {   
    Tuple tuple1 = new VTuple(3);    
    tuple1.put(new Datum[] {
        DatumFactory.createInt(1),
        DatumFactory.createInt(2),
        DatumFactory.createInt(3)
    });
    Tuple tuple2 = new VTuple(3);    
    tuple2.put(new Datum[] {
        DatumFactory.createInt(1),
        DatumFactory.createInt(2),
        DatumFactory.createInt(4)
    });
    Tuple tuple3 = new VTuple(3);    
    tuple3.put(new Datum[] {
        DatumFactory.createInt(1),
        DatumFactory.createInt(2),
        DatumFactory.createInt(5)
    });
    Tuple tuple4 = new VTuple(3);    
    tuple4.put(new Datum[] {
        DatumFactory.createInt(2),
        DatumFactory.createInt(2),
        DatumFactory.createInt(3)
    });
    Tuple tuple5 = new VTuple(3);    
    tuple5.put(new Datum[] {
        DatumFactory.createInt(2),
        DatumFactory.createInt(2),
        DatumFactory.createInt(4)
    });
    
    int [] partKeys = {0,1};
    Partitioner p = new HashPartitioner(partKeys, 2);
    
    int part1 = p.getPartition(tuple1);
    assertEquals(part1, p.getPartition(tuple2));
    assertEquals(part1, p.getPartition(tuple3));
    
    int part2 = p.getPartition(tuple4);
    assertEquals(part2, p.getPartition(tuple5));    
  }
}
