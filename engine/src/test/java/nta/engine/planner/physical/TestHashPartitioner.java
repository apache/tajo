package nta.engine.planner.physical;

import static org.junit.Assert.assertEquals;
import nta.datum.DatumFactory;
import nta.storage.Tuple;
import nta.storage.VTuple;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    tuple1.put(
        DatumFactory.createInt(1),
        DatumFactory.createInt(2),
        DatumFactory.createInt(3)
        );
    Tuple tuple2 = new VTuple(3);    
    tuple2.put(
        DatumFactory.createInt(1),
        DatumFactory.createInt(2),
        DatumFactory.createInt(4)
        );
    Tuple tuple3 = new VTuple(3);    
    tuple3.put(
        DatumFactory.createInt(1),
        DatumFactory.createInt(2),
        DatumFactory.createInt(5)
        );
    Tuple tuple4 = new VTuple(3);    
    tuple4.put(
        DatumFactory.createInt(2),
        DatumFactory.createInt(2),
        DatumFactory.createInt(3)
        );
    Tuple tuple5 = new VTuple(3);    
    tuple5.put(
        DatumFactory.createInt(2),
        DatumFactory.createInt(2),
        DatumFactory.createInt(4)
        );
    
    int [] partKeys = {0,1};
    Partitioner p = new HashPartitioner(partKeys, 2);
    
    int part1 = p.getPartition(tuple1);
    assertEquals(part1, p.getPartition(tuple2));
    assertEquals(part1, p.getPartition(tuple3));
    
    int part2 = p.getPartition(tuple4);
    assertEquals(part2, p.getPartition(tuple5));    
  }
}
