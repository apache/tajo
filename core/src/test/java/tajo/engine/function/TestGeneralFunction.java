package tajo.engine.function;

import org.junit.Test;
import tajo.datum.Datum;
import tajo.datum.LongDatum;
import tajo.datum.StringDatum;
import tajo.engine.function.builtin.Date;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.util.Calendar;

import static org.junit.Assert.assertEquals;

/**
 * @author jihoon
 */
public class TestGeneralFunction {

  @Test
  public void testDate() {
    Date date = new Date();
    Tuple tuple = new VTuple(new Datum[] {new StringDatum("25/12/2012 00:00:00")});
    LongDatum unixtime = (LongDatum) date.eval(tuple);
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(unixtime.asLong());
    assertEquals(2012, c.get(Calendar.YEAR));
    assertEquals(11, c.get(Calendar.MONTH));
    assertEquals(25, c.get(Calendar.DAY_OF_MONTH));
    assertEquals(0, c.get(Calendar.HOUR_OF_DAY));
    assertEquals(0, c.get(Calendar.MINUTE));
    assertEquals(0, c.get(Calendar.SECOND));
  }
}
