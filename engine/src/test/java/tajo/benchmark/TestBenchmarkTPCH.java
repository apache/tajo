package tajo.benchmark;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBenchmarkTPCH {

  @Before
  public void setUp() throws Exception {
    // nothing
  }

  @After
  public void tearDown() throws Exception {
    // nothing
  }

  @Test
  public final void testQueries() {
    for (int i = 0; i < BenchmarkTPCH.Queries.length; i++) {
      assertNotNull(BenchmarkTPCH.Queries[i]);
    }
  }
}
