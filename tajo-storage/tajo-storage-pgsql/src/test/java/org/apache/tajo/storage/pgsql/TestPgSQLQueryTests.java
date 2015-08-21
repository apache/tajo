package org.apache.tajo.storage.pgsql;

import org.apache.tajo.QueryTestCaseBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPgSQLQueryTests extends QueryTestCaseBase {
  @BeforeClass
  public void setUp() {
    QueryTestCaseBase.testingCluster.getMaster().refresh();
  }

  @Test
  public void testSelectAll() {
    //executeString()
  }
}
