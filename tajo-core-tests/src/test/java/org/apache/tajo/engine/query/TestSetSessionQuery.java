/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.query;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestSetSessionQuery extends QueryTestCaseBase {

  public TestSetSessionQuery() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public final void testSetCatalog1() throws Exception {
    executeString("CREATE DATABASE testsetcatalog;").close();
    assertEquals(TajoConstants.DEFAULT_DATABASE_NAME, getClient().getCurrentDatabase());
    executeString("SET CATALOG testsetcatalog").close();
    assertEquals("testsetcatalog", getClient().getCurrentDatabase());
    executeString("SET CATALOG \"default\"").close();
    executeString("DROP DATABASE testsetcatalog;").close();
  }

  @Test
  public final void testSetCatalog2() throws Exception {
    executeString("CREATE DATABASE \"testSetCatalog\";").close();
    assertEquals(TajoConstants.DEFAULT_DATABASE_NAME, getClient().getCurrentDatabase());
    executeString("SET CATALOG \"testSetCatalog\"").close();
    assertEquals("testSetCatalog", getClient().getCurrentDatabase());
    executeString("SET CATALOG \"default\"").close();
    executeString("DROP DATABASE \"testSetCatalog\";").close();
  }

  @Test
  public final void testSetTimezone() throws Exception {
    assertFalse(getClient().existSessionVariable("TIMEZONE"));
    executeString("SET TIME ZONE 'GMT+9'").close();
    assertTrue(getClient().existSessionVariable("TIMEZONE"));
    executeString("SET TIME ZONE to DEFAULT").close();
  }

  @Test
  public final void testSetSession1() throws Exception {
    assertFalse(getClient().existSessionVariable("KEY1"));
    executeString("SET SESSION KEY1 to true").close();
    assertTrue(getClient().existSessionVariable("KEY1"));

    executeString("SET SESSION KEY2 to true").close();
    executeString("SET SESSION KEY2 to 'val1'").close();
    assertTrue(getClient().existSessionVariable("KEY1"));
    assertTrue(getClient().existSessionVariable("KEY2"));
    executeString("RESET KEY1").close();
    executeString("SET SESSION KEY2 to DEFAULT").close();
    assertFalse(getClient().existSessionVariable("KEY2"));
  }
}