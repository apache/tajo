/**
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

package org.apache.tajo.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.conf.TajoConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestTajoCli {
  @Test
  public void testParseParam() throws Exception {
    String[] args = new String[]{"-f", "test.sql", "--param", "test1=10", "--param", "test2=20"};

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(TajoCli.options, args);

    String fileName = cmd.getOptionValue("f");
    assertNotNull(fileName);
    assertEquals(args[1], fileName);

    String[] paramValues = cmd.getOptionValues("param");

    assertNotNull(paramValues);
    assertEquals(2, paramValues.length);

    assertEquals("test1=10", paramValues[0]);
    assertEquals("test2=20", paramValues[1]);
  }

  @Test
  public void testParseConf() throws Exception {
    String[] args = new String[]{"--conf", "tajo.cli.print.pause=false",
        "--conf", "tajo.executor.join.inner.in-memory-table-num=256"};

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(TajoCli.options, args);
    String[] confValues = cmd.getOptionValues("conf");

    assertNotNull(confValues);
    assertEquals(2, confValues.length);

    assertEquals("tajo.cli.print.pause=false", confValues[0]);
    assertEquals("tajo.executor.join.inner.in-memory-table-num=256", confValues[1]);

    TajoConf tajoConf = TpchTestBase.getInstance().getTestingCluster().getConfiguration();

    TajoCli shell = new TajoCli(tajoConf, args, System.in, System.out);
    assertEquals("false", shell.getContext().getConf().get("tajo.cli.print.pause"));
    assertEquals("256", shell.getContext().getConf().get("tajo.executor.join.inner.in-memory-table-num"));
  }

  @Test
  public void testReplaceParam() throws Exception {
    String sql = "select * from lineitem where l_tax > ${tax} and l_returnflag > '${returnflag}'";
    String[] params = new String[]{"tax=10", "returnflag=A"};


    String expected = "select * from lineitem where l_tax > 10 and l_returnflag > 'A'";
    assertEquals(expected, TajoCli.replaceParam(sql, params));
  }
}
