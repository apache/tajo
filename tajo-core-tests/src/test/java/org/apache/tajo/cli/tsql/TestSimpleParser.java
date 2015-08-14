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

package org.apache.tajo.cli.tsql;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSimpleParser {

  @Test
  public final void testSpecialCases() throws InvalidStatementException {
    List<ParsedResult> res1 = SimpleParser.parseScript("");
    assertEquals(0, res1.size());

    List<ParsedResult> res2 = SimpleParser.parseScript("a");
    assertEquals(1, res2.size());

    List<ParsedResult> res3 = SimpleParser.parseScript("?");
    assertEquals(0, res3.size());

    List<ParsedResult> res4 = SimpleParser.parseScript("\\");
    assertEquals(1, res4.size());
  }

  @Test
  public final void testMetaCommands() throws InvalidStatementException {
    List<ParsedResult> res1 = SimpleParser.parseScript("\\d");
    assertEquals(1, res1.size());
    assertEquals(ParsedResult.StatementType.META, res1.get(0).getType());
    assertEquals("\\d", res1.get(0).getHistoryStatement());

    List<ParsedResult> res2 = SimpleParser.parseScript("\\d;\\c;\\f;");
    assertEquals(3, res2.size());
    assertEquals(ParsedResult.StatementType.META, res2.get(0).getType());
    assertEquals("\\d", res2.get(0).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.META, res2.get(1).getType());
    assertEquals("\\c", res2.get(1).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.META, res2.get(2).getType());
    assertEquals("\\f", res2.get(2).getHistoryStatement());

    List<ParsedResult> res3 = SimpleParser.parseScript("\n\t\t  \\d;\n\\c;\t\t\\f  ;");
    assertEquals(3, res3.size());
    assertEquals(ParsedResult.StatementType.META, res3.get(0).getType());
    assertEquals("\\d", res3.get(0).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.META, res3.get(1).getType());
    assertEquals("\\c", res3.get(1).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.META, res3.get(2).getType());
    assertEquals("\\f", res3.get(2).getHistoryStatement());

    List<ParsedResult> res4 = SimpleParser.parseScript("\\\td;");
    assertEquals(1, res4.size());
    assertEquals("\\\td", res4.get(0).getHistoryStatement());
  }

  @Test
  public final void testParseScript() throws InvalidStatementException {
    List<ParsedResult> res1 = SimpleParser.parseScript("select * from test;");
    assertEquals(1, res1.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res1.get(0).getType());
    assertEquals("select * from test", res1.get(0).getStatement());
    assertEquals("select * from test", res1.get(0).getHistoryStatement());

    List<ParsedResult> res2 = SimpleParser.parseScript("select * from test;");
    assertEquals(1, res2.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res2.get(0).getType());
    assertEquals("select * from test", res2.get(0).getStatement());
    assertEquals("select * from test", res2.get(0).getHistoryStatement());

    List<ParsedResult> res3 = SimpleParser.parseScript("select * from test1;select * from test2;");
    assertEquals(2, res3.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res3.get(0).getType());
    assertEquals("select * from test1", res3.get(0).getStatement());
    assertEquals("select * from test1", res3.get(0).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res3.get(1).getType());
    assertEquals("select * from test2", res3.get(1).getStatement());
    assertEquals("select * from test2", res3.get(1).getHistoryStatement());

    List<ParsedResult> res4 = SimpleParser.parseScript("\t\t\n\rselect * from \ntest1;select * from test2\n;");
    assertEquals(2, res4.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res4.get(0).getType());
    assertEquals("select * from \ntest1", res4.get(0).getStatement());
    assertEquals("select * from test1", res4.get(0).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res4.get(1).getType());
    assertEquals("select * from test2", res4.get(1).getStatement());
    assertEquals("select * from test2", res4.get(1).getHistoryStatement());

    List<ParsedResult> res5 =
        SimpleParser.parseScript("\t\t\n\rselect * from \ntest1;\\d test;select * from test2;\n\nselect 1;");
    assertEquals(4, res5.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res5.get(0).getType());
    assertEquals("select * from \ntest1", res5.get(0).getStatement());
    assertEquals("select * from test1", res5.get(0).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.META, res5.get(1).getType());
    assertEquals("\\d test", res5.get(1).getStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res5.get(2).getType());
    assertEquals("select * from test2", res5.get(2).getStatement());
    assertEquals("select * from test2", res5.get(2).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res5.get(3).getType());
    assertEquals("select 1", res5.get(3).getStatement());
    assertEquals("select 1", res5.get(3).getHistoryStatement());

    List<ParsedResult> res6 =
        SimpleParser.parseScript("select * from \n--test1; select * from test2;\ntest3;");
    assertEquals(1, res6.size());
    assertEquals("select * from test3", res6.get(0).getHistoryStatement());
    assertEquals("select * from \n--test1; select * from test2;\ntest3", res6.get(0).getStatement());

    List<ParsedResult> res7 =
        SimpleParser.parseScript("select * from --test1; select * from test2;\ntest3;");
    assertEquals(1, res7.size());
    assertEquals("select * from test3", res7.get(0).getHistoryStatement());
    assertEquals("select * from --test1; select * from test2;\ntest3", res7.get(0).getStatement());

    List<ParsedResult> res8 = SimpleParser.parseScript("\\d test\nselect * \n--from test1;\nfrom test2;\\d test2;");
    assertEquals(3, res8.size());
    assertEquals(ParsedResult.StatementType.META, res8.get(0).getType());
    assertEquals("\\d test", res8.get(0).getStatement());
    assertEquals("\\d test", res8.get(0).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res8.get(1).getType());
    assertEquals("select * \n--from test1;\nfrom test2", res8.get(1).getStatement());
    assertEquals("select * from test2", res8.get(1).getHistoryStatement());
    assertEquals(ParsedResult.StatementType.META, res8.get(2).getType());
    assertEquals("\\d test2", res8.get(2).getStatement());
    assertEquals("\\d test2", res8.get(2).getHistoryStatement());
  }

  @Test
  public final void testParseLines() throws InvalidStatementException {
    SimpleParser simpleParser = new SimpleParser();
    List<ParsedResult> res1 = null;

    res1 = simpleParser.parseLines("select * from test1; select * from test2;");
    assertEquals(2, res1.size());
    assertEquals("select * from test1", res1.get(0).getStatement());
    assertEquals("select * from test2", res1.get(1).getStatement());
    assertEquals("select * from test1", res1.get(0).getHistoryStatement());
    assertEquals("select * from test2", res1.get(1).getHistoryStatement());

    // select * from
    // test1; select * from test2;
    simpleParser = new SimpleParser();
    res1 = simpleParser.parseLines("select * from ");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("test1; select * from test2;");
    assertEquals(2, res1.size());
    assertEquals("select * from \ntest1", res1.get(0).getStatement());
    assertEquals("select * from test2", res1.get(1).getStatement());
    assertEquals("select * from test1", res1.get(0).getHistoryStatement());
    assertEquals("select * from test2", res1.get(1).getHistoryStatement());

    // select * from
    // --test1; select * from test2;
    // test3;
    simpleParser = new SimpleParser();
    res1 = simpleParser.parseLines("select * from ");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("--test1; select * from test2;");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("test3;");
    assertEquals(1, res1.size());
    assertEquals("select * from test3", res1.get(0).getHistoryStatement());
    assertEquals("select * from \n--test1; select * from test2;\ntest3", res1.get(0).getStatement());

    // select * from
    // test1 --select * from test2;
    // where col1 = '123';
    simpleParser = new SimpleParser();
    res1 = simpleParser.parseLines("select * from ");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("test1 --select * from test2;");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("where col1 = '123';");
    assertEquals(1, res1.size());
    assertEquals("select * from test1 where col1 = '123'", res1.get(0).getHistoryStatement());
    assertEquals("select * from \ntest1 --select * from test2;\nwhere col1 = '123'", res1.get(0).getStatement());

    // Case for sql statement already including '\n'
    // This test is important for tsql because CLI input always has '\n'.
    simpleParser = new SimpleParser();
    res1 = simpleParser.parseLines("select\n");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("*\n");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("from\n");
    assertEquals(0, res1.size());
    res1 = simpleParser.parseLines("test1;\n");
    assertEquals(1, res1.size());
    assertEquals("select\n*\nfrom\ntest1", res1.get(0).getStatement());
    assertEquals("select * from test1", res1.get(0).getHistoryStatement());
  }

  @Test
  public final void testQuoted() throws InvalidStatementException {
    List<ParsedResult> res1 = SimpleParser.parseScript("select '\n;' from test;");
    assertEquals(1, res1.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res1.get(0).getType());
    assertEquals("select '\n;' from test", res1.get(0).getHistoryStatement());
    assertEquals("select '\n;' from test", res1.get(0).getStatement());

    List<ParsedResult> res2 = SimpleParser.parseScript("select 'abc\nbbc\nddf' from test;");
    assertEquals(1, res2.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res2.get(0).getType());
    assertEquals("select 'abc\nbbc\nddf' from test", res2.get(0).getHistoryStatement());
    assertEquals("select 'abc\nbbc\nddf' from test", res2.get(0).getStatement());

    List<ParsedResult> res3 = SimpleParser.parseScript("select '--test', \n'--test2' from test");
    assertEquals(1, res3.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res3.get(0).getType());
    assertEquals("select '--test', '--test2' from test", res3.get(0).getHistoryStatement());
    assertEquals("select '--test', \n'--test2' from test", res3.get(0).getStatement());

    try {
      SimpleParser.parseScript("select 'abc");
      assertTrue(false);
    } catch (InvalidStatementException is) {
      assertTrue(true);
    }
  }

  @Test
  public final void testParseLines1() throws InvalidStatementException {
    String [] lines = {
      "select abc, ",
      "bbc from test"
    };
    SimpleParser parser = new SimpleParser();
    List<ParsedResult> result1 = parser.parseLines(lines[0]);
    assertEquals(0, result1.size());
    List<ParsedResult> result2 = parser.parseLines(lines[1]);
    assertEquals(0, result2.size());
    List<ParsedResult> result3 = parser.EOF();
    assertEquals(1, result3.size());
    assertEquals(lines[0] + lines[1], result3.get(0).getHistoryStatement());
    assertEquals(lines[0] + "\n" + lines[1], result3.get(0).getStatement());
  }

  @Test
  public final void testParseLines2() throws InvalidStatementException {
    String [] lines = {
        "select abc, '",
        "bbc' from test; select * from test3;"
    };
    SimpleParser parser = new SimpleParser();
    List<ParsedResult> result1 = parser.parseLines(lines[0]);
    assertEquals(0, result1.size());
    List<ParsedResult> result2 = parser.parseLines(lines[1]);
    assertEquals(2, result2.size());
    assertEquals("select abc, 'bbc' from test", result2.get(0).getHistoryStatement());
    assertEquals("select * from test3", result2.get(1).getHistoryStatement());
  }

  @Test
  public final void testParseLines3() throws InvalidStatementException {
    String [] lines = {
        "select abc, 'bbc",
        "' from test; select * from test3;"
    };
    SimpleParser parser = new SimpleParser();
    List<ParsedResult> result1 = parser.parseLines(lines[0]);
    assertEquals(0, result1.size());
    List<ParsedResult> result2 = parser.parseLines(lines[1]);
    assertEquals(2, result2.size());
    assertEquals("select abc, 'bbc' from test", result2.get(0).getHistoryStatement());
    assertEquals("select * from test3", result2.get(1).getHistoryStatement());
  }
}
