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
    assertEquals("\\d", res1.get(0).getStatement());

    List<ParsedResult> res2 = SimpleParser.parseScript("\\d;\\c;\\f;");
    assertEquals(3, res2.size());
    assertEquals(ParsedResult.StatementType.META, res2.get(0).getType());
    assertEquals("\\d", res2.get(0).getStatement());
    assertEquals(ParsedResult.StatementType.META, res2.get(1).getType());
    assertEquals("\\c", res2.get(1).getStatement());
    assertEquals(ParsedResult.StatementType.META, res2.get(2).getType());
    assertEquals("\\f", res2.get(2).getStatement());

    List<ParsedResult> res3 = SimpleParser.parseScript("\n\t\t  \\d;\n\\c;\t\t\\f  ;");
    assertEquals(3, res3.size());
    assertEquals(ParsedResult.StatementType.META, res3.get(0).getType());
    assertEquals("\\d", res3.get(0).getStatement());
    assertEquals(ParsedResult.StatementType.META, res3.get(1).getType());
    assertEquals("\\c", res3.get(1).getStatement());
    assertEquals(ParsedResult.StatementType.META, res3.get(2).getType());
    assertEquals("\\f", res3.get(2).getStatement());

    List<ParsedResult> res4 = SimpleParser.parseScript("\\\td;");
    assertEquals(1, res4.size());
    assertEquals("\\\td", res4.get(0).getStatement());
  }

  @Test
  public final void testStatements() throws InvalidStatementException {
    List<ParsedResult> res1 = SimpleParser.parseScript("select * from test;");
    assertEquals(1, res1.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res1.get(0).getType());
    assertEquals("select * from test", res1.get(0).getStatement());

    List<ParsedResult> res2 = SimpleParser.parseScript("select * from test;");
    assertEquals(1, res2.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res2.get(0).getType());
    assertEquals("select * from test", res2.get(0).getStatement());

    List<ParsedResult> res3 = SimpleParser.parseScript("select * from test1;select * from test2;");
    assertEquals(2, res3.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res3.get(0).getType());
    assertEquals("select * from test1", res3.get(0).getStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res3.get(1).getType());
    assertEquals("select * from test2", res3.get(1).getStatement());

    List<ParsedResult> res4 = SimpleParser.parseScript("\t\t\n\rselect * from \ntest1;select * from test2\n;");
    assertEquals(2, res4.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res4.get(0).getType());
    assertEquals("select * from \ntest1", res4.get(0).getStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res4.get(1).getType());
    assertEquals("select * from test2", res4.get(1).getStatement());

    List<ParsedResult> res5 =
        SimpleParser.parseScript("\t\t\n\rselect * from \ntest1;\\d test;select * from test2;\n\nselect 1;");
    assertEquals(4, res5.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res5.get(0).getType());
    assertEquals("select * from \ntest1", res5.get(0).getStatement());
    assertEquals(ParsedResult.StatementType.META, res5.get(1).getType());
    assertEquals("\\d test", res5.get(1).getStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res5.get(2).getType());
    assertEquals("select * from test2", res5.get(2).getStatement());
    assertEquals(ParsedResult.StatementType.STATEMENT, res5.get(3).getType());
    assertEquals("select 1", res5.get(3).getStatement());
  }

  @Test
  public final void testQuoted() throws InvalidStatementException {
    List<ParsedResult> res1 = SimpleParser.parseScript("select '\n;' from test;");
    assertEquals(1, res1.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res1.get(0).getType());
    assertEquals("select '\n;' from test", res1.get(0).getStatement());

    List<ParsedResult> res2 = SimpleParser.parseScript("select 'abc\nbbc\nddf' from test;");
    assertEquals(1, res2.size());
    assertEquals(ParsedResult.StatementType.STATEMENT, res2.get(0).getType());
    assertEquals("select 'abc\nbbc\nddf' from test", res2.get(0).getStatement());

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
    assertEquals(lines[0] + lines[1], result3.get(0).getStatement());
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
    assertEquals("select abc, 'bbc' from test", result2.get(0).getStatement());
    assertEquals("select * from test3", result2.get(1).getStatement());
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
    assertEquals("select abc, 'bbc' from test", result2.get(0).getStatement());
    assertEquals("select * from test3", result2.get(1).getStatement());
  }
}
