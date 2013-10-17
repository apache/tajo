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

package org.apache.tajo.engine.function;


import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.ExprTestBase;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.common.TajoDataTypes.Type.FLOAT8;
import static org.apache.tajo.common.TajoDataTypes.Type.INT4;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class TestStringOperatorsAndFunctions extends ExprTestBase {

  @Test
  public void testConcatenateOnLiteral() throws IOException {
    testSimpleEval("select ('abc' || 'def') col1 ", new String[]{"abcdef"});
    testSimpleEval("select 'abc' || 'def' as col1 ", new String[]{"abcdef"});
    testSimpleEval("select 1 || 'def' as col1 ", new String[]{"1def"});
    testSimpleEval("select 'abc' || 2 as col1 ", new String[]{"abc2"});
  }

  @Test
  public void testConcatenateOnExpressions() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", INT4);
    schema.addColumn("col3", FLOAT8);

    testSimpleEval("select (1+3) || 2 as col1 ", new String[]{"42"});

    testEval(schema, "table1", "abc,2,3.14", "select col1 || col2 || col3 from table1", new String[]{"abc23.14"});
    testEval(schema, "table1", "abc,2,3.14", "select col1 || '---' || col3 from table1", new String[]{"abc---3.14"});
  }

  @Test
  public void testLTrim() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);

    testSimpleEval("select ltrim(' trim') ", new String[]{"trim"});
    testSimpleEval("select ltrim('xxtrim', 'xx') ", new String[]{"trim"});

    testSimpleEval("select trim(leading 'xx' from 'xxtrim') ", new String[]{"trim"});
    testSimpleEval("select trim(leading from '  trim') ", new String[]{"trim"});
    testSimpleEval("select trim('  trim') ", new String[]{"trim"});

    testEval(schema, "table1", "  trim,abc", "select ltrim(col1) from table1", new String[]{"trim"});
    testEval(schema, "table1", "xxtrim,abc", "select ltrim(col1, 'xx') from table1", new String[]{"trim"});
    testEval(schema, "table1", "xxtrim,abc", "select trim(leading 'xx' from col1) from table1", new String[]{"trim"});

    testEval(schema, "table1", "  trim,  abc", "select ltrim(col1) || ltrim(col2) from table1",
        new String[]{"trimabc"});
  }

  @Test
  public void testRTrim() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);

    testSimpleEval("select rtrim('trim ') ", new String[]{"trim"});
    testSimpleEval("select rtrim('trimxx', 'xx') ", new String[]{"trim"});

    testSimpleEval("select trim(trailing 'xx' from 'trimxx') ", new String[]{"trim"});
    testSimpleEval("select trim(trailing from 'trim  ') ", new String[]{"trim"});
    testSimpleEval("select trim('trim  ') ", new String[]{"trim"});

    testEval(schema, "table1", "trim  ,abc", "select rtrim(col1) from table1", new String[]{"trim"});
    testEval(schema, "table1", "trimxx,abc", "select rtrim(col1, 'xx') from table1", new String[]{"trim"});
    testEval(schema, "table1", "trimxx,abc", "select trim(trailing 'xx' from col1) from table1", new String[]{"trim"});

    testEval(schema, "table1", "trim  ,abc  ", "select rtrim(col1) || rtrim(col2) from table1",
        new String[]{"trimabc"});
  }

  @Test
  public void testTrim() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);

    testSimpleEval("select trim(' trim ') ", new String[]{"trim"});
    testSimpleEval("select btrim('xxtrimxx', 'xx') ", new String[]{"trim"});

    testSimpleEval("select trim(both 'xx' from 'xxtrimxx') ", new String[]{"trim"});
    testSimpleEval("select trim(both from '  trim  ') ", new String[]{"trim"});
    testSimpleEval("select trim('  trim  ') ", new String[]{"trim"});

    testEval(schema, "table1", "  trim  ,abc", "select trim(col1) from table1", new String[]{"trim"});
    testEval(schema, "table1", "xxtrimxx,abc", "select trim(col1, 'xx') from table1", new String[]{"trim"});
    testEval(schema, "table1", "xxtrimxx,abc", "select trim(both 'xx' from col1) from table1", new String[]{"trim"});

    testEval(schema, "table1", "  trim  ,xxabcxx", "select trim(col1) || trim(col2,'xx') from table1",
        new String[]{"trimabc"});
  }

  @Test
  public void testRegexReplace() throws IOException {
    testSimpleEval("select regexp_replace('abcdef','bc','--') as col1 ", new String[]{"a--def"});

    // TODO - The following tests require the resolution of TAJO-215 (https://issues.apache.org/jira/browse/TAJO-215)
    // null test
    // testSimpleEval("select regexp_replace(null, 'bc', '--') as col1 ", new String[]{""});
    // testSimpleEval("select regexp_replace('abcdef', null, '--') as col1 ", new String[]{""});
    // testSimpleEval("select regexp_replace('abcdef','bc', null) as col1 ", new String[]{""});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);

    // find matches and replace from column values
    testEval(schema, "table1", "------,(^--|--$),ab", "select regexp_replace(col1, col2, col3) as str from table1",
        new String[]{"ab--ab"});

    // null test from a table
    testEval(schema, "table1", ",(^--|--$),ab", "select regexp_replace(col1, col2, col3) as str from table1",
        new String[]{""});
    testEval(schema, "table1", "------,(^--|--$),", "select regexp_replace(col1, col2, col3) as str from table1",
        new String[]{""});
  }

  @Test
  public void testUpper() throws IOException {
    testSimpleEval("select upper('abcdef') as col1 ", new String[]{"ABCDEF"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "abc,efg,3.14", "select upper(col1), upper(col2) from table1",
        new String[]{"ABC", "EFG"});
    testEval(schema, "table1", "abc,efg,3.14", "select upper(col1) || upper(col2) from table1", new String[]{"ABCEFG"});
  }

  @Test
  public void testLower() throws IOException {
    testSimpleEval("select lower('ABCdEF') as col1 ", new String[]{"abcdef"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "ABC,DEF,3.14", "select lower(col1), lower(col2) from table1",
        new String[]{"abc", "def"});
    testEval(schema, "table1", "ABC,DEF,3.14", "select lower(col1) || lower(col2) from table1", new String[]{"abcdef"});
  }

  @Test
  public void testCharLength() throws IOException {
    testSimpleEval("select char_length('123456') as col1 ", new String[]{"6"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "ABC,DEF,3.14", "select character_length(lower(col1) || lower(col2)) from table1",
        new String[]{"6"});
  }
}
