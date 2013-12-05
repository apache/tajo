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
  public void testFunctionCallIngoreCases() throws IOException {
    testSimpleEval("select ltrim(' trim') ", new String[]{"trim"});
    testSimpleEval("select LTRIM(' trim') ", new String[]{"trim"});
    testSimpleEval("select lTRim(' trim') ", new String[]{"trim"});
    testSimpleEval("select ltrIM(' trim') ", new String[]{"trim"});
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
  public void testLeft() throws IOException {
    testSimpleEval("select left('abcdef',1) as col1 ", new String[]{"a"});
    testSimpleEval("select left('abcdef',2) as col1 ", new String[]{"ab"});
    testSimpleEval("select left('abcdef',3) as col1 ", new String[]{"abc"});
    testSimpleEval("select left('abcdef',4) as col1 ", new String[]{"abcd"});
    testSimpleEval("select left('abcdef',5) as col1 ", new String[]{"abcde"});
    testSimpleEval("select left('abcdef',6) as col1 ", new String[]{"abcdef"});
    testSimpleEval("select left('abcdef',7) as col1 ", new String[]{"abcdef"});
//    testSimpleEval("select from_left('abcdef',-1) as col1 ", new String[]{"abcde"});
//    testSimpleEval("select from_left('abcdef',-2) as col1 ", new String[]{"abcd"});
//    testSimpleEval("select from_left('abcdef',-3) as col1 ", new String[]{"abc"});
//    testSimpleEval("select from_left('abcdef',-4) as col1 ", new String[]{"ab"});
//    testSimpleEval("select from_left('abcdef',-5) as col1 ", new String[]{"a"});
//    testSimpleEval("select from_left('abcdef',-6) as col1 ", new String[]{""});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", INT4);
    schema.addColumn("col3", TEXT);

    // for null tests
    testEval(schema, "table1", ",1,ghi", "select left(col1,1) is null from table1", new String[]{"t"});
    testEval(schema, "table1", "abc,,ghi", "select left(col1,col2) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "abc,1,ghi", "select left(col1,1) || left(col3,3) from table1", new String[]{"aghi"});
  }

  @Test
  public void testRight() throws IOException {
    testSimpleEval("select right('abcdef',1) as col1 ", new String[]{"f"});
    testSimpleEval("select right('abcdef',2) as col1 ", new String[]{"ef"});
    testSimpleEval("select right('abcdef',3) as col1 ", new String[]{"def"});
    testSimpleEval("select right('abcdef',4) as col1 ", new String[]{"cdef"});
    testSimpleEval("select right('abcdef',5) as col1 ", new String[]{"bcdef"});
    testSimpleEval("select right('abcdef',6) as col1 ", new String[]{"abcdef"});
    testSimpleEval("select right('abcdef',7) as col1 ", new String[]{"abcdef"});
//    testSimpleEval("select from_right('abcdef',-1) as col1 ", new String[]{"bcdef"});
//    testSimpleEval("select from_right('abcdef',-2) as col1 ", new String[]{"cdef"});
//    testSimpleEval("select from_right('abcdef',-3) as col1 ", new String[]{"def"});
//    testSimpleEval("select from_right('abcdef',-4) as col1 ", new String[]{"ef"});
//    testSimpleEval("select from_right('abcdef',-5) as col1 ", new String[]{"f"});
//    testSimpleEval("select from_right('abcdef',-6) as col1 ", new String[]{""});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", INT4);
    schema.addColumn("col3", TEXT);

    // for null tests
    testEval(schema, "table1", ",1,ghi", "select right(col1,1) is null from table1", new String[]{"t"});
    testEval(schema, "table1", "abc,,ghi", "select right(col1,col2) is null from table1", new String[]{"t"});

    testEval(schema, "table1", "abc,1,ghi", "select right(col1,1) || right(col3,3) from table1", new String[]{"cghi"});
  }

  @Test
  public void testReverse() throws IOException {
    testSimpleEval("select reverse('abcdef') as col1 ", new String[]{"fedcba"});
    testSimpleEval("select reverse('가') as col1 ", new String[]{"가"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "abc,efg,3.14", "select reverse(col1) || reverse(col2) from table1", new String[]{"cbagfe"});
  }

  @Test
  public void testRepeat() throws IOException {
    testSimpleEval("select repeat('ab',4) as col1 ", new String[]{"abababab"});
    testSimpleEval("select repeat('가',3) as col1 ", new String[]{"가가가"});
    testSimpleEval("select repeat('a',2) as col1 ", new String[]{"aa"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "abc,efg,3.14", "select repeat(col1,2) from table1", new String[]{"abcabc"});
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

  @Test
  public void testLength() throws IOException {
    testSimpleEval("select length('123456') as col1 ", new String[]{"6"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "ABC,DEF,3.14", "select length(lower(col1) || lower(col2)) from table1",
        new String[]{"6"});
  }

  @Test
  public void testMd5() throws IOException {
    testSimpleEval("select md5('1') as col1 ", new String[]{"c4ca4238a0b923820dcc509a6f75849b"});
    testSimpleEval("select md5('tajo') as col1 ", new String[]{"742721b3a79f71a9491681b8e8a7ce85"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "abc,efg,3.14", "select md5(col1) from table1", new String[]{"900150983cd24fb0d6963f7d28e17f72"});
  }

  @Test
  public void testHex() throws IOException {
    testSimpleEval("select to_hex(1) as col1 ", new String[]{"1"});
    testSimpleEval("select to_hex(10) as col1 ", new String[]{"a"});
    testSimpleEval("select to_hex(1234) as col1 ", new String[]{"4d2"});
    testSimpleEval("select to_hex(1023456788888888) as col1 ", new String[]{"3a2d41a583d38"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", ",abcdef,3.14", "select to_hex(10) from table1",
        new String[]{"a"});
  }

  @Test
  public void testOctetLength() throws IOException {
    testSimpleEval("select octet_length('123456') as col1 ", new String[]{"6"});
    testSimpleEval("select octet_length('1') as col1 ", new String[]{"1"});
    testSimpleEval("select octet_length('가') as col1 ", new String[]{"3"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "ABC,DEF,3.14", "select octet_length(lower(col1) || lower(col2)) from table1",
        new String[]{"6"});
  }

  @Test
  public void testSubstr() throws IOException {
    testSimpleEval("select substr('abcdef', 3, 2) as col1 ", new String[]{"cd"});
    testSimpleEval("select substr('abcdef', 3) as col1 ", new String[]{"cdef"});
    testSimpleEval("select substr('abcdef', 1, 1) as col1 ", new String[]{"a"});
    testSimpleEval("select substr('abcdef', 0, 1) as col1 ", new String[]{""});
    testSimpleEval("select substr('abcdef', 0, 2) as col1 ", new String[]{"a"});
    testSimpleEval("select substr('abcdef', 0) as col1 ", new String[]{"abcdef"});
    testSimpleEval("select substr('abcdef', 1, 100) as col1 ", new String[]{"abcdef"});
    testSimpleEval("select substr('abcdef', 0, 100) as col1 ", new String[]{"abcdef"});
    testSimpleEval("select substr('일이삼사오', 2, 2) as col1 ", new String[]{"이삼"});
    testSimpleEval("select substr('일이삼사오', 3) as col1 ", new String[]{"삼사오"});

    //TODO If there is a minus value in function argument, next error occurred.
    //org.apache.tajo.engine.parser.SQLSyntaxError: ERROR: syntax error at or near 'substr'
    //LINE 1:7 select substr('abcdef', -1, 100) as col1
    //               ^^^^^^
    //at org.apache.tajo.engine.parser.SQLAnalyzer.parse(SQLAnalyzer.java:64)

//    testSimpleEval("select substr('abcdef', -1) as col1 ", new String[]{"abcdef"});
//    testSimpleEval("select substr('abcdef', -1, 100) as col1 ", new String[]{"abcdef"});
//    testSimpleEval("select substr('abcdef', -1, 3) as col1 ", new String[]{"a"});
//    testSimpleEval("select substr('abcdef', -1, 1) as col1 ", new String[]{""});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", ",abcdef,3.14", "select substr(lower(col2), 2, 3) from table1",
        new String[]{"bcd"});
  }

  @Test
  public void testBitLength() throws IOException {
    testSimpleEval("select bit_length('123456') as col1 ", new String[]{"48"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "ABC,DEF,3.14", "select bit_length(lower(col1) || lower(col2)) from table1",
        new String[]{"48"});
  }

  @Test
  public void testStrpos() throws IOException {
    testSimpleEval("select strpos('tajo','jo') as col1 ", new String[]{"3"});
    testSimpleEval("select strpos('tajo','') as col1 ", new String[]{"1"});
    testSimpleEval("select strpos('tajo','abcdef') as col1 ", new String[]{"0"});
    testSimpleEval("select strpos('일이삼사오육','삼사') as col1 ", new String[]{"3"});
    testSimpleEval("select strpos('일이삼사오육','일이삼') as col1 ", new String[]{"1"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "ABCDEF,HIJKLMN,3.14", "select strpos(lower(col1) || lower(col2), 'fh') from table1",
        new String[]{"6"});
  }

  @Test
  public void testStrposb() throws IOException {
    testSimpleEval("select strposb('tajo','jo') as col1 ", new String[]{"3"});
    testSimpleEval("select strposb('tajo','') as col1 ", new String[]{"1"});
    testSimpleEval("select strposb('tajo','abcdef') as col1 ", new String[]{"0"});
    testSimpleEval("select strposb('일이삼사오육','삼사') as col1 ", new String[]{"7"});    //utf8 1 korean word = 3 chars
    testSimpleEval("select strposb('일이삼사오육','삼사일') as col1 ", new String[]{"0"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    schema.addColumn("col2", TEXT);
    schema.addColumn("col3", TEXT);
    testEval(schema, "table1", "ABCDEF,HIJKLMN,3.14", "select strposb(lower(col1) || lower(col2), 'fh') from table1",
        new String[]{"6"});
  }

  @Test
  public void testInitcap() throws IOException {
    testSimpleEval("select initcap('hi bro') ", new String[]{"Hi Bro"});
    testSimpleEval("select initcap('HI BRO') ", new String[]{"Hi Bro"});
  }

  @Test
  public void testAscii() throws IOException {
    testSimpleEval("select ascii('abc') as col1 ", new String[]{"97"});

    Schema schema = new Schema();
    schema.addColumn("col1", TEXT);
    testEval(schema, "table1", "abc", "select ascii(col1) from table1",
            new String[]{"97"});
    testEval(schema, "table1", "12", "select ascii(col1) from table1",
            new String[]{"49"});

  }

  @Test
  public void testLpad() throws IOException {
    testSimpleEval("select lpad('hi', 5, 'xy') ", new String[]{"xyxhi"});
    testSimpleEval("select LPAD('hello', 7, 'xy') ", new String[]{"xyhello"});
    testSimpleEval("select LPAD('hello', 3, 'xy') ", new String[]{"hel"});
    testSimpleEval("select lPAD('hello', 7) ", new String[]{"  hello"});
    testSimpleEval("select lPAD('가나다라', 3) ", new String[]{"가나다"});

  }
}
