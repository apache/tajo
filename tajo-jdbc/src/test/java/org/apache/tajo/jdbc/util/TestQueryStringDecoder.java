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

package org.apache.tajo.jdbc.util;

import java.net.MalformedURLException;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class TestQueryStringDecoder {

  @Test
  public void testEmptyQuery() throws Exception {
    QueryStringDecoder decoder = null;
    String rawUriStr = "";
    
    rawUriStr = "http://127.0.0.1:26002/";
    decoder = new QueryStringDecoder(rawUriStr);
    assertThat(decoder.getQueries(), is(notNullValue()));
    assertThat(decoder.getParameters(), is(notNullValue()));
    assertThat(decoder.getParameters().size(), is(0));
    
    rawUriStr = "/test_path/test2?";
    decoder = new QueryStringDecoder(rawUriStr);
    assertThat(decoder.getQueries(), is(notNullValue()));
    assertThat(decoder.getParameters(), is(notNullValue()));
    assertThat(decoder.getParameters().size(), is(0));
  }
  
  @Test
  public void testSingleQueries() throws Exception {
    QueryStringDecoder decoder = null;
    String rawUriStr = "";
    
    rawUriStr = "http://127.0.0.1:26200/?qid=1234&tid=2345&partition_id=4567";
    decoder = new QueryStringDecoder(rawUriStr);
    assertThat(decoder.getQueries(), is("qid=1234&tid=2345&partition_id=4567"));
    assertThat(decoder.getParameters(), is(notNullValue()));
    assertThat(decoder.getParameters().size(), is(3));
    assertThat(decoder.getParameters().get("qid").get(0), is("1234"));
    assertThat(decoder.getParameters().get("partition_id").get(0), is("4567"));
    
    rawUriStr = "http://127.0.0.1:26200/?tid=2345";
    decoder = new QueryStringDecoder(rawUriStr);
    assertThat(decoder.getQueries(), is("tid=2345"));
    assertThat(decoder.getParameters(), is(notNullValue()));
    assertThat(decoder.getParameters().size(), is(1));
    assertThat(decoder.getParameters().get("tid").get(0), is("2345"));
  }
  
  @Test
  public void testMultipleQueries() throws Exception {
    QueryStringDecoder decoder = null;
    String rawUriStr = "";
    
    rawUriStr = "http://127.0.0.1:26200/?qid=1234&tid=2345&partition_id=4567&tid=4890";
    decoder = new QueryStringDecoder(rawUriStr);
    assertThat(decoder.getQueries(), is("qid=1234&tid=2345&partition_id=4567&tid=4890"));
    assertThat(decoder.getParameters(), is(notNullValue()));
    assertThat(decoder.getParameters().size(), is(3));
    assertThat(decoder.getParameters().get("tid").size(), is(2));
    assertThat(decoder.getParameters().get("tid").get(0), is("2345"));
    assertThat(decoder.getParameters().get("tid").get(1), is("4890"));
  }
  
  @Test(expected=MalformedURLException.class)
  public void testMalformedURI() throws Exception {
    QueryStringDecoder decoder = null;
    String rawUriStr = "";
    
    rawUriStr = "http://127.0.0.1:26200/?=1234&tid=&partition_id=4567";
    decoder = new QueryStringDecoder(rawUriStr);
    assertThat(decoder.getQueries(), is("=1234&tid=&partition_id=4567"));
    decoder.getParameters();
  }
}
