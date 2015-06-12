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

package org.apache.tajo.master;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.benchmark.TPCH;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.TajoMaster.MasterContext;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.master.exec.NonForwardQueryResultSystemScanner;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.logical.LimitNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.RowStoreUtil.RowStoreDecoder;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.KeyValueSet;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestNonForwardQueryResultSystemScanner {
  
  private class CollectionMatcher<T> extends TypeSafeDiagnosingMatcher<Iterable<? extends T>> {
    
    private final Matcher<? extends T> matcher;
    
    public CollectionMatcher(Matcher<? extends T> matcher) {
      this.matcher = matcher;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a collection containing ").appendDescriptionOf(this.matcher);
    }

    @Override
    protected boolean matchesSafely(Iterable<? extends T> item, Description mismatchDescription) {
      boolean isFirst = true;
      Iterator<? extends T> iterator = item.iterator();
      
      while (iterator.hasNext()) {
        T obj = iterator.next();
        if (this.matcher.matches(obj)) {
          return true;
        }
        
        if (!isFirst) {
          mismatchDescription.appendText(", ");
        }
        
        this.matcher.describeMismatch(obj, mismatchDescription);
        isFirst = false;
      }
      return false;
    }
    
  }
  
  private <T> Matcher<Iterable<? extends T>> hasItem(Matcher<? extends T> matcher) {
    return new CollectionMatcher<T>(matcher);
  }

  private static LocalTajoTestingUtility testUtil;
  private static TajoTestingCluster testingCluster;
  private static TajoConf conf;
  private static MasterContext masterContext;
  
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner logicalPlanner;
  private static LogicalOptimizer logicalOptimizer;
  
  private static void setupTestingCluster() throws Exception {
    testUtil = new LocalTajoTestingUtility();
    String[] names, paths;
    Schema[] schemas;
    
    TPCH tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadQueries();
    
    names = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", 
        "region", "supplier", "empty_orders"};
    schemas = new Schema[names.length];
    for (int i = 0; i < names.length; i++) {
      schemas[i] = tpch.getSchema(names[i]);
    }

    File file;
    paths = new String[names.length];
    for (int i = 0; i < names.length; i++) {
      file = new File("src/test/tpch/" + names[i] + ".tbl");
      if(!file.exists()) {
        file = new File(System.getProperty("user.dir") + "/tajo-core/src/test/tpch/" + names[i]
            + ".tbl");
      }
      paths[i] = file.getAbsolutePath();
    }
    
    KeyValueSet opt = new KeyValueSet();
    opt.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    testUtil.setup(names, paths, schemas, opt);
    
    testingCluster = testUtil.getTestingCluster();
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    setupTestingCluster();
    
    conf = testingCluster.getConfiguration();
    masterContext = testingCluster.getMaster().getContext();
    
    GlobalEngine globalEngine = masterContext.getGlobalEngine();
    analyzer = globalEngine.getAnalyzer();
    logicalPlanner = globalEngine.getLogicalPlanner();
    logicalOptimizer = globalEngine.getLogicalOptimizer();
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    try {
      Thread.sleep(2000);
    } catch (Exception ignored) {
    }
    
    testUtil.shutdown();
  }
  
  private NonForwardQueryResultScanner getScanner(String sql) throws Exception {
    QueryId queryId = QueryIdFactory.newQueryId(masterContext.getResourceManager().getSeedQueryId());
    String sessionId = UUID.randomUUID().toString();
    
    return getScanner(sql, queryId, sessionId);
  }
  
  private NonForwardQueryResultScanner getScanner(String sql, QueryId queryId, String sessionId) throws Exception {
    QueryContext queryContext = LocalTajoTestingUtility.createDummyContext(conf);
    
    Expr expr = analyzer.parse(sql);
    LogicalPlan logicalPlan = logicalPlanner.createPlan(queryContext, expr);
    logicalOptimizer.optimize(logicalPlan);
    
    int maxRow = Integer.MAX_VALUE;
    if (logicalPlan.getRootBlock().hasNode(NodeType.LIMIT)) {
      LimitNode limitNode = logicalPlan.getRootBlock().getNode(NodeType.LIMIT);
      maxRow = (int) limitNode.getFetchFirstNum();
    }
    
    NonForwardQueryResultScanner queryResultScanner = 
        new NonForwardQueryResultSystemScanner(masterContext, logicalPlan, queryId,
            sessionId, maxRow);
    
    return queryResultScanner;
  }
  
  @Test
  public void testInit() throws Exception {
    QueryId queryId = QueryIdFactory.newQueryId(masterContext.getResourceManager().getSeedQueryId());
    String sessionId = UUID.randomUUID().toString();
    NonForwardQueryResultScanner queryResultScanner = 
        getScanner("SELECT SPACE_ID, SPACE_URI FROM INFORMATION_SCHEMA.TABLESPACE",
            queryId, sessionId);
    
    queryResultScanner.init();
    
    assertThat(queryResultScanner.getQueryId(), is(notNullValue()));
    assertThat(queryResultScanner.getLogicalSchema(), is(notNullValue()));
    assertThat(queryResultScanner.getSessionId(), is(notNullValue()));
    assertThat(queryResultScanner.getTableDesc(), is(notNullValue()));
    
    assertThat(queryResultScanner.getQueryId(), is(queryId));
    assertThat(queryResultScanner.getSessionId(), is(sessionId));
    
    assertThat(queryResultScanner.getLogicalSchema().size(), is(2));
    assertThat(queryResultScanner.getLogicalSchema().getColumn("space_id"), is(notNullValue()));
  }
  
  private List<Tuple> getTupleList(RowStoreDecoder decoder, List<ByteString> bytes) {
    List<Tuple> tuples = new ArrayList<Tuple>(bytes.size());
    
    for (ByteString byteString: bytes) {
      Tuple aTuple = decoder.toTuple(byteString.toByteArray());
      tuples.add(aTuple);
    }
    
    return tuples;
  }
  
  private <T> Matcher<Tuple> getTupleMatcher(final int fieldId, final Matcher<T> matcher) {
    return new TypeSafeDiagnosingMatcher<Tuple>() {

      @Override
      public void describeTo(Description description) {
        description.appendDescriptionOf(matcher);
      }

      @Override
      protected boolean matchesSafely(Tuple item, Description mismatchDescription) {
        Object itemValue = null;

        Type type = item.type(fieldId);
        if (type == Type.TEXT) {
          itemValue = item.getText(fieldId);
        } else if (type == Type.INT4) {
          itemValue = item.getInt4(fieldId);
        } else if (type == Type.INT8) {
          itemValue = item.getInt8(fieldId);
        }
        
        if (itemValue != null && matcher.matches(itemValue)) {
          return true;
        }
        
        matcher.describeMismatch(itemValue, mismatchDescription);
        return false;
      }
    };
  }
  
  @Test
  public void testGetNextRowsForAggregateFunction() throws Exception {
    NonForwardQueryResultScanner queryResultScanner = 
        getScanner("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES");
    
    queryResultScanner.init();
    
    List<ByteString> rowBytes = queryResultScanner.getNextRows(100);
    
    assertThat(rowBytes.size(), is(1));
    
    RowStoreDecoder decoder = RowStoreUtil.createDecoder(queryResultScanner.getLogicalSchema());
    List<Tuple> tuples = getTupleList(decoder, rowBytes);
    
    assertThat(tuples.size(), is(1));
    assertThat(tuples, hasItem(getTupleMatcher(0, is(9L))));
  }
  
  @Test
  public void testGetNextRowsForTable() throws Exception {
    NonForwardQueryResultScanner queryResultScanner =
        getScanner("SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES");
    
    queryResultScanner.init();
    
    List<ByteString> rowBytes = queryResultScanner.getNextRows(100);
    
    assertThat(rowBytes.size(), is(9));
    
    RowStoreDecoder decoder = RowStoreUtil.createDecoder(queryResultScanner.getLogicalSchema());
    List<Tuple> tuples = getTupleList(decoder, rowBytes);;
    
    assertThat(tuples.size(), is(9));
    assertThat(tuples, hasItem(getTupleMatcher(0, is("lineitem"))));
  }
  
  @Test
  public void testGetClusterDetails() throws Exception {
    NonForwardQueryResultScanner queryResultScanner =
        getScanner("SELECT TYPE FROM INFORMATION_SCHEMA.CLUSTER");
    
    queryResultScanner.init();
    
    List<ByteString> rowBytes = queryResultScanner.getNextRows(100);
    
    assertThat(rowBytes.size(), is(2));
    
    RowStoreDecoder decoder = RowStoreUtil.createDecoder(queryResultScanner.getLogicalSchema());
    List<Tuple> tuples = getTupleList(decoder, rowBytes);
    
    assertThat(tuples.size(), is(2));
    assertThat(tuples, hasItem(getTupleMatcher(0, is("QueryMaster"))));
  }
}
