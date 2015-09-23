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

package org.apache.tajo.storage.hbase;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestHBaseTableSpace {
  @BeforeClass
  public static void setUp() throws IOException {
    String tableSpaceUri = "hbase:zk://host1:2171";
    HBaseTablespace hBaseTablespace = new HBaseTablespace("cluster1", URI.create(tableSpaceUri), null);
    hBaseTablespace.init(new TajoConf());
    TablespaceManager.addTableSpaceForTest(hBaseTablespace);
  }

  @Test
  public void testExtractQuorum() {
    assertEquals("host1:2171", HBaseTablespace.extractQuorum(URI.create("hbase:zk://host1:2171")));
    assertEquals("host1:2171", HBaseTablespace.extractQuorum(URI.create("hbase:zk://host1:2171/table1")));
    assertEquals("host1:2171,host2:2172",
        HBaseTablespace.extractQuorum(URI.create("hbase:zk://host1:2171,host2:2172/table1")));
  }

  @Test
  public void testTablespaceHandler() throws Exception {
    assertTrue((TablespaceManager.getByName("cluster1")) instanceof HBaseTablespace);
    assertTrue((TablespaceManager.get(URI.create("hbase:zk://host1:2171")))
        instanceof HBaseTablespace);
  }

  @Test
  public void testGetIndexPredications() throws Exception {
    Column rowkeyColumn = new Column("rk", Type.TEXT);
    // where rk >= '020' and rk <= '055'
    ScanNode scanNode = new ScanNode(1);
    EvalNode evalNode1 = new BinaryEval(EvalType.GEQ, new FieldEval(rowkeyColumn), new ConstEval(new TextDatum("020")));
    EvalNode evalNode2 = new BinaryEval(EvalType.LEQ, new FieldEval(rowkeyColumn), new ConstEval(new TextDatum("055")));
    EvalNode evalNodeA = new BinaryEval(EvalType.AND, evalNode1, evalNode2);
    scanNode.setQual(evalNodeA);

    HBaseTablespace storageManager = (HBaseTablespace) TablespaceManager.getByName("cluster1");
    List<Set<EvalNode>> indexEvals =
        storageManager.findIndexablePredicateSet(scanNode.getQual(), new Column[]{rowkeyColumn});
    assertNotNull(indexEvals);
    assertEquals(1, indexEvals.size());
    Pair<Datum, Datum> indexPredicateValue = storageManager.getIndexablePredicateValue(null, indexEvals.get(0));
    assertEquals("020", indexPredicateValue.getFirst().asChars());
    assertEquals("055", indexPredicateValue.getSecond().asChars());

    // where (rk >= '020' and rk <= '055') or rk = '075'
    EvalNode evalNode3 = new BinaryEval(EvalType.EQUAL, new FieldEval(rowkeyColumn),new ConstEval(new TextDatum("075")));
    EvalNode evalNodeB = new BinaryEval(EvalType.OR, evalNodeA, evalNode3);
    scanNode.setQual(evalNodeB);
    indexEvals = storageManager.findIndexablePredicateSet(scanNode.getQual(), new Column[]{rowkeyColumn});
    assertEquals(2, indexEvals.size());
    indexPredicateValue = storageManager.getIndexablePredicateValue(null, indexEvals.get(0));
    assertEquals("020", indexPredicateValue.getFirst().asChars());
    assertEquals("055", indexPredicateValue.getSecond().asChars());

    indexPredicateValue = storageManager.getIndexablePredicateValue(null, indexEvals.get(1));
    assertEquals("075", indexPredicateValue.getFirst().asChars());
    assertEquals("075", indexPredicateValue.getSecond().asChars());

    // where (rk >= '020' and rk <= '055') or (rk >= '072' and rk <= '078')
    EvalNode evalNode4 = new BinaryEval(EvalType.GEQ, new FieldEval(rowkeyColumn), new ConstEval(new TextDatum("072")));
    EvalNode evalNode5 = new BinaryEval(EvalType.LEQ, new FieldEval(rowkeyColumn), new ConstEval(new TextDatum("078")));
    EvalNode evalNodeC = new BinaryEval(EvalType.AND, evalNode4, evalNode5);
    EvalNode evalNodeD = new BinaryEval(EvalType.OR, evalNodeA, evalNodeC);
    scanNode.setQual(evalNodeD);
    indexEvals = storageManager.findIndexablePredicateSet(scanNode.getQual(), new Column[]{rowkeyColumn});
    assertEquals(2, indexEvals.size());

    indexPredicateValue = storageManager.getIndexablePredicateValue(null, indexEvals.get(0));
    assertEquals("020", indexPredicateValue.getFirst().asChars());
    assertEquals("055", indexPredicateValue.getSecond().asChars());

    indexPredicateValue = storageManager.getIndexablePredicateValue(null, indexEvals.get(1));
    assertEquals("072", indexPredicateValue.getFirst().asChars());
    assertEquals("078", indexPredicateValue.getSecond().asChars());

    // where (rk >= '020' and rk <= '055') or (rk >= '072' and rk <= '078' and rk >= '073')
    evalNode4 = new BinaryEval(EvalType.GEQ, new FieldEval(rowkeyColumn), new ConstEval(new TextDatum("072")));
    evalNode5 = new BinaryEval(EvalType.LEQ, new FieldEval(rowkeyColumn), new ConstEval(new TextDatum("078")));
    evalNodeC = new BinaryEval(EvalType.AND, evalNode4, evalNode5);
    EvalNode evalNode6 = new BinaryEval(EvalType.GEQ, new FieldEval(rowkeyColumn), new ConstEval(new TextDatum("073")));
    evalNodeD = new BinaryEval(EvalType.AND, evalNodeC, evalNode6);
    EvalNode evalNodeE = new BinaryEval(EvalType.OR, evalNodeA, evalNodeD);
    scanNode.setQual(evalNodeE);
    indexEvals = storageManager.findIndexablePredicateSet(scanNode.getQual(), new Column[]{rowkeyColumn});
    assertEquals(2, indexEvals.size());

    indexPredicateValue = storageManager.getIndexablePredicateValue(null, indexEvals.get(0));
    assertEquals("020", indexPredicateValue.getFirst().asChars());
    assertEquals("055", indexPredicateValue.getSecond().asChars());

    indexPredicateValue = storageManager.getIndexablePredicateValue(null, indexEvals.get(1));
    assertEquals("073", indexPredicateValue.getFirst().asChars());
    assertEquals("078", indexPredicateValue.getSecond().asChars());
  }
}
