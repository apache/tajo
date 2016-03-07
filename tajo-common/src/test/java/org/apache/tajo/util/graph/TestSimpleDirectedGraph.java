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

package org.apache.tajo.util.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.util.graph.DirectedGraphVisitor;
import org.apache.tajo.util.graph.SimpleDirectedGraph;
import org.junit.Test;

import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSimpleDirectedGraph {
  private static final Log LOG = LogFactory.getLog(TestSimpleDirectedGraph.class);

  @Test
  public final void test() throws TajoException {
    SimpleDirectedGraph<String, Integer> graph = new SimpleDirectedGraph<>();

    //     root
    //     /  \
    // (1)/    \ (2)
    //   /      \
    // child1  child2
    //           / \
    //       (3)/   \(4)
    //         /     \
    //    child3   child4
    //
    String root = "root";
    String child1 = "child1";
    String child2 = "child2";
    String child3 = "child3";
    String child4 = "child4";

    graph.addEdge(child1, root, 1);
    graph.addEdge(child2, root, 2);
    graph.addEdge(child3, child2, 3);
    graph.addEdge(child4, child2, 4);

    assertEquals(4, graph.getEdgeNum());
    assertEquals(4, graph.getEdgesAll().size());

    // tree features
    assertTrue(graph.isRoot(root));
    assertFalse(graph.isLeaf(root));

    assertEquals(2, graph.getChildCount(root));
    assertEquals(2, graph.getChildCount(child2));

    // visitor
    graph.accept(null, root, new Visitor());
  }

  private class Visitor implements DirectedGraphVisitor<Object, String> {

    @Override
    public void visit(Object context, Stack<String> stack, String s) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Element:" + s);
      }
    }
  }
}
