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

package org.apache.tajo.engine.planner;

import org.apache.tajo.engine.planner.graph.DirectedGraphVisitor;
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.junit.Test;

import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestGenericDirectedGraph {

  @Test
  public final void test() {
    SimpleDirectedGraph<String, Integer> graph = new SimpleDirectedGraph<String, Integer>();

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

    graph.connect(child1, root, 1);
    graph.connect(child2, root, 2);
    graph.connect(child3, child2, 3);
    graph.connect(child4, child2, 4);

    assertTrue(graph.isRoot(root));
    assertFalse(graph.isLeaf(root));

    assertEquals(2, graph.getChildCount(root));
    assertEquals(2, graph.getChildCount(child2));

    graph.accept(root, new Visitor());
  }

  private class Visitor implements DirectedGraphVisitor<String> {
    @Override
    public void visit(Stack<String> stack, String s) {
      System.out.println("===> " + s);
    }
  }
}
