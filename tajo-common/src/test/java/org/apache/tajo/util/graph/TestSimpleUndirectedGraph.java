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

import com.google.common.collect.Sets;
import org.apache.tajo.util.graph.SimpleUndirectedGraph;
import org.apache.tajo.util.graph.UndirectedGraph;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class TestSimpleUndirectedGraph {

  @Test
  public final void test() {
    UndirectedGraph<String, Integer> graph = new SimpleUndirectedGraph<String, Integer>();

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

    // for connected edges
    assertNotNull(graph.getEdge(child1, root));
    assertNotNull(graph.getEdge(root, child1));

    assertNotNull(graph.getEdge(root, child2));
    assertNotNull(graph.getEdge(child2, root));

    assertNotNull(graph.getEdge(child2, child3));
    assertNotNull(graph.getEdge(child3, child2));

    assertNotNull(graph.getEdge(child2, child4));
    assertNotNull(graph.getEdge(child4, child2));

    // for not-connected edges
    assertNull(graph.getEdge(root, child4));
    assertNull(graph.getEdge(child4, root));

    assertNull(graph.getEdge(root, child3));
    assertNull(graph.getEdge(child3, root));

    assertNull(graph.getEdge(child1, child2));
    assertNull(graph.getEdge(child2, child1));

    assertNull(graph.getEdge(child3, child4));
    assertNull(graph.getEdge(child4, child3));

    // number
    assertEquals(4, graph.getEdgeNum());
    assertEquals(4, graph.getEdgesAll().size());

    assertEquals(2, graph.getDegree(root));
    assertEquals(1, graph.getDegree(child1));
    assertEquals(3, graph.getDegree(child2));
    assertEquals(1, graph.getDegree(child3));
    assertEquals(1, graph.getDegree(child4));

    Set<Integer> edges = Sets.newHashSet(2, 3, 4);
    assertEquals(edges, Sets.newHashSet(graph.getEdges(child2)));
  }
}
