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

import java.util.ArrayList;

public class DirectedGraphCursor<V,E> {
  private DirectedGraph<V,E> graph;
  private ArrayList<V> orderedVertices = new ArrayList<>();
  private int cursor = 0;

  public DirectedGraphCursor(DirectedGraph<V, E> graph, V startVertex) {
    this.graph = graph;
    buildOrder(startVertex);
  }

  public int size() {
    return orderedVertices.size();
  }

  private void buildOrder(V current) {
    if (!graph.isLeaf(current)) {
      for (V child : graph.getChilds(current)) {
        buildOrder(child);
      }
    }
    orderedVertices.add(current);
  }

  public boolean hasNext() {
    return cursor < orderedVertices.size();
  }

  public V nextBlock() {
    return orderedVertices.get(cursor++);
  }

  public V peek() {
    return orderedVertices.get(cursor);
  }

  public V peek(int skip) {
    return orderedVertices.get(cursor + skip);
  }

  public void reset() {
    cursor = 0;
  }
}
