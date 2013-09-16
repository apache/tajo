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

package org.apache.tajo.engine.planner.graph;

import java.util.List;

/**
 * This represents a directed graph.
 *
 * @param <V> The vertex class type
 * @param <E> The edge class type
 */
public interface DirectedGraph<V, E> {

  int size();

  void connect(V tail, V head, E edge);

  void disconnect(V tail, V head);

  boolean isConnected(V tail, V head);

  boolean isReversedConnected(V head, V tail);

  E getEdge(V tail, V head);

  E getReverseEdge(V head, V tail);

  int getChildCount(V v);

  List<E> getIncomingEdges(V head);

  List<E> getOutgoingEdges(V tail);

  List<V> getChilds(V v);

  V getChild(V block, int idx);

  V getParent(V v);

  boolean isRoot(V v);

  boolean isLeaf(V v);

  /**
   * It visits all vertices in a post-order traverse way.
   */
  void accept(V src, DirectedGraphVisitor<V> visitor);
}
