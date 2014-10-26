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

import com.google.common.collect.Lists;
import org.apache.tajo.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This implementation is based on a directed graph implementation. Each edge is connected in bidirection
 * between two vertices.
 *
 * @param <V> Vertex Class
 * @param <E> Edge Class
 */
public class SimpleUndirectedGraph<V, E> extends SimpleDirectedGraph<V, E> implements UndirectedGraph<V, E> {

  @Override
  public @Nullable E getEdge(V tail, V head) {
    E edge = super.getEdge(tail, head);
    if (edge != null) {
      return edge;
    }
    edge = super.getEdge(head, tail);
    if (edge != null) {
      return edge;
    }
    return null;
  }

  @Override
  public Collection<E> getEdges(V v) {
    List<E> edges = Lists.newArrayList();
    List<E> outgoingEdges = getOutgoingEdges(v);
    if (outgoingEdges != null) {
      edges.addAll(outgoingEdges);
    }
    List<E> incomingEdges = getIncomingEdges(v);
    if (incomingEdges != null) {
      edges.addAll(incomingEdges);
    }
    return edges;
  }

  @Override
  public int getDegree(V v) {
    return getEdges(v).size();
  }

  @Override
  public Collection<E> getEdgesAll() {
    List<E> edges = Lists.newArrayList();
    for (Map<V, E> map : directedEdges.values()) {
      edges.addAll(map.values());
    }
    return edges;
  }

  @Override
  public V getParent(V v, int idx) {
    throw new UnsupportedOperationException("Cannot support getParent(V v) in UndirectedGraph");
  }

  @Override
  public int getParentCount(V v) {
    throw new UnsupportedOperationException("Cannot support getParent(V v) in UndirectedGraph");
  }

  @Override
  public List<V> getParents(V v) {
    throw new UnsupportedOperationException("Cannot support getParent(V v) in UndirectedGraph");
  }

  @Override
  public boolean isRoot(V v) {
    throw new UnsupportedOperationException("Cannot support isRoot(V v) in UndirectedGraph");
  }

  @Override
  public boolean isLeaf(V v) {
    throw new UnsupportedOperationException("Cannot support isLeaf(V v) in UndirectedGraph");
  }
}
