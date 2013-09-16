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

import com.google.common.collect.ImmutableList;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * This represents a simple directed graph. It does not support multiple edges between both vertices.
 *
 * @param <V> The vertex class type
 * @param <E> The edge class type
 */
public class SimpleDirectedGraph<V, E> implements DirectedGraph<V,E> {
  /** map: child -> parent */
  private Map<V, Map<V, E>> directedEdges = TUtil.newLinkedHashMap();
  /** map: parent -> child */
  private Map<V, Map<V, E>> reversedEdges = TUtil.newLinkedHashMap();

  @Override
  public int size() {
    return directedEdges.size();
  }

  @Override
  public void connect(V tail, V head, E edge) {
    TUtil.putToNestedMap(directedEdges, tail, head, edge);
    TUtil.putToNestedMap(reversedEdges, head, tail, edge);
  }

  @Override
  public void disconnect(V tail, V head) {
    if (directedEdges.containsKey(tail)) {
      directedEdges.get(tail).remove(head);
      if (directedEdges.get(tail).isEmpty()) {
        directedEdges.remove(tail);
      }

      reversedEdges.get(head).remove(tail);
      if (reversedEdges.get(head).isEmpty()) {
        reversedEdges.remove(head);
      }
    } else {
      throw new RuntimeException("Not connected channel: " + tail + " -> " + head);
    }
  }

  @Override
  public boolean isConnected(V tail, V head) {
    return directedEdges.containsKey(tail) && directedEdges.get(tail).containsKey(head);
  }

  @Override
  public boolean isReversedConnected(V head, V tail) {
    return reversedEdges.containsKey(head) && reversedEdges.get(head).containsKey(tail);
  }

  @Override
  public E getEdge(V tail, V head) {
    if (isConnected(tail, head)) {
      return directedEdges.get(tail).get(head);
    } else {
      return null;
    }
  }

  @Override
  public E getReverseEdge(V head, V tail) {
    if (isReversedConnected(head, tail)) {
      return reversedEdges.get(head).get(tail);
    } else {
      return null;
    }
  }

  @Override
  public int getChildCount(V v) {
    if (reversedEdges.containsKey(v)) {
      return reversedEdges.get(v).size();
    } else {
      return -1;
    }
  }

  @Override
  public List<E> getIncomingEdges(V head) {
    if (reversedEdges.containsKey(head)) {
      return ImmutableList.copyOf(reversedEdges.get(head).values());
    } else {
      return null;
    }
  }

  @Override
  public List<E> getOutgoingEdges(V tail) {
    if (directedEdges.containsKey(tail)) {
      return ImmutableList.copyOf(directedEdges.get(tail).values());
    } else {
      return null;
    }
  }

  @Override
  public List<V> getChilds(V v) {
    List<V> childBlocks = new ArrayList<V>();
    if (reversedEdges.containsKey(v)) {
      for (Map.Entry<V, E> entry: reversedEdges.get(v).entrySet()) {
        childBlocks.add(entry.getKey());
      }
    }
    return childBlocks;
  }

  @Override
  public V getChild(V block, int idx) {
    return getChilds(block).get(idx);
  }

  @Override
  public V getParent(V v) {
    if (directedEdges.containsKey(v)) {
      return directedEdges.get(v).keySet().iterator().next();
    } else {
      return null;
    }
  }

  @Override
  public boolean isRoot(V v) {
    return !directedEdges.containsKey(v);
  }

  @Override
  public boolean isLeaf(V v) {
    return !reversedEdges.containsKey(v);
  }

  @Override
  public void accept(V source, DirectedGraphVisitor<V> visitor) {
    Stack<V> stack = new Stack<V>();
    visitRecursive(stack, source, visitor);
  }

  private void visitRecursive(Stack<V> stack, V current, DirectedGraphVisitor<V> visitor) {
    stack.push(current);
    for (V child : getChilds(current)) {
      visitRecursive(stack, child, visitor);
    }
    stack.pop();
    visitor.visit(stack, current);
  }

  public String toString() {
    return "G (|v| = " + directedEdges.size() +")";
  }

  public String printDepthString(DepthString planStr) {
    StringBuilder output = new StringBuilder();
    String pad = new String(new char[planStr.depth * 3]).replace('\0', ' ');
    output.append(pad + "|-" + planStr.vertexStr).append("\n");

    return output.toString();
  }

  public String toStringGraph(V vertex) {
    StringBuilder sb = new StringBuilder();
    QueryGraphTopologyStringBuilder visitor = new QueryGraphTopologyStringBuilder();
    accept(vertex, visitor);
    Stack<DepthString> depthStrings = visitor.getDepthStrings();
    while(!depthStrings.isEmpty()) {
      sb.append(printDepthString(depthStrings.pop()));
    }
    return sb.toString();
  }

  private class DepthString {
    int depth;
    String vertexStr;

    DepthString(int depth, String vertexStr) {
      this.depth = depth;
      this.vertexStr = vertexStr;
    }
  }

  private class QueryGraphTopologyStringBuilder implements DirectedGraphVisitor<V> {
    Stack<DepthString> depthString = new Stack<DepthString>();

    @Override
    public void visit(Stack<V> stack, V vertex) {
      depthString.push(new DepthString(stack.size(), vertex.toString()));
    }

    public Stack<DepthString> getDepthStrings() {
      return depthString;
    }
  }
}
