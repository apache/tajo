/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner.logical.join;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import tajo.catalog.Column;
import tajo.engine.eval.EvalNode;
import tajo.engine.eval.EvalTreeUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public class JoinTree {
  private Map<String,List<Edge>> map
      = Maps.newHashMap();

  public void addJoin(EvalNode node) {
    List<Column> left = EvalTreeUtil.findAllColumnRefs(node.getLeftExpr());
    List<Column> right = EvalTreeUtil.findAllColumnRefs(node.getRightExpr());

    String ltbName = left.get(0).getTableName();
    String rtbName = right.get(0).getTableName();

    Edge l2r = new Edge(ltbName, rtbName, node);
    Edge r2l = new Edge(rtbName, ltbName, node);
    List<Edge> edges;
    if (map.containsKey(ltbName)) {
      edges = map.get(ltbName);
    } else {
      edges = Lists.newArrayList();
    }
    edges.add(l2r);
    map.put(ltbName, edges);

    if (map.containsKey(rtbName)) {
      edges = map.get(rtbName);
    } else {
      edges = Lists.newArrayList();
    }
    edges.add(r2l);
    map.put(rtbName, edges);
  }

  public int degree(String tableName) {
    return this.map.get(tableName).size();
  }

  public Collection<String> getTables() {
    return Collections.unmodifiableCollection(this.map.keySet());
  }

  public Collection<Edge> getEdges(String tableName) {
    return Collections.unmodifiableCollection(this.map.get(tableName));
  }

  public Collection<Edge> getAllEdges() {
    List<Edge> edges = Lists.newArrayList();
    for (List<Edge> edgeList : map.values()) {
      edges.addAll(edgeList);
    }
    return Collections.unmodifiableCollection(edges);
  }

  public int getTableNum() {
    return this.map.size();
  }

  public int getJoinNum() {
    int sum = 0;
    for (List<Edge> edgeList : map.values()) {
      sum += edgeList.size();
    }

    return sum / 2;
  }
}
