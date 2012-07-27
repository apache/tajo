package nta.engine.planner.logical.join;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import nta.catalog.Column;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalTreeUtil;

import java.util.*;

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
