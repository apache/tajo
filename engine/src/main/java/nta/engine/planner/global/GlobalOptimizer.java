/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nta.distexec.DistPlan;
import nta.engine.QueryIdFactory;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalRootNode;

/**
 * @author jihoon
 * 
 */
public class GlobalOptimizer {
  // 질의 최적화에 필요한 정보
  // 알고리즘의 종류 및 특징
  private Map<ExprType, List<OptimizationPlan>> planMap;

  public GlobalOptimizer() {
    planMap = new HashMap<ExprType, List<OptimizationPlan>>();
  }

  public void addOptimizationPlan(ExprType type, OptimizationPlan plan) {
    if (planMap.containsKey(type)) {
      planMap.get(type).add(plan);
    } else {
      ArrayList<OptimizationPlan> planList = new ArrayList<OptimizationPlan>();
      planList.add(plan);
      planMap.put(type, planList);
    }
  }

  public QueryUnitGraph optimize(QueryUnitGraph graph) {
    QueryUnit query = graph.getRoot();
    List<QueryUnit> q = new ArrayList<QueryUnit>();
    q.add(query); // in
    q.add(query); // out

    while (!q.isEmpty()) {
      query = q.remove(0);
      if (q.size() > 0 && query.getId() == q.get(0).getId()) {
        // in
        for (QueryUnit uq : query.getNextQueries()) {
          q.add(0, uq); // in
          q.add(0, uq); // out
        }
      } else {
        // out
        optimizeUnitQuery(query);
      }
    }
    query = graph.getRoot();
    while (query.getPrevQueries().size() > 0) {
      query = query.getPrevQueries().iterator().next();
    }
    graph.setRoot(query);

    return graph;
  }

  private void optimizeUnitQuery(QueryUnit query) {
    List<OptimizationPlan> plans;
    OptimizationPlan bestPlan;
    int i, j, k;
    QueryUnit curQuery, prevQuery;
    DistPlan distPlan;
    Set<QueryUnit> prevList = new HashSet<QueryUnit>();
    Set<QueryUnit> curList = new HashSet<QueryUnit>();
    Set<QueryUnit> tmpList;
    Iterator<QueryUnit> prevIt, curIt;
    int outputNum = -1, maxOutputNum = query.getNextQueries().size();

    plans = planMap.get(query.getOp().getType());

    if (query.getOp().getType() == ExprType.GROUP_BY
        || query.getOp().getType() == ExprType.SORT
        || query.getOp().getType() == ExprType.SORT) {
      // 입력 query의 prev query들을 prevList에 추가하여 연결함
      if (query.getPrevQueries().size() > 0) {
        prevIt = query.getPrevQueries().iterator();
        while (prevIt.hasNext()) {
          prevQuery = prevIt.next();
          prevQuery.removeNextQuery(query);
          curList.add(prevQuery);
        }
      } else {
        curList.add(new QueryUnit(QueryIdFactory.newQueryUnitId(),
            new LogicalRootNode()));
      }
    }

    switch (query.getOp().getType()) {
    case GROUP_BY:
      // TODO: select the best plan
      bestPlan = plans.get(0);
      for (i = 0; i < bestPlan.getStepNum(); i++) {
        tmpList = prevList;
        prevList = curList;
        curList = tmpList;
        prevIt = prevList.iterator();
        curList.clear();
        for (j = 0; j < bestPlan.getNodeNum()[i]; j++) {
          curQuery = new QueryUnit(QueryIdFactory.newQueryUnitId(),
              query.getOp());
          distPlan = new DistPlan();
          distPlan.setPlanName(bestPlan.getExecPlan()[i]);
          // set output number according to the mapping type
          if (bestPlan.getMappingType()[i] == MappingType.ONE_TO_MANY) {
            // TODO: choose the # of output
            outputNum = maxOutputNum;
          } else if (bestPlan.getMappingType()[i] == MappingType.ONE_TO_ONE) {
            outputNum = 1;
          }
          distPlan.setOutputNum(outputNum);
          curQuery.setDistPlan(distPlan);
          // TODO: connect with the prev list
          for (k = 0; k < outputNum; k++) {
            if (!prevIt.hasNext()) {
              prevIt = prevList.iterator();
            }
            prevQuery = prevIt.next();
            curQuery.addPrevQuery(prevQuery);
            prevQuery.addNextQuery(curQuery);
          }
          curList.add(curQuery);
        }
      }
      break;
    case SORT:
      // TODO: select the best plan
      bestPlan = plans.get(0);
      for (i = 0; i < bestPlan.getStepNum(); i++) {
        tmpList = prevList;
        prevList = curList;
        curList = tmpList;
        prevIt = prevList.iterator();
        curList.clear();
        for (j = 0; j < bestPlan.getNodeNum()[i]; j++) {
          curQuery = new QueryUnit(QueryIdFactory.newQueryUnitId(),
              query.getOp());
          distPlan = new DistPlan();
          distPlan.setPlanName(bestPlan.getExecPlan()[i]);
          // set output number according to the mapping type
          if (bestPlan.getMappingType()[i] == MappingType.ONE_TO_MANY) {
            // TODO: choose the # of output
            outputNum = maxOutputNum;
          } else if (bestPlan.getMappingType()[i] == MappingType.ONE_TO_ONE) {
            outputNum = 1;
          }
          distPlan.setOutputNum(outputNum);
          curQuery.setDistPlan(distPlan);
          // TODO: connect with the prev list
          for (k = 0; k < outputNum; k++) {
            if (!prevIt.hasNext()) {
              prevIt = prevList.iterator();
            }
            prevQuery = prevIt.next();
            curQuery.addPrevQuery(prevQuery);
            prevQuery.addNextQuery(curQuery);
          }
          curList.add(curQuery);
        }
      }
      break;
    case JOIN:
      break;
    }

    if (query.getOp().getType() == ExprType.GROUP_BY
        || query.getOp().getType() == ExprType.SORT
        || query.getOp().getType() == ExprType.SORT) {
      prevList = curList;
      curList = query.getNextQueries();
      // TODO: 입력 query의 next query들과 연결
      curIt = curList.iterator();
      prevIt = prevList.iterator();
      while (curIt.hasNext()) {
        curQuery = curIt.next();
        if (!prevIt.hasNext()) {
          prevIt = prevList.iterator();
        }
        prevQuery = prevIt.next();
        curQuery.removePrevQuery(query);
        curQuery.addPrevQuery(prevQuery);
        prevQuery.addNextQuery(curQuery);
      }
    }
  }
}
