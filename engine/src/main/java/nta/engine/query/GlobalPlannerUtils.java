package nta.engine.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.*;
import nta.catalog.*;
import nta.catalog.proto.CatalogProtos;
import nta.catalog.proto.CatalogProtos.*;
import nta.engine.QueryIdFactory;
import nta.engine.cluster.FragmentServingInfo;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.exec.eval.EvalNode;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.global.ScheduleUnit;
import nta.engine.planner.logical.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

/**
 * @author jihoon
 */
public class GlobalPlannerUtils {
  private static Log LOG = LogFactory.getLog(GlobalPlannerUtils.class);

  static class WorkerComparatorByNumOfQueryUnits implements Comparator {
    Map base;
    public WorkerComparatorByNumOfQueryUnits(Map base) {
      this.base = base;
    }
    public int compare(Object a, Object b) {
      Collection<QueryUnit> l1 = (Collection<QueryUnit>)base.get(a);
      Collection<QueryUnit> l2 = (Collection<QueryUnit>)base.get(b);
      if (l1.size() > l2.size()) {
        return 1;
      } else {
        return -1;
      }
    }
  }

  public static QueryUnit[] buildQueryDistributionPlan(
      Map<Fragment, FragmentServingInfo> servingMap,
      Map<String, List<String>> DNSNameToHostsMap,
//      Set<String> failedHost,
      QueryUnit[] queryUnits
  ) throws UnknownWorkerException {
    Map<String, Collection<QueryUnit>> map = Maps.newHashMap();
    ListMultimap<String, QueryUnit> distStatus =
        Multimaps.newListMultimap(map,
            new Supplier<List<QueryUnit>>() {
              @Override
              public List<QueryUnit> get() {
                return Lists.newArrayList();
              }
            });

    String host;
    Fragment fragment;
    FragmentServingInfo servingInfo = null;
    // build the initial query distribution status
    for (QueryUnit unit : queryUnits) {
      Preconditions.checkState(unit.getScanNodes().length == 1);
      fragment = unit.getFragment(unit.getScanNodes()[0].getTableId());
      if (servingMap.containsKey(fragment)) {
        servingInfo = servingMap.get(fragment);
      } else {
        servingInfo = null;
        // error
      }
      host = servingInfo.getPrimaryHost();
      distStatus.put(host, unit);
    }

    /*LOG.info("===== before re-balancing =====");
    for (Map.Entry<String, Collection<QueryUnit>> e : map.entrySet()) {
      LOG.info(e.getKey() + " : " + e.getValue().size());
    }
    LOG.info("\n");*/

    // re-balancing the query distribution
    Preconditions.checkState(queryUnits.length >= servingMap.size());
    int threshold = 0;
    int mean = queryUnits.length / map.size();
    int maxQueryUnitNum = mean + threshold;
    WorkerComparatorByNumOfQueryUnits comp =
        new WorkerComparatorByNumOfQueryUnits(map);
    TreeMap<String, Collection<QueryUnit>> sortedMap =
        Maps.newTreeMap(comp);
    sortedMap.putAll(map);

    Collection<QueryUnit> fromUnits;
    Collection<QueryUnit> toUnits;
    QueryUnit moved;
    int moveNum = 0;
    List<Map.Entry<String, Collection<QueryUnit>>> list =
        Lists.newArrayList(sortedMap.entrySet());
    int fromIdx = list.size()-1, toIdx = 0;
    while (fromIdx > toIdx) {
      toUnits = list.get(toIdx).getValue();
      fromUnits = list.get(fromIdx).getValue();

      do{
        moved = fromUnits.iterator().next();
        toUnits.add(moved);
        fromUnits.remove(moved);
        moveNum++;
      } while (toUnits.size() < maxQueryUnitNum &&
          fromUnits.size() > maxQueryUnitNum);
      if (fromUnits.size() <= maxQueryUnitNum) {
        fromIdx--;
      }
      if (toUnits.size() >= maxQueryUnitNum) {
        toIdx++;
      }
    }

    /*LOG.info("===== after re-balancing " + maxQueryUnitNum + " =====");
    for (Map.Entry<String, Collection<QueryUnit>> e : list) {
      LOG.info(e.getKey() + " : " + e.getValue().size());
    }
    LOG.info("\n");*/

    LOG.info(moveNum + " query units among " +
        queryUnits.length + " are moved!");

    List<String> hosts;
    int rrIdx = 0;
    for (Map.Entry<String, Collection<QueryUnit>> e : list) {
      hosts = DNSNameToHostsMap.get(e.getKey());
      if (hosts == null) {
        throw new UnknownWorkerException(e.getKey() + "");
      }
      for (QueryUnit unit : e.getValue()) {
/*
        while (failedHost.contains(hosts.get(rrIdx))) {
          if (++rrIdx == hosts.size()) {
            rrIdx = 0;
          }
        }
*/
        unit.setHost(hosts.get(rrIdx++));
        if (rrIdx == hosts.size()) {
          rrIdx = 0;
        }
      }
    }

    return queryUnits;
  }

  public static StoreTableNode newStorePlan(Schema outputSchema,
                                            String outputTableId) {
    StoreTableNode store = new StoreTableNode(outputTableId);
    store.setInputSchema(outputSchema);
    store.setOutputSchema(outputSchema);
    return store;
  }

  public static ScanNode newScanPlan(Schema inputSchema,
                                     String inputTableId,
                                     Path inputPath) {
    TableMeta meta = TCatUtil.newTableMeta(inputSchema, StoreType.CSV);
    TableDesc desc = TCatUtil.newTableDesc(inputTableId, meta, inputPath);
    ScanNode newScan = new ScanNode(new QueryBlock.FromTable(desc));
    newScan.setInputSchema(desc.getMeta().getSchema());
    newScan.setOutputSchema(desc.getMeta().getSchema());
    return newScan;
  }

  public static GroupbyNode newGroupbyPlan(Schema inputSchema,
                                           Schema outputSchema,
                                           Column[] keys,
                                           EvalNode havingCondition,
                                           QueryBlock.Target[] targets) {
    GroupbyNode groupby = new GroupbyNode(keys);
    groupby.setInputSchema(inputSchema);
    groupby.setOutputSchema(outputSchema);
    groupby.setHavingCondition(havingCondition);
    groupby.setTargetList(targets);

    return groupby;
  }
}
