/**
 * 
 */
package nta.engine.planner.global;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import nta.catalog.statistics.Stat;
import nta.catalog.statistics.StatSet;
import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.TCommonProtos.StatType;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.cluster.QueryManager;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.query.GlobalQueryPlanner;

/**
 * @author jihoon
 *
 */
public class MockQueryUnitScheduler {

  private final GlobalQueryPlanner planner;
  private final QueryManager qm;
  private final ScheduleUnit plan;
  
  public MockQueryUnitScheduler(GlobalQueryPlanner planner, 
      QueryManager qm, ScheduleUnit plan) {
    this.planner = planner;
    this.qm = qm;
    this.plan = plan;
  }
  
  public void run() throws NoSuchQueryIdException, IOException, InterruptedException {
    recursiveExecuteQueryUnit(plan);
  }
  
  private void recursiveExecuteQueryUnit(ScheduleUnit plan) 
      throws NoSuchQueryIdException, IOException, InterruptedException {
    if (plan.hasPrevQuery()) {
      Iterator<ScheduleUnit> it = plan.getPrevIterator();
      while (it.hasNext()) {
        recursiveExecuteQueryUnit(it.next());
      }
    }
    
    qm.addLogicalQueryUnit(plan);
    QueryUnit[] units = mockLocalize(plan, 3);
    MockWorkerListener [] listener = new MockWorkerListener[3];
    for (int i = 0; i < 3; i++) {
      listener[i] = new MockWorkerListener(units[i].getId());
      listener[i].start();
    }
    for (int i = 0; i < 3; i++) {
      listener[i].join();
    }
    
  }
  
  private QueryUnit[] mockLocalize(ScheduleUnit plan, int n) {
    QueryUnit[] units = new QueryUnit[n];
    for (int i = 0; i < units.length; i++) {
      units[i] = new QueryUnit(QueryIdFactory.newQueryUnitId());
      units[i].setLogicalPlan(plan.getLogicalPlan());
    }
    plan.setQueryUnits(units);
    return units;
  }
  
  private class MockWorkerListener extends Thread {
    private QueryUnitId id;
    
    public MockWorkerListener(QueryUnitId id) {
      this.id = id;
    }
    
    @Override
    public void run() {
      InProgressStatus.Builder builder = InProgressStatus.newBuilder();
      try {
        for (int i = 0; i < 3; i++) {
          Thread.sleep(1000);
          builder.setId("test"+i);
          builder.setProgress(i/3.f);
          builder.setStatus(QueryStatus.INPROGRESS);
          builder.setStats(buildStatSet().getProto());
          qm.updateProgress(id, builder.build());
        }
        builder.setStatus(QueryStatus.FINISHED);
        qm.updateProgress(id, builder.build());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    private StatSet buildStatSet() {
      StatSet statSet = new StatSet();
      Stat stat = new Stat(StatType.COLUMN_NUM_NDV);
      stat.setValue(1);
      statSet.putStat(stat);
      stat = new Stat(StatType.COLUMN_NUM_NULLS);
      stat.setValue(2);
      statSet.putStat(stat);
      stat = new Stat(StatType.TABLE_AVG_ROWS);
      stat.setValue(3);
      statSet.putStat(stat);
      stat = new Stat(StatType.TABLE_NUM_BLOCKS);
      stat.setValue(4);
      statSet.putStat(stat);
      stat = new Stat(StatType.TABLE_NUM_PARTITIONS);
      stat.setValue(5);
      statSet.putStat(stat);
      stat = new Stat(StatType.TABLE_NUM_ROWS);
      stat.setValue(6);
      statSet.putStat(stat);
      
      return statSet;
    }
  }
}
