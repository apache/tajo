/**
 * 
 */
package nta.engine.planner.global;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.statistics.Stat;
import nta.catalog.statistics.StatSet;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
import nta.engine.TCommonProtos.StatType;
import nta.engine.cluster.QueryManager;
import nta.engine.exception.NoSuchQueryIdException;
import nta.engine.query.GlobalPlanner;

/**
 * @author jihoon
 *
 */
public class MockupQueryUnitScheduler {

  private final QueryManager qm;
  private final ScheduleUnit plan;
  
  public MockupQueryUnitScheduler(GlobalPlanner planner, 
      QueryManager qm, ScheduleUnit plan) {
    this.qm = qm;
    this.plan = plan;
  }
  
  public void run() throws NoSuchQueryIdException, IOException, InterruptedException {
    recursiveExecuteQueryUnit(plan);
  }
  
  private void recursiveExecuteQueryUnit(ScheduleUnit plan) 
      throws NoSuchQueryIdException, IOException, InterruptedException {
    if (plan.hasChildQuery()) {
      Iterator<ScheduleUnit> it = plan.getChildIterator();
      while (it.hasNext()) {
        recursiveExecuteQueryUnit(it.next());
      }
    }
    
    qm.addScheduleUnit(plan);
    QueryUnit[] units = localize(plan, 3);
    MockupWorkerListener [] listener = new MockupWorkerListener[3];
    for (int i = 0; i < 3; i++) {
      listener[i] = new MockupWorkerListener(units[i].getId());
      listener[i].start();
    }
    for (int i = 0; i < 3; i++) {
      listener[i].join();
    }
    
  }
  
  public QueryUnit[] localize(ScheduleUnit plan, int n) {
    QueryUnit[] units = new QueryUnit[n];
    for (int i = 0; i < units.length; i++) {
      units[i] = new QueryUnit(QueryIdFactory.newQueryUnitId(plan.getId()));
      units[i].setLogicalPlan(plan.getLogicalPlan());
    }
    plan.setQueryUnits(units);
    return units;
  }
  
  private class MockupWorkerListener extends Thread {
    private QueryUnitId id;
    
    public MockupWorkerListener(QueryUnitId id) {
      this.id = id;
    }
    
    @Override
    public void run() {
      InProgressStatusProto.Builder builder = InProgressStatusProto.newBuilder();
      try {
        for (int i = 0; i < 3; i++) {
          Thread.sleep(1000);
          builder.setId(id.getProto());
          builder.setProgress(i/3.f);
          builder.setStatus(QueryStatus.INPROGRESS);
          builder.setStats(buildStatSet().getProto());
          qm.updateProgress(id, builder.build());
        }
        builder.setStatus(QueryStatus.FINISHED);
        qm.updateProgress(id, builder.build());
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (NoSuchQueryIdException ie) {
        ie.printStackTrace();
      }
    }
    
    private StatSet buildStatSet() {
      StatSet statSet = new StatSet();
      Stat stat = new Stat(StatType.COLUMN_NUM_NDV);
      stat.setValue(1);
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
