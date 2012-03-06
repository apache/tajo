/**
 * 
 */
package nta.engine.planner.global;

import java.io.IOException;
import java.util.Iterator;

import nta.engine.MasterInterfaceProtos.InProgressStatus;
import nta.engine.MasterInterfaceProtos.QueryStatus;
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
  private final LogicalQueryUnit plan;
  
  public MockQueryUnitScheduler(GlobalQueryPlanner planner, 
      QueryManager qm, LogicalQueryUnit plan) {
    this.planner = planner;
    this.qm = qm;
    this.plan = plan;
  }
  
  public void run() throws NoSuchQueryIdException, IOException, InterruptedException {
    recursiveExecuteQueryUnit(plan);
  }
  
  private void recursiveExecuteQueryUnit(LogicalQueryUnit plan) 
      throws NoSuchQueryIdException, IOException, InterruptedException {
    if (plan.hasPrevQuery()) {
      Iterator<LogicalQueryUnit> it = plan.getPrevIterator();
      while (it.hasNext()) {
        recursiveExecuteQueryUnit(it.next());
      }
    }
    
    qm.addLogicalQueryUnit(plan);
    QueryUnit[] units = mockLocalize(plan, 3);
    qm.addQueryUnits(units);
    MockWorkerListener [] listener = new MockWorkerListener[3];
    for (int i = 0; i < 3; i++) {
      listener[i] = new MockWorkerListener(units[i].getId());
      listener[i].start();
    }
    for (int i = 0; i < 3; i++) {
      listener[i].join();
    }
    
  }
  
  private QueryUnit[] mockLocalize(LogicalQueryUnit plan, int n) {
    QueryUnit[] units = new QueryUnit[n];
    for (int i = 0; i < units.length; i++) {
      units[i] = new QueryUnit(QueryIdFactory.newQueryUnitId());
      units[i].setLogicalPlan(plan.getLogicalPlan());
    }
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
          qm.updateProgress(id, builder.build());
        }
        builder.setStatus(QueryStatus.FINISHED);
        qm.updateProgress(id, builder.build());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
