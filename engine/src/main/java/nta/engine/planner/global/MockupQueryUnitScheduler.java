/**
 * 
 */
package nta.engine.planner.global;

import java.io.IOException;
import java.util.Iterator;

import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos;
import nta.catalog.statistics.ColumnStat;
import nta.catalog.statistics.TableStat;
import nta.datum.DatumFactory;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.MasterInterfaceProtos.QueryStatus;
import nta.engine.QueryIdFactory;
import nta.engine.QueryUnitId;
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
    for (QueryUnit unit : units) {
      qm.updateQueryUnitStatus(unit.getId(), 1, QueryStatus.QUERY_SUBMITED);
    }
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
          builder.setStatus(QueryStatus.QUERY_INPROGRESS);
          builder.setResultStats(buildStatSet().getProto());
          qm.updateProgress(id, builder.build());
        }
        builder.setStatus(QueryStatus.QUERY_FINISHED);
        qm.updateProgress(id, builder.build());
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (NoSuchQueryIdException ie) {
        ie.printStackTrace();
      }
    }

    private TableStat buildStatSet() {
      TableStat statSet = new TableStat();
      ColumnStat cs1 = new ColumnStat(new Column("test", CatalogProtos.DataType.LONG));
      cs1.setNumDistVals(1);
      cs1.setNumNulls(2);
      cs1.setMinValue(DatumFactory.createLong(5));
      cs1.setMaxValue(DatumFactory.createLong(100));
      statSet.addColumnStat(cs1);
      //statSet.setAvgRows(3);
      statSet.setNumBlocks(4);
      statSet.setNumPartitions(5);
      statSet.setNumRows(6);
      statSet.setNumBytes(7);
      return statSet;
    }
  }
}
