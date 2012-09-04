/**
 * 
 */
package tajo.master;

import tajo.QueryUnitId;
import tajo.ScheduleUnitId;
import tajo.SubQueryId;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterInterfaceProtos.QueryStatus;
import tajo.engine.planner.global.QueryUnit;
import tajo.engine.planner.global.ScheduleUnit;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author jihoon
 *
 */
public class SubQuery extends AbstractQuery {

  private final SubQueryId id;
  private Map<ScheduleUnitId, ScheduleUnit> units;
  private TableStat resultStats;
  private QueryStatus status;
  
  public SubQuery(SubQueryId id) {
    this.id = id;
    units = new HashMap<ScheduleUnitId, ScheduleUnit>();
  }
  
  public void addScheduleUnit(ScheduleUnit unit) {
    units.put(unit.getId(), unit);
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public Iterator<ScheduleUnit> getScheduleUnitIterator() {
    return this.units.values().iterator();
  }
  
  public ScheduleUnit getScheduleUnit(ScheduleUnitId id) {
    return this.units.get(id);
  }
  
  public Collection<ScheduleUnit> getScheduleUnits() {
    return this.units.values();
  }
  
  public QueryUnit getQueryUnit(QueryUnitId id) {
    return this.getScheduleUnit(id.getScheduleUnitId()).getQueryUnit(id);
  }

  public void setTableStat(TableStat stat) {
    this.resultStats = stat;
  }

  public TableStat getTableStat() {
    return this.resultStats;
  }

  public void setStatus(QueryStatus status) {
    this.status = status;
  }

  public QueryStatus getStatus() {
    return this.status;
  }
}