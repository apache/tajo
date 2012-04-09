/**
 * 
 */
package nta.engine;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import nta.engine.planner.global.ScheduleUnit;

/**
 * @author jihoon
 *
 */
public class SubQuery {

  private final SubQueryId id;
  private Map<LogicalQueryUnitId, ScheduleUnit> units;
  
  public SubQuery(SubQueryId id) {
    this.id = id;
    units = new LinkedHashMap<LogicalQueryUnitId, ScheduleUnit>();
  }
  
  public void addLogicalQueryUnit(ScheduleUnit unit) {
    units.put(unit.getId(), unit);
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public Iterator<ScheduleUnit> getLogicalQueryUnitIterator() {
    return this.units.values().iterator();
  }
  
  public ScheduleUnit getLogicalQueryUnit(LogicalQueryUnitId id) {
    return this.units.get(id);
  }
}
