/**
 * 
 */
package nta.engine;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import nta.engine.planner.global.LogicalQueryUnit;
import nta.engine.planner.global.LogicalQueryUnitGraph;

/**
 * @author jihoon
 *
 */
public class SubQuery {

  private final SubQueryId id;
  private Map<LogicalQueryUnitId, LogicalQueryUnit> units;
  
  public SubQuery(SubQueryId id) {
    this.id = id;
    units = new LinkedHashMap<LogicalQueryUnitId, LogicalQueryUnit>();
  }
  
  public void addLogicalQueryUnit(LogicalQueryUnit unit) {
    units.put(unit.getId(), unit);
  }
  
  public SubQueryId getId() {
    return this.id;
  }
  
  public Iterator<LogicalQueryUnit> getLogicalQueryUnitIterator() {
    return this.units.values().iterator();
  }
  
  public LogicalQueryUnit getLogicalQueryUnit(LogicalQueryUnitId id) {
    return this.units.get(id);
  }
}
