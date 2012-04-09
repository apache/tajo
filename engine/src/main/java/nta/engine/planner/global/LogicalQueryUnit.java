/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import nta.catalog.Schema;
import nta.engine.LogicalQueryUnitId;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.StoreTableNode;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.ScanNode;
import nta.engine.planner.logical.UnaryNode;

import com.google.common.base.Preconditions;

/**
 * @author jihoon
 *
 */
public class LogicalQueryUnit {
  
  public enum PARTITION_TYPE {
    HASH,
    LIST,
    BROADCAST
  }

  private LogicalQueryUnitId id;
  private LogicalNode plan = null;
  private StoreTableNode store = null;
  private ScanNode[] scan = null;
  private LogicalQueryUnit next;
  private Map<ScanNode, LogicalQueryUnit> prevs;
  private PARTITION_TYPE outputType;
  private QueryUnit[] queryUnits;
  
  public LogicalQueryUnit(LogicalQueryUnitId id) {
    this.id = id;
    prevs = new HashMap<ScanNode, LogicalQueryUnit>();
  }
  
  public void setOutputType(PARTITION_TYPE type) {
    this.outputType = type;
  }
  
  public void setLogicalPlan(LogicalNode plan) {
    Preconditions.checkArgument(plan.getType() == ExprType.STORE);
    
    this.plan = plan;
    store = (StoreTableNode) plan;
    LogicalNode node = plan;
    ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
    s.add(node);
    int i = 0;
    while (!s.isEmpty()) {
      node = s.remove(s.size()-1);
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        s.add(s.size(), unary.getSubNode());
      } else if (node instanceof BinaryNode) {
        scan = new ScanNode[2];
        BinaryNode binary = (BinaryNode) node;
        s.add(s.size(), binary.getOuterNode());
        s.add(s.size(), binary.getInnerNode());
      } else if (node instanceof ScanNode) {
        if (scan == null) {
          scan = new ScanNode[1];
        }
        scan[i++] = (ScanNode) node;
      }
    }
  }
  
  public void setNextQuery(LogicalQueryUnit next) {
    this.next = next;
  }
  
  public void addPrevQuery(ScanNode prevscan, LogicalQueryUnit prev) {
    prevs.put(prevscan, prev);
  }
  
  public void addPrevQueries(Map<ScanNode, LogicalQueryUnit> prevs) {
    this.prevs.putAll(prevs);
  }
  
  public void setQueryUnits(QueryUnit[] queryUnits) {
    this.queryUnits = queryUnits;
  }
  
  public void removePrevQuery(ScanNode prevscan) {
    this.prevs.remove(prevscan);
  }
  
  public LogicalQueryUnit getNextQuery() {
    return this.next;
  }
  
  public boolean hasPrevQuery() {
    return !this.prevs.isEmpty();
  }
  
  public Iterator<LogicalQueryUnit> getPrevIterator() {
    return this.prevs.values().iterator();
  }
  
  public Collection<LogicalQueryUnit> getPrevQueries() {
    return this.prevs.values();
  }
  
  public Map<ScanNode, LogicalQueryUnit> getPrevMaps() {
    return this.prevs;
  }
  
  public LogicalQueryUnit getPrevQuery(ScanNode prevscan) {
    return this.prevs.get(prevscan);
  }
  
  public String getOutputName() {
    return this.store.getTableName();
  }
  
  public PARTITION_TYPE getOutputType() {
    return this.outputType;
  }
  
  public Schema getOutputSchema() {
    return this.store.getOutputSchema();
  }
  
  public StoreTableNode getStoreTableNode() {
    return this.store;
  }
  
  public ScanNode[] getScanNodes() {
    return this.scan;
  }
  
  public LogicalNode getLogicalPlan() {
    return this.plan;
  }
  
  public LogicalQueryUnitId getId() {
    return this.id;
  }
  
  public QueryUnit[] getQueryUnits() {
    return this.queryUnits;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(plan.toString());
    sb.append("next: " + next + " prevs:");
    Iterator<LogicalQueryUnit> it = getPrevIterator();
    while (it.hasNext()) {
      sb.append(" " + it.next());
    }
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof LogicalQueryUnit) {
      LogicalQueryUnit other = (LogicalQueryUnit)o;
      return this.id.equals(other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.id.hashCode();
  }
  
  public int compareTo(LogicalQueryUnit other) {
    return this.id.compareTo(other.id);
  }
}
