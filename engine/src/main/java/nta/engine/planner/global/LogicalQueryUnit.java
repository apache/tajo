/**
 * 
 */
package nta.engine.planner.global;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import nta.catalog.Schema;
import nta.engine.LogicalQueryUnitId;
import nta.engine.planner.logical.BinaryNode;
import nta.engine.planner.logical.CreateTableNode;
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
  
  public enum Phase {
    LOCAL,
    MAP,
    MERGE
  }

  private LogicalQueryUnitId id;
  private LogicalNode plan = null;
  private CreateTableNode store = null;
  private ScanNode[] scan = null;
  private LogicalQueryUnit next;
  private Set<LogicalQueryUnit> prevs;
  private Phase phase;
  
  public LogicalQueryUnit(LogicalQueryUnitId id, Phase phase) {
    this.id = id;
    this.phase = phase;
    prevs = new HashSet<LogicalQueryUnit>();
  }
  
  public void setLogicalPlan(LogicalNode plan) {
    Preconditions.checkArgument(plan.getType() == ExprType.STORE);
    
    this.plan = plan;
    store = (CreateTableNode) plan;
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
  
  public void addPrevQuery(LogicalQueryUnit prev) {
    prevs.add(prev);
  }
  
  public LogicalQueryUnit getNextQuery() {
    return this.next;
  }
  
  public boolean hasPrevQuery() {
    return !this.prevs.isEmpty();
  }
  
  public Iterator<LogicalQueryUnit> getPrevIterator() {
    return this.prevs.iterator();
  }
  
  public String getOutputName() {
    return this.store.getTableName();
  }
  
  public Phase getPhase() {
    return this.phase;
  }
  
  public Schema getOutputSchema() {
    return this.store.getOutputSchema();
  }
  
  public CreateTableNode getStoreTableNode() {
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
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(plan.toString());
    sb.append("next: " + next + " prevs:");
    Iterator<LogicalQueryUnit> it = prevs.iterator();
    while (it.hasNext()) {
      sb.append(" " + it.next());
    }
    return sb.toString();
  }
}
