/**
 * 
 */
package nta.engine.planner.global;

import com.google.common.base.Preconditions;
import nta.catalog.Schema;
import nta.catalog.statistics.TableStat;
import nta.engine.AbstractQuery;
import nta.engine.QueryUnitId;
import nta.engine.ScheduleUnitId;
import nta.engine.planner.logical.*;
import nta.engine.query.Priority;

import java.util.*;


/**
 * @author jihoon
 *
 */
public class ScheduleUnit extends AbstractQuery {
  
  public enum PARTITION_TYPE {
    /** for hash partitioning */
    HASH,
    LIST,
    /** for map-side join */
    BROADCAST,
    /** for range partitioning */
    RANGE
  }

  private ScheduleUnitId id;
  private LogicalNode plan = null;
  private StoreTableNode store = null;
  private List<ScanNode> scanlist = null;
  private ScheduleUnit next;
  private Map<ScanNode, ScheduleUnit> prevs;
  private PARTITION_TYPE outputType;
  private QueryUnit[] queryUnits;
  private boolean hasJoinPlan;
  private boolean hasUnionPlan;
  private Priority priority;
  private TableStat stats;
  
  public ScheduleUnit(ScheduleUnitId id) {
    this.id = id;
    prevs = new HashMap<ScanNode, ScheduleUnit>();
    scanlist = new ArrayList<ScanNode>();
    hasJoinPlan = false;
    hasUnionPlan = false;
  }
  
  public void setOutputType(PARTITION_TYPE type) {
    this.outputType = type;
  }
  
  public void setLogicalPlan(LogicalNode plan) {
    hasJoinPlan = false;
    Preconditions.checkArgument(plan.getType() == ExprType.STORE 
        || plan.getType() == ExprType.CREATE_INDEX);

    this.plan = plan;
    if (plan instanceof StoreTableNode) {
      store = (StoreTableNode) plan;      
    } else {
      store = (StoreTableNode) ((IndexWriteNode)plan).getSubNode();
    }
    
    LogicalNode node = plan;
    ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
    s.add(node);
    while (!s.isEmpty()) {
      node = s.remove(s.size()-1);
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        s.add(s.size(), unary.getSubNode());
      } else if (node instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) node;
        if (binary.getType() == ExprType.JOIN) {
          hasJoinPlan = true;
        } else if (binary.getType() == ExprType.UNION) {
          hasUnionPlan = true;
        }
        s.add(s.size(), binary.getOuterNode());
        s.add(s.size(), binary.getInnerNode());
      } else if (node instanceof ScanNode) {
        scanlist.add((ScanNode)node);
      }
    }
  }

  public boolean hasJoinPlan() {
    return this.hasJoinPlan;
  }

  public boolean hasUnionPlan() {
    return this.hasUnionPlan;
  }
  
  public void setParentQuery(ScheduleUnit next) {
    this.next = next;
  }
  
  public void addChildQuery(ScanNode prevscan, ScheduleUnit prev) {
    prevs.put(prevscan, prev);
  }
  
  public void addChildQueries(Map<ScanNode, ScheduleUnit> prevs) {
    this.prevs.putAll(prevs);
  }
  
  public void setQueryUnits(QueryUnit[] queryUnits) {
    this.queryUnits = queryUnits;
  }
  
  public void removeChildQuery(ScanNode scan) {
    scanlist.remove(scan);
    this.prevs.remove(scan);
  }
  
  public void removeScan(ScanNode scan) {
    scanlist.remove(scan);
  }
  
  public void addScan(ScanNode scan) {
    scanlist.add(scan);
  }

  public void setPriority(Priority priority) {
    this.priority = priority;
  }

  public void setPriority(int priority) {
    if (this.priority == null) {
      this.priority = new Priority(priority);
    }
  }

  public void setStats(TableStat stat) {
    this.stats = stat;
  }
  
  public ScheduleUnit getParentQuery() {
    return this.next;
  }
  
  public boolean hasChildQuery() {
    return !this.prevs.isEmpty();
  }
  
  public Iterator<ScheduleUnit> getChildIterator() {
    return this.prevs.values().iterator();
  }
  
  public Collection<ScheduleUnit> getChildQueries() {
    return this.prevs.values();
  }
  
  public Map<ScanNode, ScheduleUnit> getChildMaps() {
    return this.prevs;
  }
  
  public ScheduleUnit getChildQuery(ScanNode prevscan) {
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
    return this.scanlist.toArray(new ScanNode[scanlist.size()]);
  }
  
  public LogicalNode getLogicalPlan() {
    return this.plan;
  }
  
  public ScheduleUnitId getId() {
    return this.id;
  }
  
  public QueryUnit[] getQueryUnits() {
    return this.queryUnits;
  }
  
  public QueryUnit getQueryUnit(QueryUnitId qid) {
    for (QueryUnit unit : queryUnits) {
      if (unit.getId().equals(qid)) {
        return unit;
      }
    }
    return null;
  }

  public Priority getPriority() {
    return this.priority;
  }

  public TableStat getStats() {
    return this.stats;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.id);
/*    sb.append(" plan: " + plan.toString());
    sb.append("next: " + next + " prevs:");
    Iterator<ScheduleUnit> it = getChildIterator();
    while (it.hasNext()) {
      sb.append(" " + it.next());
    }*/
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof ScheduleUnit) {
      ScheduleUnit other = (ScheduleUnit)o;
      return this.id.equals(other.getId());
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.id.hashCode();
  }
  
  public int compareTo(ScheduleUnit other) {
    return this.id.compareTo(other.id);
  }
}
