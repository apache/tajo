package nta.engine.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.exec.eval.EvalNode;
import nta.engine.json.GsonCreator;
import nta.engine.planner.JoinType;
import nta.engine.utils.TUtil;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * This class contains a set of meta data about a query statement.
 * 
 * @author Hyunsik Choi
 *
 */
public class QueryBlock extends ParseTree {
  private String storeTable = null;
  private boolean projectAll = false;
  private boolean distinct = false;
  /* select target list */
  private Target [] targetList = null;
  /* from clause */
  private List<FromTable> fromTables = Lists.newArrayList();
  /* from clause with join */
  private JoinClause joinClause = null;
  /* where clause */
  private EvalNode whereCond = null;
  /* if true, there is at least one aggregation function. */
  private boolean aggregation = false;
  /* if true, there is at least grouping field. */
  private GroupByClause groupbyClause = null;
  /* having condition */
  private EvalNode havingCond = null;
  /* keys for ordering */
  private SortKey [] sortKeys = null;
  
  public QueryBlock() {
    super(StatementType.SELECT);
  }
  
  public StatementType getStatementType() {
    return this.type;
  }
  
  public void setStoreTable(String storeTable) {
    this.storeTable = storeTable;
  }
  
  public String getStoreTable() {
    return this.storeTable;
  }
  
  public final void setProjectAll() {
    this.projectAll = true;
  }
  
  public final boolean getProjectAll() {
    return this.projectAll;
  }
  
  public final void setDistinct() {
    this.distinct = true;
  }
  
  public final boolean getDistinct() {
    return this.distinct;
  }
  
  public final void setTargetList(Target [] targets) {
    this.targetList = targets;
  }
  
  public final Target [] getTargetList() {
    return this.targetList;
  }
  
  public final boolean hasAggregation() {
    return this.aggregation || hasGroupbyClause();
  }
  
  public final void setAggregation() {
    this.aggregation = true;
  }
  
  public final boolean hasGroupbyClause() {
    return this.groupbyClause != null;
  }
  
  public final void setGroupByClause(final GroupByClause groupbyClause) {
    this.groupbyClause = groupbyClause;
  }
  
  public final GroupByClause getGroupByClause() {
    return this.groupbyClause;
  }
  
  public final boolean hasHavingCond() {
    return this.havingCond != null;
  }
  
  public final void setHavingCond(final EvalNode havingCond) {
    this.havingCond = havingCond;
  }
  
  public final EvalNode getHavingCond() {
    return this.havingCond;
  }
  
  public final boolean hasOrderByClause() {
    return this.sortKeys != null;
  }
  
  public final SortKey [] getSortKeys() {
    return this.sortKeys;
  }
  
  public void setSortKeys(final SortKey [] keys) {
    this.sortKeys = keys;
  }
  
  // From Clause
  public final boolean hasFromClause() {
    return fromTables.size() > 0 || joinClause != null;
  }
  
  public final void addFromTable(final FromTable table) {
    this.fromTables.add(table);
  }
  
  public final boolean hasJoinClause() {
    return this.joinClause != null;
  }
  
  public final void setJoinClause(final JoinClause joinClause) {
    this.joinClause = joinClause;
  }
  
  public final JoinClause getJoinClause() {
    return this.joinClause;
  }
  
  public final int getNumFromTables() {
    return fromTables.size();
  }
  
  public final FromTable [] getFromTables() {
    return this.fromTables.toArray(new FromTable[fromTables.size()]);
  }
  
  public final boolean hasWhereClause() {
    return this.whereCond != null;
  }
  
  public final void setWhereCondition(final EvalNode whereCond) {
    this.whereCond = whereCond;
  }
  
  public final EvalNode getWhereCondition() {
    return this.whereCond;
  }
  
  public static class GroupByClause implements Cloneable {
    @Expose private boolean isEmptyGroupSet = false;
    @Expose private List<GroupElement> groups 
      = new ArrayList<QueryBlock.GroupElement>();
    
    public GroupByClause() {      
    }
    
    public void setEmptyGroupSet() {
      isEmptyGroupSet = true;
    }
    
    public void addGroupSet(GroupElement group) {
      groups.add(group);
      if (isEmptyGroupSet) {
        isEmptyGroupSet = false;
      }
    }
    
    public boolean isEmptyGroupSet() {
      return this.isEmptyGroupSet;
    }
    
    public List<GroupElement> getGroupSet() {
      return Collections.unmodifiableList(groups);
    }
    
    public String toString() {
      Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
          .setPrettyPrinting().create();
      return gson.toJson(this);
    }
    
    public Object clone() throws CloneNotSupportedException {
      GroupByClause g = (GroupByClause) super.clone();
      g.isEmptyGroupSet = isEmptyGroupSet;
      g.groups = new ArrayList<QueryBlock.GroupElement>(groups);
      
      return g;
    }
  }
  
  public static class GroupElement implements Cloneable {
    @Expose private GroupType type;
    @Expose private Column [] columns;
    
    @SuppressWarnings("unused")
    private GroupElement() {
      // for gson
    }
    
    public GroupElement(GroupType type, Column [] columns) {
      this.type = type;
      this.columns = columns;
    }
    
    public GroupType getType() {
      return this.type;
    }
    
    public Column [] getColumns() {
      return this.columns;
    }
    
    public String toString() {
      Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
          .setPrettyPrinting().create();
      return gson.toJson(this);
    }
    
    public Object clone() throws CloneNotSupportedException {
      GroupElement groups = (GroupElement) super.clone();
      groups.type = type;
      groups.columns = new Column[columns.length];      
      for (int i = 0; i < columns.length; i++) {
          groups.columns[i++] = (Column) columns[i].clone();
      }      
      return groups;
    }
  }
  
  public static enum GroupType {
    GROUPBY,
    CUBE,
    ROLLUP,
    EMPTYSET
  }
  
  public static class JoinClause implements Cloneable {
    @Expose private JoinType joinType;
    @Expose private FromTable left;
    @Expose private FromTable right;
    @Expose private JoinClause rightJoin;
    @Expose private EvalNode joinQual;
    @Expose private Column [] joinColumns;
    
    @SuppressWarnings("unused")
    private JoinClause() {
      // for gson
    }
    
    public JoinClause(final JoinType joinType) {
      this.joinType = joinType;
    }
    
    public JoinClause(final JoinType joinType, final FromTable left) {
      this.joinType = joinType;
      this.left = left;
    }
    
    public JoinClause(final JoinType joinType, final FromTable left, 
        final FromTable right) {
      this(joinType, left);
      this.right = right;
    }
    
    public JoinType getJoinType() {
      return this.joinType;
    }
    
    public void setRight(FromTable right) {
      this.right = right;
    }
    
    public void setRight(JoinClause right) {
      this.rightJoin = right;
    }
    
    public boolean hasRightJoin() {
      return rightJoin != null;
    }
    
    public FromTable getLeft() {
      return this.left;
    }
    
    public FromTable getRight() {
      return this.right;
    }
    
    public JoinClause getRightJoin() {
      return this.rightJoin;
    }
    
    public void setJoinQual(EvalNode cond) {
      this.joinQual = cond;
    }
    
    public boolean hasJoinQual() {
      return this.joinQual != null;
    }
    
    public EvalNode getJoinQual() {
      return this.joinQual;
    }
    
    public void setJoinColumns(Column [] columns) {
      this.joinColumns = columns;
    }
    
    public boolean hasJoinColumns() {
      return this.joinColumns != null;
    }
    
    public Column [] getJoinColumns() {
      return this.joinColumns;
    }
    
    
    public String toString() {
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      return gson.toJson(this);
    }
  }
  
  public static class Target implements Cloneable {
	  @Expose private EvalNode eval; 
	  @Expose private Column column;
	  @Expose private String alias = null;
    
    public Target(EvalNode eval) {
      this.eval = eval;
      this.column = new Column(eval.getName(), eval.getValueType());
    }
    
    public Target(final EvalNode eval, final String alias) {
      this(eval);
      this.alias = alias;
    }
    
    public final void setAlias(String alias) {
      this.alias = alias;
    }
    
    public final String getAlias() {
      return alias;
    }

    public final boolean hasAlias() {
      return alias != null;
    }
    
    public EvalNode getEvalTree() {
      return this.eval;
    }
    
    public Column getColumnSchema() {
      return this.column;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder(eval.toString());      
      if(hasAlias()) {
        sb.append(", alias="+alias);
      }
      return sb.toString();
    }
    
    public boolean equals(Object obj) {
      if(obj instanceof Target) {
        Target other = (Target) obj;
        
        boolean b1 = eval.equals(other.eval);
        boolean b2 = column.equals(other.column);
        boolean b3 = TUtil.checkEquals(alias, other.alias);
        
        return b1 && b2 && b3;
      } else {
        return false;
      }
    }
    
    public int hashCode() {
      return this.eval.getName().hashCode();
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
      Target target = (Target) super.clone();
      target.eval = (EvalNode) eval.clone();
      target.column = (Column) column.clone();
      target.alias = alias != null ? alias : null;
      
      return target;
    }
    
    public String toJSON() {
      return GsonCreator.getInstance().toJson(this, Target.class);
    }
  }
  
  public static class FromTable implements Cloneable {
    @Expose
    private TableDesc desc;
    @Expose
    private String alias = null;

    public FromTable(final TableDesc desc) {
      this.desc = desc;
    }

    public FromTable(final TableDesc desc, final String alias) {
      this(desc);
      this.alias = alias;
    }
    
    public final String getTableId() {
      return desc.getId();
    }
    
    public final StoreType getStoreType() {
      return desc.getMeta().getStoreType();
    }
    
    public final Schema getSchema() {
      return desc.getMeta().getSchema();
    }
    
    public final void setAlias(String alias) {
      this.alias = alias;
    }

    public final String getAlias() {
      return alias;
    }

    public final boolean hasAlias() {
      return alias != null;
    }

    public final String toString() {
      if (alias != null)
        return desc.getId() + " as " + alias;
      else
        return desc.getId();
    }
    
    public boolean equals(Object obj) {
      if (obj instanceof FromTable) {
        FromTable other = (FromTable) obj;
        return this.desc.equals(other.desc) 
            && TUtil.checkEquals(this.alias, other.alias);
      } else {
        return false;
      }
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
      FromTable table = (FromTable) super.clone();
      table.desc = (TableDesc) desc.clone();
      table.alias = alias;
      
      return table;
    }
    
    public String toJSON() {
      desc.initFromProto();
      Gson gson = GsonCreator.getInstance();
      return gson.toJson(this, FromTable.class);
    }
  }
  
  public static class SortKey implements Cloneable {
    @Expose private Column sortKey;
    @Expose private boolean ascending = true;
    @Expose private boolean nullFirst = false;
    
    public SortKey(final Column sortKey) {
      this.sortKey = sortKey;
    }
    
    /**
     * 
     * @param sortKey
     * @param asc true if the sort order is ascending order
     * @param nullFirst
     * Otherwise, it should be false.
     */
    public SortKey(final Column sortKey, final boolean asc, 
        final boolean nullFirst) {
      this(sortKey);
      this.ascending = asc;
      this.nullFirst = nullFirst;
    }
    
    public final boolean isAscending() {
      return this.ascending;
    }
    
    public final void setDescOrder() {
      this.ascending = false;
    }
    
    public final boolean isNullFirst() {
      return this.nullFirst;
    }
    
    public final void setNullFirst() {
      this.nullFirst = true;
    }
    
    public final Column getSortKey() {
      return this.sortKey;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
      SortKey key = (SortKey) super.clone();
      key.sortKey = (Column) sortKey.clone();
      key.ascending = ascending;
      
      return key;
    }
    
    public String toString() {
      return "Sortkey (key="+sortKey
          + " "+(ascending == true ? "asc" : "desc")+")"; 
    }
  }
}