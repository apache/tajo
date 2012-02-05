package nta.engine.parser;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.exec.eval.EvalNode;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.json.GsonCreator;

/**
 * This class contains a set of meta data about a query statement.
 * 
 * @author Hyunsik Choi
 *
 */
public class QueryBlock {
  private StatementType type;
  private String storeTable = null;
  private boolean projectAll = false;
  private boolean distinct = false;
  private Target [] targetList = null;
  private FromTable [] fromTables = null;
  private EvalNode whereCond = null;
  private Column [] groupFields = null;
  private EvalNode havingCond = null;
  private SortKey [] sortKeys = null;
  
  public QueryBlock(StatementType type) {
    this.type = type;
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
  
  public final boolean hasGroupbyClause() {
    return this.groupFields != null;
  }
  
  public final void setGroupingFields(final Column [] groupFields) {
    this.groupFields = groupFields;
  }
  
  public final Column [] getGroupFields() {
    return this.groupFields;
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
    return fromTables != null;
  }
  
  public final void setFromTables(final FromTable [] tables) {
    this.fromTables = tables;
  }
  
  public final int getNumFromTables() {
    return fromTables.length;
  }
  
  public final FromTable [] getFromTables() {
    return this.fromTables;
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
  
  public static class Target {
	  @Expose
    private final EvalNode eval;
	  @Expose
    private Column column;
	  @Expose
    private String alias = null;
    
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
    
    public String toJSON() {
      return GsonCreator.getInstance().toJson(this, Target.class);
    }
    
    public int hashCode() {
      return this.eval.getName().hashCode();
    }
    
    public boolean equals(Object obj) {
      if(obj instanceof Target) {
        return this.eval.getName().equals(((Target)obj).eval.getName());
      } else {
        return false;
      }
    }
  }
  
  public static class FromTable {
    @Expose
	private final TableDesc desc;
    @Expose
    private String alias = null;
    @Expose
    private boolean isFragment;

    public FromTable(final TableDesc desc) {
      this.desc = desc;
      isFragment = desc.getClass().getName().equals(Fragment.class.getName());
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
    
    public String toJSON() {
      desc.initFromProto();
      Gson gson = GsonCreator.getInstance();
      return gson.toJson(this, FromTable.class);
    }
  }
  
  public static class SortKey {
    private final Column sortKey;
    private boolean ascending = true;
    
    public SortKey(final Column sortKey) {
      this.sortKey = sortKey;
    }
    
    /**
     * 
     * @param sortKey
     * @param asc true if the sort order is ascending order. 
     * Otherwise, it should be false.
     */
    public SortKey(final Column sortKey, final boolean asc) {
      this(sortKey);
      this.ascending = asc;
    }
    
    public final boolean isAscending() {
      return this.ascending;
    }
    
    public final void setDesc() {
      this.ascending = false;
    }
    
    public final Column getSortKey() {
      return this.sortKey;
    }
    
    public String toString() {
      return "Sortkey (key="+sortKey
          + " "+(ascending == true ? "asc" : "desc")+")"; 
    }
  }
}