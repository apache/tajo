/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.parser;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.json.GsonCreator;
import org.apache.tajo.engine.planner.JoinType;
import org.apache.tajo.engine.planner.PlanningContext;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class contains a set of meta data about a query statement
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
  private SortSpec [] sortKeys = null;
  /* limit clause */
  private LimitClause limitClause = null;

  public QueryBlock(PlanningContext context) {
    super(context, StatementType.SELECT);
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

  public final boolean isDistinct() {
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

  public final boolean hasLimitClause() {
    return this.limitClause != null;
  }

  public final SortSpec[] getSortKeys() {
    return this.sortKeys;
  }

  public void setSortKeys(final SortSpec [] keys) {
    this.sortKeys = keys;
  }

  public void setLimit(final LimitClause limit) {
    this.limitClause = limit;
  }

  public LimitClause getLimitClause() {
    return this.limitClause;
  }

  // From Clause
  public final boolean hasFromClause() {
    return fromTables.size() > 0 || joinClause != null;
  }

  public final void addFromTable(final FromTable table, boolean global) {
    super.addTableRef(table);
    this.fromTables.add(table);
  }

  public final boolean hasExplicitJoinClause() {
    return joinClause != null;
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
    @Expose private JoinClause leftJoin;
    @Expose private EvalNode joinQual;
    @Expose private Column [] joinColumns;
    @Expose private boolean natural = false;

    @SuppressWarnings("unused")
    private JoinClause() {
      // for gson
    }

    public JoinClause(final JoinType joinType) {
      this.joinType = joinType;
    }

    public JoinClause(final JoinType joinType, final FromTable right) {
      this.joinType = joinType;
      this.right = right;
    }

    public JoinClause(final JoinType joinType, final FromTable left,
                      final FromTable right) {
      this(joinType, right);
      this.left = left;
    }

    public JoinType getJoinType() {
      return this.joinType;
    }

    public void setNatural() {
      this.natural = true;
    }

    public boolean isNatural() {
      return this.natural;
    }

    public void setRight(FromTable right) {
      this.right = right;
    }

    public void setLeft(FromTable left) {
      this.left = left;
    }

    public void setLeft(JoinClause left) {
      this.leftJoin = left;
    }

    public boolean hasLeftJoin() {
      return leftJoin != null;
    }

    public FromTable getLeft() {
      return this.left;
    }

    public FromTable getRight() {
      return this.right;
    }

    public JoinClause getLeftJoin() {
      return this.leftJoin;
    }

    public void setJoinQual(EvalNode qual) {
      this.joinQual = qual;
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
    @Expose private int targetId;

    public Target(EvalNode eval, int targetId) {
      this.eval = eval;
      if (eval.getType() == EvalNode.Type.AGG_FUNCTION &&
          eval.getValueType().length > 1) { // hack for partial result
        this.column = new Column(eval.getName(), Type.ARRAY);
      } else {
        this.column = new Column(eval.getName(), eval.getValueType()[0]);
      }
      this.targetId = targetId;
    }

    public Target(final EvalNode eval, final String alias, int targetId) {
      this(eval, targetId);
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

    public int getTargetId() {
      return this.targetId;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder(eval.toString());
      if(hasAlias()) {
        sb.append(", alias=").append(alias);
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

    public FromTable() {}

    public FromTable(final TableDesc desc) {
      this.desc = desc;
    }

    public FromTable(final TableDesc desc, final String alias) {
      this(desc);
      this.alias = alias;
    }

    public final String getTableName() {
      return desc.getId();
    }

    public final String getTableId() {
      return alias == null ? desc.getId() : alias;
    }

    public final CatalogProtos.StoreType getStoreType() {
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

  public static class LimitClause implements Cloneable {
    @Expose private long fetchFirstNum;

    public LimitClause(long fetchFirstNum) {
      this.fetchFirstNum = fetchFirstNum;
    }

    public long getLimitRow() {
      return this.fetchFirstNum;
    }

    @Override
    public String toString() {
      return "LIMIT " + fetchFirstNum;
    }

    public boolean equals(Object obj) {
      return obj instanceof LimitClause &&
          fetchFirstNum == ((LimitClause)obj).fetchFirstNum;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      LimitClause newLimit = (LimitClause) super.clone();
      newLimit.fetchFirstNum = fetchFirstNum;
      return newLimit;
    }
  }
}