/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.parser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import tajo.catalog.*;
import tajo.catalog.exception.NoSuchTableException;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.GeneralFunction;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.FunctionType;
import tajo.catalog.proto.CatalogProtos.IndexMethod;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.datum.DatumFactory;
import tajo.engine.eval.*;
import tajo.engine.parser.QueryBlock.*;
import tajo.engine.planner.JoinType;
import tajo.engine.planner.PlanningContext;
import tajo.engine.planner.PlanningContextImpl;
import tajo.engine.query.exception.*;
import tajo.exception.InternalException;

import java.util.ArrayList;
import java.util.List;

/**
 * This class transforms a query statement into a QueryBlock. 
 *
 * @author Hyunsik Choi
 *
 * @see tajo.engine.parser.QueryBlock
 */
public final class QueryAnalyzer {
  private static final Log LOG = LogFactory.getLog(QueryAnalyzer.class);
  private final CatalogService catalog;

  public QueryAnalyzer(CatalogService catalog) {
    this.catalog = catalog;
  }

  public PlanningContext parse(final String query) {

    CommonTree ast;
    PlanningContextImpl context;
    try {
      ast = parseTree(query);
      context = new PlanningContextImpl(query);
      ParseTree parseTree = parseQueryTree(context,ast);
      context.setParseTree(parseTree);
    } catch (TQLParseError e) {
      throw new TQLSyntaxError(query, e.getMessage());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Analyzer: " + ast.toStringTree());
    }

    return context;
  }

  private ParseTree parseQueryTree(PlanningContextImpl context,
                                   final CommonTree ast) {
    ParseTree parseTree = null;

    switch (getCmdType(ast)) {
      case SELECT:
        parseTree = parseSelectStatement(context, ast);
        break;

      case UNION:
      case EXCEPT:
      case INTERSECT:
        parseTree = parseSetStatement(context, ast);
        break;

      case CREATE_INDEX:
        parseTree = parseIndexStatement(context, ast);
        break;

      case CREATE_TABLE:
        parseTree = parseCreateStatement(context, ast);
        break;

      case COPY:
        parseTree = parseCopyStatement(context, ast);
        break;

      default:
        break;
    }

    return parseTree;
  }

  private CopyStmt parseCopyStatement(final PlanningContextImpl context,
                                      final CommonTree ast) {
    CopyStmt stmt = new CopyStmt(context);
    int idx = 0;
    stmt.setTableName(ast.getChild(idx++).getText());
    Path path = new Path(ast.getChild(idx++).getText());
    stmt.setPath(path);
    StoreType storeType = CatalogUtil.getStoreType(ast.getChild(idx).getText());
    stmt.setStoreType(storeType);

    if ((ast.getChildCount() - idx) > 1) {
      idx++;
      if (ast.getChild(idx).getType() == NQLParser.PARAMS) {
        Options options = parseParams((CommonTree) ast.getChild(idx));
        stmt.setParams(options);
      }
    }

    return stmt;
  }

  /**
   * t=table ASSIGN select_stmt -> ^(CREATE_TABLE $t select_stmt)
   * | CREATE TABLE t=table AS select_stmt -> ^(CREATE_TABLE $t select_stmt)
   *
   * @param ast
   * @return
   */
  private CreateTableStmt parseCreateStatement(final PlanningContextImpl context,
                                               final CommonTree ast) {
    CreateTableStmt stmt;

    int idx = 0;
    CommonTree node;
    String tableName = ast.getChild(idx).getText();
    idx++;

    boolean external = false;
    Schema tableDef = null;
    StoreType storeType = null;
    Options options = null;
    QueryBlock selectStmt = null;
    Path location = null;
    while(idx < ast.getChildCount()) {
      node = (CommonTree) ast.getChild(idx);
      switch (node.getType()) {
        case NQLParser.EXTERNAL:
          external = true;
          break;

        case NQLParser.TABLE_DEF:
          tableDef = parseCreateTableDef(node);
          break;

        case NQLParser.FORMAT:
        case NQLParser.USING:
          storeType = CatalogUtil.getStoreType(node.getChild(0).getText());
          break;

        case NQLParser.PARAMS:
          options = parseParams(node);
          break;

        case NQLParser.AS:
          selectStmt = parseSelectStatement(context, (CommonTree) node.getChild(0));
          break;

        case NQLParser.LOCATION:
          location = new Path(node.getChild(0).getText());
          break;

        default:
          throw new NotSupportQueryException("ERROR: not yet supported query");
      }
      idx++;
    }

    if (selectStmt != null) {
      stmt = new CreateTableStmt(context, tableName, selectStmt);
    } else {
      stmt = new CreateTableStmt(context, tableName);
      if (external) {
        if (location != null) {
          stmt.setPath(location);
        }
      }
    }

    if (tableDef != null) {
      stmt.setTableDef(tableDef);
    }

    if (storeType != null) {
      stmt.setStoreType(storeType);
      if (options != null) {
        stmt.setOptions(options);
      }
    }

    // constraints
    if (external && location == null) {
      throw new TQLSyntaxError(context.getRawQuery(), "CREATE EXTERNAL TABLE statement requires LOCATION clause.");
    }
    if (!external && location != null) {
      throw new TQLSyntaxError(context.getRawQuery(), "LOCATION clause can be only used in CREATE EXTERNAL TABLE statement.");
    }
    if (tableDef == null && location != null) {
      throw new TQLSyntaxError(context.getRawQuery(), "LOCATION clause requires a schema definition.");
    }

    return stmt;
  }

  private Schema parseCreateTableDef(final CommonTree ast) {
    Schema tableDef = new Schema();
    DataType type;
    for (int i = 0; i < ast.getChildCount(); i++) {
      switch(ast.getChild(i).getChild(1).getType()) {
        case NQLParser.BOOL: type = DataType.BOOLEAN; break;
        case NQLParser.BYTE: type = DataType.BYTE; break;
        case NQLParser.INT: type = DataType.INT; break;
        case NQLParser.LONG: type = DataType.LONG; break;
        case NQLParser.FLOAT: type = DataType.FLOAT; break;
        case NQLParser.DOUBLE: type = DataType.DOUBLE; break;
        case NQLParser.TEXT: type = DataType.STRING; break;
        case NQLParser.BYTES: type = DataType.BYTES; break;
        case NQLParser.IPv4: type = DataType.IPv4; break;
        default: throw new InvalidQueryException(ast.toStringTree());
      }

      tableDef.addColumn(ast.getChild(i).getChild(0).getText(), type);
    }

    return tableDef;
  }

  private SetStmt parseSetStatement(final PlanningContextImpl context,
                                    final CommonTree ast) {
    StatementType type;
    boolean distinct = true;
    ParseTree left;
    ParseTree right;

    switch (ast.getType()) {
      case NQLParser.UNION:
        type = StatementType.UNION;
        break;
      case NQLParser.EXCEPT:
        type = StatementType.EXCEPT;
        break;
      case NQLParser.INTERSECT:
        type = StatementType.INTERSECT;
        break;
      default:
        throw new InvalidQueryException("Illegal AST:\n" + ast.toStringTree());
    }

    int idx = 0;
    left = parseQueryTree(context, (CommonTree) ast.getChild(idx));
    idx++;
    int nodeType = ast.getChild(idx).getType();
    if (nodeType == NQLParser.ALL) {
      distinct = true;
      idx++;
    } else if (nodeType == NQLParser.DISTINCT) {
      distinct = false;
      idx++;
    }
    right = parseQueryTree(context, (CommonTree) ast.getChild(idx));
    return new SetStmt(context, type, left, right, distinct);
  }

  private QueryBlock parseSelectStatement(PlanningContext context,
                                          final CommonTree ast) {

    QueryBlock block = new QueryBlock(context);

    CommonTree node;
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
        case NQLParser.FROM:
          parseFromClause(context, block, node);
          break;

        case NQLParser.SET_QUALIFIER:
          parseSetQualifier(block, node);
          break;

        case NQLParser.SEL_LIST:
          parseSelectList(context, block, node);
          break;

        case NQLParser.WHERE:
          parseWhereClause(context, block, node);
          break;

        case NQLParser.GROUP_BY:
          parseGroupByClause(context, block, node);
          break;

        case NQLParser.HAVING:
          parseHavingClause(context, block, node);
          break;

        case NQLParser.ORDER_BY:
          SortSpec[] sortKeys = parseSortSpecifiers(context, block,
              (CommonTree) node.getChild(0));
          block.setSortKeys(sortKeys);
          break;

        case NQLParser.LIMIT:
          block.setLimit(parseLimitClause(context, block, node));
          break;

        default:

      }
    }

    return block;
  }

  private void parseSetQualifier(final QueryBlock block, final CommonTree ast) {
    int idx = 0;

    if (ast.getChild(idx).getType() == NQLParser.DISTINCT) {
      block.setDistinct();
    }
  }

  /**
   * EBNF: CREATE (UNIQUE?) INDEX n=ID ON t=ID LEFT_PAREN s=sort_specifier_list 
   * RIGHT_PAREN p=param_clause? <br />
   * AST:  ^(CREATE_INDEX $n $t $s $p)
   *
   * @param ast
   */
  private CreateIndexStmt parseIndexStatement(PlanningContext context,
                                              final CommonTree ast) {
    CreateIndexStmt indexStmt = new CreateIndexStmt(context);

    int idx = 0;
    boolean unique = false;
    // the below things are optional
    if (ast.getChild(idx).getType() == NQLParser.UNIQUE) {
      unique = true;
      idx++;
    }

    IndexMethod method = null;
    if (ast.getChild(idx).getType() == NQLParser.USING) {
      method = getIndexMethod(ast.getChild(idx).getText());
      idx++;
    }

    // It's optional, so it can be null if there is no params clause.
    Options params = null;
    if (ast.getChild(idx).getType() == NQLParser.PARAMS) {
      params = parseParams((CommonTree) ast.getChild(idx));
      idx++;
    }

    // They are required, so they are always filled.
    String idxName = ast.getChild(idx++).getText();
    String tbName = ast.getChild(idx++).getText();
    indexStmt.setIndexName(idxName);
    indexStmt.setTableName(tbName);

    SortSpec [] sortSpecs = parseSortSpecifiers(context, indexStmt,
        (CommonTree) ast.getChild(idx++));

    if (unique) {
      indexStmt.setUnique();
    }
    indexStmt.setSortSpecs(sortSpecs);

    if (method != null) {
      indexStmt.setMethod(method);
    }

    if (params != null) {
      indexStmt.setParams(params);
    }

    return indexStmt;
  }

  /**
   * EBNF: table_list -> tableRef (COMMA tableRef)
   * @param block
   * @param ast
   */
  private void parseFromClause(PlanningContext context,
                               final QueryBlock block, final CommonTree ast) {
    // implicit join or the from clause on single relation
    boolean isPrevJoin = false;
    FromTable table = null;
    JoinClause joinClause = null;
    CommonTree node;
    for (int i = 0; i < ast.getChildCount(); i++) {
      node = (CommonTree) ast.getChild(i);

      switch (node.getType()) {

        case NQLParser.TABLE:
          // table (AS ID)?
          // 0 - a table name, 1 - table alias
          table = parseTable(node);
          block.addFromTable(table, true);
          break;
        case NQLParser.JOIN:
          QueryBlock.JoinClause newJoin = parseExplicitJoinClause(context, block, node);
          if (isPrevJoin) {
            newJoin.setLeft(joinClause);
            joinClause = newJoin;
          } else {
            newJoin.setLeft(table);
            isPrevJoin = true;
            joinClause = newJoin;
          }
          break;
        default:
          throw new TQLSyntaxError(context.getRawQuery(), "Wrong From Clause");
      } // switch
    } // for each derievedTable

    if (joinClause != null) {
      block.setJoinClause(joinClause);
    }
  }


  private JoinClause parseExplicitJoinClause(final PlanningContext context,
                                             final QueryBlock block,
                                             final CommonTree ast) {

    int idx = 0;
    int parsedJoinType = ast.getChild(idx).getType();

    JoinClause joinClause;

    switch (parsedJoinType) {
      case NQLParser.CROSS:
      case NQLParser.UNION:
        joinClause = parseCrossAndUnionJoin(context, block, ast);
        break;

      case NQLParser.NATURAL:
        joinClause = parseNaturalJoinClause(context, block, ast);
        break;

      case NQLParser.INNER:
      case NQLParser.OUTER:
        joinClause = parseQualifiedJoinClause(context, block, ast, 0);
        break;

      default: // default join (without join type) is inner join
        joinClause = parseQualifiedJoinClause(context, block, ast, 0);
    }

    return joinClause;
  }

  private JoinClause parseNaturalJoinClause(final PlanningContext context,
                                            final QueryBlock block, Tree ast) {
    JoinClause join = parseQualifiedJoinClause(context, block, ast, 1);
    join.setNatural();
    return join;
  }

  private JoinClause parseQualifiedJoinClause(PlanningContext context,
                                              QueryBlock block,
                                              Tree ast, final int idx) {
    int childIdx = idx;
    JoinClause join = null;

    if (ast.getChild(childIdx).getType() == NQLParser.TABLE) { // default join
      join = new JoinClause(JoinType.INNER);
      join.setRight(parseTable((CommonTree) ast.getChild(childIdx)));
      block.addFromTable(join.getRight(), true);

    } else {

      if (ast.getChild(childIdx).getType() == NQLParser.INNER) {
        join = new JoinClause(JoinType.INNER);

      } else if (ast.getChild(childIdx).getType() == NQLParser.OUTER) {

        switch (ast.getChild(childIdx).getChild(0).getType()) {
          case NQLParser.LEFT:
            join = new JoinClause(JoinType.LEFT_OUTER);
            break;
          case NQLParser.RIGHT:
            join = new JoinClause(JoinType.RIGHT_OUTER);
            break;
          case NQLParser.FULL:
            join = new JoinClause(JoinType.FULL_OUTER);
            break;
          default:
            throw new TQLSyntaxError(context.getRawQuery(), "Unknown Join Type");
        }
      }

      childIdx++;
      join.setRight(parseTable((CommonTree) ast.getChild(childIdx)));
      block.addFromTable(join.getRight(), true);
    }

    childIdx++;

    if (ast.getChildCount() > childIdx) {
      CommonTree joinQual = (CommonTree) ast.getChild(childIdx);

      if (joinQual.getType() == NQLParser.ON) {
        EvalNode joinCond = parseJoinCondition(context, block, joinQual);
        join.setJoinQual(joinCond);

      } else if (joinQual.getType() == NQLParser.USING) {
        Column[] joinColumns = parseJoinColumns(context, block, joinQual);
        join.setJoinColumns(joinColumns);
      }
    }

    return join;
  }

  private JoinClause parseCrossAndUnionJoin(final PlanningContext context,
                                            final QueryBlock block, Tree ast) {
    JoinType joinType;

    if (ast.getChild(0).getType() == NQLParser.CROSS) {
      joinType = JoinType.CROSS_JOIN;
    } else if (ast.getChild(0).getType() == NQLParser.UNION) {
      joinType = JoinType.UNION;
    } else {
      throw new IllegalStateException("Neither the AST has cross join or union join:\n" + ast.toStringTree());
    }

    JoinClause join = new JoinClause(joinType);
    Preconditions.checkState(ast.getChild(1).getType() == NQLParser.TABLE);
    join.setRight(parseTable((CommonTree) ast.getChild(1)));
    block.addFromTable(join.getRight(), true);

    return join;
  }

  private Column [] parseJoinColumns(final PlanningContext context,
                                     final QueryBlock block,
                                     final CommonTree ast) {
    Column [] joinColumns = new Column[ast.getChildCount()];

    for (int i = 0; i < ast.getChildCount(); i++) {
      joinColumns[i] = checkAndGetColumnByAST(context,
          block, (CommonTree) ast.getChild(i));
    }
    return joinColumns;
  }

  private EvalNode parseJoinCondition(final PlanningContext context,
                                      QueryBlock tree, CommonTree ast) {
    return createEvalTree(context, tree, ast.getChild(0));
  }

  private FromTable parseTable(final CommonTree tableAST) {
    String tableName = tableAST.getChild(0).getText();
    TableDesc desc = checkAndGetTableByName(tableName);
    FromTable table;

    if (tableAST.getChildCount() > 1) {
      table = new FromTable(desc,
          tableAST.getChild(1).getText());
    } else {
      table = new FromTable(desc);
    }

    return table;
  }

  /**
   * This method parses the select list of a query statement.
   * <pre>
   * EBNF: 
   *
   * selectList
   * : MULTIPLY -> ^(SEL_LIST ALL)
   * | derivedColumn (COMMA derivedColumn)* -> ^(SEL_LIST derivedColumn+)
   * ;
   *
   * derivedColumn
   * : bool_expr asClause? -> ^(COLUMN bool_expr asClause?)
   * ;
   *
   * @param block
   * @param ast
   */
  private void parseSelectList(final PlanningContext context,
                               final QueryBlock block, final CommonTree ast) {

    if (ast.getChild(0).getType() == NQLParser.ALL) {
      block.setProjectAll();
    } else {
      CommonTree node;
      int numTargets = ast.getChildCount();
      Target [] targets = new Target[numTargets];
      EvalNode evalTree;
      String alias;

      // the final one for each target is the alias
      // EBNF: bool_expr AS? fieldName
      for (int i = 0; i < ast.getChildCount(); i++) {
        node = (CommonTree) ast.getChild(i);
        evalTree = createEvalTree(context, block, node);
        targets[i] = new Target(evalTree, i);
        if (node.getChildCount() > 1) {
          alias = node.getChild(node.getChildCount() - 1).getChild(0).getText();
          targets[i].setAlias(alias);
        }
      }

      block.setTargetList(targets);
    }
  }

  private void parseWhereClause(final PlanningContext context,
                                final QueryBlock block, final CommonTree ast) {
    EvalNode whereCond = createEvalTree(context, block, ast.getChild(0));
    block.setWhereCondition(whereCond);
  }

  /**
   * See 'groupby_clause' rule in NQL.g
   *
   * @param block
   * @param ast
   */
  private void parseGroupByClause(final PlanningContext context,
                                  final QueryBlock block,
                                  final CommonTree ast) {
    GroupByClause clause = new GroupByClause();

    int idx = 0;

    if (ast.getChild(idx).getType() == NQLParser.EMPTY_GROUPING_SET) {
      clause.setEmptyGroupSet();
    } else {
      // the remain ones are grouping fields.
      Tree group;
      List<Column> columnRefs = new ArrayList<>();
      Column [] columns;
      Column column;
      for (; idx < ast.getChildCount(); idx++) {
        group = ast.getChild(idx);
        switch (group.getType()) {
          case NQLParser.CUBE:
            columns = parseColumnReferences(context, block, (CommonTree) group);
            GroupElement cube = new GroupElement(GroupType.CUBE, columns);
            clause.addGroupSet(cube);
            break;

          case NQLParser.ROLLUP:
            columns = parseColumnReferences(context, block, (CommonTree) group);
            GroupElement rollup = new GroupElement(GroupType.ROLLUP, columns);
            clause.addGroupSet(rollup);
            break;

          case NQLParser.FIELD_NAME:
            column = checkAndGetColumnByAST(context, block, (CommonTree) group);
            columnRefs.add(column);
            break;
        }
      }

      if (columnRefs.size() > 0) {
        Column [] groupingFields = columnRefs.toArray(new Column[columnRefs.size()]);
        GroupElement g = new GroupElement(GroupType.GROUPBY, groupingFields);
        clause.addGroupSet(g);
      }
    }

    block.setGroupByClause(clause);
  }

  private void parseHavingClause(final PlanningContext context,
                                 final QueryBlock block, final CommonTree ast) {
    EvalNode evalTree =
        createEvalTree(context, block, ast.getChild(0));
    block.setHavingCond(evalTree);
  }

  /**
   * Should be given Params Node
   *
   * EBNF: WITH LEFT_PAREN param (COMMA param)* RIGHT_PAREN 
   * AST: ^(PARAMS param+)
   *
   * @param ast
   * @return
   */
  private static Options parseParams(final CommonTree ast) {
    Options params = new Options();

    Tree child;
    for (int i = 0; i < ast.getChildCount(); i++) {
      child = ast.getChild(i);
      params.put(child.getChild(0).getText(), child.getChild(1).getText());
    }
    return params;
  }


  /**
   * Should be given SortSpecifiers Node
   *
   * EBNF: sort_specifier (COMMA sort_specifier)* -> sort_specifier+
   *
   * @param tree
   * @param ast
   */
  private SortSpec[] parseSortSpecifiers(final PlanningContext context,
                                          final ParseTree tree,
                                          final CommonTree ast) {
    int numSortKeys = ast.getChildCount();
    SortSpec[] sortKeys = new SortSpec[numSortKeys];
    CommonTree node;
    Column column;

    // Each child has the following EBNF and AST:
    // EBNF: fn=fieldName a=order_specification? o=null_ordering? 
    // AST: ^(SORT_KEY $fn $a? $o?)
    for (int i = 0; i < numSortKeys; i++) {
      node = (CommonTree) ast.getChild(i);
      column = checkAndGetColumnByAST(context, tree, (CommonTree) node.getChild(0));
      sortKeys[i] = new SortSpec(column);

      if (node.getChildCount() > 1) {
        Tree child;
        for (int j = 1; j < node.getChildCount(); j++) {
          child = node.getChild(j);

          // AST: ^(ORDER ASC) | ^(ORDER DESC)
          if (child.getType() == NQLParser.ORDER) {
            if (child.getChild(0).getType() == NQLParser.DESC) {
              sortKeys[i].setDescOrder();
            }
          } else if (child.getType() == NQLParser.NULL_ORDER) {
            // AST: ^(NULL_ORDER FIRST) | ^(NULL_ORDER LAST)
            if (child.getChild(0).getType() == NQLParser.FIRST) {
              sortKeys[i].setNullFirst();
            }
          }
        }
      }
    }

    return sortKeys;
  }

  private LimitClause parseLimitClause(final PlanningContext context,
                                       final ParseTree tree,
                                       final CommonTree ast) {
    EvalNode evalNode = createEvalTree(context, (QueryBlock) tree, ast.getChild(0));


    if (evalNode instanceof ConstEval) {
      ConstEval fetchFirst = (ConstEval) evalNode;
      LimitClause limitClause = new LimitClause(fetchFirst.getValue().asLong());
      return limitClause;
    }

    throw new TQLSyntaxError(context.getRawQuery(), "LIMIT clause cannot have the parameter "
        + evalNode);
  }

  private Column checkAndGetColumnByAST(final PlanningContext context,
                                        final ParseTree tree,
                                        final CommonTree fieldNode) {
    Preconditions.checkArgument(NQLParser.FIELD_NAME == fieldNode.getType());

    String columnName = fieldNode.getChild(0).getText();
    String tableName = null;
    if (fieldNode.getChildCount() > 1) {
      tableName = fieldNode.getChild(1).getText();
    }

    Column column;
    if(tableName != null) {
      TableDesc desc;
      desc = checkAndGetTableByMappedName(context, tree, tableName);
      column = checkAndGetFieldByName(desc, columnName);
    } else {
      column = expectTableByField(context, tree, columnName);
    }

    return column;
  }

  private TableDesc checkAndGetTableByMappedName(PlanningContext context,
                                                 ParseTree tree,
                                                 final String tableName) {
    String realName = tree.getTableNameByAlias(tableName);
    return checkAndGetTableByName(realName);
  }

  private TableDesc checkAndGetTableByName(final String tableName) {
    TableDesc desc;

    try {
      desc = catalog.getTableDesc(tableName);
    } catch (NoSuchTableException nst) {
      throw new InvalidQueryException("ERROR: table \"" + tableName
          + "\" does not exist");
    }

    return desc;
  }

  private static Column checkAndGetFieldByName(final TableDesc desc,
                                               final String columnName) {
    Column column;

    column = desc.getMeta().getSchema().getColumn(desc.getId()+"."+columnName);
    if(column == null) {
      throw new InvalidQueryException("ERROR: column \"" + columnName
          + "\" does not exist");
    }

    return column;
  }

  /**
   * determine a column by finding tables which are given by 'from clause'.
   *
   * @param tree
   * @param columnName field name to be find
   * @return a found column
   */
  private Column expectTableByField(final PlanningContext context,
                                    final ParseTree tree, String columnName) {
    TableDesc desc;
    Schema schema;
    Column column = null;
    int count = 0;
    for(String table : tree.getAllTableNames()) {
      desc =
          catalog.getTableDesc(table);
      schema = desc.getMeta().getSchema();

      if(schema.contains(table+"."+columnName)) {
        column = schema.getColumn(table+"."+columnName);
        count++;
      }

      if (tree instanceof QueryBlock) {
        QueryBlock block = ((QueryBlock)tree);
        if (block.getTargetList() != null) {
          for (Target target : block.getTargetList()) {
            if (target.hasAlias() && target.getAlias().equals(columnName)) {
              try {
                column = (Column) target.getColumnSchema().clone();
                column.setName(target.getAlias());
              } catch (CloneNotSupportedException e) {
                e.printStackTrace();
              }
              count++;
            }
          }
        }
      }

      // if there are more than one column, we cannot expect
      // that this column belongs to which table.
      if(count > 1)
        throw new AmbiguousFieldException(columnName);
    }

    if(column == null) { // if there are no matched column
      throw new InvalidQueryException("ERROR: column \"" + columnName
          + "\" does not exist");
    }

    return column;
  }

  private static CommonTree parseTree(final String query) {
    ANTLRStringStream input = new ANTLRStringStream(query);
    NQLLexer lexer = new NQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    NQLParser parser = new NQLParser(tokens);

    CommonTree ast;
    try {
      ast = ((CommonTree) parser.statement().getTree());
    } catch (RecognitionException e) {
      throw new TQLParseError(e.getMessage());
    }

    return ast;
  }

  private static StatementType getCmdType(final CommonTree ast) {
    switch (ast.getType()) {
      case NQLParser.STORE:
        return StatementType.STORE;
      case NQLParser.SELECT:
        return StatementType.SELECT;
      case NQLParser.UNION:
        return StatementType.UNION;
      case NQLParser.EXCEPT:
        return StatementType.EXCEPT;
      case NQLParser.INTERSECT:
        return StatementType.INTERSECT;
      case NQLParser.INSERT:
        return StatementType.INSERT;
      case NQLParser.COPY:
        return StatementType.COPY;
      case NQLParser.CREATE_INDEX:
        return StatementType.CREATE_INDEX;
      case NQLParser.CREATE_TABLE:
        return StatementType.CREATE_TABLE;
      case NQLParser.DROP_TABLE:
        return StatementType.DROP_TABLE;
      case NQLParser.SHOW_TABLE:
        return StatementType.SHOW_TABLES;
      case NQLParser.DESC_TABLE:
        return StatementType.DESC_TABLE;
      case NQLParser.SHOW_FUNCTION:
        return StatementType.SHOW_FUNCTION;
      default:
        return null;
    }
  }

  private static IndexMethod getIndexMethod(String method) {
    Preconditions.checkNotNull(method);
    if (method.equals("bst")) {
      return IndexMethod.TWO_LEVEL_BIN_TREE;
    } else if (method.equals("btree")) {
      return IndexMethod.BTREE;
    } else if (method.equals("hash")) {
      return IndexMethod.HASH;
    } else if (method.equals("bitmap")) {
      return IndexMethod.BITMAP;
    } else {
      throw new TQLParseError("ERROR: unknown index: " + method);
    }
  }

  public EvalNode createEvalTree(final PlanningContext context,
                                 final QueryBlock tree, final Tree ast) {
    switch(ast.getType()) {

      // constants
      case NQLParser.DIGIT:
        return new ConstEval(DatumFactory.createInt(
            Integer.valueOf(ast.getText())));

      case NQLParser.REAL:
        return new ConstEval(DatumFactory.createDouble(
            Double.valueOf(ast.getText())));

      case NQLParser.STRING:
        return new ConstEval(DatumFactory.createString(ast.getText()));

      // unary expression
      case NQLParser.NOT:
        return new NotEval(createEvalTree(context, tree, ast.getChild(0)));

      // binary expressions
      case NQLParser.LIKE:
        return parseLike(context, tree, ast);

      case NQLParser.IS:
        return parseIsNullPredicate(context, tree, ast);

      case NQLParser.AND:
      case NQLParser.OR:
      case NQLParser.EQUAL:
      case NQLParser.NOT_EQUAL:
      case NQLParser.LTH:
      case NQLParser.LEQ:
      case NQLParser.GTH:
      case NQLParser.GEQ:
      case NQLParser.PLUS:
      case NQLParser.MINUS:
      case NQLParser.MULTIPLY:
      case NQLParser.DIVIDE:
      case NQLParser.MODULAR:
        return parseBinaryExpr(context, tree, ast);

      // others
      case NQLParser.COLUMN:
        return createEvalTree(context, tree, ast.getChild(0));

      case NQLParser.FIELD_NAME:
        Column column = checkAndGetColumnByAST(context, tree, (CommonTree) ast);
        return new FieldEval(column);

      case NQLParser.FUNCTION:
        String signature = ast.getText();

        EvalNode [] givenArgs = new EvalNode[ast.getChildCount()];
        DataType [] paramTypes = new DataType[ast.getChildCount()];

        for (int i = 0; i < ast.getChildCount(); i++) {
          givenArgs[i] = createEvalTree(context, tree, ast.getChild(i));
          paramTypes[i] = givenArgs[i].getValueType()[0];
        }
        if (!catalog.containFunction(signature, paramTypes)) {
          throw new UndefinedFunctionException(TCatUtil.
              getCanonicalName(signature, paramTypes));
        }
        FunctionDesc funcDesc = catalog.getFunction(signature, paramTypes);

        try {
          if (funcDesc.getFuncType() == FunctionType.GENERAL)

            return new FuncCallEval(funcDesc,
                (GeneralFunction) funcDesc.newInstance(), givenArgs);
          else {
            tree.setAggregation();

            return new AggFuncCallEval(funcDesc,
                (AggFunction) funcDesc.newInstance(), givenArgs);
          }
        } catch (InternalException e) {
          e.printStackTrace();
        }

        break;
      case NQLParser.COUNT_VAL:
        // Getting the first argument
        EvalNode colRef = createEvalTree(context, tree, ast.getChild(0));

        FunctionDesc countVals = catalog.getFunction("count", DataType.ANY);
        tree.setAggregation();
        try {
          return new AggFuncCallEval(countVals, (AggFunction) countVals.newInstance(),
              new EvalNode [] {colRef});
        } catch (InternalException e1) {
          e1.printStackTrace();
        }
        break;

      case NQLParser.COUNT_ROWS:
        FunctionDesc countRows = catalog.getFunction("count", new DataType [] {});
        tree.setAggregation();
        try {
          return new AggFuncCallEval(countRows, (AggFunction) countRows.newInstance(),
              new EvalNode [] {});
        } catch (InternalException e) {
          e.printStackTrace();
        }
        break;

      case NQLParser.CASE:
        return parseCaseWhen(context, tree, ast);

      default:
    }
    return null;
  }

  public IsNullEval parseIsNullPredicate(final PlanningContext context,
                                         QueryBlock block, Tree tree) {
    boolean not;

    Preconditions.checkArgument(tree.getType() == NQLParser.IS, "The AST is not IS (NOT) NULL clause");
    int idx = 0;

    FieldEval field = (FieldEval) createEvalTree(context, block,
        tree.getChild(idx++));
    Preconditions.checkArgument(
        tree.getChild(idx++).getType() == NQLParser.NULL,
        "We does not support another kind of IS clause yet");
    not = tree.getChildCount() == (idx + 1)
        && tree.getChild(idx).getType() == NQLParser.NOT;

    return new IsNullEval(not, field);
  }

  /**
   * The EBNF of case statement
   * <pre>
   * searched_case
   * : CASE s=searched_when_clauses e=else_clause END -> ^(CASE $s $e)
   * ;
   *
   * searched_when_clauses
   * : searched_when_clause searched_when_clause* -> searched_when_clause+
   * ;
   *
   * searched_when_clause
   * : WHEN c=search_condition THEN r=result -> ^(WHEN $c $r)
   * ;
   *
   * else_clause
   * : ELSE r=result -> ^(ELSE $r)
   * ;
   * </pre>
   * @param block
   * @param tree
   * @return
   */
  public CaseWhenEval parseCaseWhen(final PlanningContext context,
                                    final QueryBlock block, final Tree tree) {
    int idx = 0;

    CaseWhenEval caseEval = new CaseWhenEval();
    EvalNode cond;
    EvalNode thenResult;
    Tree when;

    for (; idx < tree.getChildCount() &&
        tree.getChild(idx).getType() == NQLParser.WHEN; idx++) {

      when = tree.getChild(idx);
      cond = createEvalTree(context, block, when.getChild(0));
      thenResult = createEvalTree(context, block, when.getChild(1));
      caseEval.addWhen(cond, thenResult);
    }

    if (tree.getChild(idx) != null &&
        tree.getChild(idx).getType() == NQLParser.ELSE) {
      EvalNode elseResult = createEvalTree(context, block,
          tree.getChild(idx).getChild(0));
      caseEval.setElseResult(elseResult);
    }

    return caseEval;
  }

  public EvalNode parseDigitByTypeInfer(final PlanningContext context,
                                        final QueryBlock block, final Tree tree,
                                        DataType type) {
    switch (type) {
      case SHORT:
        return new ConstEval(DatumFactory.createShort(tree.getText()));
      case INT:
        return new ConstEval(DatumFactory.createInt(tree.getText()));
      case LONG:
        return new ConstEval(DatumFactory.createLong(tree.getText()));
      default: return createEvalTree(context, block, tree);
    }
  }

  private EvalNode parseRealByTypeInfer(final PlanningContext context,
                                        final QueryBlock block, final Tree tree,
                                        DataType type) {
    switch (type) {
      case FLOAT:
        return new ConstEval(DatumFactory.createFloat(tree.getText()));
      case DOUBLE:
        return new ConstEval(DatumFactory.createDouble(tree.getText()));
      default: return createEvalTree(context, block, tree);
    }
  }

  private EvalNode parseStringByTypeInfer(final PlanningContext context,
                                          final QueryBlock block,
                                          final Tree tree,
                                          DataType type) {
    switch (type) {
      case CHAR:
        return new ConstEval(DatumFactory.createChar(tree.getText().charAt(0)));
      case STRING:
        return new ConstEval(DatumFactory.createString(tree.getText()));
      default: return createEvalTree(context, block, tree);
    }
  }

  @VisibleForTesting
  EvalNode parseBinaryExpr(final PlanningContext context,
                           final QueryBlock block, final Tree tree) {
    int constId = -1;
    int fieldId = -1;

    for (int i = 0; i < 2; i++) {
      if (ParseUtil.isConstant(tree.getChild(i))) {
        constId = i;
      } else if (tree.getChild(i).getType() == NQLParser.FIELD_NAME) {
        fieldId = i;
      }
    }

    if (constId != -1 && fieldId != -1) {
      EvalNode [] exprs = new EvalNode[2];
      exprs[fieldId] = createEvalTree(context, block,
          tree.getChild(fieldId));

      Tree constAst = tree.getChild(constId);

      switch (tree.getChild(constId).getType()) {
        case NQLParser.DIGIT:
          exprs[constId] = parseDigitByTypeInfer(context, block, constAst,
              exprs[fieldId].getValueType()[0]);
          break;

        case NQLParser.REAL:
          exprs[constId] = parseRealByTypeInfer(context, block, constAst,
              exprs[fieldId].getValueType()[0]);
          break;

        case NQLParser.STRING:
          exprs[constId] = parseStringByTypeInfer(context, block, constAst,
              exprs[fieldId].getValueType()[0]);
          break;

        default: throw new tajo.engine.eval.InvalidEvalException();
      }

      if (constId == 0) {
        return new BinaryEval(ParseUtil.getTypeByParseCode(tree.getType()),
            exprs[constId], exprs[fieldId]);
      } else {
        return new BinaryEval(ParseUtil.getTypeByParseCode(tree.getType()),
            exprs[fieldId], exprs[constId]);
      }

    } else {
      return new BinaryEval(ParseUtil.getTypeByParseCode(tree.getType()),
          createEvalTree(context, block, tree.getChild(0)),
          createEvalTree(context, block, tree.getChild(1)));
    }
  }

  /**
   * <pre>
   * like_predicate : fieldName NOT? LIKE string_value_expr 
   * -> ^(LIKE NOT? fieldName string_value_expr)
   * </pre>
   * @param block
   * @param tree
   * @return
   */
  private LikeEval parseLike(final PlanningContext context,
                             final QueryBlock block, final Tree tree) {
    int idx = 0;

    boolean not = false;
    if (tree.getChild(idx).getType() == NQLParser.NOT) {
      not = true;
      idx++;
    }

    FieldEval field = (FieldEval) createEvalTree(context, block,
        tree.getChild(idx));
    idx++;
    ConstEval pattern = (ConstEval) createEvalTree(context, block,
        tree.getChild(idx));

    return new LikeEval(not, field, pattern);
  }

  /**
   * It parses the below EBNF.
   * <pre>
   * column_reference  
   * : fieldName (COMMA fieldName)* -> fieldName+
   * ;
   * </pre>
   * @param tree
   * @param parent
   * @return
   */
  private Column [] parseColumnReferences(final PlanningContext context,
                                          final ParseTree tree,
                                          final CommonTree parent) {
    Column [] columns = new Column[parent.getChildCount()];

    for (int i = 0; i < columns.length; i++) {
      columns[i] = checkAndGetColumnByAST(context, tree,
          (CommonTree) parent.getChild(i));
    }

    return columns;
  }
}
