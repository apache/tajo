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

package org.apache.tajo.parser.sql;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.*;
import org.apache.tajo.algebra.Aggregation.GroupType;
import org.apache.tajo.algebra.CreateIndex.IndexMethodSpec;
import org.apache.tajo.algebra.DataTypeExpr.MapType;
import org.apache.tajo.algebra.LiteralValue.LiteralType;
import org.apache.tajo.algebra.Sort.SortSpec;
import org.apache.tajo.exception.SQLSyntaxError;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TimeZoneUtil;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tajo.algebra.Aggregation.GroupElement;
import static org.apache.tajo.algebra.CreateTable.*;
import static org.apache.tajo.algebra.WindowSpec.WindowFrameEndBoundType;
import static org.apache.tajo.algebra.WindowSpec.WindowFrameStartBoundType;
import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.apache.tajo.parser.sql.SQLParser.*;

public class SQLAnalyzer extends SQLParserBaseVisitor<Expr> {

  public Expr parse(String sql) throws SQLSyntaxError {

    final ANTLRInputStream input = new ANTLRInputStream(sql);
    final SQLLexer lexer = new SQLLexer(input);
    lexer.removeErrorListeners();
    lexer.addErrorListener(new SQLErrorListener());

    final CommonTokenStream tokens = new CommonTokenStream(lexer);


    final SQLParser parser = new SQLParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(new SQLErrorListener());
    parser.setBuildParseTree(true);

    SqlContext context;
    try {
      context = parser.sql();
    } catch (SQLParseError e) {
      throw new SQLSyntaxError(e.getMessage());
    } catch (Throwable t) {
      throw new TajoInternalError(t.getMessage());
    }

    return visitSql(context);
  }

  private static boolean checkIfExist(Object obj) {
    return obj != null;
  }

  @Override
  public Expr visitSql(SqlContext ctx) {
    Expr statement = visit(ctx.statement());
    if (checkIfExist(ctx.explain_clause())) {
      return new Explain(statement, checkIfExist(ctx.explain_clause().GLOBAL()));
    } else {
      return statement;
    }
  }

  public Expr visitSession_statement(@NotNull Session_statementContext ctx) {

    if (checkIfExist(ctx.CATALOG())) {

      return new SetSession(SessionVars.CURRENT_DATABASE.name(), ctx.dbname.getText());


    } else if (checkIfExist(ctx.name)) {
      String value;
      if (checkIfExist(ctx.boolean_literal())) {
        value = ctx.boolean_literal().getText();
      } else if (checkIfExist(ctx.Character_String_Literal())) {
        value = stripQuote(ctx.Character_String_Literal().getText());
      } else if (checkIfExist(ctx.signed_numerical_literal())) {
        value = ctx.signed_numerical_literal().getText();
      } else {
        value = null;
      }
      // Keep upper case letters (workaround temporarily)
      return new SetSession(ctx.name.getText().toUpperCase(), value);


    } else if (checkIfExist(ctx.TIME()) && checkIfExist(ctx.ZONE())) {

      String value;
      if (checkIfExist(ctx.Character_String_Literal())) {
        value = stripQuote(ctx.Character_String_Literal().getText());
      } else if (checkIfExist(ctx.signed_numerical_literal())) {
        value = ctx.signed_numerical_literal().getText();
      } else {
        value = null;
      }
      return new SetSession(SessionVars.TIMEZONE.name(), value);

    } else {
      throw new TajoInternalError("Invalid session statement");
    }
  }

  @Override
  public Expr visitNon_join_query_expression(Non_join_query_expressionContext ctx) {

    Expr current = visitNon_join_query_term(ctx.non_join_query_term());
    if (ctx.getChildCount() == 1) {
      return current;
    }

    OpType operatorType;
    Expr left;
    for (int i = 1; i < ctx.getChildCount(); i++) {
      int idx = i;
      boolean distinct = true;

      if (ctx.getChild(idx) instanceof TerminalNode) {
        if (((TerminalNode) ctx.getChild(idx)).getSymbol().getType() == UNION) {
          operatorType = OpType.Union;
        } else {
          operatorType = OpType.Except;
        }

        idx++;

        if (ctx.getChild(idx) instanceof TerminalNode) {
          if (((TerminalNode) ctx.getChild(idx)).getSymbol().getType() == ALL) {
            distinct = false;
          }

          idx++;
        }

        Query_termContext queryTermContext =
            (Query_termContext) ctx.getChild(idx);
        Expr right = visitQuery_term(queryTermContext);

        left = current;
        current = new SetOperation(operatorType, left, right, distinct);

        i = idx;
      }
    }

    return current;
  }

  @Override
  public Expr visitNon_join_query_term(Non_join_query_termContext ctx) {

    Expr current = visitNon_join_query_primary(ctx.non_join_query_primary());
    Expr left;

    for (int i = 1; i < ctx.getChildCount(); ) {
      int idx = i;
      boolean distinct = true;

      if (ctx.getChild(idx) instanceof TerminalNode) {
        if (((TerminalNode) ctx.getChild(idx)).getSymbol().getType() == INTERSECT) {
          idx++;
        }

        if (ctx.getChild(idx) instanceof TerminalNode) {
          if (((TerminalNode) ctx.getChild(idx)).getSymbol().getType() == ALL) {
            distinct = false;
            idx++;
          }
        }

        Query_primaryContext queryPrimaryContext = (Query_primaryContext) ctx.getChild(idx);
        Expr right = visitQuery_primary(queryPrimaryContext);

        left = current;
        current = new SetOperation(OpType.Intersect, left, right, distinct);

        i += idx;
      }
    }

    return current;

  }

  @Override
  public Expr visitNon_join_query_primary(Non_join_query_primaryContext ctx) {
    if (ctx.simple_table() != null) {
      return visitSimple_table(ctx.simple_table());
    } else if (ctx.non_join_query_expression() != null) {
      return visitNon_join_query_expression(ctx.non_join_query_expression());
    }
    return visitChildren(ctx);
  }

  @Override
  public Expr visitQuery_specification(Query_specificationContext ctx) {
    Expr current = null;
    if (ctx.table_expression() != null) {
      current = visitFrom_clause(ctx.table_expression().from_clause());

      if (ctx.table_expression().where_clause() != null) {
        Selection selection = visitWhere_clause(ctx.table_expression().where_clause());
        selection.setChild(current);
        current = selection;
      }

      if (ctx.table_expression().groupby_clause() != null) {
        Aggregation aggregation = visitGroupby_clause(ctx.table_expression().groupby_clause());
        aggregation.setChild(current);
        current = aggregation;

        if (ctx.table_expression().having_clause() != null) {
          Expr havingCondition = visitBoolean_value_expression(
              ctx.table_expression().having_clause().boolean_value_expression());
          Having having = new Having(havingCondition);
          having.setChild(current);
          current = having;
        }
      }

      if (ctx.table_expression().orderby_clause() != null) {
        Sort sort = visitOrderby_clause(ctx.table_expression().orderby_clause());
        sort.setChild(current);
        current = sort;
      }

      if (checkIfExist(ctx.table_expression().window_clause())) {
        Window window = visitWindow_clause(ctx.table_expression().window_clause());
        window.setChild(current);
        current = window;
      }

      if (ctx.table_expression().limit_clause() != null) {
        Limit limit = visitLimit_clause(ctx.table_expression().limit_clause());
        limit.setChild(current);
        current = limit;
      }
    }

    Projection projection = visitSelect_list(ctx.select_list());

    if (ctx.set_qualifier() != null && ctx.set_qualifier().DISTINCT() != null) {
      projection.setDistinct();
    }

    if (current != null) {
      projection.setChild(current);
    }

    current = projection;

    return current;
  }

  /**
   * <pre>
   *   select_list
   *   : select_sublist (COMMA select_sublist)*
   *   ;
   * </pre>
   *
   * @param ctx
   * @return
   */
  @Override
  public Projection visitSelect_list(Select_listContext ctx) {
    Projection projection = new Projection();
    List<NamedExpr> targets = new ArrayList<>();
    for (int i = 0; i < ctx.select_sublist().size(); i++) {
      targets.add(visitSelect_sublist(ctx.select_sublist(i)));
    }
    projection.setNamedExprs(targets);

    return projection;
  }

  /**
   * <pre>
   *   select_sublist
   *   : derived_column
   *   | asterisked_qualifier
   *   ;
   * </pre>
   *
   * @param ctx
   * @return
   */
  @Override
  public NamedExpr visitSelect_sublist(Select_sublistContext ctx) {
    if (ctx.qualified_asterisk() != null) {
      return visitQualified_asterisk(ctx.qualified_asterisk());
    } else {
      return visitDerived_column(ctx.derived_column());
    }
  }

  @Override
  public RelationList visitFrom_clause(From_clauseContext ctx) {
    Expr[] relations = new Expr[ctx.table_reference_list().table_reference().size()];
    for (int i = 0; i < relations.length; i++) {
      relations[i] = visitTable_reference(ctx.table_reference_list().table_reference(i));
    }
    return new RelationList(relations);
  }

  @Override
  public Selection visitWhere_clause(Where_clauseContext ctx) {
    return new Selection(visitSearch_condition(ctx.search_condition()));
  }

  @Override
  public Aggregation visitGroupby_clause(Groupby_clauseContext ctx) {
    Aggregation clause = new Aggregation();

    // If grouping group is not empty
    if (ctx.grouping_element_list().grouping_element().get(0).empty_grouping_set() == null) {
      int elementSize = ctx.grouping_element_list().grouping_element().size();
      ArrayList<GroupElement> groups = new ArrayList<>(elementSize + 1);
      ArrayList<Expr> ordinaryExprs = null;
      int groupSize = 1;
      groups.add(null);

      for (int i = 0; i < elementSize; i++) {
        Grouping_elementContext element =
            ctx.grouping_element_list().grouping_element().get(i);
        if (element.ordinary_grouping_set() != null) {
          if (ordinaryExprs == null) {
            ordinaryExprs = new ArrayList<>();
          }
          Collections.addAll(ordinaryExprs, getRowValuePredicandsFromOrdinaryGroupingSet(element.ordinary_grouping_set()));
        } else if (element.rollup_list() != null) {
          groupSize++;
          groups.add(new GroupElement(GroupType.Rollup,
              getRowValuePredicandsFromOrdinaryGroupingSetList(element.rollup_list().c)));
        } else if (element.cube_list() != null) {
          groupSize++;
          groups.add(new GroupElement(GroupType.Cube,
              getRowValuePredicandsFromOrdinaryGroupingSetList(element.cube_list().c)));
        }
      }

      if (ordinaryExprs != null) {
        groups.set(0, new GroupElement(GroupType.OrdinaryGroup, ordinaryExprs.toArray(new Expr[ordinaryExprs.size()])));
        clause.setGroups(groups.subList(0, groupSize).toArray(new GroupElement[groupSize]));
      } else if (groupSize > 1) {
        clause.setGroups(groups.subList(1, groupSize).toArray(new GroupElement[groupSize - 1]));
      }
    }

    return clause;
  }

  @Override public WindowFunctionExpr visitWindow_function(@NotNull Window_functionContext context) {
    WindowFunctionExpr windowFunction = null;

    Window_function_typeContext functionType = context.window_function_type();
    GeneralSetFunctionExpr functionBody;
    if (checkIfExist(functionType.rank_function_type())) {
      Rank_function_typeContext rankFunction = functionType.rank_function_type();
      if (checkIfExist(rankFunction.RANK())) {
        functionBody = new GeneralSetFunctionExpr("rank", false, new Expr[] {});
      } else if (checkIfExist(rankFunction.DENSE_RANK())) {
        functionBody = new GeneralSetFunctionExpr("dense_rank", false, new Expr[] {});
      } else if (checkIfExist(rankFunction.PERCENT_RANK())) {
        functionBody = new GeneralSetFunctionExpr("percent_rank", false, new Expr[] {});
      } else {
        functionBody = new GeneralSetFunctionExpr("cume_dist", false, new Expr[] {});
      }
    } else if (checkIfExist(functionType.ROW_NUMBER())) {
      functionBody = new GeneralSetFunctionExpr("row_number", false, new Expr[] {});
    } else if (checkIfExist(functionType.FIRST_VALUE())) {
      functionBody = new GeneralSetFunctionExpr("first_value", false,
          new Expr[]{ visitColumn_reference(functionType.column_reference())});
    } else if (checkIfExist(functionType.LAST_VALUE())) {
      functionBody = new GeneralSetFunctionExpr("last_value", false, new Expr[]{visitColumn_reference(functionType.column_reference())});
    } else if (checkIfExist(functionType.LAG())) {
      if (checkIfExist(functionType.numeric_value_expression())) {
        if (checkIfExist(functionType.common_value_expression())) {
          functionBody = new GeneralSetFunctionExpr("lag", false, new Expr[]{visitColumn_reference(functionType.column_reference()),
              visitNumeric_value_expression(functionType.numeric_value_expression()),
              visitCommon_value_expression(functionType.common_value_expression())});
        } else {
          functionBody = new GeneralSetFunctionExpr("lag", false, new Expr[]{visitColumn_reference(functionType.column_reference()),
              visitNumeric_value_expression(functionType.numeric_value_expression())});
        }
      } else {
        functionBody = new GeneralSetFunctionExpr("lag", false, new Expr[]{visitColumn_reference(functionType.column_reference())});
      }
    } else if (checkIfExist(functionType.LEAD())) {
      if (checkIfExist(functionType.numeric_value_expression())) {
        if (checkIfExist(functionType.common_value_expression())) {
          functionBody = new GeneralSetFunctionExpr("lead", false, new Expr[]{visitColumn_reference(functionType.column_reference()),
              visitNumeric_value_expression(functionType.numeric_value_expression()),
              visitCommon_value_expression(functionType.common_value_expression())});
        } else {
          functionBody = new GeneralSetFunctionExpr("lead", false, new Expr[]{visitColumn_reference(functionType.column_reference()),
              visitNumeric_value_expression(functionType.numeric_value_expression())});
        }
      } else {
        functionBody = new GeneralSetFunctionExpr("lead", false, new Expr[]{visitColumn_reference(functionType.column_reference())});
      }
    } else {
      functionBody = visitAggregate_function(functionType.aggregate_function());
    }
    windowFunction = new WindowFunctionExpr(functionBody);

    Window_name_or_specificationContext windowNameOrSpec = context.window_name_or_specification();
    if (checkIfExist(windowNameOrSpec.window_name())) {
      windowFunction.setWindowName(windowNameOrSpec.window_name().getText());
    } else {
      windowFunction.setWindowSpec(buildWindowSpec(windowNameOrSpec.window_specification()));
    }

    return windowFunction;
  }

  @Override
  public Window visitWindow_clause(@NotNull Window_clauseContext ctx) {
    Window.WindowDefinition [] definitions =
        new Window.WindowDefinition[ctx.window_definition_list().window_definition().size()];
    for (int i = 0; i < definitions.length; i++) {
      Window_definitionContext windowDefinitionContext = ctx.window_definition_list().window_definition(i);
      String windowName = buildIdentifier(windowDefinitionContext.window_name().identifier());
      WindowSpec windowSpec = buildWindowSpec(windowDefinitionContext.window_specification());
      definitions[i] = new Window.WindowDefinition(windowName, windowSpec);
    }
    return new Window(definitions);
  }

  public WindowSpec buildWindowSpec(Window_specificationContext ctx) {
    WindowSpec windowSpec = new WindowSpec();
    if (checkIfExist(ctx.window_specification_details())) {
      Window_specification_detailsContext windowSpecDetail = ctx.window_specification_details();

      if (checkIfExist(windowSpecDetail.existing_window_name())) {
        windowSpec.setWindowName(windowSpecDetail.existing_window_name().getText());
      }

      if (checkIfExist(windowSpecDetail.window_partition_clause())) {
        windowSpec.setPartitionKeys(
            buildRowValuePredicands(windowSpecDetail.window_partition_clause().row_value_predicand_list()));
      }

      if (checkIfExist(windowSpecDetail.window_order_clause())) {
        windowSpec.setSortSpecs(
            buildSortSpecs(windowSpecDetail.window_order_clause().orderby_clause().sort_specifier_list()));
      }

      if (checkIfExist(windowSpecDetail.window_frame_clause())) {
        Window_frame_clauseContext frameContext = windowSpecDetail.window_frame_clause();

        WindowSpec.WindowFrameUnit unit;
        // frame unit - there are only two cases: RANGE and ROW
        if (checkIfExist(frameContext.window_frame_units().RANGE())) {
          unit = WindowSpec.WindowFrameUnit.RANGE;
        } else {
          unit = WindowSpec.WindowFrameUnit.ROW;
        }

        WindowSpec.WindowFrame windowFrame;

        if (checkIfExist(frameContext.window_frame_extent().window_frame_between())) { // when 'between' is given
          Window_frame_betweenContext between = frameContext.window_frame_extent().window_frame_between();
          WindowSpec.WindowStartBound startBound = buildWindowStartBound(between.window_frame_start_bound());
          WindowSpec.WindowEndBound endBound = buildWindowEndBound(between.window_frame_end_bound());

          windowFrame = new WindowSpec.WindowFrame(unit, startBound, endBound);
        } else { // if there is only start bound
          WindowSpec.WindowStartBound startBound =
              buildWindowStartBound(frameContext.window_frame_extent().window_frame_start_bound());
          windowFrame = new WindowSpec.WindowFrame(unit, startBound);
        }

        windowSpec.setWindowFrame(windowFrame);
      }
    }
    return windowSpec;
  }

  public WindowSpec.WindowStartBound buildWindowStartBound(Window_frame_start_boundContext context) {
    WindowFrameStartBoundType boundType = null;
    if (checkIfExist(context.UNBOUNDED())) {
      boundType = WindowFrameStartBoundType.UNBOUNDED_PRECEDING;
    } else if (checkIfExist(context.unsigned_value_specification())) {
      boundType = WindowFrameStartBoundType.PRECEDING;
    } else {
      boundType = WindowFrameStartBoundType.CURRENT_ROW;
    }

    WindowSpec.WindowStartBound bound = new WindowSpec.WindowStartBound(boundType);
    if (boundType == WindowFrameStartBoundType.PRECEDING) {
      bound.setNumber(visitUnsigned_value_specification(context.unsigned_value_specification()));
    }

    return bound;
  }

  public WindowSpec.WindowEndBound buildWindowEndBound(Window_frame_end_boundContext context) {
    WindowFrameEndBoundType boundType;
    if (checkIfExist(context.UNBOUNDED())) {
      boundType = WindowFrameEndBoundType.UNBOUNDED_FOLLOWING;
    } else if (checkIfExist(context.unsigned_value_specification())) {
      boundType = WindowFrameEndBoundType.FOLLOWING;
    } else {
      boundType = WindowFrameEndBoundType.CURRENT_ROW;
    }

    WindowSpec.WindowEndBound endBound = new WindowSpec.WindowEndBound(boundType);
    if (boundType == WindowFrameEndBoundType.FOLLOWING) {
      endBound.setNumber(visitUnsigned_value_specification(context.unsigned_value_specification()));
    }

    return endBound;
  }

  public Sort.SortSpec[] buildSortSpecs(Sort_specifier_listContext context) {
    int size = context.sort_specifier().size();

    Sort.SortSpec specs[] = new Sort.SortSpec[size];
    for (int i = 0; i < size; i++) {
      Sort_specifierContext specContext = context.sort_specifier(i);
      Expr sortKeyExpr = visitRow_value_predicand(specContext.key);
      specs[i] = new Sort.SortSpec(sortKeyExpr);
      if (specContext.order_specification() != null) {
        if (specContext.order.DESC() != null) {
          specs[i].setDescending();
        }
      }

      if (specContext.null_ordering() != null) {
        if (specContext.null_ordering().FIRST() != null) {
          specs[i].setNullsFirst();
        } else if (specContext.null_ordering().LAST() != null) {
          specs[i].setNullsLast();
        }
      }
    }

    return specs;
  }

  @Override
  public Sort visitOrderby_clause(Orderby_clauseContext ctx) {
    return new Sort(buildSortSpecs(ctx.sort_specifier_list()));
  }

  @Override
  public Limit visitLimit_clause(Limit_clauseContext ctx) {
    return new Limit(visitNumeric_value_expression(ctx.numeric_value_expression()));
  }

  @Override
  public Expr visitJoined_table(Joined_tableContext ctx) {
    Expr top = visitTable_primary(ctx.table_primary());

    // The following loop builds a left deep join tree.
    Join join;
    for (int i = 0; i < ctx.joined_table_primary().size(); i++) {
      join = visitJoined_table_primary(ctx.joined_table_primary(i));
      join.setLeft(top);
      top = join;
    }

    return top;
  }

  @Override
  public Join visitJoined_table_primary(Joined_table_primaryContext ctx) {
    Join join;
    if (ctx.CROSS() != null) {
      join = new Join(JoinType.CROSS);
    } else if (ctx.UNION() != null) {
      join = new Join(JoinType.UNION);
    } else { // qualified join or natural
      if (ctx.join_type() != null && ctx.join_type().outer_join_type() != null) {
        Outer_join_type_part2Context outer_join_typeContext = ctx.join_type().outer_join_type()
            .outer_join_type_part2();
        if (outer_join_typeContext.FULL() != null) {
          join = new Join(JoinType.FULL_OUTER);
        } else if (outer_join_typeContext.LEFT() != null) {
          join = new Join(JoinType.LEFT_OUTER);
        } else {
          join = new Join(JoinType.RIGHT_OUTER);
        }
      } else {
        join = new Join(JoinType.INNER);
      }

      if (ctx.NATURAL() != null) {
        join.setNatural();
      }

      if (ctx.join_specification() != null) { // only for qualified join
        if (ctx.join_specification().join_condition() != null) {
          Expr searchCondition = visitSearch_condition(ctx.join_specification().
              join_condition().search_condition());
          join.setQual(searchCondition);
        } else if (ctx.join_specification().named_columns_join() != null) {
          ColumnReferenceExpr[] columns = buildColumnReferenceList(ctx.join_specification().
              named_columns_join().column_reference_list());
          join.setJoinColumns(columns);
        }
      }
    }

    join.setRight(visitTable_primary(ctx.right));
    return join;
  }

  private Expr[] getRowValuePredicandsFromOrdinaryGroupingSetList(Ordinary_grouping_set_listContext ctx) {
    ArrayList<Expr> rowValuePredicands = new ArrayList<>();
    for (int i = 0; i < ctx.ordinary_grouping_set().size(); i++) {
      Collections.addAll(rowValuePredicands, getRowValuePredicandsFromOrdinaryGroupingSet(ctx.ordinary_grouping_set(i)));
    }
    return rowValuePredicands.toArray(new Expr[rowValuePredicands.size()]);
  }

  private Expr[] getRowValuePredicandsFromOrdinaryGroupingSet(Ordinary_grouping_setContext ctx) {
    ArrayList<Expr> rowValuePredicands = new ArrayList<>();
    if (ctx.row_value_predicand() != null) {
      rowValuePredicands.add(visitRow_value_predicand(ctx.row_value_predicand()));
    }
    if (ctx.row_value_predicand_list() != null) {
      Collections.addAll(rowValuePredicands, buildRowValuePredicands(ctx.row_value_predicand_list()));
    }
    return rowValuePredicands.toArray(new Expr[rowValuePredicands.size()]);
  }

  private Expr[] buildRowValuePredicands(Row_value_predicand_listContext ctx) {
    Expr[] rowValuePredicands = new Expr[ctx.row_value_predicand().size()];
    for (int i = 0; i < rowValuePredicands.length; i++) {
      rowValuePredicands[i] = visitRow_value_predicand(ctx.row_value_predicand(i));
    }
    return rowValuePredicands;
  }

  private ColumnReferenceExpr[] buildColumnReferenceList(Column_reference_listContext ctx) {
    ColumnReferenceExpr[] columnRefs = new ColumnReferenceExpr[ctx.column_reference().size()];
    for (int i = 0; i < columnRefs.length; i++) {
      columnRefs[i] = visitColumn_reference(ctx.column_reference(i));
    }
    return columnRefs;
  }

  @Override
  public Expr visitTable_primary(Table_primaryContext ctx) {
    if (ctx.table_or_query_name() != null) {
      Relation relation = new Relation(ctx.table_or_query_name().getText());
      if (ctx.alias != null) {
        relation.setAlias(ctx.alias.getText());
      }
      return relation;
    } else if (ctx.derived_table() != null) {
      return new TablePrimarySubQuery(ctx.name.getText(), visit(ctx.derived_table().table_subquery()));
    } else {
      return null;
    }
  }


  @Override
  public Expr visitSubquery(SubqueryContext ctx) {
    return visitQuery_expression(ctx.query_expression());
  }

  @Override
  public BetweenPredicate visitBetween_predicate(Between_predicateContext ctx) {
    Expr predicand = visitRow_value_predicand(ctx.predicand);
    Expr begin = visitRow_value_predicand(ctx.between_predicate_part_2().begin);
    Expr end = visitRow_value_predicand(ctx.between_predicate_part_2().end);
    return new BetweenPredicate(checkIfExist(ctx.between_predicate_part_2().NOT()),
        checkIfExist(ctx.between_predicate_part_2().SYMMETRIC()), predicand, begin, end);
  }

  @Override
  public CaseWhenPredicate visitSimple_case(Simple_caseContext ctx) {
    Expr leftTerm = visitBoolean_value_expression(ctx.boolean_value_expression());
    CaseWhenPredicate caseWhen = new CaseWhenPredicate();

    for (int i = 0; i < ctx.simple_when_clause().size(); i++) {
      Simple_when_clauseContext simpleWhenCtx = ctx.simple_when_clause(i);
      BinaryOperator bin = new BinaryOperator(OpType.Equals, leftTerm,
          visitValue_expression(simpleWhenCtx.search_condition().value_expression()));
      caseWhen.addWhen(bin, buildCaseResult(simpleWhenCtx.result()));
    }
    if (ctx.else_clause() != null) {
      caseWhen.setElseResult(buildCaseResult(ctx.else_clause().result()));
    }
    return caseWhen;
  }

  private Expr buildCaseResult(ResultContext result) {
    if (result.NULL() != null) {
      return new NullLiteral();
    } else {
      return visitValue_expression(result.value_expression());
    }
  }

  @Override
  public CaseWhenPredicate visitSearched_case(Searched_caseContext ctx) {
    CaseWhenPredicate caseWhen = new CaseWhenPredicate();

    for (int i = 0; i < ctx.searched_when_clause().size(); i++) {
      Searched_when_clauseContext searchedWhenCtx = ctx.searched_when_clause(i);
      caseWhen.addWhen(
          visitSearch_condition(searchedWhenCtx.search_condition()),
          buildCaseResult(searchedWhenCtx.result()));
    }
    if (ctx.else_clause() != null) {
      caseWhen.setElseResult(buildCaseResult(ctx.else_clause().result()));
    }
    return caseWhen;
  }

  @Override
  public Expr visitCommon_value_expression(Common_value_expressionContext ctx) {
    if (checkIfExist(ctx.NULL())) {
      return new NullLiteral();
    } else {
      return visitChildren(ctx);
    }
  }

  @Override
  public Expr visitParenthesized_value_expression(Parenthesized_value_expressionContext ctx) {
    return visitValue_expression(ctx.value_expression());
  }

  @Override
  public Expr visitBoolean_value_expression(Boolean_value_expressionContext ctx) {
    Expr current = visitOr_predicate(ctx.or_predicate());
    return current;
  }

  @Override
  public Expr visitOr_predicate(Or_predicateContext ctx) {
    Expr current = visitAnd_predicate(ctx.and_predicate());

    Expr left;
    Expr right;
    for (int i = 0; i < ctx.or_predicate().size(); i++) {
      left = current;
      right = visitOr_predicate(ctx.or_predicate(i));
      current = new BinaryOperator(OpType.Or, left, right);
    }

    return current;
  }

  @Override
  public Expr visitAnd_predicate(And_predicateContext ctx) {
    Expr current = visitBoolean_factor(ctx.boolean_factor());

    Expr left;
    Expr right;
    for (int i = 0; i < ctx.and_predicate().size(); i++) {
      left = current;
      right = visitAnd_predicate(ctx.and_predicate(i));
      current = new BinaryOperator(OpType.And, left, right);
    }

    return current;
  }

  @Override
  public Expr visitBoolean_factor(Boolean_factorContext ctx) {
    if (ctx.NOT() != null) {
      return new NotExpr(visitBoolean_test(ctx.boolean_test()));
    } else {
      return visitBoolean_test(ctx.boolean_test());
    }
  }

  @Override
  public Expr visitBoolean_test(Boolean_testContext ctx) {
    if (checkIfExist(ctx.is_clause())) {
      Is_clauseContext isClauseContext = ctx.is_clause();
      if (checkIfExist(isClauseContext.NOT())) {
        if (checkIfExist(ctx.is_clause().truth_value().TRUE())) {
          return new NotExpr(visitBoolean_primary(ctx.boolean_primary()));
        } else {
          return visitBoolean_primary(ctx.boolean_primary());
        }
      } else {
        if (checkIfExist(ctx.is_clause().truth_value().TRUE())) {
          return visitBoolean_primary(ctx.boolean_primary());
        } else {
          return new NotExpr(visitBoolean_primary(ctx.boolean_primary()));
        }
      }
    } else {
      return visitBoolean_primary(ctx.boolean_primary());
    }
  }

  @Override
  public Expr visitBoolean_primary(Boolean_primaryContext ctx) {
    if (ctx.predicate() != null) {
      return visitPredicate(ctx.predicate());
    } else {
      return visitBoolean_predicand(ctx.boolean_predicand());
    }
  }

  @Override
  public Expr visitBoolean_predicand(Boolean_predicandContext ctx) {
    if (checkIfExist(ctx.nonparenthesized_value_expression_primary())) {
      return visitNonparenthesized_value_expression_primary(ctx.nonparenthesized_value_expression_primary());
    } else {
      return visitBoolean_value_expression(ctx.parenthesized_boolean_value_expression().boolean_value_expression());
    }
  }

  @Override
  public Expr visitNonparenthesized_value_expression_primary(
      Nonparenthesized_value_expression_primaryContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Expr visitRow_value_predicand(Row_value_predicandContext ctx) {
    if (checkIfExist(ctx.row_value_special_case())) {
      return visitRow_value_special_case(ctx.row_value_special_case());
    } else {
      return visitRow_value_constructor_predicand(ctx.row_value_constructor_predicand());
    }
  }

  @Override
  public Expr visitRow_value_constructor_predicand(Row_value_constructor_predicandContext ctx) {
    if (checkIfExist(ctx.boolean_predicand())) {
      return visitBoolean_predicand(ctx.boolean_predicand());
    } else {
      return visitCommon_value_expression(ctx.common_value_expression());
    }
  }

  @Override
  public BinaryOperator visitComparison_predicate(Comparison_predicateContext ctx) {
    TerminalNode operator = (TerminalNode) ctx.comp_op().getChild(0);
    return new BinaryOperator(tokenToExprType(operator.getSymbol().getType()),
        visitRow_value_predicand(ctx.left),
        visitRow_value_predicand(ctx.right));
  }

  @Override
  public Expr visitNumeric_value_expression(Numeric_value_expressionContext ctx) {
    Expr current = visitTerm(ctx.term(0));

    Expr left;
    Expr right;
    for (int i = 1; i < ctx.getChildCount(); i++) {
      left = current;
      TerminalNode operator = (TerminalNode) ctx.getChild(i++);
      right = visitTerm((TermContext) ctx.getChild(i));

      if (operator.getSymbol().getType() == PLUS) {
        current = new BinaryOperator(OpType.Plus, left, right);
      } else {
        current = new BinaryOperator(OpType.Minus, left, right);
      }
    }

    return current;
  }

  @Override
  public Expr visitTerm(TermContext ctx) {
    Expr current = visitFactor(ctx.factor(0));

    Expr left;
    Expr right;
    for (int i = 1; i < ctx.getChildCount(); i++) {
      left = current;
      TerminalNode operator = (TerminalNode) ctx.getChild(i++);
      right = visitFactor((FactorContext) ctx.getChild(i));

      if (operator.getSymbol().getType() == MULTIPLY) {
        current = new BinaryOperator(OpType.Multiply, left, right);
      } else if (operator.getSymbol().getType() == DIVIDE) {
        current = new BinaryOperator(OpType.Divide, left, right);
      } else {
        current = new BinaryOperator(OpType.Modular, left, right);
      }
    }

    return current;
  }

  @Override
  public Expr visitFactor(FactorContext ctx) {
    Expr current = visitNumeric_primary(ctx.numeric_primary());
    if (checkIfExist(ctx.sign()) && checkIfExist(ctx.sign().MINUS())) {
      current = new SignedExpr(true, current);
    }
    return current;
  }

  @Override
  public Expr visitNumeric_primary(Numeric_primaryContext ctx) {
    Expr current = null;
    if (checkIfExist(ctx.value_expression_primary())) {
      current = visitValue_expression_primary(ctx.value_expression_primary());
      for (int i = 0; i < ctx.CAST_EXPRESSION().size(); i++) {
        current = new CastExpr(current, visitData_type(ctx.cast_target(i).data_type()));
      }
    } else if (checkIfExist(ctx.numeric_value_function())) {
      current = visitNumeric_value_function(ctx.numeric_value_function());
    }

    return current;
  }

  public static OpType tokenToExprType(int tokenId) {
    switch (tokenId) {
      case UNION:
        return OpType.Union;
      case EXCEPT:
        return OpType.Except;
      case INTERSECT:
        return OpType.Intersect;

      case AND:
        return OpType.And;
      case OR:
        return OpType.Or;

      case EQUAL:
        return OpType.Equals;
      case NOT_EQUAL:
        return OpType.NotEquals;
      case LTH:
        return OpType.LessThan;
      case LEQ:
        return OpType.LessThanOrEquals;
      case GTH:
        return OpType.GreaterThan;
      case GEQ:
        return OpType.GreaterThanOrEquals;

      case MULTIPLY:
        return OpType.Multiply;
      case DIVIDE:
        return OpType.Divide;
      case MODULAR:
        return OpType.Modular;
      case PLUS:
        return OpType.Plus;
      case MINUS:
        return OpType.Minus;

      default:
        throw new TajoInternalError("unknown Token Id: " + tokenId);
    }
  }

  @Override
  public InPredicate visitIn_predicate(In_predicateContext ctx) {
    return new InPredicate(visitChildren(ctx.numeric_value_expression()),
        visitIn_predicate_value(ctx.in_predicate_value()), ctx.NOT() != null);
  }

  @Override
  public Expr visitIn_predicate_value(In_predicate_valueContext ctx) {
    if (checkIfExist(ctx.in_value_list())) {
      int size = ctx.in_value_list().row_value_predicand().size();
      Expr [] exprs = new Expr[size];
      for (int i = 0; i < size; i++) {
        exprs[i] = visitRow_value_predicand(ctx.in_value_list().row_value_predicand(i));
      }
      return new ValueListExpr(exprs);
    } else {
      return new SimpleTableSubquery(visitChildren(ctx.table_subquery()));
    }
  }

  @Override
  public Expr visitArray(ArrayContext ctx) {
    int size = ctx.numeric_value_expression().size();
    Expr[] exprs = new Expr[size];
    for (int i = 0; i < size; i++) {
      exprs[i] = visit(ctx.numeric_value_expression(i));
    }
    return new ValueListExpr(exprs);
  }

  @Override
  public Expr visitPattern_matching_predicate(Pattern_matching_predicateContext ctx) {
    Expr predicand = visitChildren(ctx.row_value_predicand());
    Expr pattern = new LiteralValue(stripQuote(ctx.Character_String_Literal().getText()),
        LiteralType.String);

    if (checkIfExist(ctx.pattern_matcher().negativable_matcher())) {
      boolean not = ctx.pattern_matcher().NOT() != null;
      Negativable_matcherContext matcher = ctx.pattern_matcher().negativable_matcher();
      if (checkIfExist(matcher.LIKE())) {
        return new PatternMatchPredicate(OpType.LikePredicate, not, predicand, pattern);
      } else if (checkIfExist(matcher.ILIKE())) {
        return new PatternMatchPredicate(OpType.LikePredicate, not, predicand, pattern, true);
      } else if (checkIfExist(matcher.SIMILAR())) {
        return new PatternMatchPredicate(OpType.SimilarToPredicate, not, predicand, pattern);
      } else if (checkIfExist(matcher.REGEXP()) || checkIfExist(matcher.RLIKE())) {
        return new PatternMatchPredicate(OpType.Regexp, not, predicand, pattern);
      } else {
        throw new TajoInternalError("unknown pattern matching predicate: " + matcher.getText());
      }
    } else if (checkIfExist(ctx.pattern_matcher().regex_matcher())) {
      Regex_matcherContext matcher = ctx.pattern_matcher().regex_matcher();
      if (checkIfExist(matcher.Similar_To())) {
        return new PatternMatchPredicate(OpType.Regexp, false, predicand, pattern, false);
      } else if (checkIfExist(matcher.Not_Similar_To())) {
        return new PatternMatchPredicate(OpType.Regexp, true, predicand, pattern, false);
      } else if (checkIfExist(matcher.Similar_To_Case_Insensitive())) {
        return new PatternMatchPredicate(OpType.Regexp, false, predicand, pattern, true);
      } else if (checkIfExist(matcher.Not_Similar_To_Case_Insensitive())) {
        return new PatternMatchPredicate(OpType.Regexp, true, predicand, pattern, true);
      } else {
        throw new TajoInternalError("Unsupported predicate: " + matcher.getText());
      }
    } else {
      throw new TajoInternalError("invalid pattern matching predicate: " + ctx.pattern_matcher().getText());
    }
  }

  @Override
  public IsNullPredicate visitNull_predicate(Null_predicateContext ctx) {
    Expr predicand = visitRow_value_predicand(ctx.row_value_predicand());
    return new IsNullPredicate(ctx.NOT() != null, predicand);
  }

  @Override
  public ExistsPredicate visitExists_predicate(Exists_predicateContext ctx) {
    return new ExistsPredicate(new SimpleTableSubquery(visitTable_subquery(ctx.table_subquery())), ctx.NOT() != null);
  }

  @Override
  public ColumnReferenceExpr visitColumn_reference(Column_referenceContext ctx) {
    String columnReferenceName = buildIdentifierChain(ctx.identifier());
    // find the last dot (.) position to separate a name into both a qualifier and name
    int lastDotIdx = columnReferenceName.lastIndexOf(".");

    if (lastDotIdx > 0) { // if any qualifier is given
      String qualifier = columnReferenceName.substring(0, lastDotIdx);
      String name = columnReferenceName.substring(lastDotIdx + 1, columnReferenceName.length());
      return new ColumnReferenceExpr(qualifier, name);
    } else {
      return new ColumnReferenceExpr(ctx.getText());
    }
  }

  @Override
  public LiteralValue visitUnsigned_numeric_literal(@NotNull Unsigned_numeric_literalContext ctx) {
    if (ctx.NUMBER() != null) {
      long lValue = Long.parseLong(ctx.getText());
      if (lValue >= Integer.MIN_VALUE && lValue <= Integer.MAX_VALUE) {
        return new LiteralValue(ctx.getText(), LiteralType.Unsigned_Integer);
      } else {
        return new LiteralValue(ctx.getText(), LiteralType.Unsigned_Large_Integer);
      }
    } else {
      return new LiteralValue(ctx.getText(), LiteralType.Unsigned_Float);
    }
  }

  @Override
  public GeneralSetFunctionExpr visitAggregate_function(Aggregate_functionContext ctx) {
    if (ctx.COUNT() != null && ctx.MULTIPLY() != null) {
      return new CountRowsFunctionExpr();
    } else {
      return visitGeneral_set_function(ctx.general_set_function());
    }
  }

  @Override
  public GeneralSetFunctionExpr visitGeneral_set_function(General_set_functionContext ctx) {
    String signature = ctx.set_function_type().getText();
    boolean distinct = checkIfExist(ctx.set_qualifier()) && checkIfExist(ctx.set_qualifier().DISTINCT());
    Expr param = visitValue_expression(ctx.value_expression());

    return new GeneralSetFunctionExpr(signature, distinct, new Expr [] {param});
  }

  @Override
  public FunctionExpr visitRoutine_invocation(Routine_invocationContext ctx) {
    String signature = ctx.function_name().getText();
    FunctionExpr function = new FunctionExpr(signature);
    if (ctx.sql_argument_list() != null) {
      int numArgs = ctx.sql_argument_list().value_expression().size();
      Expr[] argument_list = new Expr[numArgs];
      for (int i = 0; i < numArgs; i++) {
        argument_list[i] = visitValue_expression(ctx.sql_argument_list().
            value_expression().get(i));
      }

      function.setParams(argument_list);
    }
    return function;
  }

  @Override
  public NamedExpr visitDerived_column(Derived_columnContext ctx) {
    NamedExpr target = new NamedExpr(visitValue_expression(ctx.value_expression()));
    if (ctx.as_clause() != null) {
      target.setAlias(buildIdentifier(ctx.as_clause().identifier()));
    }
    return target;
  }

  @Override
  public NamedExpr visitQualified_asterisk(Qualified_asteriskContext ctx) {
    QualifiedAsteriskExpr target = new QualifiedAsteriskExpr();
    if (ctx.tb_name != null) {
      target.setQualifier(ctx.tb_name.getText());
    }

    return new NamedExpr(target);
  }

  @Override
  public Expr visitCharacter_string_type(Character_string_typeContext ctx) {
    return new LiteralValue(stripQuote(ctx.getText()), LiteralType.String);
  }

  @Override
  public Expr visitCharacter_value_expression(Character_value_expressionContext ctx) {
    Expr current = visitCharacter_factor(ctx.character_factor(0));

    Expr left;
    Expr right;
    for (int i = 1; i < ctx.getChildCount(); i++) {
      left = current;
      i++; // skip '||' operator
      right = visitCharacter_factor((Character_factorContext) ctx.getChild(i));

      if (left.getType() == OpType.Literal && right.getType() == OpType.Literal) {
        current = new LiteralValue(((LiteralValue) left).getValue() + ((LiteralValue) right).getValue(),
            LiteralType.String);
      } else {
        current = new BinaryOperator(OpType.Concatenate, left, right);
      }
    }

    return current;
  }


  @Override
  public Expr visitNumeric_value_function(Numeric_value_functionContext ctx) {
    if (checkIfExist(ctx.extract_expression())) {
      return visitExtract_expression(ctx.extract_expression());
    }
    if (checkIfExist(ctx.datetime_value_function())) {
      return visitDatetime_value_function(ctx.datetime_value_function());
    }
    return null;
  }

  @Override
  public Expr visitExtract_expression(Extract_expressionContext ctx) {
    Expr extractTarget = new LiteralValue(ctx.extract_field_string.getText(), LiteralType.String);
    Expr extractSource = visitDatetime_value_expression(ctx.extract_source().datetime_value_expression());

    String functionName = "date_part";
    Expr[] params = new Expr[]{extractTarget, extractSource};

    return new FunctionExpr(functionName, params);
  }

  @Override
  public Expr visitTrim_function(Trim_functionContext ctx) {
    Expr trimSource = visitChildren(ctx.trim_operands().trim_source);
    String functionName = "trim";
    if (checkIfExist(ctx.trim_operands().FROM())) {
      if (checkIfExist(ctx.trim_operands().trim_specification())) {
        Trim_specificationContext specification = ctx.trim_operands().trim_specification();
        if (checkIfExist(specification.LEADING())) {
          functionName = "ltrim";
        } else if (checkIfExist(specification.TRAILING())) {
          functionName = "rtrim";
        } else {
          functionName = "trim";
        }
      }
    }

    Expr trimCharacters = null;
    if (checkIfExist(ctx.trim_operands().trim_character)) {
      trimCharacters = visitCharacter_value_expression(ctx.trim_operands().trim_character);
    }

    Expr[] params;
    if (trimCharacters != null) {
      params = new Expr[]{trimSource, trimCharacters};
    } else {
      params = new Expr[]{trimSource};
    }

    return new FunctionExpr(functionName, params);
  }

  @Override
  public Expr visitCreate_index_statement(Create_index_statementContext ctx) {
    String indexName = ctx.index_name.getText();
    String tableName = buildIdentifierChain(ctx.table_name().identifier());
    Relation relation = new Relation(tableName);
    SortSpec[] sortSpecs = buildSortSpecs(ctx.sort_specifier_list());
    List<NamedExpr> targets = new ArrayList<>();
    Projection projection = new Projection();
    int i = 0;
    for (SortSpec sortSpec : sortSpecs) {
      targets.add(new NamedExpr(sortSpec.getKey()));
    }
    projection.setNamedExprs(targets);
    projection.setChild(relation);

    CreateIndex createIndex = new CreateIndex(indexName, sortSpecs);
    if (checkIfExist(ctx.UNIQUE())) {
      createIndex.setUnique(true);
    }
    if (checkIfExist(ctx.method_specifier())) {
      String methodName = buildIdentifier(ctx.method_specifier().identifier());
      createIndex.setMethodSpec(new IndexMethodSpec(methodName));
    }
    if (checkIfExist(ctx.param_clause())) {
      Map<String, String> params = getParams(ctx.param_clause());
      createIndex.setParams(params);
    }
    if (checkIfExist(ctx.where_clause())) {
      Selection selection = visitWhere_clause(ctx.where_clause());
      selection.setChild(relation);
      projection.setChild(selection);
    }
    if (checkIfExist(ctx.LOCATION())) {
      createIndex.setIndexPath(stripQuote(ctx.path.getText()));
    }
    createIndex.setChild(projection);
    return createIndex;
  }

  @Override
  public Expr visitDrop_index_statement(Drop_index_statementContext ctx) {
    String indexName = buildIdentifier(ctx.identifier());
    return new DropIndex(indexName);
  }

  @Override
  public Expr visitDatabase_definition(@NotNull Database_definitionContext ctx) {
    return new CreateDatabase(buildIdentifier(ctx.identifier()), null, checkIfExist(ctx.if_not_exists()));
  }

  @Override
  public Expr visitDrop_database_statement(@NotNull Drop_database_statementContext ctx) {
    return new DropDatabase(buildIdentifier(ctx.identifier()), checkIfExist(ctx.if_exists()));
  }

  @Override
  public Expr visitCreate_table_statement(Create_table_statementContext ctx) {
    String tableName = buildIdentifierChain(ctx.table_name(0).identifier());
    CreateTable createTable = new CreateTable(tableName, checkIfExist(ctx.if_not_exists()));
    if(checkIfExist(ctx.LIKE()))  {
      createTable.setLikeParentTable(buildIdentifierChain(ctx.like_table_name.identifier()));
      return createTable;
    }

    if (checkIfExist(ctx.EXTERNAL())) {
      createTable.setExternal();

      if (checkIfExist(ctx.table_elements().asterisk())) {
        createTable.setHasSelfDescSchema();
      } else {
        ColumnDefinition[] elements = getDefinitions(ctx.table_elements());
        createTable.setTableElements(elements);
      }

      if (checkIfExist(ctx.TABLESPACE())) {
        throw new TajoRuntimeException(new SQLSyntaxError("Tablespace clause is not allowed for an external table."));
      }

      String storageType = ctx.storage_type.getText();
      createTable.setStorageType(storageType);

      if (checkIfExist(ctx.LOCATION())) {
        String uri = stripQuote(ctx.uri.getText());
        createTable.setLocation(uri);
      } else {
        throw new TajoRuntimeException(new SQLSyntaxError("LOCATION clause must be required for an external table."));
      }
    } else {
      if (checkIfExist(ctx.table_elements())) {
        if (checkIfExist(ctx.table_elements().asterisk())) {
          createTable.setHasSelfDescSchema();
        } else {
          ColumnDefinition[] elements = getDefinitions(ctx.table_elements());
          createTable.setTableElements(elements);
        }
      }

      if (checkIfExist(ctx.TABLESPACE())) {
        String spaceName = ctx.spacename.getText();
        createTable.setTableSpaceName(spaceName);
      }

      if (checkIfExist(ctx.USING())) {
        String fileType = ctx.storage_type.getText();
        createTable.setStorageType(fileType);
      }

      if (checkIfExist(ctx.query_expression())) {
        Expr subquery = visitQuery_expression(ctx.query_expression());
        createTable.setSubQuery(subquery);
        createTable.unsetHasSelfDescSchema();
      }
    }

    if (checkIfExist(ctx.param_clause())) {
      Map<String, String> params = escapeTableMeta(getParams(ctx.param_clause()));
      createTable.setParams(params);
    }

    if (checkIfExist(ctx.table_partitioning_clauses())) {
      PartitionMethodDescExpr partitionMethodDesc =
          parseTablePartitioningClause(ctx.table_partitioning_clauses());
      createTable.setPartitionMethod(partitionMethodDesc);
    }
    return createTable;
  }

  @Override
  public Expr visitTruncate_table_statement(@NotNull Truncate_table_statementContext ctx) {
    List<Table_nameContext> tableNameContexts = ctx.table_name();
    List<String> tableNames = new ArrayList<>();

    for (Table_nameContext eachTableNameContext: tableNameContexts) {
      tableNames.add(buildIdentifierChain(eachTableNameContext.identifier()));
    }

    return new TruncateTable(tableNames);
  }

  private ColumnDefinition[] getDefinitions(Table_elementsContext ctx) {
    int size = ctx.field_element().size();
    ColumnDefinition[] elements = new ColumnDefinition[size];
    for (int i = 0; i < size; i++) {
      String name = ctx.field_element(i).name.getText();

      String dataTypeName = ctx.field_element(i).field_type().data_type().getText();
      DataTypeExpr typeDef = visitData_type(ctx.field_element(i).field_type().data_type());
      Preconditions.checkNotNull(typeDef, dataTypeName + " is not handled correctly");
      elements[i] = new ColumnDefinition(name, typeDef);
    }

    return elements;
  }

  public PartitionMethodDescExpr parseTablePartitioningClause(Table_partitioning_clausesContext ctx) {

    if (checkIfExist(ctx.range_partitions())) { // For Range Partition
      Range_partitionsContext rangePartitionsContext = ctx.range_partitions();
      List<Range_value_clauseContext> rangeValueClause = rangePartitionsContext.
          range_value_clause_list().range_value_clause();

      List<RangePartitionSpecifier> specifiers = Lists.newArrayList();

      for (Range_value_clauseContext rangeValue : rangeValueClause) {
        if (checkIfExist(rangeValue.MAXVALUE())) { // LESS THAN (MAXVALUE)
          specifiers.add(new RangePartitionSpecifier(rangeValue.partition_name().getText()));
        } else { // LESS THAN (expr)
          specifiers.add(new RangePartitionSpecifier(rangeValue.partition_name().getText(),
              visitValue_expression(rangeValue.value_expression())));
        }
      }
      return new CreateTable.RangePartition(buildColumnReferenceList(ctx.range_partitions().column_reference_list()),
          specifiers);

    } else if (checkIfExist(ctx.hash_partitions())) { // For Hash Partition
      Hash_partitionsContext hashPartitions = ctx.hash_partitions();

      if (checkIfExist(hashPartitions.hash_partitions_by_quantity())) { // PARTITIONS (num)
        return new HashPartition(buildColumnReferenceList(hashPartitions.column_reference_list()),
            visitNumeric_value_expression(hashPartitions.hash_partitions_by_quantity().quantity));

      } else { // ( PARTITION part_name , ...)
        List<CreateTable.PartitionSpecifier> specifiers = Lists.newArrayList();
        for (Individual_hash_partitionContext partition :
            hashPartitions.individual_hash_partitions().individual_hash_partition()) {
          specifiers.add(new CreateTable.PartitionSpecifier(partition.partition_name().getText()));
        }
        return new HashPartition(buildColumnReferenceList(hashPartitions.column_reference_list()), specifiers);
      }

    } else if (checkIfExist(ctx.list_partitions())) { // For List Partition
      List_partitionsContext listPartitions = ctx.list_partitions();
      List<List_value_partitionContext> partitions = listPartitions.list_value_clause_list().list_value_partition();
      List<ListPartitionSpecifier> specifiers = Lists.newArrayList();

      for (List_value_partitionContext listValuePartition : partitions) {
        int size = listValuePartition.in_value_list().row_value_predicand().size();
        Expr [] exprs = new Expr[size];
        for (int i = 0; i < size; i++) {
          exprs[i] = visitRow_value_predicand(listValuePartition.in_value_list().row_value_predicand(i));
        }
        specifiers.add(new ListPartitionSpecifier(listValuePartition.partition_name().getText(),
            new ValueListExpr(exprs)));
      }
      return new ListPartition(buildColumnReferenceList(ctx.list_partitions().column_reference_list()), specifiers);

    } else if (checkIfExist(ctx.column_partitions())) { // For Column Partition (Hive Style)
      return new CreateTable.ColumnPartition(getDefinitions(ctx.column_partitions().table_elements()));
    } else {
      throw new TajoInternalError("invalid partition type: " + ctx.toStringTree());
    }
  }

  @Override
  public DataTypeExpr visitData_type(Data_typeContext ctx) {
    Predefined_typeContext predefined_type = ctx.predefined_type();

    DataTypeExpr typeDefinition = null;

    // CHAR -> FIXED   CHAR
    //      |- VARYING CHAR
    // TEXT
    if (checkIfExist(predefined_type.character_string_type())) {

      Character_string_typeContext character_string_type = predefined_type.character_string_type();


      if ((checkIfExist(character_string_type.CHARACTER()) || checkIfExist(character_string_type.CHAR())) &&
          !checkIfExist(character_string_type.VARYING())) {

        typeDefinition = new DataTypeExpr(Type.CHAR.name());

        if (character_string_type.type_length() != null) {
          typeDefinition.setLengthOrPrecision(
              Integer.parseInt(character_string_type.type_length().NUMBER().getText()));
        }

      } else if (checkIfExist(character_string_type.VARCHAR()) || checkIfExist(character_string_type.VARYING())) {

        typeDefinition = new DataTypeExpr(Type.VARCHAR.name());

        if (character_string_type.type_length() != null) {
          typeDefinition.setLengthOrPrecision(
              Integer.parseInt(character_string_type.type_length().NUMBER().getText()));
        }

      } else if (checkIfExist(character_string_type.TEXT())) {
        typeDefinition = new DataTypeExpr(Type.TEXT.name());
      }

    // NCHAR
    } else if (checkIfExist(predefined_type.national_character_string_type())) {

      National_character_string_typeContext nchar_type = predefined_type.national_character_string_type();

      if ((checkIfExist(nchar_type.CHAR()) || checkIfExist(nchar_type.CHARACTER()) ||
          checkIfExist(nchar_type.NCHAR()) && !checkIfExist(nchar_type.VARYING()))) {

        typeDefinition = new DataTypeExpr(Type.NCHAR.name());

      } else if (checkIfExist(nchar_type.NVARCHAR()) || checkIfExist(nchar_type.VARYING())) {

        typeDefinition = new DataTypeExpr(Type.NVARCHAR.name());
      }

      // if a length is given
      if (checkIfExist(nchar_type.type_length())) {
        typeDefinition.setLengthOrPrecision(Integer.parseInt(nchar_type.type_length().NUMBER().getText()));
      }

    // BLOB types
    } else if (checkIfExist(predefined_type.binary_large_object_string_type())) {

      Binary_large_object_string_typeContext blob_type = predefined_type.binary_large_object_string_type();

      typeDefinition = new DataTypeExpr(Type.BLOB.name());

      if (checkIfExist(blob_type.type_length())) {
        typeDefinition.setLengthOrPrecision(Integer.parseInt(blob_type.type_length().NUMBER().getText()));
      }

    // NUMERIC types
    } else if (checkIfExist(predefined_type.numeric_type())) {
      // exact number
      if (checkIfExist(predefined_type.numeric_type().exact_numeric_type())) {

        Exact_numeric_typeContext exactType = predefined_type.numeric_type().exact_numeric_type();

        if (checkIfExist(exactType.TINYINT()) || checkIfExist(exactType.INT1())) {
          typeDefinition = new DataTypeExpr(Type.INT1.name());

        } else if (checkIfExist(exactType.INT2()) || checkIfExist(exactType.SMALLINT())) {
          typeDefinition = new DataTypeExpr(Type.INT2.name());

        } else if (checkIfExist(exactType.INT4()) ||
            checkIfExist(exactType.INTEGER()) ||
            checkIfExist(exactType.INT())) {
          typeDefinition = new DataTypeExpr(Type.INT4.name());

        } else if (checkIfExist(exactType.INT8()) || checkIfExist(exactType.BIGINT()) ) {
          typeDefinition = new DataTypeExpr(Type.INT8.name());

        } else if (checkIfExist(exactType.NUMERIC()) ||
            checkIfExist(exactType.DECIMAL()) ||
            checkIfExist(exactType.DEC())) {
          typeDefinition = new DataTypeExpr(Type.NUMERIC.name());

          if (checkIfExist(exactType.precision_param())) {
            typeDefinition.setLengthOrPrecision(Integer.parseInt(exactType.precision_param().precision.getText()));

            if (checkIfExist(exactType.precision_param().scale)) {
              typeDefinition.setScale(Integer.parseInt(exactType.precision_param().scale.getText()));
            }
          }
        }


      } else { // approximate number
        Approximate_numeric_typeContext approximateType = predefined_type.numeric_type().approximate_numeric_type();
        if (checkIfExist(approximateType.FLOAT()) ||
            checkIfExist(approximateType.FLOAT4()) ||
            checkIfExist(approximateType.REAL())) {
          typeDefinition = new DataTypeExpr(Type.FLOAT4.name());

        } else if (checkIfExist(approximateType.FLOAT8()) || checkIfExist(approximateType.DOUBLE())) {
          typeDefinition = new DataTypeExpr(Type.FLOAT8.name());
        }
      }

    } else if (checkIfExist(predefined_type.boolean_type())) {
      typeDefinition = new DataTypeExpr(Type.BOOLEAN.name());

    } else if (checkIfExist(predefined_type.datetime_type())) {

      Datetime_typeContext dateTimeType = predefined_type.datetime_type();
      if (checkIfExist(dateTimeType.DATE())) {
        typeDefinition = new DataTypeExpr(Type.DATE.name());

      } else if (checkIfExist(dateTimeType.TIME(0))) {
        if (checkIfExist(dateTimeType.ZONE())) {
          typeDefinition = new DataTypeExpr(Type.TIMEZ.name());
        } else {
          typeDefinition = new DataTypeExpr(Type.TIME.name());
        }

      } else if (checkIfExist(dateTimeType.TIMETZ())) {
        typeDefinition = new DataTypeExpr(Type.TIMEZ.name());

      } else if (checkIfExist(dateTimeType.TIMESTAMP())) {
         if (checkIfExist(dateTimeType.ZONE())) {
           typeDefinition = new DataTypeExpr(Type.TIMESTAMPZ.name());
         }  else {
           typeDefinition = new DataTypeExpr(Type.TIMESTAMP.name());
         }

      } else if (checkIfExist(dateTimeType.TIMESTAMPTZ())) {
        typeDefinition = new DataTypeExpr(Type.TIMESTAMPZ.name());
      }

      // bit data types
    } else if (predefined_type.bit_type() != null) {
      Bit_typeContext bitType = predefined_type.bit_type();

      if (checkIfExist(bitType.VARBIT()) || checkIfExist(bitType.VARYING())) {
        typeDefinition = new DataTypeExpr(Type.VARBIT.name());

      } else {
        typeDefinition = new DataTypeExpr(Type.BIT.name());
      }

      if (checkIfExist(bitType.type_length())) {
        typeDefinition.setLengthOrPrecision(
            Integer.parseInt(bitType.type_length().NUMBER().getText()));
      }


      // binary data types
    } else if (checkIfExist(predefined_type.binary_type())) {
      Binary_typeContext binaryType = predefined_type.binary_type();

      if (checkIfExist(binaryType.VARBINARY()) || checkIfExist(binaryType.VARYING())) {
        typeDefinition = new DataTypeExpr(Type.VARBINARY.name());
      } else {
        typeDefinition = new DataTypeExpr(Type.BINARY.name());
      }


      if (checkIfExist(binaryType.type_length())) {
        typeDefinition.setLengthOrPrecision(Integer.parseInt(binaryType.type_length().NUMBER().getText()));
      }

      // inet
    } else if (checkIfExist(predefined_type.network_type())) {
      typeDefinition = new DataTypeExpr(Type.INET4.name());


    } else if (checkIfExist(predefined_type.record_type())) {
      ColumnDefinition [] nestedRecordDefine = getDefinitions(predefined_type.record_type().table_elements());
      typeDefinition = new DataTypeExpr(new DataTypeExpr.RecordType(nestedRecordDefine));

    } else if (checkIfExist(predefined_type.map_type())) {
      Map_typeContext mapTypeContext = predefined_type.map_type();
      typeDefinition = new DataTypeExpr(
          new MapType(visitData_type(mapTypeContext.key_type), visitData_type(mapTypeContext.value_type)));
    }

    return typeDefinition;
  }

  @Override
  public Expr visitInsert_statement(Insert_statementContext ctx) {
    Insert insertExpr = new Insert();

    if (ctx.OVERWRITE() != null) {
      insertExpr.setOverwrite();
    }

    if (ctx.table_name() != null) {
      insertExpr.setTableName(buildIdentifierChain(ctx.table_name().identifier()));

      if (ctx.column_reference_list() != null) {
        ColumnReferenceExpr [] targetColumns =
            new ColumnReferenceExpr[ctx.column_reference_list().column_reference().size()];

        for (int i = 0; i < targetColumns.length; i++) {
          targetColumns[i] = visitColumn_reference(ctx.column_reference_list().column_reference(i));
        }

        insertExpr.setTargetColumns(targetColumns);
      }
    }

    if (ctx.LOCATION() != null) {
      insertExpr.setLocation(stripQuote(ctx.path.getText()));

      if (ctx.USING() != null) {
        insertExpr.setStorageType(ctx.storage_type.getText());

        if (ctx.param_clause() != null) {
          insertExpr.setParams(escapeTableMeta(getParams(ctx.param_clause())));
        }
      }
    }

    if (checkIfExist(ctx.VALUES())) {
      List<NamedExpr> values = ctx.row_value_predicand().stream()
          .map(value -> new NamedExpr(visitRow_value_predicand(value)))
          .collect(Collectors.toList());
      Projection projection = new Projection();
      projection.setNamedExprs(values);
      insertExpr.setSubQuery(projection);
    } else {
      insertExpr.setSubQuery(visitQuery_expression(ctx.query_expression()));
    }

    Preconditions.checkState(insertExpr.hasTableName() || insertExpr.hasLocation(),
        "Either a table name or a location should be given.");
    Preconditions.checkState(insertExpr.hasTableName() ^ insertExpr.hasLocation(),
        "A table name and a location cannot coexist.");
    return insertExpr;
  }

  @Override
  public Expr visitDrop_table_statement(Drop_table_statementContext ctx) {
    return new DropTable(buildIdentifierChain(ctx.table_name().identifier()),
        checkIfExist(ctx.if_exists()), checkIfExist(ctx.PURGE()));
  }


  private Map<String, String> getParams(Param_clauseContext ctx) {
    Map<String, String> params = new HashMap<>();
    for (int i = 0; i < ctx.param().size(); i++) {
      params.put(stripQuote(ctx.param(i).key.getText()), stripQuote(ctx.param(i).value.getText()));
    }

    return params;
  }

  public Map<String, String> escapeTableMeta(Map<String, String> map) {
    Map<String, String> params = new HashMap<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey().equals(StorageConstants.TEXT_DELIMITER)) {
        params.put(StorageConstants.TEXT_DELIMITER, StringUtils.unicodeEscapedDelimiter(entry.getValue()));
      } else if (TimeZoneUtil.isTimezone(entry.getKey())) {
        params.put(StorageConstants.TIMEZONE, TimeZoneUtil.getValidTimezone(entry.getValue()));
      } else {
        params.put(entry.getKey(), entry.getValue());
      }
    }
    return params;
  }

  private static String stripQuote(String str) {
    return str.substring(1, str.length() - 1);
  }

  @Override
  public Expr visitCast_specification(Cast_specificationContext ctx) {
    Expr operand = visitChildren(ctx.cast_operand());
    DataTypeExpr castTarget = visitData_type(ctx.cast_target().data_type());
    return new CastExpr(operand, castTarget);
  }

  @Override
  public Expr visitUnsigned_value_specification(@NotNull Unsigned_value_specificationContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Expr visitUnsigned_literal(@NotNull Unsigned_literalContext ctx) {
    if (checkIfExist(ctx.unsigned_numeric_literal())) {
      return visitUnsigned_numeric_literal(ctx.unsigned_numeric_literal());
    } else {
      return visitGeneral_literal(ctx.general_literal());
    }
  }

  @Override
  public Expr visitGeneral_literal(General_literalContext ctx) {
    if (checkIfExist(ctx.Character_String_Literal())) {
      return new LiteralValue(stripQuote(ctx.Character_String_Literal().getText()), LiteralType.String);
    } else if (checkIfExist(ctx.datetime_literal())) {
      return visitDatetime_literal(ctx.datetime_literal());
    } else {
      return new BooleanLiteral(checkIfExist(ctx.boolean_literal().TRUE()));
    }
  }

  @Override
  public Expr visitDatetime_literal(@NotNull Datetime_literalContext ctx) {
    if (checkIfExist(ctx.time_literal())) {
      return visitTime_literal(ctx.time_literal());
    } else if (checkIfExist(ctx.date_literal())) {
      return visitDate_literal(ctx.date_literal());
    } else if (checkIfExist(ctx.interval_literal())) {
      return visitInterval_literal(ctx.interval_literal());
    } else {
      return visitTimestamp_literal(ctx.timestamp_literal());
    }
  }

  @Override
  public Expr visitTime_literal(Time_literalContext ctx) {
    String timePart = stripQuote(ctx.time_string.getText());
    return new TimeLiteral(parseTime(timePart));
  }

  @Override
  public Expr visitDate_literal(Date_literalContext ctx) {
    String datePart = stripQuote(ctx.date_string.getText());
    return new DateLiteral(parseDate(datePart));
  }

  @Override
  public Expr visitTimestamp_literal(Timestamp_literalContext ctx) {
    String timestampStr = stripQuote(ctx.timestamp_string.getText());
    String[] parts = timestampStr.split(" ");
    String datePart = parts[0];
    String timePart = parts[1];
    return new TimestampLiteral(parseDate(datePart), parseTime(timePart));
  }

  @Override
  public Expr visitInterval_literal(@NotNull Interval_literalContext ctx) {
    String intervalStr = stripQuote(ctx.interval_string.getText());
    return new IntervalLiteral(intervalStr);
  }

  @Override
  public Expr visitDatetime_value_expression(@NotNull Datetime_value_expressionContext ctx) {
    return visitDatetime_term(ctx.datetime_term());
  }

  @Override
  public Expr visitDatetime_term(@NotNull Datetime_termContext ctx) {
    return visitDatetime_factor(ctx.datetime_factor());
  }

  @Override
  public Expr visitDatetime_factor(@NotNull Datetime_factorContext ctx) {
    return visitDatetime_primary(ctx.datetime_primary());
  }

  @Override
  public Expr visitDatetime_primary(@NotNull Datetime_primaryContext ctx) {
    if (checkIfExist(ctx.value_expression_primary())) {
      return visitValue_expression_primary(ctx.value_expression_primary());
    } else {
      return visitDatetime_value_function(ctx.datetime_value_function());
    }
  }

  @Override
  public Expr visitDatetime_value_function(@NotNull Datetime_value_functionContext ctx) {
    if (checkIfExist(ctx.current_date_value_function())) {
      return visitCurrent_date_value_function(ctx.current_date_value_function());
    } else if (checkIfExist(ctx.current_time_value_function())) {
      return visitCurrent_time_value_function(ctx.current_time_value_function());
    } else {
      return visitCurrent_timestamp_value_function(ctx.current_timestamp_value_function());
    }
  }

  @Override
  public Expr visitCurrent_date_value_function(@NotNull Current_date_value_functionContext ctx) {
    String functionName = "current_date";
    Expr[] params = new Expr[]{};
    return new FunctionExpr(functionName, params);
  }

  @Override
  public Expr visitCurrent_time_value_function(@NotNull Current_time_value_functionContext ctx) {
    String functionName = "current_time";
    Expr[] params = new Expr[]{};
    return new FunctionExpr(functionName, params);
  }

  @Override
  public Expr visitCurrent_timestamp_value_function(@NotNull Current_timestamp_value_functionContext ctx) {
    String functionName = "now";
    Expr[] params = new Expr[]{};
    return new FunctionExpr(functionName, params);
  }

  private DateValue parseDate(String datePart) {
    // e.g., 1980-04-01
    String[] parts = datePart.split("-");
    return new DateValue(parts[0], parts[1], parts[2]);
  }

  private TimeValue parseTime(String timePart) {
    // e.g., 12:01:50.399
    String[] parts = timePart.split(":");

    TimeValue time;
    boolean hasFractionOfSeconds = (parts.length > 2 && parts[2].indexOf('.') > 0);
    if (hasFractionOfSeconds) {
      String[] secondsParts = parts[2].split("\\.");
      time = new TimeValue(parts[0], parts[1], secondsParts[0]);
      if (secondsParts.length == 2) {
        time.setSecondsFraction(secondsParts[1]);
      }
    } else {
      time = new TimeValue(parts[0],
          (parts.length > 1 ? parts[1] : "0"),
          (parts.length > 2 ? parts[2] : "0"));
    }
    return time;
  }

  @Override
  public Expr visitAlter_tablespace_statement(@NotNull Alter_tablespace_statementContext ctx) {
    AlterTablespace alter = new AlterTablespace(ctx.space_name.getText());
    alter.setLocation(stripQuote(ctx.uri.getText()));
    return alter;
  }

  @Override
  public Expr visitAlter_table_statement(Alter_table_statementContext ctx) {

    final List<Table_nameContext> tables = ctx.table_name();

    final AlterTable alterTable = new AlterTable(buildIdentifierChain(tables.get(0).identifier()));

    if (tables.size() == 2) {
      alterTable.setNewTableName(buildIdentifierChain(tables.get(1).identifier()));
    }

    if (checkIfExist(ctx.column_name()) && ctx.column_name().size() == 2) {
      final List<Column_nameContext> columns = ctx.column_name();
      alterTable.setColumnName(buildIdentifier(columns.get(0).identifier()));
      alterTable.setNewColumnName(buildIdentifier(columns.get(1).identifier()));
    }

    Field_elementContext field_elementContext = ctx.field_element();
    if (checkIfExist(field_elementContext)) {
      final String name = field_elementContext.name.getText();
      final DataTypeExpr typeDef = visitData_type(field_elementContext.field_type().data_type());
      final ColumnDefinition columnDefinition = new ColumnDefinition(name, typeDef);
      alterTable.setAddNewColumn(columnDefinition);
    }

    if (checkIfExist(ctx.partition_column_value_list())) {
      List<Partition_column_valueContext> columnValueList = ctx.partition_column_value_list().partition_column_value();
      int size = columnValueList.size();
      ColumnReferenceExpr[] columns = new ColumnReferenceExpr[size];
      Expr[] values = new Expr[size];
      for (int i = 0; i < size; i++) {
        Partition_column_valueContext columnValue = columnValueList.get(i);
        columns[i] = new ColumnReferenceExpr(buildIdentifier(columnValue.identifier()));
        values[i] = visitRow_value_predicand(columnValue.row_value_predicand());
      }
      alterTable.setColumns(columns);
      alterTable.setValues(values);
      if (ctx.LOCATION() != null) {
        String path = stripQuote(ctx.path.getText());
        alterTable.setLocation(path);
      }
      alterTable.setPurge(checkIfExist(ctx.PURGE()));
      alterTable.setIfNotExists(checkIfExist(ctx.if_not_exists()));
      alterTable.setIfExists(checkIfExist(ctx.if_exists()));
    }

    if (checkIfExist(ctx.property_list())) {
      alterTable.setParams(getProperties(ctx.property_list()));
    }

    alterTable.setAlterTableOpType(determineAlterTableType(ctx));

    return alterTable;
  }

  private Map<String, String> getProperties(Property_listContext ctx) {
    Map<String, String> params = new HashMap<>();
    for (int i = 0; i < ctx.property().size(); i++) {
      params.put(stripQuote(ctx.property(i).key.getText()), stripQuote(ctx.property(i).value.getText()));
    }

    return params;
  }

  private AlterTableOpType determineAlterTableType(Alter_table_statementContext ctx) {

    final int RENAME_MASK = 00000001;
    final int COLUMN_MASK = 00000010;
    final int TO_MASK = 00000100;
    final int ADD_MASK = 00001000;
    final int DROP_MASK = 00001001;
    final int PARTITION_MASK = 00000020;
    final int SET_MASK = 00000002;
    final int PROPERTY_MASK = 00010000;
    final int REPAIR_MASK = 00000003;

    int val = 00000000;

    for (int idx = 1; idx < ctx.getChildCount(); idx++) {

      if (ctx.getChild(idx) instanceof TerminalNode) {
        switch (((TerminalNode) ctx.getChild(idx)).getSymbol().getType()) {
          case RENAME:
            val = val | RENAME_MASK;
            break;
          case COLUMN:
            val = val | COLUMN_MASK;
            break;
          case TO:
            val = val | TO_MASK;
            break;
          case ADD:
            val = val | ADD_MASK;
            break;
          case DROP:
            val = val | DROP_MASK;
            break;
          case PARTITION:
            val = val | PARTITION_MASK;
            break;
          case SET:
            val = val | SET_MASK;
            break;
          case PROPERTY:
            val = val | PROPERTY_MASK;
            break;
          case REPAIR:
            val = val | REPAIR_MASK;
            break;
          default:
            break;
        }
      }
    }
    return evaluateAlterTableOperationTye(val);
  }

  private AlterTableOpType evaluateAlterTableOperationTye(final int value) {

    switch (value) {
      case 19:
        return AlterTableOpType.REPAIR_PARTITION;
      case 65:
        return AlterTableOpType.RENAME_TABLE;
      case 73:
        return AlterTableOpType.RENAME_COLUMN;
      case 520:
        return AlterTableOpType.ADD_COLUMN;
      case 528:
        return AlterTableOpType.ADD_PARTITION;
      case 529:
        return AlterTableOpType.DROP_PARTITION;
      case 4098:
        return AlterTableOpType.SET_PROPERTY;
      default:
        return null;
    }
  }

  /**
   * Return identifier, where text case sensitivity is kept depending on the kind of identifier and
   *
   * @param identifier IdentifierContext
   * @return Identifier
   */
  private static String buildIdentifier(IdentifierContext identifier) {
    if (checkIfExist(identifier.nonreserved_keywords())) {
      return identifier.getText().toLowerCase();
    } else {
      return identifier.getText();
    }
  }

  private static String buildIdentifierChain(final Collection<IdentifierContext> identifierChains) {
    return Joiner.on(".").join(Collections2.transform(identifierChains, new Function<IdentifierContext, String>() {
      @Override
      public String apply(IdentifierContext identifierContext) {
        return buildIdentifier(identifierContext);
      }
    }));
  }
}
