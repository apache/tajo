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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tajo.algebra.*;
import org.apache.tajo.algebra.Aggregation.GroupType;
import org.apache.tajo.algebra.LiteralValue.LiteralType;
import org.apache.tajo.engine.parser.SQLParser.*;
import org.apache.tajo.storage.CSVFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.algebra.Aggregation.GroupElement;
import static org.apache.tajo.algebra.CreateTable.*;

import org.apache.tajo.algebra.DateValue;
import org.apache.tajo.algebra.TimeValue;
import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.apache.tajo.engine.parser.SQLParser.*;

public class SQLAnalyzer extends SQLParserBaseVisitor<Expr> {
  private SQLParser parser;

  public SQLAnalyzer() {
  }

  public Expr parse(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    this.parser = new SQLParser(tokens);
    parser.setBuildParseTree(true);
    parser.removeErrorListeners();

    parser.setErrorHandler(new SQLErrorStrategy());
    parser.addErrorListener(new SQLErrorListener());

    SqlContext context;
    try {
      context = parser.sql();
    } catch (SQLParseError e) {
      throw new SQLSyntaxError(e);
    }
    return visitSql(context);
  }

  private static boolean checkIfExist(Object obj) {
    return obj != null;
  }

  @Override
  public Expr visitSql(SqlContext ctx) {
    return visit(ctx.statement());
  }

  @Override
  public Expr visitNon_join_query_expression(SQLParser.Non_join_query_expressionContext ctx) {

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

        SQLParser.Query_termContext queryTermContext =
            (SQLParser.Query_termContext) ctx.getChild(idx);
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

    for (int i = 1; i < ctx.getChildCount();) {
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

        i+=idx;
      }
    }

    return current;

  }

  @Override
  public Expr visitQuery_specification(SQLParser.Query_specificationContext ctx) {
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
   *   : MULTIPLY
   *   | select_sublist (COMMA select_sublist)*
   *   ;
   * </pre>
   * @param ctx
   * @return
   */
  @Override
  public Projection visitSelect_list(SQLParser.Select_listContext ctx) {
    Projection projection = new Projection();
    if (ctx.MULTIPLY() != null) {
      projection.setAll();
    } else {
      TargetExpr[] targets = new TargetExpr[ctx.select_sublist().size()];
      for (int i = 0; i < targets.length; i++) {
        targets[i] = visitSelect_sublist(ctx.select_sublist(i));
      }
      projection.setTargets(targets);
    }

    return projection;
  }

  /**
   * <pre>
   *   select_sublist
   *   : derived_column
   *   | asterisked_qualifier=Identifier DOT MULTIPLY
   *   ;
   * </pre>
   * @param ctx
   * @return
   */
  @Override
  public TargetExpr visitSelect_sublist(SQLParser.Select_sublistContext ctx) {
    if (ctx.asterisked_qualifier != null) {
      return new TargetExpr(new ColumnReferenceExpr(ctx.asterisked_qualifier.getText(), "*"));
    } else {
      return visitDerived_column(ctx.derived_column());
    }
  }

  @Override
  public RelationList visitFrom_clause(SQLParser.From_clauseContext ctx) {
    Expr [] relations = new Expr[ctx.table_reference_list().table_reference().size()];
    for (int i = 0; i < relations.length; i++) {
      relations[i] = visitTable_reference(ctx.table_reference_list().table_reference(i));
    }
    return new RelationList(relations);
  }

  @Override
  public Selection visitWhere_clause(SQLParser.Where_clauseContext ctx) {
    return new Selection(visitSearch_condition(ctx.search_condition()));
  }

  @Override
  public Aggregation visitGroupby_clause(SQLParser.Groupby_clauseContext ctx) {
    Aggregation clause = new Aggregation();

    // If grouping group is not empty
    if (ctx.grouping_element_list().grouping_element().get(0).empty_grouping_set() == null) {
      GroupElement [] groups = new GroupElement[ctx.grouping_element_list().
          grouping_element().size()];
      for (int i = 0; i < groups.length; i++) {
        SQLParser.Grouping_elementContext element =
            ctx.grouping_element_list().grouping_element().get(i);
        if (element.ordinary_grouping_set() != null) {
          groups[i] = new GroupElement(GroupType.OrdinaryGroup,
              getColumnReferences(element.ordinary_grouping_set().column_reference_list()));
        } else if (element.rollup_list() != null) {
          groups[i] = new GroupElement(GroupType.Rollup,
              getColumnReferences(element.rollup_list().c.column_reference_list()));
        } else if (element.cube_list() != null) {
          groups[i] = new GroupElement(GroupType.Cube,
              getColumnReferences(element.cube_list().c.column_reference_list()));
        }
      }
      clause.setGroups(groups);
    }

    return clause;
  }

  @Override
  public Sort visitOrderby_clause(SQLParser.Orderby_clauseContext ctx) {
    int size = ctx.sort_specifier_list().sort_specifier().size();
    Sort.SortSpec specs [] = new Sort.SortSpec[size];
    for (int i = 0; i < size; i++) {
      SQLParser.Sort_specifierContext specContext = ctx.sort_specifier_list().sort_specifier(i);
      ColumnReferenceExpr column = visitColumn_reference(specContext.column);
      specs[i] = new Sort.SortSpec(column);
      if (specContext.order_specification() != null) {
        if (specContext.order.DESC() != null) {
          specs[i].setDescending();
        }
      }

      if (specContext.null_ordering() != null) {
        if (specContext.null_ordering().FIRST() != null) {
          specs[i].setNullFirst();
        }
      }
    }

    return new Sort(specs);
  }

  @Override
  public Limit visitLimit_clause(SQLParser.Limit_clauseContext ctx) {
    return new Limit(visitNumeric_value_expression(ctx.numeric_value_expression()));
  }

  @Override
  public Expr visitJoined_table(SQLParser.Joined_tableContext ctx) {
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
  public Join visitJoined_table_primary(SQLParser.Joined_table_primaryContext ctx) {
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
          ColumnReferenceExpr [] columns = getColumnReferences(ctx.join_specification().
              named_columns_join().column_reference_list());
          join.setJoinColumns(columns);
        }
      }
    }

    join.setRight(visitTable_primary(ctx.right));
    return join;
  }

  private ColumnReferenceExpr [] getColumnReferences(Column_reference_listContext ctx) {
    ColumnReferenceExpr [] columnRefs = new ColumnReferenceExpr[ctx.column_reference().size()];
    for (int i = 0; i < columnRefs.length; i++) {
      columnRefs[i] = visitColumn_reference(ctx.column_reference(i));
    }
    return columnRefs;
  }

  @Override
  public Expr visitTable_primary(SQLParser.Table_primaryContext ctx) {
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
  public Expr visitSubquery(SQLParser.SubqueryContext ctx) {
    return visitQuery_expression(ctx.query_expression());
  }

  @Override
  public BetweenPredicate visitBetween_predicate(SQLParser.Between_predicateContext ctx) {
    Expr predicand = visitRow_value_predicand(ctx.predicand);
    Expr begin = visitRow_value_predicand(ctx.between_predicate_part_2().begin);
    Expr end = visitRow_value_predicand(ctx.between_predicate_part_2().end);
    return new BetweenPredicate(checkIfExist(ctx.between_predicate_part_2().NOT()),
        checkIfExist(ctx.between_predicate_part_2().SYMMETRIC()), predicand, begin, end);
  }

  @Override
  public CaseWhenPredicate visitSimple_case(SQLParser.Simple_caseContext ctx) {
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

  @Override public CaseWhenPredicate visitSearched_case(SQLParser.Searched_caseContext ctx) {
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

  @Override public Expr visitCommon_value_expression(SQLParser.Common_value_expressionContext ctx) {
    if (checkIfExist(ctx.NULL())) {
      return new NullLiteral();
    } else {
      return visitChildren(ctx);
    }
  }

  @Override
  public Expr visitParenthesized_value_expression(SQLParser.Parenthesized_value_expressionContext ctx) {
    return visitValue_expression(ctx.value_expression());
  }

  @Override
  public Expr visitBoolean_value_expression(SQLParser.Boolean_value_expressionContext ctx) {
    Expr current = visitOr_predicate(ctx.or_predicate());
    return current;
  }

  @Override
  public Expr visitOr_predicate(SQLParser.Or_predicateContext ctx) {
    Expr current = visitAnd_predicate(ctx.and_predicate());

    Expr left;
    Expr right;
    for (int i = 0; i < ctx.boolean_value_expression().size(); i++) {
      left = current;
      right = visitBoolean_value_expression(ctx.boolean_value_expression(i));
      current = new BinaryOperator(OpType.Or, left, right);
    }

    return current;
  }

  @Override
  public Expr visitAnd_predicate(SQLParser.And_predicateContext ctx) {
    Expr current = visitBoolean_factor(ctx.boolean_factor());

    Expr left;
    Expr right;
    for (int i = 0; i < ctx.boolean_value_expression().size(); i++) {
      left = current;
      right = visitBoolean_value_expression(ctx.boolean_value_expression(i));
      current = new BinaryOperator(OpType.And, left, right);
    }

    return current;
  }

  @Override
  public Expr visitBoolean_factor(SQLParser.Boolean_factorContext ctx) {
    if (ctx.NOT() != null) {
      return new NotExpr(visitBoolean_test(ctx.boolean_test()));
    } else {
      return visitBoolean_test(ctx.boolean_test());
    }
  }

  @Override
  public Expr visitBoolean_test(SQLParser.Boolean_testContext ctx) {
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
  public Expr visitBoolean_primary(SQLParser.Boolean_primaryContext ctx) {
    if (ctx.predicate() != null) {
      return visitPredicate(ctx.predicate());
    } else {
      return visitBoolean_predicand(ctx.boolean_predicand());
    }
  }

  @Override
  public Expr visitBoolean_predicand(SQLParser.Boolean_predicandContext ctx) {
    if (checkIfExist(ctx.nonparenthesized_value_expression_primary())) {
      return visitNonparenthesized_value_expression_primary(ctx.nonparenthesized_value_expression_primary());
    } else {
      return visitBoolean_value_expression(ctx.parenthesized_boolean_value_expression().boolean_value_expression());
    }
  }

  @Override
  public Expr visitNonparenthesized_value_expression_primary(
      SQLParser.Nonparenthesized_value_expression_primaryContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Expr visitRow_value_predicand(SQLParser.Row_value_predicandContext ctx) {
    if (checkIfExist(ctx.row_value_special_case())) {
      return visitRow_value_special_case(ctx.row_value_special_case());
    } else {
      return visitRow_value_constructor_predicand(ctx.row_value_constructor_predicand());
    }
  }

  @Override
  public Expr visitRow_value_constructor_predicand(SQLParser.Row_value_constructor_predicandContext ctx) {
    if (checkIfExist(ctx.boolean_predicand())) {
      return visitBoolean_predicand(ctx.boolean_predicand());
    } else {
      return visitCommon_value_expression(ctx.common_value_expression());
    }
  }

  @Override
  public BinaryOperator visitComparison_predicate(SQLParser.Comparison_predicateContext ctx) {
    TerminalNode operator = (TerminalNode) ctx.comp_op().getChild(0);
    return new BinaryOperator(tokenToExprType(operator.getSymbol().getType()),
        visitRow_value_predicand(ctx.left),
        visitRow_value_predicand(ctx.right));
  }

  @Override
  public Expr visitNumeric_value_expression(SQLParser.Numeric_value_expressionContext ctx) {
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
  public Expr visitTerm(SQLParser.TermContext ctx) {
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

  @Override public Expr visitFactor(SQLParser.FactorContext ctx) {
    Expr current = visitNumeric_primary(ctx.numeric_primary());
    if (checkIfExist(ctx.sign()) && checkIfExist(ctx.sign().MINUS())) {
      current = new SignedExpr(true, current);
    }
    return current;
  }

  @Override
  public Expr visitNumeric_primary(SQLParser.Numeric_primaryContext ctx) {
    Expr current = visitValue_expression_primary(ctx.value_expression_primary());

    for (int i = 0; i < ctx.CAST_EXPRESSION().size(); i++) {
      current = new CastExpr(current, visitData_type(ctx.cast_target(i).data_type()));
    }

    return current;
  }

  public static OpType tokenToExprType(int tokenId) {
    switch (tokenId) {
      case SQLParser.UNION: return OpType.Union;
      case SQLParser.EXCEPT: return OpType.Except;
      case SQLParser.INTERSECT: return OpType.Intersect;

      case SQLParser.AND: return OpType.And;
      case SQLParser.OR: return OpType.Or;

      case SQLParser.EQUAL: return OpType.Equals;
      case SQLParser.NOT_EQUAL: return OpType.NotEquals;
      case SQLParser.LTH: return OpType.LessThan;
      case SQLParser.LEQ: return OpType.LessThanOrEquals;
      case SQLParser.GTH: return OpType.GreaterThan;
      case SQLParser.GEQ: return OpType.GreaterThanOrEquals;

      case SQLParser.MULTIPLY: return OpType.Multiply;
      case SQLParser.DIVIDE: return OpType.Divide;
      case SQLParser.MODULAR: return OpType.Modular;
      case SQLParser.PLUS: return OpType.Plus;
      case SQLParser.MINUS: return OpType.Minus;

      default: throw new RuntimeException("Unknown Token Id: " + tokenId);
    }
  }

  @Override
  public InPredicate visitIn_predicate(SQLParser.In_predicateContext ctx) {
    return new InPredicate(visitChildren(ctx.numeric_value_expression()),
        visitIn_predicate_value(ctx.in_predicate_value()), ctx.NOT() != null);
  }

  @Override
  public Expr visitIn_predicate_value(SQLParser.In_predicate_valueContext ctx) {
    if (checkIfExist(ctx.in_value_list())) {
      int size = ctx.in_value_list().row_value_expression().size();
      Expr [] exprs = new Expr[size];
      for (int i = 0; i < size; i++) {
        exprs[i] = visitRow_value_expression(ctx.in_value_list().row_value_expression(i));
      }
      return new ValueListExpr(exprs);
    } else {
      return new SimpleTableSubQuery(visitChildren(ctx.table_subquery()));
    }
  }

  @Override
  public Expr visitArray(SQLParser.ArrayContext ctx) {
    int size = ctx.numeric_value_expression().size();
    Expr [] exprs = new Expr[size];
    for (int i = 0; i < size; i++) {
      exprs[i] = visit(ctx.numeric_value_expression(i));
    }
    return new ValueListExpr(exprs);
  }

  @Override
  public Expr visitPattern_matching_predicate(SQLParser.Pattern_matching_predicateContext ctx) {
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
        throw new SQLSyntaxError("Unsupported predicate: " + matcher.getText());
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
        throw new SQLSyntaxError("Unsupported predicate: " + matcher.getText());
      }
    } else {
      throw new SQLSyntaxError("Unsupported predicate: " + ctx.pattern_matcher().getText());
    }
  }

  @Override
  public IsNullPredicate visitNull_predicate(SQLParser.Null_predicateContext ctx) {
    Expr predicand = visitRow_value_predicand(ctx.row_value_predicand());
    return new IsNullPredicate(ctx.NOT() != null, predicand);
  }

  @Override
  public ExistsPredicate visitExists_predicate(SQLParser.Exists_predicateContext ctx) {
    return new ExistsPredicate(new SimpleTableSubQuery(visitTable_subquery(ctx.table_subquery())), ctx.NOT() != null);
  }

  @Override
  public ColumnReferenceExpr visitColumn_reference(SQLParser.Column_referenceContext ctx) {
    ColumnReferenceExpr column = new ColumnReferenceExpr(ctx.name.getText());
    if (ctx.tb_name != null) {
      column.setQualifier(ctx.tb_name.getText());
    }

    return column;
  }

  @Override
  public LiteralValue visitUnsigned_numeric_literal(@NotNull SQLParser.Unsigned_numeric_literalContext ctx) {
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

  @Override public FunctionExpr visitAggregate_function( SQLParser.Aggregate_functionContext ctx) {
    if (ctx.COUNT() != null && ctx.MULTIPLY() != null) {
      return new CountRowsFunctionExpr();
    } else {
      return visitGeneral_set_function(ctx.general_set_function());
    }
  }

  @Override public FunctionExpr visitGeneral_set_function(SQLParser.General_set_functionContext ctx) {
    String signature = ctx.set_function_type().getText();
    boolean distinct = checkIfExist(ctx.set_qualifier()) && checkIfExist(ctx.set_qualifier().DISTINCT());
    Expr param = visitValue_expression(ctx.value_expression());

    return new GeneralSetFunctionExpr(signature, distinct, param);
  }

  @Override
  public FunctionExpr visitRoutine_invocation(SQLParser.Routine_invocationContext ctx) {
    String signature = ctx.function_name().getText();
    FunctionExpr function = new FunctionExpr(signature);
    if (ctx.sql_argument_list() != null) {
      int numArgs = ctx.sql_argument_list().value_expression().size();
      Expr [] argument_list = new Expr[numArgs];
      for (int i = 0; i < numArgs; i++) {
        argument_list[i] = visitValue_expression(ctx.sql_argument_list().
            value_expression().get(i));
      }

      function.setParams(argument_list);
    }
    return function;
  }

  @Override
  public TargetExpr visitDerived_column(SQLParser.Derived_columnContext ctx) {
    TargetExpr target = new TargetExpr(visitValue_expression(ctx.value_expression()));
    if (ctx.as_clause() != null) {
      target.setAlias(ctx.as_clause().Identifier().getText());
    }
    return target;
  }



  @Override
  public Expr visitCharacter_string_type(SQLParser.Character_string_typeContext ctx) {
    return new LiteralValue(stripQuote(ctx.getText()), LiteralType.String);
  }

  @Override
  public Expr visitCharacter_value_expression(SQLParser.Character_value_expressionContext ctx) {
    Expr current = visitCharacter_factor(ctx.character_factor(0));

    Expr left;
    Expr right;
    for (int i = 1; i < ctx.getChildCount(); i++) {
      left = current;
      i++; // skip '||' operator
      right = visitCharacter_factor((Character_factorContext) ctx.getChild(i));

      if (left.getType() == OpType.Literal && right.getType() == OpType.Literal) {
        current = new LiteralValue(((LiteralValue)left).getValue() + ((LiteralValue)right).getValue(),
            LiteralType.String);
      } else {
        current = new BinaryOperator(OpType.Concatenate, left, right);
      }
    }

    return current;
  }

  @Override
  public Expr visitTrim_function(SQLParser.Trim_functionContext ctx) {
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

    Expr [] params;
    if (trimCharacters != null) {
      params = new Expr[] {trimSource, trimCharacters};
    } else {
      params = new Expr[] {trimSource};
    }

    return new FunctionExpr(functionName, params);
  }

  @Override
  public Expr visitCreate_table_statement(SQLParser.Create_table_statementContext ctx) {
    String tableName = ctx.table_name().getText();
    CreateTable createTable = new CreateTable(tableName);

    if (checkIfExist(ctx.EXTERNAL())) {
      createTable.setExternal();

      CreateTable.ColumnDefinition [] elements = getDefinitions(ctx.table_elements());
      String fileType = ctx.file_type.getText();
      String path = stripQuote(ctx.path.getText());

      createTable.setTableElements(elements);
      createTable.setStorageType(fileType);
      createTable.setLocation(path);
    } else {
      if (checkIfExist(ctx.table_elements())) {
        CreateTable.ColumnDefinition [] elements = getDefinitions(ctx.table_elements());
        createTable.setTableElements(elements);
      }

      if (checkIfExist(ctx.USING())) {
        String fileType = ctx.file_type.getText();
        createTable.setStorageType(fileType);
      }

      if (checkIfExist(ctx.query_expression())) {
        Expr subquery = visitQuery_expression(ctx.query_expression());
        createTable.setSubQuery(subquery);
      }
    }

    if (checkIfExist(ctx.param_clause())) {
      Map<String, String> params = escapeTableMeta(getParams(ctx.param_clause()));
      createTable.setParams(params);
    }

    if (checkIfExist(ctx.table_partitioning_clauses())) {
      PartitionDescExpr partitionDesc =
          parseTablePartitioningClause(ctx.table_partitioning_clauses());
      createTable.setPartition(partitionDesc);
    }
    return createTable;
  }

  private CreateTable.ColumnDefinition [] getDefinitions(SQLParser.Table_elementsContext ctx) {
    int size = ctx.field_element().size();
    CreateTable.ColumnDefinition [] elements = new CreateTable.ColumnDefinition[size];
    for (int i = 0; i < size; i++) {
      String name = ctx.field_element(i).name.getText();
      DataTypeExpr typeDef = visitData_type(ctx.field_element(i).field_type().data_type());
      elements[i] = new CreateTable.ColumnDefinition(name, typeDef);
    }

    return elements;
  }

  public PartitionDescExpr parseTablePartitioningClause(SQLParser.Table_partitioning_clausesContext ctx) {

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
      return new CreateTable.RangePartition(getColumnReferences(ctx.range_partitions().column_reference_list()),
          specifiers);

    } else if (checkIfExist(ctx.hash_partitions())) { // For Hash Partition
      Hash_partitionsContext hashPartitions = ctx.hash_partitions();

      if (checkIfExist(hashPartitions.hash_partitions_by_quantity())) { // PARTITIONS (num)
        return new HashPartition(getColumnReferences(hashPartitions.column_reference_list()),
            visitNumeric_value_expression(hashPartitions.hash_partitions_by_quantity().quantity));

      } else { // ( PARTITION part_name , ...)
        List<CreateTable.PartitionSpecifier> specifiers = Lists.newArrayList();
        for (Individual_hash_partitionContext partition :
            hashPartitions.individual_hash_partitions().individual_hash_partition()) {
          specifiers.add(new CreateTable.PartitionSpecifier(partition.partition_name().getText()));
        }
        return new HashPartition(getColumnReferences(hashPartitions.column_reference_list()), specifiers);
      }

    } else if (checkIfExist(ctx.list_partitions())) { // For List Partition
      List_partitionsContext listPartitions = ctx.list_partitions();
      List<List_value_partitionContext> partitions = listPartitions.list_value_clause_list().list_value_partition();
      List<ListPartitionSpecifier> specifiers = Lists.newArrayList();

      for (List_value_partitionContext listValuePartition : partitions) {
        int size = listValuePartition.in_value_list().row_value_expression().size();
        Expr [] exprs = new Expr[size];
        for (int i = 0; i < size; i++) {
          exprs[i] = visitRow_value_expression(listValuePartition.in_value_list().row_value_expression(i));
        }
        specifiers.add(new ListPartitionSpecifier(listValuePartition.partition_name().getText(),
            new ValueListExpr(exprs)));
      }
      return new ListPartition(getColumnReferences(ctx.list_partitions().column_reference_list()), specifiers);

    } else if (checkIfExist(ctx.column_partitions())) { // For Column Partition (Hive Style)
      return new CreateTable.ColumnPartition(getDefinitions(ctx.column_partitions().table_elements()), true);
    } else {
      throw new SQLSyntaxError("Invalid Partition Type: " + ctx.toStringTree());
    }
  }

  @Override
  public DataTypeExpr visitData_type(SQLParser.Data_typeContext ctx) {
    SQLParser.Predefined_typeContext predefined_type = ctx.predefined_type();

    DataTypeExpr typeDefinition = null;
    if (predefined_type.character_string_type() != null) {
      SQLParser.Character_string_typeContext character_string_type =
          predefined_type.character_string_type();

      if ((character_string_type.CHARACTER() != null || character_string_type.CHAR() != null) &&
          character_string_type.VARYING() == null) {

        typeDefinition = new DataTypeExpr(Type.CHAR.name());

        if (character_string_type.type_length() != null) {
          typeDefinition.setLengthOrPrecision(
              Integer.parseInt(character_string_type.type_length().NUMBER().getText()));
        }

      } else if (character_string_type.VARCHAR() != null
          || character_string_type.VARYING() != null) {

        typeDefinition = new DataTypeExpr(Type.VARCHAR.name());

        if (character_string_type.type_length() != null) {
          typeDefinition.setLengthOrPrecision(
              Integer.parseInt(character_string_type.type_length().NUMBER().getText()));
        }

      } else if (character_string_type.TEXT() != null) {
        typeDefinition =  new DataTypeExpr(Type.TEXT.name());
      }

    } else if (predefined_type.national_character_string_type() != null) {
      SQLParser.National_character_string_typeContext nchar_type =
          predefined_type.national_character_string_type();
      if ((nchar_type.CHAR() != null || nchar_type.CHARACTER() != null
          || nchar_type.NCHAR() != null) && nchar_type.VARYING() == null) {
        typeDefinition = new DataTypeExpr(Type.NCHAR.name());
      } else if (nchar_type.NVARCHAR() != null || nchar_type.VARYING() != null) {
        typeDefinition = new DataTypeExpr(Type.NVARCHAR.name());
      }

      if (nchar_type.type_length() != null) {
        typeDefinition.setLengthOrPrecision(
            Integer.parseInt(nchar_type.type_length().NUMBER().getText()));
      }

    } else if (predefined_type.binary_large_object_string_type() != null) {
      SQLParser.Binary_large_object_string_typeContext blob_type =
          predefined_type.binary_large_object_string_type();
      typeDefinition = new DataTypeExpr(Type.BLOB.name());
      if (blob_type.type_length() != null) {
        typeDefinition.setLengthOrPrecision(
            Integer.parseInt(blob_type.type_length().NUMBER().getText()));
      }
    } else if (predefined_type.numeric_type() != null) {
      // exact number
      if (predefined_type.numeric_type().exact_numeric_type() != null) {
        SQLParser.Exact_numeric_typeContext exactType =
            predefined_type.numeric_type().exact_numeric_type();
        if (exactType.TINYINT() != null || exactType.INT1() != null) {
          typeDefinition = new DataTypeExpr(Type.INT1.name());
        } else if (exactType.INT2() != null || exactType.SMALLINT() != null) {
          typeDefinition = new DataTypeExpr(Type.INT2.name());
        } else if (exactType.INT4() != null || exactType.INTEGER() != null ||
            exactType.INT() != null) {
          typeDefinition = new DataTypeExpr(Type.INT4.name());
        } else if (exactType.INT8() != null || exactType.BIGINT() != null) {
          typeDefinition = new DataTypeExpr(Type.INT8.name());
        } else if (exactType.NUMERIC() != null) {
          typeDefinition = new DataTypeExpr(Type.NUMERIC.name());
        } else if (exactType.DECIMAL() != null || exactType.DEC() != null) {
          typeDefinition = new DataTypeExpr(Type.DECIMAL.name());
        }

        if (typeDefinition.getTypeName().equals(Type.NUMERIC.name()) ||
            typeDefinition.getTypeName().equals(Type.DECIMAL.name())) {
          if (exactType.precision_param() != null) {
            if (exactType.precision_param().scale != null) {
              typeDefinition.setScale(
                  Integer.parseInt(exactType.precision_param().scale.getText()));
            }
            typeDefinition.setLengthOrPrecision(
                Integer.parseInt(exactType.precision_param().precision.getText()));
          }
        }
      } else { // approximate number
        SQLParser.Approximate_numeric_typeContext approximateType =
            predefined_type.numeric_type().approximate_numeric_type();
        if (approximateType.FLOAT() != null || approximateType.FLOAT4() != null
            || approximateType.REAL() != null) {
          typeDefinition = new DataTypeExpr(Type.FLOAT4.name());
        } else if (approximateType.FLOAT8() != null || approximateType.DOUBLE() != null) {
          typeDefinition = new DataTypeExpr(Type.FLOAT8.name());
        }
      }
    } else if (predefined_type.boolean_type() != null)  {
      typeDefinition = new DataTypeExpr(Type.BOOLEAN.name());
    } else if (predefined_type.datetime_type() != null) {
      SQLParser.Datetime_typeContext dateTimeType = predefined_type.datetime_type();
      if (dateTimeType.DATE() != null) {
        typeDefinition = new DataTypeExpr(Type.DATE.name());
      } else if (dateTimeType.TIME(0) != null && dateTimeType.ZONE() == null) {
        typeDefinition = new DataTypeExpr(Type.TIME.name());
      } else if ((dateTimeType.TIME(0) != null && dateTimeType.ZONE() != null) ||
          dateTimeType.TIMETZ() != null) {
        typeDefinition = new DataTypeExpr(Type.TIMEZ.name());
      } else if (dateTimeType.TIMESTAMP() != null && dateTimeType.ZONE() == null) {
        typeDefinition = new DataTypeExpr(Type.TIMESTAMP.name());
      } else if ((dateTimeType.TIMESTAMP() != null && dateTimeType.ZONE() != null) ||
          dateTimeType.TIMESTAMPTZ() != null) {
        typeDefinition = new DataTypeExpr(Type.TIMESTAMPZ.name());
      }
    } else if (predefined_type.bit_type() != null) {
      SQLParser.Bit_typeContext bitType = predefined_type.bit_type();
      if (bitType.VARBIT() != null || bitType.VARYING() != null) {
        typeDefinition = new DataTypeExpr(Type.VARBIT.name());
      } else {
        typeDefinition = new DataTypeExpr(Type.BIT.name());
      }
      if (bitType.type_length() != null) {
        typeDefinition.setLengthOrPrecision(
            Integer.parseInt(bitType.type_length().NUMBER().getText()));
      }
    } else if (predefined_type.binary_type() != null) {
      SQLParser.Binary_typeContext binaryType = predefined_type.binary_type();
      if (binaryType.VARBINARY() != null || binaryType.VARYING() != null) {
        typeDefinition = new DataTypeExpr(Type.VARBINARY.name());
      } else {
        typeDefinition = new DataTypeExpr(Type.BINARY.name());
      }

      if (binaryType.type_length() != null) {
        typeDefinition.setLengthOrPrecision(
            Integer.parseInt(binaryType.type_length().NUMBER().getText()));
      }
    }

    return typeDefinition;
  }

  @Override
  public Expr visitInsert_statement(SQLParser.Insert_statementContext ctx) {
    Insert insertExpr = new Insert();

    if (ctx.OVERWRITE() != null) {
      insertExpr.setOverwrite();
    }

    if (ctx.table_name() != null) {
      insertExpr.setTableName(ctx.table_name().getText());

      if (ctx.column_name_list() != null) {
        String [] targetColumns = new String[ctx.column_name_list().Identifier().size()];
        for (int i = 0; i < targetColumns.length; i++) {
          targetColumns[i] = ctx.column_name_list().Identifier().get(i).getText();
        }

        insertExpr.setTargetColumns(targetColumns);
      }
    }

    if (ctx.LOCATION() != null) {
      insertExpr.setLocation(stripQuote(ctx.path.getText()));

      if (ctx.USING() != null) {
        insertExpr.setStorageType(ctx.file_type.getText());

        if (ctx.param_clause() != null) {
          insertExpr.setParams(escapeTableMeta(getParams(ctx.param_clause())));
        }
      }
    }

    insertExpr.setSubQuery(visitQuery_expression(ctx.query_expression()));

    Preconditions.checkState(insertExpr.hasTableName() || insertExpr.hasLocation(),
        "Either a table name or a location should be given.");
    Preconditions.checkState(insertExpr.hasTableName() ^ insertExpr.hasLocation(),
        "A table name and a location cannot coexist.");
    return insertExpr;
  }

  @Override
  public Expr visitDrop_table_statement(SQLParser.Drop_table_statementContext ctx) {
    return new DropTable(ctx.table_name().getText(), checkIfExist(ctx.PURGE()));
  }


  private Map<String, String> getParams(SQLParser.Param_clauseContext ctx) {
    Map<String, String> params = new HashMap<String, String>();
    for (int i = 0; i < ctx.param().size(); i++) {
      params.put(stripQuote(ctx.param(i).key.getText()), stripQuote(ctx.param(i).value.getText()));
    }

    return params;
  }

  public Map<String, String> escapeTableMeta(Map<String, String> map) {
    Map<String, String> params = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey().equals(CSVFile.DELIMITER)) {
        params.put(entry.getKey(), escapeDelimiter(entry.getValue()));
      }  else {
        params.put(entry.getKey(), entry.getValue());
      }
    }
    return params;
  }

  public static String escapeDelimiter(String value) {
    try {
      String delimiter = StringEscapeUtils.unescapeJava(value);
      delimiter = new String(new byte[]{Byte.valueOf(delimiter).byteValue()});
      return StringEscapeUtils.escapeJava(delimiter);
    } catch (NumberFormatException e) {
    }
    return value;
  }

  private static String stripQuote(String str) {
    return str.substring(1, str.length() - 1);
  }

  @Override public Expr visitCast_specification(SQLParser.Cast_specificationContext ctx) {
    Expr operand = visitChildren(ctx.cast_operand());
    DataTypeExpr castTarget = visitData_type(ctx.cast_target().data_type());
    return new CastExpr(operand, castTarget);
  }

  @Override public Expr visitUnsigned_value_specification(@NotNull SQLParser.Unsigned_value_specificationContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Expr visitUnsigned_literal(@NotNull SQLParser.Unsigned_literalContext ctx) {
    if (checkIfExist(ctx.unsigned_numeric_literal())) {
      return visitUnsigned_numeric_literal(ctx.unsigned_numeric_literal());
    } else {
      return visitGeneral_literal(ctx.general_literal());
    }
  }

  @Override
  public Expr visitGeneral_literal(SQLParser.General_literalContext ctx) {
    if (checkIfExist(ctx.Character_String_Literal())) {
      return new LiteralValue(stripQuote(ctx.Character_String_Literal().getText()), LiteralType.String);
    } else if (checkIfExist(ctx.datetime_literal())) {
      return visitDatetime_literal(ctx.datetime_literal());
    } else {
      return new BooleanLiteral(checkIfExist(ctx.boolean_literal().TRUE()));
    }
  }

  @Override
  public Expr visitDatetime_literal(@NotNull SQLParser.Datetime_literalContext ctx) {
    if (checkIfExist(ctx.time_literal())) {
      return visitTime_literal(ctx.time_literal());
    } else { 
      return visitTimestamp_literal(ctx.timestamp_literal());
    }
  }

  @Override
  public Expr visitTime_literal(SQLParser.Time_literalContext ctx) {
    String timePart = stripQuote(ctx.time_string.getText());
    return new TimeLiteral(parseTime(timePart));
  }

  @Override
  public Expr visitTimestamp_literal(SQLParser.Timestamp_literalContext ctx) {
    String timestampStr = stripQuote(ctx.timestamp_string.getText());
    String [] parts = timestampStr.split(" ");
    String datePart = parts[0];
    String timePart = parts[1];
    return new TimestampLiteral(parseDate(datePart), parseTime(timePart));
  }

  private DateValue parseDate(String datePart) {
    // e.g., 1980-04-01
    String [] parts = datePart.split("-");
    return new DateValue(parts[0], parts[1], parts[2]);
  }

  private TimeValue parseTime(String timePart) {
    // e.g., 12:01:50.399
    String [] parts = timePart.split(":");

    TimeValue time;
    boolean hasFractionOfSeconds = parts[2].indexOf('.') > 0;
    if (hasFractionOfSeconds) {
      String [] secondsParts = parts[2].split(".");
      time = new TimeValue(parts[0], parts[1], secondsParts[0]);
      if (secondsParts.length == 2) {
        time.setSecondsFraction(secondsParts[1]);
      }
    } else {
      time = new TimeValue(parts[0], parts[1], parts[2]);
    }
    return time;
  }
}
