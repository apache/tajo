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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.algebra.*;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.engine.parser.HiveParser.TableAllColumnsContext;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveConverter extends HiveParserBaseVisitor<Expr> {
  private static final Log LOG = LogFactory.getLog(HiveConverter.class.getName());
  private HiveParser parser;

  public Expr parse(String sql) {
    HiveLexer lexer = new HiveLexer(new ANTLRNoCaseStringStream(sql));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    parser = new HiveParser(tokens);
    parser.setBuildParseTree(true);

    HiveParser.StatementContext context;
    try {
      context = parser.statement();
    } catch (SQLParseError e) {
      throw new SQLSyntaxError(e);
    }

    return visit(context);
  }

  @Override
  public Expr visitStatement(HiveParser.StatementContext ctx) {
    return visitExecStatement(ctx.execStatement());
  }

  @Override
  public Expr visitQueryStatement(HiveParser.QueryStatementContext ctx) {
    Expr current = null;

    if (ctx.body != null) {
      current = visitBody(ctx.body(0));
    }

    if (ctx.regular_body() != null) {
      current = visitRegular_body(ctx.regular_body());
    }

    return current;
  }

  @Override
  public Expr visitBody(HiveParser.BodyContext ctx) {

    Expr current = null;
    Insert insert = null;

    Projection select = null;

    if (ctx.insertClause() != null) {
      insert = visitInsertClause(ctx.insertClause());
    }

    if (ctx.selectClause() != null) {
      select = (Projection) visitSelectClause(ctx.selectClause());
      if (ctx.selectClause().KW_DISTINCT() != null) {
        select.setDistinct();
      }

    }

    for (int i = 0; i < ctx.getParent().getChildCount(); i++) {
      if (ctx.getParent().getChild(i) instanceof HiveParser.FromClauseContext) {
        HiveParser.FromClauseContext fromClauseContext = (HiveParser.FromClauseContext) ctx.getParent().getChild(i);
        Expr from = visitFromClause(fromClauseContext);
        current = from;
      }
    }

    if (ctx.whereClause() != null) {
      Selection where = new Selection(visitWhereClause(ctx.whereClause()));
      where.setChild(current);
      current = where;
    }

    if (ctx.groupByClause() != null) {
      Aggregation aggregation = visitGroupByClause(ctx.groupByClause());
      aggregation.setChild(current);
      current = aggregation;

      if (ctx.havingClause() != null) {
        Expr havingCondition = visitHavingClause(ctx.havingClause());
        Having having = new Having(havingCondition);
        having.setChild(current);
        current = having;
      }
    }

    if (ctx.orderByClause() != null) {
      Sort sort = visitOrderByClause(ctx.orderByClause());
      sort.setChild(current);
      current = sort;
    }

    if (ctx.clusterByClause() != null) {
      visitClusterByClause(ctx.clusterByClause());
    }

    if (ctx.distributeByClause() != null) {
      visitDistributeByClause(ctx.distributeByClause());
    }

    if (ctx.sortByClause() != null) {
      Sort sort = visitSortByClause(ctx.sortByClause());
      sort.setChild(current);
      current = sort;
    }

    if (ctx.window_clause() != null) {
      Expr window = visitWindow_clause(ctx.window_clause());
    }

    if (ctx.limitClause() != null) {
      Limit limit = visitLimitClause(ctx.limitClause());
      limit.setChild(current);
      current = limit;
    }

    Projection projection = new Projection();
    projection.setNamedExprs(select.getNamedExprs());

    if (current != null)
      projection.setChild(current);

    if (select.isDistinct())
      projection.setDistinct();


    if (insert != null) {
      insert.setSubQuery(projection);
      current = insert;
    } else {
      current = projection;
    }

    return current;
  }

  @Override
  public Expr visitRegular_body(HiveParser.Regular_bodyContext ctx) {
    Expr current = null;
    Insert insert = null;

    if (ctx.selectStatement() != null) {
      current = visitSelectStatement(ctx.selectStatement());
    } else {
      Projection select = null;

      if (ctx.insertClause() != null) {
        insert = visitInsertClause(ctx.insertClause());
      }

      if (ctx.selectClause() != null) {
        select = (Projection) visitSelectClause(ctx.selectClause());
        if (ctx.selectClause().KW_DISTINCT() != null) {
          select.setDistinct();
        }

      }

      if (ctx.fromClause() != null) {
        Expr from = visitFromClause(ctx.fromClause());
        current = from;
      }

      if (ctx.whereClause() != null) {
        Selection where = new Selection(visitWhereClause(ctx.whereClause()));
        where.setChild(current);
        current = where;
      }

      if (ctx.groupByClause() != null) {
        Aggregation aggregation = visitGroupByClause(ctx.groupByClause());
        aggregation.setChild(current);
        current = aggregation;

        if (ctx.havingClause() != null) {
          Expr havingCondition = visitHavingClause(ctx.havingClause());
          Having having = new Having(havingCondition);
          having.setChild(current);
          current = having;
        }
      }

      if (ctx.orderByClause() != null) {
        Sort sort = visitOrderByClause(ctx.orderByClause());
        sort.setChild(current);
        current = sort;
      }

      if (ctx.clusterByClause() != null) {
        visitClusterByClause(ctx.clusterByClause());
      }

      if (ctx.distributeByClause() != null) {
        visitDistributeByClause(ctx.distributeByClause());
      }

      if (ctx.sortByClause() != null) {
        Sort sort = visitSortByClause(ctx.sortByClause());
        sort.setChild(current);
        current = sort;
      }

      if (ctx.window_clause() != null) {
        Expr window = visitWindow_clause(ctx.window_clause());
      }

      if (ctx.limitClause() != null) {
        Limit limit = visitLimitClause(ctx.limitClause());
        limit.setChild(current);
        current = limit;
      }

      Projection projection = new Projection();
      projection.setNamedExprs(select.getNamedExprs());

      if (current != null)
        projection.setChild(current);

      if (select.isDistinct())
        projection.setDistinct();

      if (insert != null) {
        insert.setSubQuery(projection);
        current = insert;
      } else {
        current = projection;
      }


    }
    return current;
  }

  /**
   * This method implemented for parsing union all clause.
   *
   * @param ctx
   * @return
   */
  @Override
  public Expr visitQueryStatementExpression(HiveParser.QueryStatementExpressionContext ctx) {
    Expr left = null, right = null, current = null;
    if (ctx.queryStatement() != null) {
      if (ctx.queryStatement().size() == 1)
        return visitQueryStatement(ctx.queryStatement(0));

      for (int i = 0; i < ctx.queryStatement().size(); i++) {
        if (i == 0)
          current = visitQueryStatement(ctx.queryStatement(i));
        else
          left = current;

        if (i > 0) {
          right = visitQueryStatement(ctx.queryStatement(i));
          current = new SetOperation(OpType.Union, left, right, false);
        }
      }
    }
    return current;
  }

  @Override
  public Expr visitSelectStatement(HiveParser.SelectStatementContext ctx) {
    Expr current = null;

    Projection select = (Projection) visitSelectClause(ctx.selectClause());

    if (ctx.selectClause().KW_DISTINCT() != null) {
      select.setDistinct();
    }

    Expr from = visitFromClause(ctx.fromClause());
    current = from;

    if (ctx.whereClause() != null) {
      Selection where = new Selection(visitWhereClause(ctx.whereClause()));
      where.setChild(current);
      current = where;
    }

    if (ctx.groupByClause() != null) {
      Aggregation aggregation = visitGroupByClause(ctx.groupByClause());
      aggregation.setChild(current);
      current = aggregation;

      if (ctx.havingClause() != null) {
        Expr havingCondition = visitHavingClause(ctx.havingClause());
        Having having = new Having(havingCondition);
        having.setChild(current);
        current = having;
      }
    }

    if (ctx.orderByClause() != null) {
      Sort sort = visitOrderByClause(ctx.orderByClause());
      sort.setChild(current);
      current = sort;
    }

    if (ctx.clusterByClause() != null) {
      visitClusterByClause(ctx.clusterByClause());
    }

    if (ctx.distributeByClause() != null) {
      visitDistributeByClause(ctx.distributeByClause());
    }

    if (ctx.sortByClause() != null) {
      Sort sort = visitSortByClause(ctx.sortByClause());
      sort.setChild(current);
      current = sort;
    }

    if (ctx.window_clause() != null) {
      Expr window = visitWindow_clause(ctx.window_clause());
    }

    if (ctx.limitClause() != null) {
      Limit limit = visitLimitClause(ctx.limitClause());
      limit.setChild(current);
      current = limit;
    }

    Projection projection = new Projection();
    projection.setNamedExprs(select.getNamedExprs());

    if (current != null)
      projection.setChild(current);

    if (select.isDistinct())
      projection.setDistinct();

    current = projection;

    return current;
  }

  @Override
  public Expr visitFromClause(HiveParser.FromClauseContext ctx) {
    return visitJoinSource(ctx.joinSource());
  }

  @Override
  public Expr visitJoinSource(HiveParser.JoinSourceContext ctx) {
    Expr[] relations = null;
    RelationList relationList = null;

    if (ctx.fromSource() != null) {
      int fromCount = ctx.fromSource().size();
      int uniqueJoinCount = ctx.uniqueJoinSource().size();

      relations = new Expr[1];

      Join current = null, parent = null;
      JoinType type = null;
      Expr left = null, right = null, condition = null;


      if (fromCount == 1) {
        relations[0] = visitFromSource(ctx.fromSource(0));
      } else {
        left = visitFromSource((HiveParser.FromSourceContext) ctx.getChild(0));

        for (int i = 1; i < ctx.getChildCount(); i++) {
          type = null;
          right = null;
          condition = null;

          if (ctx.getChild(i) instanceof HiveParser.JoinTokenContext) {
            type = getJoinType((HiveParser.JoinTokenContext) ctx.getChild(i));
            if (i > 1)
              left = parent;

            if (i + 1 < ctx.getChildCount() && ctx.getChild(i + 1) instanceof HiveParser.FromSourceContext) {
              right = visitFromSource((HiveParser.FromSourceContext) ctx.getChild(i + 1));
            }

            if (i + 3 < ctx.getChildCount() && ctx.getChild(i + 3) instanceof HiveParser.ExpressionContext) {
              condition = visitExpression((HiveParser.ExpressionContext) ctx.getChild(i + 3));
            }

            if (type != null) {
              current = new Join(type);
              current.setLeft(left);
              current.setRight(right);

              if (condition != null)
                current.setQual(condition);

              parent = current;
            }
          }

        }
        relations[0] = current;
      }

      //TODO: implement unique join.
      relationList = new RelationList(relations);
    }

    return relationList;
  }

  public JoinType getJoinType(HiveParser.JoinTokenContext context) {
    JoinType type = JoinType.INNER;

    if (context.KW_INNER() != null) {
      type = JoinType.INNER;
    }

    if (context.KW_LEFT() != null && context.KW_OUTER() != null) {
      type = JoinType.LEFT_OUTER;
    }

    if (context.KW_RIGHT() != null && context.KW_OUTER() != null) {
      type = JoinType.RIGHT_OUTER;
    }

    if (context.KW_CROSS() != null) {
      type = JoinType.CROSS;
    }

    if (context.KW_FULL() != null) {
      type = JoinType.FULL_OUTER;
    }

    if (context.KW_SEMI() != null) {
      type = null;
    }
    return type;
  }

  @Override
  public Expr visitFromSource(HiveParser.FromSourceContext ctx) {
    Expr current = null;

    if (ctx.Identifier() != null && ctx.LPAREN() != null) {
      current = new LiteralValue(ctx.Identifier().getText(), LiteralValue.LiteralType.String);
    }

    if (ctx.tableSource() != null) {
      current = visitTableSource(ctx.tableSource());
    }

    if (ctx.subQuerySource() != null) {
      current = visitSubQuerySource(ctx.subQuerySource());

      String tableAlias = "";
      for (int i = 0; i < ctx.subQuerySource().getChildCount(); i++) {
        if (ctx.subQuerySource().getChild(i) instanceof HiveParser.IdentifierContext) {
          tableAlias = (ctx.subQuerySource().getChild(i)).getText();
        }
      }

      TablePrimarySubQuery subQuery = new TablePrimarySubQuery(tableAlias, current);
      current = subQuery;
    }
    // TODO: implement lateralView

    return current;
  }

  @Override
  public Expr visitSubQuerySource(HiveParser.SubQuerySourceContext ctx) {
    Expr current = visitQueryStatementExpression(ctx.queryStatementExpression());
    return current;
  }

  @Override
  public Expr visitTableSource(HiveParser.TableSourceContext ctx) {
    String tableName = "", alias = "";

    if (ctx.tableName() != null)
      tableName = ctx.tableName().getText();

    if (ctx.alias != null) {
      alias = ctx.alias.getText();
      for (String token : HiveParser.tokenNames) {
        if (token.replaceAll("'", "").equalsIgnoreCase(alias))
          alias = "";
      }
    }

    Relation relation = new Relation(tableName);
    if (!alias.equals(""))
      relation.setAlias(alias);

    return relation;
  }

  @Override
  public Expr visitSelectList(HiveParser.SelectListContext ctx) {
    Expr current = null;
    Projection projection = new Projection();
    NamedExpr[] targets = new NamedExpr[ctx.selectItem().size()];
    for (int i = 0; i < targets.length; i++) {
      targets[i] = visitSelectItem(ctx.selectItem(i));
    }

    projection.setNamedExprs(targets);
    current = projection;
    return current;
  }

  @Override
  public NamedExpr visitSelectItem(HiveParser.SelectItemContext ctx) {
    NamedExpr target = null;

    if (ctx.selectExpression() != null) {
      target = new NamedExpr(visitSelectExpression(ctx.selectExpression()));
    } else if (ctx.window_specification() != null) {
      // TODO: if there is a window specification clause, we should handle it properly.
    }

    if (ctx.identifier().size() > 0 && target != null) {
      target.setAlias(ctx.identifier(0).getText());
    }
    return target;
  }

  @Override
  public Expr visitSelectExpression(HiveParser.SelectExpressionContext ctx) {
    Expr current = null;

    if (ctx.tableAllColumns() != null) {
      current = visitTableAllColumns(ctx.tableAllColumns());
    } else {
      if (ctx.expression() != null) {
        current = visitExpression(ctx.expression());
      }
    }

    return current;
  }

  @Override
  public Expr visitTableAllColumns(TableAllColumnsContext ctx) {
    QualifiedAsteriskExpr target = new QualifiedAsteriskExpr();
    if (ctx.tableName() != null) {
      target.setQualifier(ctx.tableName().getText());
    }

    return target;
  }

  @Override
  public Expr visitExpression(HiveParser.ExpressionContext ctx) {
    Expr current = visitPrecedenceOrExpression(ctx.precedenceOrExpression());
    return current;
  }

  @Override
  public Expr visitPrecedenceOrExpression(HiveParser.PrecedenceOrExpressionContext ctx) {
    Expr current = null, left = null, right = null;

    for (int i = 0; i < ctx.precedenceAndExpression().size(); i++) {
      if (i == 0) {
        left = visitPrecedenceAndExpression(ctx.precedenceAndExpression(i));
        current = left;
      } else {
        left = current;
        right = visitPrecedenceAndExpression(ctx.precedenceAndExpression(i));
        current = new BinaryOperator(OpType.Or, left, right);
      }

    }
    return current;
  }

  /**
   * This method parse AND expressions at WHERE clause.
   * And this convert 'x BETWEEN y AND z' expression into 'x >= y AND x <= z' expression
   * because Tajo doesn't provide 'BETWEEN' expression.
   *
   * @param ctx
   * @return
   */
  @Override
  public Expr visitPrecedenceAndExpression(HiveParser.PrecedenceAndExpressionContext ctx) {
    Expr current = null, left = null, right = null;

    for (int i = 0; i < ctx.precedenceNotExpression().size(); i++) {
      Expr min = null, max = null;

      if (ctx.precedenceNotExpression(i).precedenceEqualExpression() != null) {
        HiveParser.PrecedenceEqualExpressionContext expressionContext = ctx.precedenceNotExpression(i)
            .precedenceEqualExpression();
        if (expressionContext.KW_BETWEEN() != null) {

          if (expressionContext.min != null) {
            min = visitPrecedenceBitwiseOrExpression(expressionContext.min);
          }

          if (expressionContext.max != null) {
            max = visitPrecedenceBitwiseOrExpression(expressionContext.max);
          }
        }
      }

      if (min != null && max != null) {
        left = visitPrecedenceNotExpression(ctx.precedenceNotExpression(i));
        if (left != null) {
          if (i == 0) {
            BinaryOperator minOperator = new BinaryOperator(OpType.GreaterThanOrEquals, left, min);
            BinaryOperator maxOperator = new BinaryOperator(OpType.LessThanOrEquals, left, max);
            current = new BinaryOperator(OpType.And, minOperator, maxOperator);
          } else {
            BinaryOperator minOperator = new BinaryOperator(OpType.GreaterThanOrEquals, left, min);
            current = new BinaryOperator(OpType.And, current, minOperator);

            BinaryOperator maxOperator = new BinaryOperator(OpType.LessThanOrEquals, left, max);
            current = new BinaryOperator(OpType.And, current, maxOperator);
          }
        }
      } else {
        if (i == 0) {
          left = visitPrecedenceNotExpression(ctx.precedenceNotExpression(i));
          current = left;
        } else {
          left = current;
          right = visitPrecedenceNotExpression(ctx.precedenceNotExpression(i));
          current = new BinaryOperator(OpType.And, left, right);
        }
      }
    }
    return current;
  }

  @Override
  public Expr visitPrecedenceNotExpression(HiveParser.PrecedenceNotExpressionContext ctx) {
    HiveParser.PrecedenceEqualExpressionContext expressionContext = ctx.precedenceEqualExpression();
    Expr current = visitPrecedenceEqualExpression(expressionContext);
    return current;
  }

  /**
   * This method parse operators for equals expressions as follows:
   * =, <>, !=, >=, >, <=, <, IN, NOT IN, LIKE, REGEXP, RLIKE
   * <p/>
   * In this case, this make RuntimeException>
   *
   * @param ctx
   * @return
   */
  @Override
  public Expr visitPrecedenceEqualExpression(HiveParser.PrecedenceEqualExpressionContext ctx) {
    Expr current = null, left = null, right = null, min = null, max = null;
    OpType type = null;
    boolean isNot = false, isIn = false;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      if (ctx.getChild(i) instanceof HiveParser.PrecedenceBitwiseOrExpressionContext) {
        if (i == 0) {
          left = visitPrecedenceBitwiseOrExpression((HiveParser.PrecedenceBitwiseOrExpressionContext) ctx.getChild(i));
        } else {
          right = visitPrecedenceBitwiseOrExpression((HiveParser.PrecedenceBitwiseOrExpressionContext) ctx.getChild(i));
        }
      } else if (ctx.getChild(i) instanceof HiveParser.ExpressionsContext) {
        right = visitExpressions((HiveParser.ExpressionsContext) ctx.getChild(i));
      } else if (ctx.getChild(i) instanceof TerminalNodeImpl) {
        int symbolType = ((TerminalNodeImpl) ctx.getChild(i)).getSymbol().getType();
        switch (symbolType) {
          case HiveLexer.KW_NOT:
            isNot = true;
            break;
          case HiveLexer.KW_IN:
            isIn = true;
            break;
          default:
            break;
        }
      } else if (ctx.getChild(i) instanceof HiveParser.PrecedenceEqualOperatorContext
          || ctx.getChild(i) instanceof HiveParser.PrecedenceEqualNegatableOperatorContext) {
        String keyword = ctx.getChild(i).getText().toUpperCase();

        if (keyword.equals(">")) {
          type = OpType.GreaterThan;
        } else if (keyword.equals("<=>")) {
          throw new RuntimeException("Unexpected operator : <=>");
        } else if (keyword.equals("=")) {
          type = OpType.Equals;
        } else if (keyword.equals("<=")) {
          type = OpType.LessThanOrEquals;
        } else if (keyword.equals("<")) {
          type = OpType.LessThan;
        } else if (keyword.equals(">=")) {
          type = OpType.GreaterThanOrEquals;
        } else if (keyword.equals("<>")) {
          type = OpType.NotEquals;
        } else if (keyword.equals("!=")) {
          type = OpType.NotEquals;
        } else if (keyword.equals("REGEXP")) {
          type = OpType.Regexp;
        } else if (keyword.equals("RLIKE")) {
          type = OpType.Regexp;
        } else if (keyword.equals("LIKE")) {
          type = OpType.LikePredicate;
        }
      }
    }

    if (type != null && right != null) {
      if (type.equals(OpType.LikePredicate)) {
        PatternMatchPredicate like = new PatternMatchPredicate(OpType.LikePredicate,
            isNot, left, right);
        current = like;
      } else if (type.equals(OpType.Regexp)) {
        PatternMatchPredicate regex = new PatternMatchPredicate(OpType.Regexp, isNot, left, right);
        current = regex;
      } else {
        BinaryOperator binaryOperator = new BinaryOperator(type, left, right);
        current = binaryOperator;
      }
    } else if (isIn) {
      InPredicate inPredicate = new InPredicate(left, right, isNot);
      current = inPredicate;
    } else {
      current = left;
    }

    return current;
  }

  @Override
  public ValueListExpr visitExpressions(HiveParser.ExpressionsContext ctx) {
    int size = ctx.expression().size();
    Expr[] exprs = new Expr[size];
    for (int i = 0; i < size; i++) {
      exprs[i] = visitExpression(ctx.expression(i));
    }
    return new ValueListExpr(exprs);
  }

  @Override
  public Expr visitPrecedenceBitwiseOrExpression(HiveParser.PrecedenceBitwiseOrExpressionContext ctx) {
    int expressionCount = ctx.precedenceAmpersandExpression().size();

    Expr current = null, left = null, right = null, parentLeft, parentRight;
    OpType type = null, parentType = null;

    for (int i = 0; i < expressionCount; i += 2) {
      int operatorIndex = (i == 0) ? 0 : i - 1;

      if (ctx.precedenceBitwiseOrOperator(operatorIndex) != null) {
        type = getPrecedenceBitwiseOrOperator(ctx.precedenceBitwiseOrOperator(operatorIndex));
      }

      if (i == 0) {
        left = visitPrecedenceAmpersandExpression(ctx.precedenceAmpersandExpression(i));
        if (ctx.precedenceAmpersandExpression(i + 1) != null)
          right = visitPrecedenceAmpersandExpression(ctx.precedenceAmpersandExpression(i + 1));
      } else {
        parentType = getPrecedenceBitwiseOrOperator((ctx.precedenceBitwiseOrOperator(operatorIndex - 1)));
        parentLeft = visitPrecedenceAmpersandExpression(ctx.precedenceAmpersandExpression(i - 2));
        parentRight = visitPrecedenceAmpersandExpression(ctx.precedenceAmpersandExpression(i - 1));
        left = new BinaryOperator(parentType, parentLeft, parentRight);
        right = visitPrecedenceAmpersandExpression(ctx.precedenceAmpersandExpression(i));
      }

      if (right != null) {
        current = new BinaryOperator(type, left, right);
      } else {
        current = left;
      }
    }
    return current;
  }

  public OpType getPrecedenceBitwiseOrOperator(HiveParser.PrecedenceBitwiseOrOperatorContext ctx) {
    OpType type = null;
    // TODO: It needs to consider how to support.
    return type;
  }

  @Override
  public Expr visitPrecedenceAmpersandExpression(HiveParser.PrecedenceAmpersandExpressionContext ctx) {
    int expressionCount = ctx.precedencePlusExpression().size();

    Expr current = null, left = null, right = null, parentLeft, parentRight;
    OpType type = null, parentType = null;

    for (int i = 0; i < expressionCount; i += 2) {
      int operatorIndex = (i == 0) ? 0 : i - 1;

      if (ctx.precedenceAmpersandOperator(operatorIndex) != null) {
        type = getPrecedenceAmpersandOperator(ctx.precedenceAmpersandOperator(operatorIndex));
      }

      if (i == 0) {
        left = visitPrecedencePlusExpression(ctx.precedencePlusExpression(i));
        if (ctx.precedencePlusExpression(i + 1) != null)
          right = visitPrecedencePlusExpression(ctx.precedencePlusExpression(i + 1));
      } else {
        parentType = getPrecedenceAmpersandOperator((ctx.precedenceAmpersandOperator(operatorIndex - 1)));
        parentLeft = visitPrecedencePlusExpression(ctx.precedencePlusExpression(i - 2));
        parentRight = visitPrecedencePlusExpression(ctx.precedencePlusExpression(i - 1));
        left = new BinaryOperator(parentType, parentLeft, parentRight);
        right = visitPrecedencePlusExpression(ctx.precedencePlusExpression(i));
      }

      if (right != null) {
        current = new BinaryOperator(type, left, right);
      } else {
        current = left;
      }
    }
    return current;
  }

  public OpType getPrecedenceAmpersandOperator(HiveParser.PrecedenceAmpersandOperatorContext ctx) {
    OpType type = null;
    // TODO: It needs to consider how to support.
    return type;
  }

  @Override
  public Expr visitPrecedencePlusExpression(HiveParser.PrecedencePlusExpressionContext ctx) {
    int expressionCount = ctx.precedenceStarExpression().size();

    Expr current = null, left = null, right = null, parentLeft, parentRight;
    OpType type = null, parentType = null;

    for (int i = 0; i < expressionCount; i += 2) {
      int operatorIndex = (i == 0) ? 0 : i - 1;

      if (ctx.precedencePlusOperator(operatorIndex) != null) {
        type = getPrecedencePlusOperator(ctx.precedencePlusOperator(operatorIndex));
      }

      if (i == 0) {
        left = visitPrecedenceStarExpression(ctx.precedenceStarExpression(i));
        if (ctx.precedenceStarExpression(i + 1) != null)
          right = visitPrecedenceStarExpression(ctx.precedenceStarExpression(i + 1));
      } else {
        parentType = getPrecedencePlusOperator((ctx.precedencePlusOperator(operatorIndex - 1)));
        parentLeft = visitPrecedenceStarExpression(ctx.precedenceStarExpression(i - 2));
        parentRight = visitPrecedenceStarExpression(ctx.precedenceStarExpression(i - 1));
        left = new BinaryOperator(parentType, parentLeft, parentRight);
        right = visitPrecedenceStarExpression(ctx.precedenceStarExpression(i));
      }

      if (right != null) {
        current = new BinaryOperator(type, left, right);
      } else {
        current = left;
      }
    }
    return current;
  }

  public OpType getPrecedencePlusOperator(HiveParser.PrecedencePlusOperatorContext ctx) {
    OpType type = null;

    if (ctx.MINUS() != null) {
      type = OpType.Minus;
    } else if (ctx.PLUS() != null) {
      type = OpType.Plus;
    }

    return type;
  }

  @Override
  public Expr visitPrecedenceStarExpression(HiveParser.PrecedenceStarExpressionContext ctx) {
    int expressionCount = ctx.precedenceBitwiseXorExpression().size();

    Expr current = null, left = null, right = null, parentLeft, parentRight;
    OpType type = null, parentType = null;

    for (int i = 0; i < expressionCount; i += 2) {
      int operatorIndex = (i == 0) ? 0 : i - 1;

      if (ctx.precedenceStarOperator(operatorIndex) != null) {
        type = getPrecedenceStarOperator(ctx.precedenceStarOperator(operatorIndex));
      }

      if (i == 0) {
        left = visitPrecedenceBitwiseXorExpression(ctx.precedenceBitwiseXorExpression(i));
        if (ctx.precedenceBitwiseXorExpression(i + 1) != null)
          right = visitPrecedenceBitwiseXorExpression(ctx.precedenceBitwiseXorExpression(i + 1));
      } else {
        parentType = getPrecedenceStarOperator((ctx.precedenceStarOperator(operatorIndex - 1)));
        parentLeft = visitPrecedenceBitwiseXorExpression(ctx.precedenceBitwiseXorExpression(i - 2));
        parentRight = visitPrecedenceBitwiseXorExpression(ctx.precedenceBitwiseXorExpression(i - 1));
        left = new BinaryOperator(parentType, parentLeft, parentRight);
        right = visitPrecedenceBitwiseXorExpression(ctx.precedenceBitwiseXorExpression(i));
      }

      if (right != null) {
        current = new BinaryOperator(type, left, right);
      } else {
        current = left;
      }
    }

    return current;
  }

  public OpType getPrecedenceStarOperator(HiveParser.PrecedenceStarOperatorContext ctx) {
    OpType type = null;

    if (ctx.DIV() != null) {
      type = OpType.Divide;
    } else if (ctx.DIVIDE() != null) {
      type = OpType.Divide;
    } else if (ctx.MOD() != null) {
      type = OpType.Modular;
    } else if (ctx.STAR() != null) {
      type = OpType.Multiply;
    }

    return type;
  }

  @Override
  public Expr visitPrecedenceBitwiseXorExpression(HiveParser.PrecedenceBitwiseXorExpressionContext ctx) {
    int expressionCount = ctx.precedenceUnarySuffixExpression().size();

    Expr current = null, left = null, right = null, parentLeft, parentRight;
    OpType type = null, parentType = null;

    for (int i = 0; i < expressionCount; i += 2) {
      int operatorIndex = (i == 0) ? 0 : i - 1;

      if (ctx.precedenceBitwiseXorOperator(operatorIndex) != null) {
        type = getPrecedenceBitwiseXorOperator(ctx.precedenceBitwiseXorOperator(operatorIndex));
      }

      if (i == 0) {
        left = visitPrecedenceUnarySuffixExpression(ctx.precedenceUnarySuffixExpression(i));
        if (ctx.precedenceUnarySuffixExpression(i + 1) != null)
          right = visitPrecedenceUnarySuffixExpression(ctx.precedenceUnarySuffixExpression(i + 1));
      } else {
        parentType = getPrecedenceBitwiseXorOperator((ctx.precedenceBitwiseXorOperator(operatorIndex - 1)));
        parentLeft = visitPrecedenceUnarySuffixExpression(ctx.precedenceUnarySuffixExpression(i - 2));
        parentRight = visitPrecedenceUnarySuffixExpression(ctx.precedenceUnarySuffixExpression(i - 1));
        left = new BinaryOperator(parentType, parentLeft, parentRight);
        right = visitPrecedenceUnarySuffixExpression(ctx.precedenceUnarySuffixExpression(i));
      }

      if (right != null) {
        current = new BinaryOperator(type, left, right);
      } else {
        current = left;
      }
    }

    return current;
  }

  public OpType getPrecedenceBitwiseXorOperator(HiveParser.PrecedenceBitwiseXorOperatorContext ctx) {
    OpType type = null;
    // TODO: It needs to consider how to support.

    return type;
  }

  @Override
  public Expr visitPrecedenceUnarySuffixExpression(HiveParser.PrecedenceUnarySuffixExpressionContext ctx) {
    Expr current = visitPrecedenceUnaryPrefixExpression(ctx.precedenceUnaryPrefixExpression());

    if (ctx.nullCondition() != null) {
      boolean isNot = ctx.nullCondition().KW_NOT() == null ? false : true;
      IsNullPredicate isNullPredicate = new IsNullPredicate(isNot, (ColumnReferenceExpr) current);
      current = isNullPredicate;
    }

    return current;
  }

  @Override
  public Expr visitPrecedenceUnaryPrefixExpression(HiveParser.PrecedenceUnaryPrefixExpressionContext ctx) {
    Expr current = visitPrecedenceFieldExpression(ctx.precedenceFieldExpression());
    return current;
  }

  @Override
  public Expr visitNullCondition(HiveParser.NullConditionContext ctx) {
    return new NullLiteral();
  }

  @Override
  public Expr visitPrecedenceFieldExpression(HiveParser.PrecedenceFieldExpressionContext ctx) {
    Expr current = visitAtomExpression(ctx.atomExpression());

    if (ctx.DOT().size() > 0) {
      ColumnReferenceExpr column = new ColumnReferenceExpr(ctx.identifier(0).getText());
      ColumnReferenceExpr table = (ColumnReferenceExpr) current;
      column.setQualifier(table.getName());
      current = column;
    }
    return current;
  }

  @Override
  public Expr visitAtomExpression(HiveParser.AtomExpressionContext ctx) {
    Expr current = null;

    if (ctx.KW_NULL() != null) {
      current = new NullLiteral();
    }
    if (ctx.constant() != null) {
      current = visitConstant(ctx.constant());
    }
    if (ctx.function() != null) {
      current = visitFunction(ctx.function());
    }
    if (ctx.castExpression() != null) {
      current = visitCastExpression(ctx.castExpression());
    }
    if (ctx.caseExpression() != null) {
      current = visitCaseExpression(ctx.caseExpression());
    }
    if (ctx.whenExpression() != null) {
      current = visitWhenExpression(ctx.whenExpression());
    }
    if (ctx.tableOrColumn() != null) {
      current = visitTableOrColumn(ctx.tableOrColumn());
    } else {
      if (ctx.LPAREN() != null && ctx.RPAREN() != null) {
        current = visitExpression(ctx.expression());
      }
    }

    return current;
  }

  @Override
  public Expr visitTableOrColumn(HiveParser.TableOrColumnContext ctx) {
    ColumnReferenceExpr columnReferenceExpr = new ColumnReferenceExpr(ctx.identifier().getText());
    return columnReferenceExpr;
  }

  @Override
  public Expr visitIdentifier(HiveParser.IdentifierContext ctx) {
    Expr current = null;

    if (ctx.nonReserved() != null) {
      current = new LiteralValue(ctx.nonReserved().getText(), LiteralValue.LiteralType.String);
    } else {
      current = new LiteralValue(ctx.Identifier().getText(), LiteralValue.LiteralType.String);
    }

    return current;
  }

  @Override
  public LiteralValue visitConstant(HiveParser.ConstantContext ctx) {
    LiteralValue literalValue = null;

    if (ctx.StringLiteral() != null) {
      String value = ctx.StringLiteral().getText();
      String strValue = "";
      if ((value.startsWith("'") && value.endsWith("'")) || value.startsWith("\"") && value.endsWith("\"")) {
        strValue = value.substring(1, value.length() - 1);
      } else {
        strValue = value;
      }

      literalValue = new LiteralValue(strValue, LiteralValue.LiteralType.String);
    } else if (ctx.TinyintLiteral() != null) {
      literalValue = new LiteralValue(ctx.TinyintLiteral().getSymbol().getText(),
          LiteralValue.LiteralType.Unsigned_Integer);
    } else if (ctx.BigintLiteral() != null) {
      literalValue = new LiteralValue(ctx.BigintLiteral().getSymbol().getText(),
          LiteralValue.LiteralType.Unsigned_Large_Integer);
    } else if (ctx.DecimalLiteral() != null) {
      literalValue = new LiteralValue(ctx.DecimalLiteral().getSymbol().getText(),
          LiteralValue.LiteralType.Unsigned_Integer);
    } else if (ctx.Number() != null) {
      try {
        float floatValue = NumberUtils.createFloat(ctx.getText());
        literalValue = new LiteralValue(ctx.Number().getSymbol().getText(), LiteralValue.LiteralType.Unsigned_Float);
      } catch (NumberFormatException nf) {
      }

      // TODO: double type

      try {
        BigInteger bigIntegerVallue = NumberUtils.createBigInteger(ctx.getText());
        literalValue = new LiteralValue(ctx.Number().getSymbol().getText(),
            LiteralValue.LiteralType.Unsigned_Large_Integer);
      } catch (NumberFormatException nf) {
      }

      try {
        int intValue = NumberUtils.createInteger(ctx.getText());
        literalValue = new LiteralValue(ctx.Number().getSymbol().getText(), LiteralValue.LiteralType.Unsigned_Integer);
      } catch (NumberFormatException nf) {
      }

    } else if (ctx.SmallintLiteral() != null) {
      literalValue = new LiteralValue(ctx.SmallintLiteral().getSymbol().getText(),
          LiteralValue.LiteralType.Unsigned_Integer);
    } else if (ctx.booleanValue() != null) {
      // TODO: boolean type
    }

    return literalValue;
  }

  @Override
  public Expr visitFunction(HiveParser.FunctionContext ctx) {
    Expr current = null;
    String signature = ctx.functionName().getText();

    boolean isDistinct = false;
    if (ctx.getChild(2) != null) {
      if (ctx.getChild(2) instanceof TerminalNodeImpl && ctx.getChild(2).getText().equalsIgnoreCase("DISTINCT")) {
        isDistinct = true;
      }
    }

    if (signature.equalsIgnoreCase("MIN")
        || signature.equalsIgnoreCase("MAX")
        || signature.equalsIgnoreCase("SUM")
        || signature.equalsIgnoreCase("AVG")
        || signature.equalsIgnoreCase("COUNT")
        ) {
      if (ctx.selectExpression().size() > 1) {
        throw new RuntimeException("Exactly expected one argument.");
      }

      if (ctx.selectExpression().size() == 0) {
        CountRowsFunctionExpr countRowsFunctionExpr = new CountRowsFunctionExpr();
        current = countRowsFunctionExpr;
      } else {
        GeneralSetFunctionExpr setFunctionExpr = new GeneralSetFunctionExpr(signature, isDistinct,
            visitSelectExpression(ctx.selectExpression(0)));
        current = setFunctionExpr;
      }
    } else {
      FunctionExpr functionExpr = new FunctionExpr(signature);
      Expr[] params = new Expr[ctx.selectExpression().size()];
      for (int i = 0; i < ctx.selectExpression().size(); i++) {
        params[i] = visitSelectExpression(ctx.selectExpression(i));
      }
      functionExpr.setParams(params);
      current = functionExpr;
    }


    return current;
  }

  /**
   * This method parse CAST expression.
   * This returns only expression field without casting type
   * because Tajo doesn't provide CAST expression.
   *
   * @param ctx
   * @return
   */
  @Override
  public Expr visitCastExpression(HiveParser.CastExpressionContext ctx) {
    return visitExpression(ctx.expression());
  }

  @Override
  public Expr visitCaseExpression(HiveParser.CaseExpressionContext ctx) {
    CaseWhenPredicate caseWhen = new CaseWhenPredicate();
    Expr condition = null, result = null;
    for (int i = 1; i < ctx.getChildCount(); i++) {
      if (ctx.getChild(i) instanceof TerminalNodeImpl) {
        if (((TerminalNodeImpl) ctx.getChild(i)).getSymbol().getType() == HiveLexer.KW_WHEN) {
          condition = null;
          result = null;

          if (ctx.getChild(i + 1) instanceof HiveParser.ExpressionContext) {
            condition = visitExpression((HiveParser.ExpressionContext) ctx.getChild(i + 1));
          }

          if (ctx.getChild(i + 3) instanceof HiveParser.ExpressionContext) {
            result = visitExpression((HiveParser.ExpressionContext) ctx.getChild(i + 3));
          }

          if (condition != null && result != null) {
            caseWhen.addWhen(condition, result);
          }
        } else if (((TerminalNodeImpl) ctx.getChild(i)).getSymbol().getType() == HiveLexer.KW_ELSE) {
          result = visitExpression((HiveParser.ExpressionContext) ctx.getChild(i + 1));
          caseWhen.setElseResult(result);
        }
      }
    }

    return caseWhen;
  }

  @Override
  public Expr visitWhenExpression(HiveParser.WhenExpressionContext ctx) {
    CaseWhenPredicate caseWhen = new CaseWhenPredicate();
    Expr condition = null, result = null;
    for (int i = 1; i < ctx.getChildCount(); i++) {
      if (ctx.getChild(i) instanceof TerminalNodeImpl) {
        if (((TerminalNodeImpl) ctx.getChild(i)).getSymbol().getType() == HiveLexer.KW_WHEN) {
          condition = null;
          result = null;

          if (ctx.getChild(i + 1) instanceof HiveParser.ExpressionContext) {
            condition = visitExpression((HiveParser.ExpressionContext) ctx.getChild(i + 1));
          }

          if (ctx.getChild(i + 3) instanceof HiveParser.ExpressionContext) {
            result = visitExpression((HiveParser.ExpressionContext) ctx.getChild(i + 3));
          }

          if (condition != null && result != null) {
            caseWhen.addWhen(condition, result);
          }
        } else if (((TerminalNodeImpl) ctx.getChild(i)).getSymbol().getType() == HiveLexer.KW_ELSE) {
          result = visitExpression((HiveParser.ExpressionContext) ctx.getChild(i + 1));
          caseWhen.setElseResult(result);
        }
      }
    }

    return caseWhen;
  }

  @Override
  public Aggregation visitGroupByClause(HiveParser.GroupByClauseContext ctx) {
    Aggregation clause = new Aggregation();
    if (ctx.groupByExpression().size() > 0) {
      List<Expr> columns = new ArrayList<Expr>();
      List<Expr> functions = new ArrayList<Expr>();

      for (int i = 0; i < ctx.groupByExpression().size(); i++) {
        Expr expr = visitGroupByExpression(ctx.groupByExpression(i));

        if (expr instanceof ColumnReferenceExpr) {
          columns.add(expr);
        } else if (expr instanceof FunctionExpr) {
          functions.add(expr);
        } else {
          //TODO: find another case.
        }
      }

      Aggregation.GroupElement[] groups = null;

      if (columns.size() > 0) {
        groups = new Aggregation.GroupElement[1 + functions.size()];
      } else {
        groups = new Aggregation.GroupElement[functions.size()];
      }

      int index = 0;
      if (columns.size() > 0) {
        index = 0;
        ColumnReferenceExpr[] columnReferenceExprs = new ColumnReferenceExpr[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
          ColumnReferenceExpr expr = (ColumnReferenceExpr) columns.get(i);
          columnReferenceExprs[i] = expr;
        }
        groups[index] = new Aggregation.GroupElement(Aggregation.GroupType.OrdinaryGroup, columnReferenceExprs);
      }

      if (functions.size() > 0) {
        if (columns.size() == 0) {
          index = 0;
        } else {
          index = 1;
        }

        for (int i = 0; i < functions.size(); i++) {
          FunctionExpr function = (FunctionExpr) functions.get(i);

          Expr[] params = function.getParams();
          ColumnReferenceExpr[] column = new ColumnReferenceExpr[params.length];
          for (int j = 0; j < column.length; j++)
            column[j] = (ColumnReferenceExpr) params[j];

          if (function.getSignature().equalsIgnoreCase("ROLLUP"))
            groups[i + index] = new Aggregation.GroupElement(Aggregation.GroupType.Rollup, column);
          else if (function.getSignature().equalsIgnoreCase("CUBE"))
            groups[i + index] = new Aggregation.GroupElement(Aggregation.GroupType.Cube, column);
          else
            throw new RuntimeException("Unexpected aggregation function.");
        }
      }

      clause.setGroups(groups);
    }

    //TODO: grouping set expression
    return clause;
  }

  @Override
  public Sort visitOrderByClause(HiveParser.OrderByClauseContext ctx) {
    Sort clause = null;
    Sort.SortSpec[] specs = null;

    if (ctx.columnRefOrder().size() > 0) {
      specs = new Sort.SortSpec[ctx.columnRefOrder().size()];
      for (int i = 0; i < ctx.columnRefOrder().size(); i++) {
        ColumnReferenceExpr column = (ColumnReferenceExpr) visitExpression(ctx.columnRefOrder().get(i).expression());
        specs[i] = new Sort.SortSpec(column);
        if (ctx.columnRefOrder(i).KW_DESC() != null) {
          specs[i].setDescending();
        }
      }
      clause = new Sort(specs);
    }
    return clause;

  }

  @Override
  public Expr visitHavingClause(HiveParser.HavingClauseContext ctx) {
    return visitHavingCondition(ctx.havingCondition());
  }

  @Override
  public Expr visitClusterByClause(HiveParser.ClusterByClauseContext ctx) {
    // TODO: It needs to consider how to support.
    return null;
  }

  @Override
  public Expr visitDistributeByClause(HiveParser.DistributeByClauseContext ctx) {
    // TODO: It needs to consider how to support.

    return null;
  }

  @Override
  public Sort visitSortByClause(HiveParser.SortByClauseContext ctx) {
    Sort clause = null;
    Sort.SortSpec[] specs = null;

    if (ctx.columnRefOrder().size() > 0) {
      specs = new Sort.SortSpec[ctx.columnRefOrder().size()];
      for (int i = 0; i < ctx.columnRefOrder().size(); i++) {
        ColumnReferenceExpr column = (ColumnReferenceExpr) visitColumnRefOrder(ctx.columnRefOrder(i));
        specs[i] = new Sort.SortSpec(column);

        if (ctx.columnRefOrder(i).KW_DESC() != null) {
          specs[i].setDescending();
        }
      }
      clause = new Sort(specs);
    }

    return clause;
  }

  @Override
  public Limit visitLimitClause(HiveParser.LimitClauseContext ctx) {
    LiteralValue expr = new LiteralValue(ctx.Number().getText(), LiteralValue.LiteralType.Unsigned_Integer);
    Limit limit = new Limit(expr);
    return limit;
  }

  @Override
  public Expr visitWindow_clause(HiveParser.Window_clauseContext ctx) {
    // TODO: It needs to consider how to support.
    return null;
  }

  @Override
  public Insert visitInsertClause(HiveParser.InsertClauseContext ctx) {
    Insert insert = new Insert();
    if (ctx.KW_OVERWRITE() != null)
      insert.setOverwrite();

    if (ctx.tableOrPartition() != null) {
      HiveParser.TableOrPartitionContext partitionContext = ctx.tableOrPartition();
      if (partitionContext.tableName() != null) {
        insert.setTableName(ctx.tableOrPartition().tableName().getText());
      }
    }

    if (ctx.destination() != null) {
      HiveParser.DestinationContext destination = ctx.destination();
      if (destination.KW_DIRECTORY() != null) {
        String location = destination.StringLiteral().getText();
        location = location.replaceAll("\\'", "");
        insert.setLocation(location);
      } else if (destination.KW_TABLE() != null) {
        if (destination.tableOrPartition() != null) {
          HiveParser.TableOrPartitionContext partitionContext = destination.tableOrPartition();
          if (partitionContext.tableName() != null) {
            insert.setTableName(partitionContext.tableName().getText());
          }
        }

        if (destination.tableFileFormat() != null) {
          if (destination.tableFileFormat().KW_RCFILE() != null) {
            insert.setStorageType("rcfile");
          } else if (destination.tableFileFormat().KW_TEXTFILE() != null) {
            insert.setStorageType("csv");
          }

        }
      }
    }

    return insert;
  }

  @Override
  public Expr visitCreateTableStatement(HiveParser.CreateTableStatementContext ctx) {
    CreateTable createTable = null;
    Map<String, String> params = new HashMap<String, String>();

    if (ctx.name != null) {
      createTable = new CreateTable(ctx.name.getText());
      if (ctx.KW_EXTERNAL() != null) {
        createTable.setExternal();
      }

      if (ctx.tableFileFormat() != null) {
        if (ctx.tableFileFormat().KW_RCFILE() != null) {
          createTable.setStorageType("rcfile");
        } else if (ctx.tableFileFormat().KW_TEXTFILE() != null) {
          createTable.setStorageType("csv");
        }
      }

      if (ctx.tableRowFormat() != null) {
        if (ctx.tableRowFormat().rowFormatDelimited() != null) {
          String delimiter = ctx.tableRowFormat().rowFormatDelimited().tableRowFormatFieldIdentifier().getChild(3)
              .getText().replaceAll("'", "");
          params.put("csvfile.delimiter", SQLAnalyzer.escapeDelimiter(delimiter));
        }
      }

      if (ctx.tableLocation() != null) {
        String location = ctx.tableLocation().StringLiteral().getText();
        location = location.replaceAll("'", "");
        createTable.setLocation(location);

      }

      if (ctx.columnNameTypeList() != null) {
        List<HiveParser.ColumnNameTypeContext> list = ctx.columnNameTypeList().columnNameType();

        CreateTable.ColumnDefinition[] columns = new CreateTable.ColumnDefinition[list.size()];

        for (int i = 0; i < list.size(); i++) {
          HiveParser.ColumnNameTypeContext eachColumn = list.get(i);
          String type = null;
          if (eachColumn.colType().type() != null) {
            if (eachColumn.colType().type().primitiveType() != null) {
              HiveParser.PrimitiveTypeContext primitiveType = eachColumn.colType().type().primitiveType();

              if (primitiveType.KW_STRING() != null) {
                type = TajoDataTypes.Type.TEXT.name();
              } else if (primitiveType.KW_TINYINT() != null) {
                type = TajoDataTypes.Type.INT1.name();
              } else if (primitiveType.KW_SMALLINT() != null) {
                type = TajoDataTypes.Type.INT2.name();
              } else if (primitiveType.KW_INT() != null) {
                type = TajoDataTypes.Type.INT4.name();
              } else if (primitiveType.KW_BIGINT() != null) {
                type = TajoDataTypes.Type.INT8.name();
              } else if (primitiveType.KW_FLOAT() != null) {
                type = TajoDataTypes.Type.FLOAT4.name();
              } else if (primitiveType.KW_DOUBLE() != null) {
                type = TajoDataTypes.Type.FLOAT8.name();
              } else if (primitiveType.KW_DECIMAL() != null) {
                type = TajoDataTypes.Type.DECIMAL.name();
              } else if (primitiveType.KW_BOOLEAN() != null) {
                type = TajoDataTypes.Type.BOOLEAN.name();
              } else if (primitiveType.KW_DATE() != null) {
                type = TajoDataTypes.Type.DATE.name();
              } else if (primitiveType.KW_DATETIME() != null) {
                //TODO
              } else if (primitiveType.KW_TIMESTAMP() != null) {
                type = TajoDataTypes.Type.TIMESTAMP.name();
              }

              columns[i] = new CreateTable.ColumnDefinition(eachColumn.colName.Identifier().getText(), type);
            }
          }
        }
        if (columns != null) {
          createTable.setTableElements(columns);
        }

        if (!params.isEmpty()) {
          createTable.setParams(params);
        }
      }
    }

    return createTable;
  }

  @Override
  public Expr visitDropTableStatement(HiveParser.DropTableStatementContext ctx) {
    DropTable dropTable = new DropTable(ctx.tableName().getText(), false);
    return dropTable;
  }

  /**
   * This class provides and implementation for a case insensitive token checker
   * for the lexical analysis part of antlr. By converting the token stream into
   * upper case at the time when lexical rules are checked, this class ensures that the
   * lexical rules need to just match the token with upper case letters as opposed to
   * combination of upper case and lower case characteres. This is purely used for matching lexical
   * rules. The actual token text is stored in the same way as the user input without
   * actually converting it into an upper case. The token values are generated by the consume()
   * function of the super class ANTLRStringStream. The LA() function is the lookahead funtion
   * and is purely used for matching lexical rules. This also means that the grammar will only
   * accept capitalized tokens in case it is run from other tools like antlrworks which
   * do not have the ANTLRNoCaseStringStream implementation.
   */
  public class ANTLRNoCaseStringStream extends ANTLRInputStream {

    public ANTLRNoCaseStringStream(String input) {
      super(input);
    }

    @Override
    public int LA(int i) {

      int returnChar = super.LA(i);
      if (returnChar == CharStream.EOF) {
        return returnChar;
      } else if (returnChar == 0) {
        return returnChar;
      }

      return Character.toUpperCase((char) returnChar);
    }
  }
}