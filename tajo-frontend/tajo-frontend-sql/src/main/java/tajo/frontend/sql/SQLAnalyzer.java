package tajo.frontend.sql;

import com.google.common.base.Preconditions;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import tajo.algebra.*;
import tajo.algebra.Aggregation.GroupElement;
import tajo.algebra.Aggregation.GroupType;
import tajo.algebra.LiteralExpr.LiteralType;
import tajo.algebra.Sort.SortSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tajo.algebra.CreateTable.ColumnDefinition;

public class SQLAnalyzer {

  public Expr parse(String sql) throws SQLSyntaxError {
    ParsingContext context = new ParsingContext(sql);
    CommonTree tree = parseSQL(sql);
    return transform(context, tree);
  }

  private static class ParsingContext {
    private String rawQuery;
    public ParsingContext(String sql) {
      this.rawQuery = sql;
    }
  }

  public static CommonTree parseSQL(String sql) {
    ANTLRStringStream input = new ANTLRStringStream(sql);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);

    CommonTree ast;
    try {
      ast = ((CommonTree) parser.statement().getTree());
    } catch (RecognitionException e) {
      throw new SQLParseError(e.getMessage());
    }

    return ast;
  }

  Expr transform(ParsingContext context, CommonTree ast) throws SQLSyntaxError {
    switch (ast.getType()) {
      case SQLParser.SELECT:
        return parseSelectStatement(context, ast);

      case SQLParser.UNION:
      case SQLParser.EXCEPT:
      case SQLParser.INTERSECT:
        return parseSetStatement(context, ast);

      case SQLParser.INSERT:

      case SQLParser.CREATE_INDEX:

      case SQLParser.CREATE_TABLE:
        return parseCreateStatement(context, ast);

      case SQLParser.DROP_TABLE:

      default:
        return null;
    }
  }

  /**
   * t=table ASSIGN select_stmt -> ^(CREATE_TABLE $t select_stmt)
   * | CREATE TABLE t=table AS select_stmt -> ^(CREATE_TABLE $t select_stmt)
   *
   * @param ast
   * @return
   */
  private CreateTable parseCreateStatement(final ParsingContext context,
                                               final CommonTree ast) throws SQLSyntaxError {
    CreateTable createTable;

    int idx = 0;
    CommonTree node;
    String tableName = ast.getChild(idx).getText();
    idx++;

    boolean external = false;
    ColumnDefinition [] tableElements = null;
    String storeType = null;
    Map<String, String> params = null;
    Expr nestedAlgebra = null;
    String location = null;
    while(idx < ast.getChildCount()) {
      node = (CommonTree) ast.getChild(idx);
      switch (node.getType()) {
        case SQLParser.EXTERNAL:
          external = true;
          break;

        case SQLParser.TABLE_DEF:
          tableElements = parseTableElementList(node);
          break;

        case SQLParser.USING:
          storeType = node.getChild(0).getText();
          break;

        case SQLParser.PARAMS:
          params = parseParams(node);
          break;

        case SQLParser.AS:
          nestedAlgebra = parseSelectStatement(context, node.getChild(0));
          break;

        case SQLParser.LOCATION:
          location = node.getChild(0).getText();
          break;

        default:
          throw new SQLSyntaxError(context.rawQuery, "ERROR: not yet supported query");
      }
      idx++;
    }

    if (nestedAlgebra != null) {
      createTable = new CreateTable(tableName, nestedAlgebra);
    } else {
      createTable = new CreateTable(tableName);
      if (external) {
        if (location != null) {
          createTable.setLocation(location);
        }
      }
    }

    if (tableElements != null) {
      createTable.setTableElements(tableElements);
    }

    if (storeType != null) {
      createTable.setStorageType(storeType);
      if (params != null) {
        createTable.setParams(params);
      }
    }

    // constraints
    if (external && location == null) {
      throw new SQLSyntaxError(context.rawQuery,
          "CREATE EXTERNAL TABLE statement requires LOCATION clause.");
    }
    if (!external && location != null) {
      throw new SQLSyntaxError(context.rawQuery,
          "LOCATION clause can be only used in CREATE EXTERNAL TABLE statement.");
    }
    if (tableElements == null && location != null) {
      throw new SQLSyntaxError(context.rawQuery,
          "LOCATION clause requires a schema definition.");
    }

    return createTable;
  }

  private ColumnDefinition [] parseTableElementList(final CommonTree ast) {
    ColumnDefinition [] tableDef = new ColumnDefinition[ast.getChildCount()];
    for (int i = 0; i < ast.getChildCount(); i++) {
      tableDef[i] = new ColumnDefinition(ast.getChild(i).getChild(0).getText(),
          ast.getChild(i).getChild(1).getText());;
    }

    return tableDef;
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
  private static Map<String,String> parseParams(final CommonTree ast) {
    Map<String, String> params = new HashMap<>();

    Tree child;
    for (int i = 0; i < ast.getChildCount(); i++) {
      child = ast.getChild(i);
      params.put(child.getChild(0).getText(), child.getChild(1).getText());
    }
    return params;
  }

  private Expr parseSelectStatement(final ParsingContext context, final Tree ast) throws SQLSyntaxError {
    CommonTree node;
    QueryBlock block = new QueryBlock();
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
        case SQLParser.FROM:
          Expr fromClause = parseFromClause(context, node);
          block.setTableExpression(fromClause);
          break;

        case SQLParser.SET_QUALIFIER:
          if (parseSetQualifier(node)) {
            block.setDistinct();
          }
          break;

        case SQLParser.SEL_LIST:
          Projection projection = parseSelectList(context, node);
          block.setProjection(projection);
          break;

        case SQLParser.WHERE:
          block.setSearchCondition(parseWhereClause(context, node));
          break;

        case SQLParser.GROUP_BY:
          Aggregation aggregation = parseGroupByClause(node);
          block.setAggregation(aggregation);
          break;

        case SQLParser.HAVING:
          Expr expr = parseHavingClause(context, node);
          block.setHavingCondition(expr);
          break;

        case SQLParser.ORDER_BY:
          SortSpec [] sortSpecs = parseSortSpecifiers(node.getChild(0));
          block.setSort(new Sort(sortSpecs));
          break;

        case SQLParser.LIMIT:
          block.setLimit(parseLimitClause(context, node));
          break;

        default:
      }
    }

    return transformQueryBlock(context, block);
  }

  /**
   * There is an algebraic order of a QueryBlock.
   * Relation (or Join) -> Selection -> Aggregation -> Sort -> Limit
   *
   * @param context
   * @param block
   * @return
   */
  private Expr transformQueryBlock(ParsingContext context, QueryBlock block) throws SQLSyntaxError {
    Expr current = null;

    // Selection
    if (block.hasTableExpression()) {
      current = block.getTableExpression();
    }

    if (block.hasSearchCondition()) {
      if (current == null) {
        throw new SQLSyntaxError(context.rawQuery,
            "No TableExpression, but there exists search condition");
      }
      Selection selection = new Selection(current, block.getSearchCondition());
      current = selection;
    }

    // Aggregation
    Aggregation aggregation = null;
    if (block.hasAggregation()) {
      if (current == null) {
        throw new SQLSyntaxError(context.rawQuery,
            "No TableExpression, but there exists a group-by clause");
      }
      aggregation = block.getAggregation();

      if (block.hasAggregation()) {
        aggregation.setHavingCondition(block.getHavingCondition());
      }

      aggregation.setChild(current);
      current = aggregation;
    }

    if (block.hasSort()) {
      if (current == null) {
        throw new SQLSyntaxError(context.rawQuery,
            "No TableExpression, but there exists a sort clause");
      }
      Sort sort = block.getSort();
      sort.setChild(current);
      current = sort;
    }

    if (block.hasLimit()) {
      if (current == null) {
        throw new SQLSyntaxError(context.rawQuery,
            "No TableExpression, but there exists a limit clause");
      }
      Limit limit = block.getLimit();
      limit.setChild(current);
      current = limit;
    }

    Projection projection = block.getProjection();
    if (block.hasAggregation()) {
      aggregation.setTargets(projection.getTargets());
    } else {
      if (block.isDistinct()) {
        projection.setDistinct();
      }
      if (current != null) {
        projection.setChild(current);
      }
      current = projection;
    }

    return current;
  }

  private Expr parseSetStatement(final ParsingContext context,
                                 final CommonTree ast) throws SQLSyntaxError {
    Expr left;
    Expr right;

    ExprType type = tokenToExprType(ast.getType());

    int idx = 0;
    left = transform(context, (CommonTree) ast.getChild(idx));
    idx++;
    int nodeType = ast.getChild(idx).getType();
    boolean distinct = true; // distinct is default in ANSI SQL standards
    if (nodeType == SQLParser.ALL) {
      distinct = true;
      idx++;
    } else if (nodeType == SQLParser.DISTINCT) {
      distinct = false;
      idx++;
    }
    right = transform(context, (CommonTree) ast.getChild(idx));
    return new SetOperation(type, left, right, distinct);
  }

  /**
   * @return true if SELECT DISTINCT. Otherwise, it will be false.
   */
  private boolean parseSetQualifier(final CommonTree ast) {
    int idx = 0;

    if (ast.getChild(idx).getType() == SQLParser.DISTINCT) {
      return true;
    } else {
      return false;
    }
  }

  private Expr parseHavingClause(final ParsingContext context, final CommonTree ast)
      throws SQLSyntaxError {
    return createExpression(context, ast.getChild(0));
  }

  private Limit parseLimitClause(final ParsingContext context, final CommonTree ast)
      throws SQLSyntaxError {
    Expr expr = createExpression(context, ast.getChild(0));

    if (expr instanceof LiteralExpr) {
      Limit limitClause = new Limit(expr);
      return limitClause;
    }

    throw new SQLSyntaxError(context.rawQuery, "LIMIT clause cannot have the parameter "
        + expr);
  }

  /**
   * Should be given SortSpecifiers Node
   *
   * EBNF: sort_specifier (COMMA sort_specifier)* -> sort_specifier+
   *
   * @param ast
   */
  private SortSpec[] parseSortSpecifiers(final Tree ast) {
    int numSortKeys = ast.getChildCount();
    SortSpec[] sortSpecs = new SortSpec[numSortKeys];
    CommonTree node;
    ColumnReferenceExpr column;

    // Each child has the following EBNF and AST:
    // EBNF: fn=fieldName a=order_specification? o=null_ordering?
    // AST: ^(SORT_KEY $fn $a? $o?)
    for (int i = 0; i < numSortKeys; i++) {
      node = (CommonTree) ast.getChild(i);
      column = checkAndGetColumnByAST(node.getChild(0));
      sortSpecs[i] = new SortSpec(column);

      if (node.getChildCount() > 1) {
        Tree child;
        for (int j = 1; j < node.getChildCount(); j++) {
          child = node.getChild(j);

          // AST: ^(ORDER ASC) | ^(ORDER DESC)
          if (child.getType() == SQLParser.ORDER) {
            if (child.getChild(0).getType() == SQLParser.DESC) {
              sortSpecs[i].setDescending();
            }
          } else if (child.getType() == SQLParser.NULL_ORDER) {
            // AST: ^(NULL_ORDER FIRST) | ^(NULL_ORDER LAST)
            if (child.getChild(0).getType() == SQLParser.FIRST) {
              sortSpecs[i].setNullFirst();
            }
          }
        }
      }
    }

    return sortSpecs;
  }

  /**
   * See 'groupby_clause' rule in SQL.g
   *
   * @param ast
   */
  private Aggregation parseGroupByClause(final CommonTree ast) {
    int idx = 0;
    Aggregation clause = new Aggregation();
    if (ast.getChild(idx).getType() == SQLParser.EMPTY_GROUPING_SET) {

    } else {
      // the remain ones are grouping fields.
      Tree group;
      List<ColumnReferenceExpr> columnRefs = new ArrayList<>();
      ColumnReferenceExpr[] columns;
      ColumnReferenceExpr column;
      List<GroupElement> groups = new ArrayList<>();
      for (; idx < ast.getChildCount(); idx++) {
        group = ast.getChild(idx);
        switch (group.getType()) {
          case SQLParser.CUBE:
            columns = parseColumnReferences((CommonTree) group);
            GroupElement cube = new GroupElement(GroupType.CUBE, columns);
            groups.add(cube);
            break;

          case SQLParser.ROLLUP:
            columns = parseColumnReferences((CommonTree) group);
            GroupElement rollup = new GroupElement(GroupType.ROLLUP, columns);
            groups.add(rollup);
            break;

          case SQLParser.FIELD_NAME:
            column = checkAndGetColumnByAST(group);
            columnRefs.add(column);
            break;
        }
      }

      if (columnRefs.size() > 0) {
        ColumnReferenceExpr[] groupingFields = columnRefs.toArray(new ColumnReferenceExpr[columnRefs.size()]);
        GroupElement g = new GroupElement(GroupType.GROUPBY, groupingFields);
        groups.add(g);
      }

      clause.setGroups(groups.toArray(new GroupElement[groups.size()]));
    }
    return clause;
  }



  /**
   * It parses the below EBNF.
   * <pre>
   * column_reference
   * : fieldName (COMMA fieldName)* -> fieldName+
   * ;
   * </pre>
   * @param parent
   * @return
   */
  private ColumnReferenceExpr[] parseColumnReferences(final CommonTree parent) {
    ColumnReferenceExpr[] columns = new ColumnReferenceExpr[parent.getChildCount()];

    for (int i = 0; i < columns.length; i++) {
      columns[i] = checkAndGetColumnByAST(parent.getChild(i));
    }

    return columns;
  }

  private Expr parseWhereClause(final ParsingContext context, final CommonTree ast)
      throws SQLSyntaxError {
    return createExpression(context, ast.getChild(0));
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
   * @param ast
   */
  private Projection parseSelectList(ParsingContext context, final CommonTree ast)
      throws SQLSyntaxError {
    Projection projection = new Projection();
    if (ast.getChild(0).getType() == SQLParser.ALL) {
      projection.setAll();
    } else {
      CommonTree node;
      int numTargets = ast.getChildCount();
      Target [] targets = new Target[numTargets];
      Expr evalTree;
      String alias;

      // the final one for each target is the alias
      // EBNF: bool_expr AS? fieldName
      for (int i = 0; i < ast.getChildCount(); i++) {
        node = (CommonTree) ast.getChild(i);
        evalTree = createExpression(context, node);
        targets[i] = new Target(evalTree);
        if (node.getChildCount() > 1) {
          alias = node.getChild(node.getChildCount() - 1).getChild(0).getText();
          targets[i].setAlias(alias);
        }
      }
      projection.setTargets(targets);
    }

    return projection;
  }

  /**
   * EBNF: table_list -> tableRef (COMMA tableRef)
   * @param ast
   */
  private Expr parseFromClause(ParsingContext context, final CommonTree ast) throws SQLSyntaxError {
    Expr previous = null;
    CommonTree node;
    for (int i = 0; i < ast.getChildCount(); i++) {
      node = (CommonTree) ast.getChild(i);

      switch (node.getType()) {

        case SQLParser.TABLE:
          // table (AS ID)?
          // 0 - a table name, 1 - table alias
          if (previous != null) {
            Expr inner = parseTable(node);
            Join newJoin = new Join(JoinType.INNER);
            newJoin.setLeft(previous);
            newJoin.setRight(inner);
            previous = newJoin;
          } else {
            previous = parseTable(node);
          }
          break;
        case SQLParser.JOIN:
          Join newJoin = parseExplicitJoinClause(context, node);
          newJoin.setLeft(previous);
          previous = newJoin;
          break;

        case SQLParser.SUBQUERY:
          Expr nestedAlgebra = parseSelectStatement(context, node.getChild(0));
          String alias = node.getChild(1).getText();
          previous = new TableSubQuery(alias, nestedAlgebra);
          break;

        default:
          throw new SQLSyntaxError(context.rawQuery, "Wrong From Clause");
      } // switch
    } // for each derievedTable

    return previous;
  }

  private Relation parseTable(final CommonTree tableAST) {
    String tableName = tableAST.getChild(0).getText();
    Relation table = new Relation(tableName);

    if (tableAST.getChildCount() > 1) {
      table.setAlias(tableAST.getChild(1).getText());
    }

    return table;
  }

  private Join parseExplicitJoinClause(ParsingContext ctx, final CommonTree ast)
      throws SQLSyntaxError {

    int idx = 0;
    int parsedJoinType = ast.getChild(idx).getType();

    Join joinClause;

    switch (parsedJoinType) {
      case SQLParser.CROSS:
      case SQLParser.UNION:
        joinClause = parseCrossAndUnionJoin(ast);
        break;

      case SQLParser.NATURAL:
        joinClause = parseNaturalJoinClause(ctx, ast);
        break;

      case SQLParser.INNER:
      case SQLParser.OUTER:
        joinClause = parseQualifiedJoinClause(ctx, ast, 0);
        break;

      default: // default join (without join type) is inner join
        joinClause = parseQualifiedJoinClause(ctx, ast, 0);
    }

    return joinClause;
  }

  private Join parseNaturalJoinClause(ParsingContext ctx, Tree ast) throws SQLSyntaxError {
    Join join = parseQualifiedJoinClause(ctx, ast, 1);
    join.setNatural();
    return join;
  }

  private Join parseQualifiedJoinClause(ParsingContext context, final Tree ast, final int idx)
      throws SQLSyntaxError {
    int childIdx = idx;
    Join join = null;

    if (ast.getChild(childIdx).getType() == SQLParser.TABLE) { // default join
      join = new Join(JoinType.INNER);
      join.setRight(parseTable((CommonTree) ast.getChild(childIdx)));

    } else {

      if (ast.getChild(childIdx).getType() == SQLParser.INNER) {
        join = new Join(JoinType.INNER);

      } else if (ast.getChild(childIdx).getType() == SQLParser.OUTER) {

        switch (ast.getChild(childIdx).getChild(0).getType()) {
          case SQLParser.LEFT:
            join = new Join(JoinType.LEFT_OUTER);
            break;
          case SQLParser.RIGHT:
            join = new Join(JoinType.RIGHT_OUTER);
            break;
          case SQLParser.FULL:
            join = new Join(JoinType.FULL_OUTER);
            break;
          default:
            throw new SQLSyntaxError(context.rawQuery, "Unknown Join Type");
        }
      }

      childIdx++;
      join.setRight(parseTable((CommonTree) ast.getChild(childIdx)));
    }

    childIdx++;

    if (ast.getChildCount() > childIdx) {
      CommonTree joinQual = (CommonTree) ast.getChild(childIdx);

      if (joinQual.getType() == SQLParser.ON) {
        Expr joinCond = parseJoinCondition(context, joinQual);
        join.setQual(joinCond);

      } else if (joinQual.getType() == SQLParser.USING) {
        ColumnReferenceExpr[] joinColumns = parseJoinColumns(joinQual);
        join.setJoinColumns(joinColumns);
      }
    }

    return join;
  }

  private Join parseCrossAndUnionJoin(Tree ast) {
    JoinType joinType;

    if (ast.getChild(0).getType() == SQLParser.CROSS) {
      joinType = JoinType.CROSS_JOIN;
    } else if (ast.getChild(0).getType() == SQLParser.UNION) {
      joinType = JoinType.UNION;
    } else {
      throw new IllegalStateException("Neither the AST has cross join or union join:\n"
          + ast.toStringTree());
    }

    Join join = new Join(joinType);
    Preconditions.checkState(ast.getChild(1).getType() == SQLParser.TABLE);
    join.setRight(parseTable((CommonTree) ast.getChild(1)));

    return join;
  }

  private ColumnReferenceExpr[] parseJoinColumns(final CommonTree ast) {
    ColumnReferenceExpr[] joinColumns = new ColumnReferenceExpr[ast.getChildCount()];

    for (int i = 0; i < ast.getChildCount(); i++) {
      joinColumns[i] = checkAndGetColumnByAST(ast.getChild(i));
    }
    return joinColumns;
  }

  private Expr parseJoinCondition(ParsingContext context, CommonTree ast) throws SQLSyntaxError {
    return createExpression(context, ast.getChild(0));
  }

  private ColumnReferenceExpr checkAndGetColumnByAST(final Tree fieldNode) {
    Preconditions.checkArgument(SQLParser.FIELD_NAME == fieldNode.getType());

    String columnName = fieldNode.getChild(0).getText();
    ColumnReferenceExpr column = new ColumnReferenceExpr(columnName);

    String tableName = null;
    if (fieldNode.getChildCount() > 1) {
      tableName = fieldNode.getChild(1).getText();
      column.setRelationName(tableName);
    }

    return column;
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
   * @param tree
   * @return
   */
  public CaseWhenExpr parseCaseWhen(ParsingContext context, final Tree tree) throws SQLSyntaxError {
    int idx = 0;

    CaseWhenExpr caseEval = new CaseWhenExpr();
    Expr cond;
    Expr thenResult;
    Tree when;

    for (; idx < tree.getChildCount() &&
        tree.getChild(idx).getType() == SQLParser.WHEN; idx++) {

      when = tree.getChild(idx);
      cond = createExpression(context, when.getChild(0));
      thenResult = createExpression(context, when.getChild(1));
      caseEval.addWhen(cond, thenResult);
    }

    if (tree.getChild(idx) != null &&
        tree.getChild(idx).getType() == SQLParser.ELSE) {
      Expr elseResult = createExpression(context, tree.getChild(idx).getChild(0));
      caseEval.setElseResult(elseResult);
    }

    return caseEval;
  }

  /**
   * <pre>
   * like_predicate : fieldName NOT? LIKE string_value_expr
   * -> ^(LIKE NOT? fieldName string_value_expr)
   * </pre>
   * @param tree
   * @return
   */
  private LikeExpr parseLike(ParsingContext context, final Tree tree) throws SQLSyntaxError {
    int idx = 0;

    boolean not = false;
    if (tree.getChild(idx).getType() == SQLParser.NOT) {
      not = true;
      idx++;
    }

    ColumnReferenceExpr field = (ColumnReferenceExpr) createExpression(context, tree.getChild(idx));
    idx++;
    Expr pattern = createExpression(context, tree.getChild(idx));

    return new LikeExpr(not, field, pattern);
  }

  public Expr createExpression(final ParsingContext context, final Tree ast) throws SQLSyntaxError {
    switch(ast.getType()) {

      // constants
      case SQLParser.Unsigned_Integer:
        return new LiteralExpr(ast.getText(), LiteralType.Unsigned_Integer);
      case SQLParser.Unsigned_Float:
        return new LiteralExpr(ast.getText(), LiteralType.Unsigned_Float);
      case SQLParser.Unsigned_Large_Integer:
        return new LiteralExpr(ast.getText(), LiteralType.Unsigned_Large_Integer);

      case SQLParser.Character_String_Literal:
        return new LiteralExpr(ast.getText(), LiteralType.String);

      // unary expression
      case SQLParser.NOT:
        ;

      // binary expressions
      case SQLParser.LIKE:
        return parseLike(context, ast);

      case SQLParser.IS:
        break;

      case SQLParser.AND:
      case SQLParser.OR:
      case SQLParser.Equals_Operator:
      case SQLParser.Not_Equals_Operator:
      case SQLParser.Less_Than_Operator:
      case SQLParser.Less_Or_Equals_Operator:
      case SQLParser.Greater_Than_Operator:
      case SQLParser.Greater_Or_Equals_Operator:
      case SQLParser.Plus_Sign:
      case SQLParser.Minus_Sign:
      case SQLParser.Asterisk:
      case SQLParser.Slash:
      case SQLParser.Percent:
        return new BinaryOperator(tokenToExprType(ast.getType()),
            createExpression(context, ast.getChild(0)),
            createExpression(context, ast.getChild(1)));

      // others
      case SQLParser.COLUMN:
        return createExpression(context, ast.getChild(0));

      case SQLParser.FIELD_NAME:
        return checkAndGetColumnByAST(ast);

      case SQLParser.FUNCTION:
        String signature = ast.getText();
        FunctionExpr func = new FunctionExpr(signature);
        Expr[] givenArgs = new Expr[ast.getChildCount()];

        for (int i = 0; i < ast.getChildCount(); i++) {
          givenArgs[i] = createExpression(context, ast.getChild(i));
        }
        func.setParams(givenArgs);

        break;
      case SQLParser.COUNT_VAL:

      case SQLParser.COUNT_ROWS:


      case SQLParser.CASE:
        return parseCaseWhen(context, ast);

      case SQLParser.SUBQUERY: // ^(SUBQUERY subquery)
        return new ScalarSubQuery(parseSelectStatement(context, ast.getChild(0)));

      default:
    }
    return null;
  }

  public static ExprType tokenToExprType(int tokenId) {
    switch (tokenId) {
      case SQLParser.UNION: return ExprType.Union;
      case SQLParser.EXCEPT: return ExprType.Except;
      case SQLParser.INTERSECT: return ExprType.Intersect;

      case SQLParser.AND: return ExprType.And;
      case SQLParser.OR: return ExprType.Or;
      case SQLParser.Equals_Operator: return ExprType.Equals;
      case SQLParser.Less_Than_Operator: return ExprType.LessThan;
      case SQLParser.Less_Or_Equals_Operator: return ExprType.LessThan;
      case SQLParser.Greater_Than_Operator: return ExprType.GreaterThan;
      case SQLParser.Greater_Or_Equals_Operator: return ExprType.GreaterThanOrEquals;
      case SQLParser.Plus_Sign: return ExprType.Plus;
      case SQLParser.Minus_Sign: return ExprType.Minus;
      case SQLParser.Asterisk: return ExprType.Multiply;
      case SQLParser.Slash: return ExprType.Divide;
      case SQLParser.Percent: return ExprType.Mod;

      default: throw new RuntimeException("Unknown Token Id: " + tokenId);
    }
  }
}
