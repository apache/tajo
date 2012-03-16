package nta.engine.parser;

import nta.catalog.CatalogService;
import nta.catalog.Column;
import nta.catalog.FunctionDesc;
import nta.catalog.Options;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableDesc;
import nta.catalog.exception.NoSuchTableException;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionType;
import nta.catalog.proto.CatalogProtos.IndexMethod;
import nta.datum.DatumFactory;
import nta.engine.Context;
import nta.engine.exception.InternalException;
import nta.engine.exec.eval.AggFuncCallEval;
import nta.engine.exec.eval.BinaryEval;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.CountRowEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.LikeEval;
import nta.engine.exec.eval.NotEval;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.parser.QueryBlock.JoinClause;
import nta.engine.parser.QueryBlock.SortKey;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.JoinType;
import nta.engine.query.exception.AmbiguousFieldException;
import nta.engine.query.exception.InvalidQueryException;
import nta.engine.query.exception.NQLSyntaxException;
import nta.engine.query.exception.NotSupportQueryException;
import nta.engine.query.exception.UndefinedFunctionException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

/**
 * This class transforms a query statement into a QueryBlock. 
 * 
 * @author Hyunsik Choi
 * 
 * @see QueryBlock
 */
public final class QueryAnalyzer {
  private static final Log LOG = LogFactory.getLog(QueryAnalyzer.class);
  private final CatalogService catalog;
  
  public QueryAnalyzer(CatalogService catalog) {
    this.catalog = catalog;
  }

  public ParseTree parse(final Context ctx, final String query) {
    CommonTree ast = parseTree(query);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Analyzer: " + ast.toStringTree());
    }

    ParseTree parseTree = null;

    switch (getCmdType(ast)) {
    case SELECT:
      parseTree = parseSelectStatement(ctx, ast);
      break;
      
    case CREATE_INDEX:
      parseTree = parseIndexStatement(ctx, ast);
      break;
    
    case CREATE_TABLE:
      parseTree = parseCreateStatement(ctx, ast);
    default:
      break;
    }

    ctx.makeHints(parseTree);
    return parseTree;

  }
  
  /**
   * t=table ASSIGN select_stmt -> ^(CREATE_TABLE $t select_stmt)
   * | CREATE TABLE t=table AS select_stmt -> ^(CREATE_TABLE $t select_stmt)
   * 
   * @param ctx
   * @param ast
   * @return
   */
  private final CreateTableStmt parseCreateStatement(final Context ctx,
      final CommonTree ast) {    
    CommonTree node;
    
    node = (CommonTree) ast.getChild(0);
    String tableName = node.getText();
    
    node = (CommonTree) ast.getChild(1);
    if (node.getType() != NQLParser.SELECT) {
      throw new NotSupportQueryException("ERROR: not yet supported query");
    }
    
    QueryBlock selectStmt = parseSelectStatement(ctx, node);    
    CreateTableStmt stmt = new CreateTableStmt(tableName, selectStmt);
    
    return stmt;
  }

  private final QueryBlock parseSelectStatement(final Context ctx,
      final CommonTree ast) {
    
    QueryBlock block = new QueryBlock();

    CommonTree node;
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
      case NQLParser.FROM:
        parseFromClause(ctx, node, block);
        break;
              
      case NQLParser.SET_QUALIFIER:
        
      case NQLParser.SEL_LIST:
        parseSelectList(ctx, block, node);
        break;
        
      case NQLParser.WHERE:
        parseWhereClause(ctx, block, node);
        break;

      case NQLParser.GROUP_BY:
        parseGroupByClause(ctx, block, node);
        break;
        
      case NQLParser.ORDER_BY:
        SortKey [] sortKeys = parseSortSpecifiers(ctx, 
            (CommonTree) node.getChild(0));
        block.setSortKeys(sortKeys);
        break;        
        
      default:
        
      }
    }

    return block;
  }
  
  /**
   * EBNF: CREATE (UNIQUE?) INDEX n=ID ON t=ID LEFT_PAREN s=sort_specifier_list 
   * RIGHT_PAREN p=param_clause? <br />
   * AST:  ^(CREATE_INDEX $n $t $s $p)
   * 
   * @param ctx
   * @param ast
   * @param block
   */
  private final CreateIndexStmt parseIndexStatement(final Context ctx,
      final CommonTree ast) {
    
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
      params = parseParams(ctx, (CommonTree) ast.getChild(idx));
      idx++;
    }
    
    // They are required, so they are always filled.
    String idxName = ast.getChild(idx++).getText();
    String tbName = ast.getChild(idx++).getText();
    ctx.renameTable(tbName, tbName);
    
    SortKey [] sortSpecs = parseSortSpecifiers(ctx, 
        (CommonTree) ast.getChild(idx++));

    CreateIndexStmt stmt = new CreateIndexStmt(idxName, unique, tbName, 
        sortSpecs);
    if (method != null) {
      stmt.setMethod(method);
    }
    
    if (params != null) {
      stmt.setParams(params);
    }
      
    return stmt;
  }
  
  /**
   * EBNF: table_list -> tableRef (COMMA tableRef)
   * @param ast
   * @param block
   */
  private void parseFromClause(final Context ctx, 
      final CommonTree ast, final QueryBlock block) {
    if (ast.getChild(0).getType() == NQLParser.JOIN) { // explicit join
      JoinClause joinClause = parseExplicitJoinClause(ctx, block, 
          (CommonTree) ast.getChild(0));
      block.setJoinClause(joinClause);
    
    } else { // implicit join or the from clause on single relation
      int numTables = ast.getChildCount();
  
      if (numTables == 1) { // on single relation
        FromTable table = null;
        CommonTree node = null;
        for (int i = 0; i < ast.getChildCount(); i++) {
          node = (CommonTree) ast.getChild(i);
  
          switch (node.getType()) {
  
          case NQLParser.TABLE:
            // table (AS ID)?
            // 0 - a table name, 1 - table alias
            table = parseTable(ctx, block, node);
            ctx.renameTable(table.getTableId(),
                table.hasAlias() ? table.getAlias() : table.getTableId());
            block.addFromTable(table);
            break;
  
          default:
          } // switch
        } // for each derievedTable
      } else if (numTables > 1) {
        // if the number of tables is greater than 1,
        // it means the implicit join clause
        JoinClause joinClause = parseImplicitJoinClause(ctx, block, 
            (CommonTree) ast);
        block.setJoinClause(joinClause);
      }
    }
  }
  
  private JoinClause parseImplicitJoinClause(final Context ctx,
      final QueryBlock block, final CommonTree ast) {
    int numTables = ast.getChildCount();
    Preconditions.checkArgument(numTables > 1);
    
    return parseImplicitJoinClause_(ctx, block, (CommonTree) ast, 0);
  }
  
  private JoinClause parseImplicitJoinClause_(final Context ctx,
      final QueryBlock block, final CommonTree ast, int idx) {
    JoinClause join = null;
    if (idx < ast.getChildCount() - 1) {
      CommonTree node = (CommonTree) ast.getChild(idx);
      FromTable left = parseTable(ctx, block, node);        
      ctx.renameTable(left.getTableId(),
          left.hasAlias() ? left.getAlias() : left.getTableId());
      block.addFromTable(left);
                
      join = new JoinClause(JoinType.CROSS_JOIN, left);
      idx++;
      if ((ast.getChildCount() - idx) > 1) {
        join.setRight(parseImplicitJoinClause_(ctx, block, ast, idx));
      } else {        
        FromTable right = parseTable(ctx, block, (CommonTree) ast.getChild(idx));
        ctx.renameTable(right.getTableId(),
            right.hasAlias() ? right.getAlias() : right.getTableId());
        block.addFromTable(right);
        join.setRight(right);
      }
    }

    return join;
  }
  
  private JoinClause parseExplicitJoinClause(final Context ctx, final QueryBlock block, 
      final CommonTree ast) {
    CommonTree joinAST = (CommonTree) ast;
    
    int idx = 0;
    int parsedJoinType = joinAST.getChild(idx).getType();
    JoinType joinType = null;
    
    switch (parsedJoinType) {
    case NQLParser.NATURAL_JOIN:
      joinType = JoinType.NATURAL;
      break;    
    case NQLParser.INNER_JOIN:
      joinType = JoinType.INNER;      
      break;
    case NQLParser.OUTER_JOIN:
      CommonTree outerAST = (CommonTree) joinAST.getChild(0);      
      if (outerAST.getChild(0).getType() == NQLParser.LEFT) {
        joinType = JoinType.LEFT_OUTER;
      } else if (outerAST.getChild(0).getType() == NQLParser.RIGHT) {
        joinType = JoinType.RIGHT_OUTER;
      }
      break;
    case NQLParser.CROSS_JOIN:
      joinType = JoinType.CROSS_JOIN;
      break;
    }
    
    idx++; // 1
    FromTable left = parseTable(ctx, block, (CommonTree) joinAST.getChild(idx));
    ctx.renameTable(left.getTableId(), 
        left.hasAlias() ? left.getAlias() : left.getTableId());
    JoinClause joinClause = new JoinClause(joinType, left);
    
    idx++; // 2
    if (joinAST.getChild(idx).getType() == NQLParser.JOIN) {
      joinClause.setRight(parseExplicitJoinClause(ctx, block, 
          (CommonTree) joinAST.getChild(idx)));
    } else {
      FromTable right = parseTable(ctx, block, 
          (CommonTree) joinAST.getChild(idx));
      ctx.renameTable(right.getTableId(), 
          right.hasAlias() ? right.getAlias() : right.getTableId());
      block.addFromTable(right);
      joinClause.setRight(right);
    }
    
    idx++; // 3
    if (joinAST.getChild(idx) != null) {
      if (joinType == JoinType.NATURAL) {
        throw new InvalidQueryException("Cross or natural join cannot have join conditions");
      }
      
      CommonTree joinQual = (CommonTree) joinAST.getChild(idx);
      if (joinQual.getType() == NQLParser.ON) {
        EvalNode joinCond = parseJoinCondition(ctx, block, joinQual);
        joinClause.setJoinQual(joinCond);
      } else if (joinQual.getType() == NQLParser.USING) {
        Column [] joinColumns = parseJoinColumns(ctx, block, joinQual);
        joinClause.setJoinColumns(joinColumns);
      }
    }
    
    return joinClause;
  }
  
  private Column [] parseJoinColumns(Context ctx, QueryBlock block, 
      CommonTree ast) {
    Column [] joinColumns = new Column[ast.getChildCount()]; 
    for (int i = 0; i < ast.getChildCount(); i++) {
      joinColumns[i] = checkAndGetColumnByAST(ctx, (CommonTree) ast.getChild(i));
    }
    return joinColumns;
  }
  
  private EvalNode parseJoinCondition(Context ctx, QueryBlock block, 
      CommonTree ast) {
    return createEvalTree(ctx, ast.getChild(0), block);
  }
  
  private static FromTable parseTable(final Context ctx, final QueryBlock block,
      final CommonTree tableAST) {
    String tableName = tableAST.getChild(0).getText();
    TableDesc desc = checkAndGetTableByName(ctx, tableName);
    FromTable table = null;
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
  private void parseSelectList(final Context ctx, 
      final QueryBlock block, final CommonTree ast) {    
  
    if (ast.getChild(0).getType() == NQLParser.ALL) {
      block.setProjectAll();
    } else {
      CommonTree node = null;
      int numTargets = ast.getChildCount();
      Target [] targets = new Target[numTargets];
      EvalNode evalTree = null;
      String alias = null;
      
      // the final one for each target is the alias
      // EBNF: bool_expr AS? fieldName
      for (int i = 0; i < ast.getChildCount(); i++) {        
        node = (CommonTree) ast.getChild(i);
        evalTree = createEvalTree(ctx, node, block);
        targets[i] = new Target(evalTree); 
        if (node.getChildCount() > 1) {          
          alias = node.getChild(node.getChildCount() - 1).getChild(0).getText();
          targets[i].setAlias(alias);
        }
      }
      
      block.setTargetList(targets);
    }    
  }
  
  private void parseWhereClause(final Context ctx, 
      final QueryBlock block, final CommonTree ast) {
    EvalNode whereCond = createEvalTree(ctx, ast.getChild(0), block);        
    block.setWhereCondition(whereCond);    
  }
  
  /**
   * EBNF -> AST: <br /><pre>
   * groupby_clause
   * : 'group' 'by' fieldList having_clause? -> ^(GROUP_BY having_clause? fieldList)
   * ;
   * </pre>
   * 
   * @param ctx
   * @param block
   * @param ast
   */
  private void parseGroupByClause(final Context ctx, 
      final QueryBlock block, final CommonTree ast) {
    
    int numFields = ast.getChildCount();
    
    // the first child is having clause, but it is optional.
    int idx = 0;
    if (ast.getChild(idx).getType() == NQLParser.HAVING) {      
      EvalNode evalTree = 
          createEvalTree(ctx, (CommonTree) ast.getChild(0).getChild(0), block);      
      block.setHavingCond(evalTree);
      idx++;
      numFields--; // because one children is this having clause.
    }

    // the remain ones are grouping fields.
    int i = 0;
    Tree fieldNode = null;
    Column [] groupingColumns = new Column [numFields];
    for (; idx < ast.getChildCount(); idx++) {
      fieldNode = ast.getChild(idx);                  
      Column column =
          checkAndGetColumnByAST(ctx,(CommonTree) fieldNode);
      groupingColumns[i] = column;     
      i++;
    }
    
    block.setGroupingFields(groupingColumns);
  }
  
  /**
   * Should be given Params Node
   * 
   * EBNF: WITH LEFT_PAREN param (COMMA param)* RIGHT_PAREN 
   * AST: ^(PARAMS param+)
   * 
   * @param ctx
   * @param ast
   * @return
   */
  private static final Options parseParams(final Context ctx, 
      final CommonTree ast) {
    Options params = new Options();
    
    Tree child = null;
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
   * @param ctx
   * @param block
   * @param ast
   */
  private static SortKey [] parseSortSpecifiers(final Context ctx, 
      final CommonTree ast) {
    int numSortKeys = ast.getChildCount();
    SortKey[] sortKeys = new SortKey[numSortKeys];
    CommonTree node = null;
    Column column = null;
    
    // Each child has the following EBNF and AST:
    // EBNF: fn=fieldName a=order_specification? o=null_ordering? 
    // AST: ^(SORT_KEY $fn $a? $o?)
    for (int i = 0; i < numSortKeys; i++) {
      node = (CommonTree) ast.getChild(i);
      column = checkAndGetColumnByAST(ctx, (CommonTree) node.getChild(0));
      sortKeys[i] = new SortKey(column);
            
      if (node.getChildCount() > 1) {
        Tree child = null;
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
  
  private static Column checkAndGetColumnByAST(final Context ctx,
      final CommonTree fieldNode) {
    Preconditions.checkArgument(NQLParser.FIELD_NAME == fieldNode.getType());
    
    String columnName = fieldNode.getChild(0).getText();
    String tableName = null;
    if (fieldNode.getChildCount() > 1) {
      tableName = fieldNode.getChild(1).getText();
    }
    
    Column column = null;
    if(tableName != null) {
      TableDesc desc = null;
      desc = checkAndGetTableByMappedName(ctx, tableName);  
      column = checkAndGetFieldByName(desc, columnName);
    } else {
      column = expectTableByField(ctx, columnName);
    }
    
    return column;
  }
  
  private static TableDesc checkAndGetTableByMappedName(final Context ctx,
      final String tableName) {
      String realName = ctx.getActualTableName(tableName);
      return checkAndGetTableByName(ctx, realName);
  }
  
  private static TableDesc checkAndGetTableByName(final Context ctx,
      final String tableName) {
    TableDesc desc = null;

    try {
      desc =
          ctx.getTable(tableName);
    } catch (NoSuchTableException nst) {
      throw new InvalidQueryException("ERROR: table \"" + tableName
          + "\" does not exist");
    }

    return desc;
  }
  
  private static Column checkAndGetFieldByName(final TableDesc desc,
      final String columnName) {
    Column column = null;
    
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
   * @param ctx
   * @param columnName field name to be find
   * @return a found column
   */
  private static Column expectTableByField(Context ctx, String columnName) {
    TableDesc desc = null;
    Schema schema = null;
    Column column = null;    
    int count = 0;
    for(String table : ctx.getInputTables()) {
      desc =
          ctx.getTable(table);
      schema = desc.getMeta().getSchema();
      
      if(schema.contains(table+"."+columnName)) {
        column = schema.getColumn(table+"."+columnName);
        count++;
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

    CommonTree ast = null;
    try {
      ast = ((CommonTree) parser.statement().getTree());
    } catch (RecognitionException e) {
      throw new NQLSyntaxException(query);
    }

    if (ast.getType() == 0) {
      throw new NQLSyntaxException(query);
    }

    return ast;
  }

  private static StatementType getCmdType(final CommonTree ast) {
    switch (ast.getType()) {
    case NQLParser.STORE:
      return StatementType.STORE;
    case NQLParser.SELECT:
      return StatementType.SELECT;
    case NQLParser.INSERT:
      return StatementType.INSERT;
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
    if (method.equals("twolevel-bst")) {
      return IndexMethod.TWO_LEVEL_BIN_TREE;
    } else if (method.equals("btree")) {
      return IndexMethod.BTREE;
    } else if (method.equals("hash")) {
      return IndexMethod.HASH;
    } else if (method.equals("bitmap")) {
      return IndexMethod.BITMAP;
    } else {
      throw new NQLSyntaxException("ERROR: unknown index: " + method);
    }
  }
  
  public EvalNode createEvalTree(final Context ctx, 
      final Tree ast, QueryBlock query) {
    switch(ast.getType()) {
        
    case NQLParser.DIGIT:
      return new ConstEval(DatumFactory.createInt(
          Integer.valueOf(ast.getText())));
      
    case NQLParser.REAL:
      return new ConstEval(DatumFactory.createDouble(
          Double.valueOf(ast.getText())));
    
    case NQLParser.AND:
      return new BinaryEval(Type.AND, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
    case NQLParser.OR:
      return new BinaryEval(Type.OR, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
      
    case NQLParser.LIKE:
      return parseLike(ctx, ast, query);
      
    case NQLParser.EQUAL:
      return new BinaryEval(Type.EQUAL, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
    case NQLParser.LTH: 
      return new BinaryEval(Type.LTH, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
    case NQLParser.LEQ: 
      return new BinaryEval(Type.LEQ, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
    case NQLParser.GTH: 
      return new BinaryEval(Type.GTH, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
    case NQLParser.GEQ: 
      return new BinaryEval(Type.GEQ, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));        
    case NQLParser.NOT:
      return new NotEval(createEvalTree(ctx, ast.getChild(0), query));
      
    case NQLParser.PLUS: 
      return new BinaryEval(Type.PLUS, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
    case NQLParser.MINUS: 
      return new BinaryEval(Type.MINUS, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
    case NQLParser.MULTIPLY: 
      return new BinaryEval(Type.MULTIPLY, 
          createEvalTree(ctx, ast.getChild(0), query),
          createEvalTree(ctx, ast.getChild(1), query));
      
    case NQLParser.DIVIDE: 
      return new BinaryEval(Type.DIVIDE, createEvalTree(ctx, ast.getChild(0), query), 
          createEvalTree(ctx, ast.getChild(1), query));
      
    case NQLParser.COLUMN:
      return createEvalTree(ctx, ast.getChild(0), query);
      
    case NQLParser.FIELD_NAME:              
      Column column = checkAndGetColumnByAST(ctx, (CommonTree) ast);     
  
      return new FieldEval(column); 
      
    case NQLParser.FUNCTION:
      String signature = ast.getText();
            
      EvalNode [] givenArgs = new EvalNode[ast.getChildCount()];
      DataType [] paramTypes = new DataType[ast.getChildCount()];

      for (int i = 0; i < ast.getChildCount(); i++) {
        givenArgs[i] = createEvalTree(ctx, ast.getChild(i), query);
        paramTypes[i] = givenArgs[i].getValueType();
      }
      if (!catalog.containFunction(signature, paramTypes)) {
        throw new UndefinedFunctionException(TCatUtil.
            getCanonicalName(signature, paramTypes));
      }
      FunctionDesc funcDesc = catalog.getFunction(signature, paramTypes);
      try {
        if (funcDesc.getFuncType() == FunctionType.GENERAL)
          return new FuncCallEval(funcDesc, funcDesc.newInstance(), givenArgs);
        else {
          query.setAggregation();
          return new AggFuncCallEval(funcDesc, funcDesc.newInstance(), givenArgs);
        }
      } catch (InternalException e) {
        e.printStackTrace();
      }
      
      break;
    case NQLParser.COUNT_VAL:
      // Getting the first argument
      EvalNode colRef = createEvalTree(ctx, ast.getChild(0), query);
      
      FunctionDesc countVals = catalog.getFunction("count", 
          new DataType [] {DataType.ANY});
      query.setAggregation();
      try {
        return new AggFuncCallEval(countVals, countVals.newInstance(), 
            new EvalNode [] {colRef});
      } catch (InternalException e1) {
        e1.printStackTrace();
      }
      break;
      
    case NQLParser.COUNT_ROWS:
      FunctionDesc countRows = catalog.getFunction("count", new DataType [] {});
      query.setAggregation();
      try {
        return new CountRowEval(countRows, countRows.newInstance(),
            new EvalNode [] {});
      } catch (InternalException e) {
        e.printStackTrace();
      }
      break;
      
    default:
    }
    return null;
  }
  
  /**
   * <pre>
   * like_predicate : fieldName NOT? LIKE string_value_expr 
   * -> ^(LIKE NOT? fieldName string_value_expr)
   * </pre>
   * @param ctx
   * @param tree
   * @param block
   * @return
   */
  private LikeEval parseLike(Context ctx, Tree tree, QueryBlock block) {
    int idx = 0;
    
    boolean not = false;
    if (tree.getChild(idx).getType() == NQLParser.NOT) {
      not = true;
      idx++;
    }
    
    Column column = checkAndGetColumnByAST(ctx, (CommonTree) 
        tree.getChild(idx));
    idx++;
    String pattern = tree.getChild(idx).getText();
    return new LikeEval(not, column, pattern);
  }
}
