package nta.engine.parser;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import nta.catalog.Column;
import nta.catalog.FunctionDesc;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.exception.NoSuchTableException;
import nta.datum.DatumFactory;
import nta.engine.CatalogReader;
import nta.engine.exception.InternalException;
import nta.engine.exec.eval.BinaryEval;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.exec.eval.FieldEval;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.parser.QueryBlock.FromTable;
import nta.engine.parser.QueryBlock.SortKey;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.query.exception.AmbiguousFieldException;
import nta.engine.query.exception.InvalidQueryException;
import nta.engine.query.exception.NQLSyntaxException;
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
 * @author hyunsik
 * 
 * @see QueryBlock
 */
public final class QueryAnalyzer {
  private static final Log LOG = LogFactory.getLog(QueryAnalyzer.class);
  
  private QueryAnalyzer() {
    // nothing
  }

  public static QueryBlock parse(final String query, 
      final CatalogReader catalog) {
    CommonTree ast = parseTree(query);

    QueryBlock block = null;
    ParseContext context = new ParseContext(catalog);

    switch (getCmdType(ast)) {
    case SELECT:
      block = parseSelectStatement(context, ast);
    default:
      break;
    }

    return block;

  }

  public static QueryBlock parseSelectStatement(final ParseContext ctx,
      final CommonTree ast) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Parse: " + ast.toStringTree());
    }
    QueryBlock block = new QueryBlock(StatementType.SELECT);

    CommonTree node;
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
      case NQLParser.FROM:
        parseFromClause(ctx, block, node);
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
        parseOrderByClause(ctx, block, node);
        break;        
        
      default:
        
      }
    }

    return block;
  }
  
  /**
   * EBNF: table_list -> tableRef (COMMA tableRef)*
   * 
   * @param block
   * @param ast
   */
  private static void parseFromClause(final ParseContext ctx, 
      final QueryBlock block, final CommonTree ast) {
    int numTables = ast.getChildCount(); //

    if (numTables > 0) {
      FromTable[] tables = new FromTable[numTables];
      CommonTree node = null;
      for (int i = 0; i < ast.getChildCount(); i++) {
        node = (CommonTree) ast.getChild(i);

        switch (node.getType()) {

        case NQLParser.TABLE:
          // table (AS ID)?
          // 0 - a table name, 1 - table alias
          String tableName = node.getChild(0).getText();
          TableDesc desc = checkAndGetTableByName(ctx, tableName);
          FromTable table = null;
          if (node.getChildCount() > 1) {
            table = new FromTable(desc, 
                node.getChild(1).getText());
            ctx.mapTables(table.getTableId(), table.getAlias());
          } else {
            table = new FromTable(desc);
            ctx.mapTables(table.getTableId(), table.getTableId());
          }
          
          tables[i] = table;
          
          break;        

        default:
        } // switch
      } // for each derievedTable

      block.setFromTables(tables);
    } // if the number of tables is greater than 0
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
  private static void parseSelectList(final ParseContext ctx, 
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
        //evalTreeBin = compileEvalTree(node);
        evalTree = createEvalTree(ctx, node);
        // TODO - the rettype should be expected
        targets[i] = new Target(evalTree); 
        if (node.getChildCount() > 1) {          
          alias = node.getChild(node.getChildCount() - 1).getChild(0).getText();
          targets[i].setAlias(alias);
        }
      }
      
      block.setTargetList(targets);
    }    
  }
  
  private static void parseWhereClause(final ParseContext ctx, 
      final QueryBlock block, final CommonTree ast) {
    EvalNode evalTree = createEvalTree(ctx, ast.getChild(0));
    block.setWhereCondition(evalTree);
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
  private static void parseGroupByClause(final ParseContext ctx, 
      final QueryBlock block, final CommonTree ast) {
    
    int numFields = ast.getChildCount();
    
    // the first child is having clause, but it is optional.
    int idx = 0;
    if (ast.getChild(idx).getType() == NQLParser.HAVING) {      
      EvalNode evalTree = 
          createEvalTree(ctx, (CommonTree) ast.getChild(0).getChild(0));      
      block.setHavingCond(evalTree);
      idx++;
      numFields--; // because one children is this having clause.
    }

    // the remain ones are grouping fields.
    int i = 0;
    Tree fieldNode = null;
    EvalNode [] groupingColumns = new EvalNode [numFields];
    for (; idx < ast.getChildCount(); idx++) {
      fieldNode = ast.getChild(idx);                  
      EvalNode evalTree = 
          createEvalTree(ctx, (CommonTree) fieldNode);
      groupingColumns[i] = evalTree;     
      i++;
    }
    
    block.setGroupingFields(groupingColumns);
  }
  

  private static void parseOrderByClause(final ParseContext ctx,
      final QueryBlock block, final CommonTree ast) {
    int numSortKeys = ast.getChildCount();
    SortKey[] sortKeys = new SortKey[numSortKeys];
    CommonTree node = null;
    Column column = null;
    for (int i = 0; i < numSortKeys; i++) {
      node = (CommonTree) ast.getChild(i);
      column = checkAndGetColumnByAST(ctx, (CommonTree) node.getChild(0));
      sortKeys[i] = new SortKey(column);
      if (node.getChildCount() > 1
          && node.getChild(1).getType() == NQLParser.DESC) {
        sortKeys[i].setDesc();
      }
    }
    
    block.setSortKeys(sortKeys);
  }
  
  private static Column checkAndGetColumnByAST(final ParseContext ctx,
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
  
  private static TableDesc checkAndGetTableByMappedName(final ParseContext ctx,
      final String tableName) {
      String realName = ctx.getMappedName(tableName);
      return checkAndGetTableByName(ctx, realName);
  }
  
  private static TableDesc checkAndGetTableByName(final ParseContext ctx,
      final String tableName) {
    TableDesc desc = null;

    try {
      desc = ctx.getCatalog().getTableDesc(tableName);
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
  private static Column expectTableByField(ParseContext ctx, String columnName) {
    TableDesc desc = null;
    Schema schema = null;
    Column column = null;    
    int count = 0;
    for(String table : ctx.getTables()) {
      desc = ctx.getCatalog().getTableDesc(table);
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
    case NQLParser.SELECT:
      return StatementType.SELECT;
    case NQLParser.INSERT:
      return StatementType.INSERT;
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
  
  public static EvalNode createEvalTree(final ParseContext ctx, 
      final Tree ast) {
    switch(ast.getType()) {
        
    case NQLParser.DIGIT:
      return new ConstEval(DatumFactory.createInt(Integer.valueOf(ast.getText())));
    
    case NQLParser.AND:
      return new BinaryEval(Type.AND, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    case NQLParser.OR:
      return new BinaryEval(Type.OR, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
      
      
    case NQLParser.EQUAL:
      return new BinaryEval(Type.EQUAL, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    case NQLParser.LTH: 
      return new BinaryEval(Type.LTH, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    case NQLParser.LEQ: 
      return new BinaryEval(Type.LEQ, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    case NQLParser.GTH: 
      return new BinaryEval(Type.GTH, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    case NQLParser.GEQ: 
      return new BinaryEval(Type.GEQ, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    
      
    case NQLParser.PLUS: 
      return new BinaryEval(Type.PLUS, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    case NQLParser.MINUS: 
      return new BinaryEval(Type.MINUS, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
    case NQLParser.MULTIPLY: 
      return new BinaryEval(Type.MULTIPLY, 
          createEvalTree(ctx, ast.getChild(0)),
          createEvalTree(ctx, ast.getChild(1)));
      
    case NQLParser.DIVIDE: 
      return new BinaryEval(Type.DIVIDE, createEvalTree(ctx, ast.getChild(0)), 
          createEvalTree(ctx, ast.getChild(1)));
      
    case NQLParser.COLUMN:
      // TODO - support column alias
      return createEvalTree(ctx, ast.getChild(0));
      
    case NQLParser.FIELD_NAME:              
      Column column = checkAndGetColumnByAST(ctx, (CommonTree) ast);     
  
      return new FieldEval(column); 
      
    case NQLParser.FUNCTION:
      String signature = ast.getText();
      
      if (!ctx.getCatalog().containFunction(signature)) {
        throw new UndefinedFunctionException(signature);
      }
      
      FunctionDesc funcDesc = ctx.getCatalog().getFunctionMeta(signature);
      
      int numArgs = funcDesc.getDefinedArgs().length;
      EvalNode [] givenArgs = new EvalNode[numArgs];
      
      for (int i = 0; i < ast.getChildCount(); i++) {
        givenArgs[i] = createEvalTree(ctx, ast.getChild(i)); 
      }
      try {
        return new FuncCallEval(funcDesc, funcDesc.newInstance(), givenArgs);
      } catch (InternalException e) {
        e.printStackTrace();
      }
      
    default:
    }
    return null;
  }
  
  private static class ParseContext {
    private final CatalogReader catalog;
    private final Map<String, String> tableMap = new HashMap<String, String>();
    
    public ParseContext(final CatalogReader catalog) {
      this.catalog = catalog;
    }
    
    public CatalogReader getCatalog() {
      return this.catalog;
    }
    
    public void mapTables(final String tableName, final String aliasName) {
      tableMap.put(aliasName, tableName);
    }
    
    public String getMappedName(final String givenName) {
      return this.tableMap.get(givenName);
    }
    
    public Collection<String> getTables() {
      return tableMap.values();
    }
  }
}
