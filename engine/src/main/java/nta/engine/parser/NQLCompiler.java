package nta.engine.parser;

import java.nio.ByteBuffer;

import nta.catalog.Catalog;
import nta.datum.DatumFactory;
import nta.engine.exception.NQLSyntaxException;
import nta.engine.executor.eval.BinaryExpr;
import nta.engine.executor.eval.ConstExpr;
import nta.engine.executor.eval.Expr;
import nta.engine.executor.eval.ExprType;
import nta.engine.parser.QueryBlock.FromTable;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class transforms a query statement into a QueryBlock. 
 * 
 * @author hyunsik
 * @see QueryBlock
 */
final class NQLCompiler {
  private static final Log LOG = LogFactory.getLog(NQLCompiler.class);
  
  private static final int STACK_SIZE = 32;
  private static final int ARG_SIZE = 8;
  private static final int BIN_CAPACITY = 256;
  
  private NQLCompiler() {
    // nothing
  }

  public static QueryBlock parse(final String query) throws NQLSyntaxException {
    CommonTree ast = parseTree(query);

    QueryBlock block = null;

    switch (getCmdType(ast)) {
    case SELECT:
      block = parseSelectClause(ast);
    default:
      break;
    }

    return block;

  }

  public static QueryBlock parseSelectClause(final CommonTree ast) {
    LOG.info("Begin to parse a select statement");

    QueryBlock block = new QueryBlock();

    CommonTree node;
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
      case NQLParser.FROM:
        parseFromClause(block, node);
        break;
      case NQLParser.WHERE:
        parseWhereClause(block, node);
        break;

      case NQLParser.GROUP_BY:

      case NQLParser.SEL_LIST:
      default:
        break;
      }
    }

    return block;
  }

  public static void parseWhereClause(final QueryBlock block, 
      final CommonTree ast) {
    ByteBuffer bb = ByteBuffer.allocate(BIN_CAPACITY);
    compileEvalTree((CommonTree) ast.getChild(0), bb);
    bb.flip();
    block.setWhereCond(new EvalTreeBin(bb.array()));
  }

  public static Expr evalExprTreeBin(final EvalTreeBin byteCode, 
      final Catalog cat) {
    Expr [] stack = new Expr[STACK_SIZE];
    int cur = 0;

    ByteBuffer bin = ByteBuffer.wrap(byteCode.getBinary());
    Expr [] data = new Expr[ARG_SIZE];

    do {
      byte op = bin.get();
      if(op == 0) {
        break;
      }
      
      switch (op) {
      case OP.PLUS: // PLUS
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.PLUS, data[1], data[0]);
        break;
        
      case OP.MINUS: // MINUS
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.MINUS, data[1], data[0]);
        break;

      case OP.MULTI: // MULTIPLE
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.MULTIPLY, data[1], data[0]);
        break;

      case OP.DIV: // DIVIDE
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.DIVIDE, data[1], data[0]);
        break;

      case OP.EQUAL: // EQUAL
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.EQUAL, data[1], data[0]);
        break;
        
      case OP.LTH: // LESS THAN
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.LTH, data[1], data[0]);
        break;

      case OP.LEQ: // LESS OR EQUAL
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.LEQ, data[1], data[0]);
        break;
        
      case OP.GTH: // GREATER THAN
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.GTH, data[1], data[0]);
        break;
        
      case OP.GEQ: // GREATER OR EQUAL
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.GEQ, data[1], data[0]);
        break;
        
      case OP.OR: // OR
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.OR, data[1], data[0]);
        break;
        
      case OP.AND: // AND
        data[0] = stack[--cur];
        data[1] = stack[--cur];
        stack[cur++] = new BinaryExpr(ExprType.AND, data[1], data[0]);
        break;
        
      case OP.FIELD: // FIELD
        int size = bin.getInt();
        byte[] fieldBytes = new byte[size];
        bin.get(fieldBytes);
        @SuppressWarnings("unused")
        String fieldName = new String(fieldBytes);
        // TODO - to be implemented
        break;
      
      case OP.FUNCTION: // FUNCTION
        // TODO - to be implemented
        
      case OP.BOOL: // BOOL
        byte bool = bin.get();
        if ((bool & 0x01) == 0x01)
          stack[cur++] = new ConstExpr(DatumFactory.create(true));
        else
          stack[cur++] = new ConstExpr(DatumFactory.create(false));

        break;
      
      case OP.BYTE: // BYTE

      case OP.SHORT: // SHORT
        stack[cur++] = new ConstExpr(DatumFactory.create(bin.getShort()));
        break;
        
      case OP.INT: // INT
        stack[cur++] = new ConstExpr(DatumFactory.create(bin.getInt()));
        break;
      
      case OP.LONG: // LONG
        stack[cur++] = new ConstExpr(DatumFactory.create(bin.getLong())); 
        break;      

      case OP.FLOAT: // FLOAT
        stack[cur++] = new ConstExpr(DatumFactory.create(bin.getFloat()));
        break;

      case OP.DOUBLE: // DOUBLE
        stack[cur++] = new ConstExpr(DatumFactory.create(bin.getDouble()));
        break;


      case OP.BYTES: // BYTES

      default:
      }
    } while (cur > 0);

    return stack[0];
  }

  public static void compileEvalTree(final CommonTree ast, 
      final ByteBuffer bin) {
    for (int i = 0; i < ast.getChildCount(); i++) {
      compileEvalTree((CommonTree) ast.getChild(i), bin);
    }

    switch (ast.getType()) {
    case NQLParser.PLUS: 
      bin.put(OP_CODES[OP.PLUS]);
      //System.out.println(ast.toString());
      break;   

    case NQLParser.MINUS:
      bin.put(OP_CODES[OP.MINUS]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.MULTIPLY:
      bin.put(OP_CODES[OP.MULTI]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.DIVIDE:
      bin.put(OP_CODES[OP.DIV]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.DIGIT:
      bin.put(OP_CODES[OP.INT]);
      int val = Integer.valueOf(ast.getText());
      bin.putInt(val);
      //System.out.println(ast.toString());
      break;

    case NQLParser.EQUAL:
      bin.put(OP_CODES[OP.EQUAL]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.LTH:
      bin.put(OP_CODES[OP.LTH]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.LEQ:
      bin.put(OP_CODES[OP.LEQ]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.GTH:
      bin.put(OP_CODES[OP.GTH]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.GEQ:
      bin.put(OP_CODES[OP.GEQ]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.OR:
      bin.put(OP_CODES[OP.OR]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.AND:
      bin.put(OP_CODES[OP.AND]);
      //System.out.println(ast.toString());
      break;

    case NQLParser.COLUMN:
      //System.out.println("Column: " + ast.toString());
      break;

    case NQLParser.FIELD_NAME:
      String fieldName = ast.getChild(0).getText();
      String tableName = null;
      if (ast.getChildCount() > 1) {
        tableName = ast.getChild(1).getText();
        fieldName = tableName + "." + fieldName;
      }

      bin.put(OP_CODES[OP.FIELD]);
      bin.putInt(fieldName.length());
      bin.put(fieldName.getBytes());

      //System.out.println("FIELD: " + fieldName);
      break;

    case NQLParser.FUNCTION:
      @SuppressWarnings("unused")
      String funcName = ast.getText();
      break;

    default:
      break;

    }
  }

  /**
   * EBNF: table_list -> tableRef (COMMA tableRef)*
   * 
   * @param block
   * @param ast
   */
  public static void parseFromClause(final QueryBlock block, final CommonTree ast) {
    int numTables = ast.getChildCount(); //

    if (numTables > 0) {
      FromTable[] tables = new FromTable[numTables];
      CommonTree node = null;
      for (int i = 0; i < ast.getChildCount(); i++) {
        node = (CommonTree) ast.getChild(i);

        switch (node.getType()) {

        case NQLParser.TABLE: {
          // table (AS ID)?
          // 0 - a table name, 1 - table alias
          FromTable table = new FromTable(node.getChild(0).getText());
          if (node.getChildCount() > 1) {
            table.setAlias(node.getChild(1).getText());
          }
        }

        default:
        } // switch
      } // for each derievedTable

      block.setFromTables(tables);
    } // if the number of tables is greater than 0
  }

  public static CommonTree parseTree(final String query) throws NQLSyntaxException {
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

  public static CommandType getCmdType(final CommonTree ast) {
    switch (ast.getType()) {
    case NQLParser.SELECT:
      return CommandType.SELECT;
    case NQLParser.INSERT:
      return CommandType.INSERT;
    case NQLParser.CREATE_TABLE:
      return CommandType.CREATE_TABLE;
    case NQLParser.DROP_TABLE:
      return CommandType.DROP_TABLE;
    case NQLParser.SHOW_TABLE:
      return CommandType.SHOW_TABLES;
    case NQLParser.DESC_TABLE:
      return CommandType.DESC_TABLE;
    case NQLParser.SHOW_FUNCTION:
      return CommandType.SHOW_FUNCTION;
    default:
      return null;
    }
  }
  
  public static class OP {
    public static final int PLUS = 1;
    public static final int MINUS = 2;
    public static final int MULTI = 3;
    public static final int DIV = 4;

    public static final int EQUAL = 5;
    public static final int LTH = 6;
    public static final int LEQ = 7;
    public static final int GTH = 8;
    public static final int GEQ = 9;

    public static final int OR = 10;
    public static final int AND = 11;

    public static final int FIELD = 12;
    public static final int FUNCTION = 13;

    public static final int BOOL = 14;
    public static final int BYTE = 15;
    public static final int SHORT = 16;
    public static final int INT = 17;
    public static final int LONG = 18;
    public static final int FLOAT = 19;
    public static final int DOUBLE = 20;
    public static final int BYTES = 21;
  }

  private static byte[] OP_CODES = { 0x00, // NULL
      0x01, // PLUS
      0x02, // MINUS
      0x03, // MULTIPLE
      0x04, // DIVIDE

      0x05, // EQUAL
      0x06, // LESS THAN
      0x07, // LESS OR EQUAL
      0x08, // GREATER THAN
      0x09, // GREATER OR EQUAL

      0xA, // OR
      0xB, // AND

      0xC, // FIELD
      0xD, // FUNCTION

      0xE, // BOOL
      0xF, // BYTE
      0x10, // SHORT
      0x11, // INT
      0x12, // LONG
      0x13, // FLOAT
      0x14, // DOUBLE
      0x15, // BYTES
  };
}
