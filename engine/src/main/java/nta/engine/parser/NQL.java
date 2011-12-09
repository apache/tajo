/**
 * 
 */
package nta.engine.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nta.catalog.Catalog;
import nta.catalog.Column;
import nta.catalog.ColumnBase;
import nta.catalog.FieldName;
import nta.catalog.FunctionMeta;
import nta.catalog.Schema;
import nta.catalog.TableInfo;
import nta.catalog.exception.NoSuchTableException;
import nta.catalog.proto.TableProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.SchemaObject;
import nta.engine.exception.AmbiguousFieldException;
import nta.engine.exception.InvalidFunctionException;
import nta.engine.exception.InvalidQueryException;
import nta.engine.exception.NQLSyntaxException;
import nta.engine.exception.NTAQueryException;
import nta.engine.exception.UnknownFunctionException;
import nta.engine.executor.eval.BinaryExpr;
import nta.engine.executor.eval.ConstExpr;
import nta.engine.executor.eval.Expr;
import nta.engine.executor.eval.ExprType;
import nta.engine.executor.eval.FieldExpr;
import nta.engine.executor.eval.FuncExpr;
import nta.engine.function.Function;
import nta.engine.query.TargetEntry;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;

/**
 * @author Hyunsik Choi
 *
 */
public class NQL {
	Catalog cat;
	
	
	public NQL(Catalog cat) {
		this.cat = cat;
	}
	
	public Query parse(String query) throws NTAQueryException {
		CommonTree ast = parseTree(query);
		Query stmt = new Query(query, ast);
		
		switch(stmt.getCmdType()) {
		case CREATE_TABLE: buildCreateTableStmt(stmt); break;
		case INSERT: buildInsertStmt(stmt); break;
		case SELECT: buildSelectStmt(stmt); break;
		case DESC_TABLE: buildDescTable(stmt); break;
		}
		
		return stmt;
	}
	
	private void buildInsertStmt(Query stmt) throws NoSuchTableException {		
		CommonTree ast = stmt.getAST();
		
		int cur=0;
        CommonTree node = null;
        Tree child = null;
        TargetEntry targetField;
        
        while(cur < ast.getChildCount()) {
                node = (CommonTree) ast.getChild(cur);
                switch(node.getType()) {
                case NQLParser.TABLE:                        
                        stmt.targetTableName = node.getChild(0).getText();
                        break;
                case NQLParser.TARGET_FIELDS:                	
                	for(int i=0; i < node.getChildCount(); i++) {
            			child = node.getChild(i);            			
            			targetField = new TargetEntry();
            			targetField.colName = child.getChild(0).getText();
            			stmt.targetList.add(targetField);
                	}
                        break;
                case NQLParser.VALUES:
                        stmt.values = new Datum[node.getChildCount()];
                        for(int i=0; i < node.getChildCount(); i++) {
                                CommonTree value = (CommonTree) node.getChild(i);
                                switch(value.getType()) {
                                case NQLParser.STRING:
                                        stmt.values[i] = DatumFactory.create(value.getText());
                                        break;
                                case NQLParser.DIGIT:
                                        stmt.values[i] = DatumFactory.create(Integer.valueOf(value.getText()));
                                }
                        }
                default:;
                }
                cur++;
        }
	}
	
	private void buildCreateTableStmt(Query stmt) throws NoSuchTableException {
		int cur=0;
		CommonTree node = null;
		
		String tableName = null;
		Schema tableDef = null;
		
		CommonTree ast = stmt.getAST();
		
		while(cur < ast.getChildCount()) {
			node = (CommonTree) ast.getChild(cur);
			switch(node.getType()) {
			case NQLParser.TABLE:				
				stmt.targetTableName = node.getChild(0).getText(); 
				break;
				
			case NQLParser.TABLE_DEF:
				tableDef = new Schema();
				Column f;
				DataType type = null;
				for(Tree tree : (List<Tree>)node.getChildren()) {
					switch(tree.getChild(1).getType()) {
					case NQLParser.BOOL: type = DataType.BOOLEAN; break;
					case NQLParser.BYTE: type = DataType.BYTE; break;
					case NQLParser.INT: type = DataType.INT; break;					
					case NQLParser.LONG: type = DataType.LONG; break;
					case NQLParser.FLOAT: type = DataType.FLOAT; break;
					case NQLParser.DOUBLE: type = DataType.DOUBLE; break;
					case NQLParser.IPv4: type = DataType.IPv4; break;						
					}					
					
					tableDef.addColumn(tree.getChild(0).getText(), type);					
				}
				
				stmt.tableDef = tableDef;
				break;
			case NQLParser.STORE_TYPE:
				stmt.storeName = node.getChild(0).getText();
				break;
				
			case NQLParser.SELECT: buildSelectStmt(stmt);
			}	
			cur++;
		}
	}
	
	private Query buildDescTable(Query stmt) throws NQLSyntaxException {
		CommonTree tree = stmt.getAST();
		
		if(tree.getChild(0) == null || tree.getChild(0).getText().equals("")) {
			throw new NQLSyntaxException("Need table name");
		}
		stmt.targetTableName = tree.getChild(0).getText();
		
		return stmt;
	}
	
	private Query buildSelectStmt(Query stmt) throws NoSuchTableException {
		int cur=0;
		CommonTree ast = stmt.getAST();
		CommonTree node = null;
		while(cur < ast.getChildCount()) {
			node = (CommonTree) ast.getChild(cur);
			switch(node.getType()) {
			case NQLParser.FROM: // ORDER 1st
				makeJoinList(stmt, node);
				break;
			case NQLParser.WHERE:
				makeWhereQual(stmt, node);
				break;
			case NQLParser.GROUP_BY:
				makeGroupByClause(stmt, node);
				break;
			case NQLParser.SEL_LIST:
				createProjectList(stmt, node);
				break;
			default:;
			}
			cur++;
		}
		return stmt;
	}
	
	private void makeJoinList(Query stmt, Tree from) throws NoSuchTableException {
		stmt.hasFromClause = from.getChildCount() > 0;		
		if(stmt.hasFromClause) {
			CommonTree node = null;
			TableInfo rel = null;
			stmt.numBaseRels = from.getChildCount();

			for(int i=0; i < from.getChildCount(); i++) {
				node = (CommonTree) from.getChild(i);
				if(node.getType() == NQLParser.TABLE) {
					rel = cat.getTableInfo(node.getChild(0).getText());												
					if(node.getChildCount() > 1) {					
						stmt.tableMap.put(node.getChild(1).getText(), 
								new RelInfo(rel, node.getChild(1).getText()));
					} else {
						stmt.tableMap.put(node.getChild(0).getText(), 
								new RelInfo(rel));
					}
				}
			}
		}
	}
	
	private void makeWhereQual(Query stmt, Tree where) throws NoSuchTableException {
		stmt.hasWhereClause = true;
		stmt.whereCond = buildExpr(stmt, where.getChild(0));
	}
	
	private void makeGroupByClause(Query stmt, Tree node) throws NoSuchTableException {
		stmt.hasGroupbyClause = true;
		Tree child = null;
		for(int i=0; i < node.getChildCount(); i++) {
			child = node.getChild(i);
			if(child.getType() == NQLParser.HAVING) {
				stmt.hasHavingClause = true;
				stmt.havingCond = buildExpr(stmt,child.getChild(0));
			} else if (child.getType() == NQLParser.FIELD_NAME) {
				stmt.groupByCols.add(makeTargetEntry(stmt, child, -1));				
			}
		}		
	}
		
	private TargetEntry makeTargetEntry(Query stmt, Tree node, int resNum) throws NoSuchTableException {
		TargetEntry entry  = new TargetEntry();
		
		Expr expr = buildExpr(stmt, node);
				
		if(stmt.getBaseRels().size() == 0 ||expr.getType() == ExprType.FUNCTION) {			
			entry.relId = -1;
			entry.resId = resNum;
			entry.expr = expr;
			entry.colName = entry.expr.getName();
			entry.colId = -1;
		} else {
			if(node.getType() == NQLParser.COLUMN)
				node = node.getChild(0);
			
			FieldName fieldName = new FieldName(node);
			FieldExpr field = (FieldExpr) expr;
			entry.relId = field.tableId;
			entry.colId = field.fieldId;			
			entry.resId = resNum;
			entry.colName = fieldName.getName();
			entry.expr = expr;
		}
		
		return entry;
	}
		
	private void createProjectList(Query stmt, Tree node) throws NoSuchTableException {		
		if(node.getChild(0).getType() == NQLParser.ALL) {
			stmt.allTarget = true;
		} else {
			for(int i=0; i < node.getChildCount(); i++) {
				stmt.targetList.add(makeTargetEntry(stmt, node.getChild(i), i));
			}
		}
	}
	
	public CommonTree parseTree(String query) throws NQLSyntaxException {
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
		
		if(ast.getType() == 0)
			throw new NQLSyntaxException(query);
		
		return ast;
	}	
	
	public Expr buildExpr(Query stmt, Tree tree) throws NoSuchTableException {
		switch(tree.getType()) {
				
		case NQLParser.DIGIT:
			return new ConstExpr(DatumFactory.create(Integer.valueOf(tree.getText())));
		
		case NQLParser.AND:
			return new BinaryExpr(ExprType.AND, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.OR:
			return new BinaryExpr(ExprType.OR, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
			
		case NQLParser.EQUAL:
			return new BinaryExpr(ExprType.EQUAL, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.LTH: 
			return new BinaryExpr(ExprType.LTH, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.LEQ: 
			return new BinaryExpr(ExprType.LEQ, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.GTH: 
			return new BinaryExpr(ExprType.GTH, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.GEQ: 
			return new BinaryExpr(ExprType.GEQ, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		
			
		case NQLParser.PLUS: 
			return new BinaryExpr(ExprType.PLUS, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.MINUS: 
			return new BinaryExpr(ExprType.PLUS, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.MULTIPLY: 
			return new BinaryExpr(ExprType.PLUS, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
		case NQLParser.DIVIDE: 
			return new BinaryExpr(ExprType.PLUS, buildExpr(stmt, tree.getChild(0)), 
					buildExpr(stmt,tree.getChild(1)));
			
		case NQLParser.COLUMN:
			return buildExpr(stmt, tree.getChild(0));
		case NQLParser.FIELD_NAME:
			FieldName fieldName = new FieldName(tree);
			Column field = null;
			TableInfo rel = null;
			// when given a table name
			if(fieldName.hasTableName()) {
				RelInfo rInfo = stmt.getBaseRel(fieldName.getTableName());
				field = rInfo.getSchema().getColumn(fieldName.getName());
				rel = rInfo.getRelation();
			} else {			
				for(RelInfo rInfo: stmt.getBaseRels()) {
					rel = cat.getTableInfo(rInfo.getName());
					if(rel.getSchema().getColumn(fieldName.getName()) != null) {
						if(field == null) {
							field = rel.getSchema().getColumn(fieldName.getName());								
						} else 
							throw new AmbiguousFieldException(field.getName());													
					}
				}
			}
			if(field == null)
				throw new InvalidQueryException("No such field: "+fieldName.getName());
			else {					
				return new FieldExpr(field.getDataType(), rel.getId(), field.getId());
			}			
		case NQLParser.FUNCTION:
			String funcName = tree.getText();
			FunctionMeta func = cat.getFunctionMeta(funcName);
			if(func == null) {
				throw new UnknownFunctionException(funcName);
			}
			
			Function fn = null;
			try {
				if(tree.getChildCount() == 0) {
					fn = func.newInstance(new Column [] {}, new Expr [] {});
				} else {
					Expr [] params = makeColumnList(stmt, tree);
					ColumnBase [] paramInfo = exprsToFields(stmt, params);
					fn = func.newInstance(paramInfo, params);	
				}
				return new FuncExpr(funcName, fn);
			} catch (Exception e) {
				throw new InvalidFunctionException(funcName);
			}
			
//			try {
//				Field [] paramInfo = exprsToFields(func);
//				Function fn = func.newInstance(paramInfo, subExprs);
//				res = new FuncExpr(funcName, fn);
//			} catch (Exception e) {
//				throw new InvalidFunctionException(funcName);
//			}
		default: ;
		}
		return null;
	}	
	
	public Expr [] makeColumnList(Query stmt, Tree ast) throws NoSuchTableException {
		Expr [] exprs = new Expr[ast.getChildCount()];
		for(int i=0; i < ast.getChildCount(); i++) {
			exprs[i] = buildExpr(stmt, ast.getChild(i));
		}
		
		return exprs;
	}
	
	public ColumnBase [] exprsToFields(Query stmt, Expr [] cols) {
		ColumnBase [] fields = new Column[cols.length];
		
		ColumnBase field = null;
		for(int i=0; i < cols.length; i++) {
			switch(cols[i].getType()) {
			case FIELD:			
				FieldExpr fieldExpr = (FieldExpr)cols[i];
				field = cat.getTableInfo(fieldExpr.tableId).getSchema().
					getColumn(fieldExpr.fieldId);
				break;
			case FUNCTION:
				FuncExpr funcExpr = (FuncExpr)cols[i];
				field = new ColumnBase(funcExpr.getName(),funcExpr.getValueType());
				break;
			case CONST:
				ConstExpr constExpr = (ConstExpr)cols[i];
				field = new ColumnBase(constExpr.toString(),constExpr.getValueType());
				break;
			case PLUS:
			case MINUS:
			case MULTIPLY:
			case DIVIDE:
				field = new ColumnBase(cols[i].toString(), DataType.INT);
				break;
			case AND:
			case OR:
			case EQUAL:
			case LTH:
			case LEQ:
			case GTH:
			case GEQ:
			case NOT_EQUAL:
				field = new ColumnBase(cols[i].getName(), DataType.BOOLEAN);
				break;
			}
			
			fields[i]=field;			
		}		
		return fields;
	}
	
	public TargetEntry [] getPhyTargetList(Query query) {
		Set<TargetEntry> targetList = new HashSet<TargetEntry>();
		if(!query.allTarget)
			targetList.addAll(query.targetList);
			
		if(query.hasWhereClause()) {
			
		}
		
		if(query.hasGroupbyClause) {
			targetList.addAll(query.groupByCols);
			if(query.hasHavingClause) {
				
			}
		}			
		
		return targetList.toArray(new TargetEntry[targetList.size()]);
	}
	
	
	
	public class RelInfo implements SchemaObject {
		public String alias;
		public TableInfo rel;
		
		public RelInfo(TableInfo rel) {
			this.rel = rel;
		}
		
		public RelInfo(TableInfo rel, String alias) {
			this(rel);
			this.alias = alias;
		}
		
		public void setTableDesc(TableInfo rel) {
			this.rel = rel;
		}
		
		public boolean hasAlias() {
			return alias != null;
		}
		
		public Schema getSchema() {
			return this.rel.getSchema();
		}
		
		public TableInfo getRelation() {
			return this.rel;
		}
		
		public String alias() {
			return this.alias;
		}
		
		public String getName() {
			return rel.getName();
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(this.rel);			
			if(hasAlias()) {
				sb.append("("+alias+")");
			}
			return sb.toString();
		}
	}
	
	public class Query {
		private String queryString;
		private CommonTree ast;
		
		// Command
		private CommandType cmdType;	
		
		// FROM
		boolean hasFromClause = false;
		public int numBaseRels;
		public Map<String,RelInfo> tableMap = new HashMap<String, NQL.RelInfo>();	
		
		// Join
		boolean hasJoinQual = false;		
		
		// Where
		boolean hasWhereClause = false;
		Expr whereCond;
		
		// Target List
		int targetCount = 0;
		boolean allTarget = false;
		List<TargetEntry> targetList = new ArrayList<TargetEntry>();
		
		// Group By
		private boolean hasGroupbyClause = false;
		List<TargetEntry> groupByCols = new ArrayList<TargetEntry>();
		private boolean hasHavingClause = false;
		Expr havingCond;	
		
		// Sort
		int [] sortFields;
		boolean asc = true;
		
		// Limit
		int limitOffset;
		int limitCount;
		
		String resultRelName;
		
		// Values
		Datum [] values;
		
		// It indicates the name of a target table for INSERTION and CREATE TABLE.
		String targetTableName = null;
		// It is the table definition for CREATE TABLE.
		Schema tableDef = null;
		String storeName = null;
		
		Query subQuery = null;
		
		private Query(String rawQuery, CommonTree tree) {	
			this.queryString = rawQuery;
			this.ast = tree;
			this.cmdType = getCmdType();
		}
		
		public String getQueryString() {
			return this.queryString;
		}
		
		public CommonTree getAST() {
			return this.ast;
		}
	
		public CommandType getCmdType() {
			if(this.cmdType == null) {
				switch(ast.getType()) {
				case NQLParser.SELECT: this.cmdType = CommandType.SELECT; break;
				case NQLParser.INSERT: this.cmdType = CommandType.INSERT; break;
				case NQLParser.CREATE_TABLE: this.cmdType = CommandType.CREATE_TABLE; break;
				case NQLParser.DROP_TABLE: this.cmdType = CommandType.DROP_TABLE; break;
				case NQLParser.SHOW_TABLE: this.cmdType = CommandType.SHOW_TABLES; break;
				case NQLParser.DESC_TABLE: this.cmdType = CommandType.DESC_TABLE; break;
				case NQLParser.SHOW_FUNCTION: this.cmdType = CommandType.SHOW_FUNCTION; break;
				default: return null;
				}
			}
			
			return this.cmdType;
		}
		
		public Datum [] getValues() {
			return this.values;
		}
		
		public boolean hasFromClause() {
			return this.hasFromClause;
		}
		
		public int getNumBaseRels() {
			return this.numBaseRels;
		}
		
		public RelInfo getBaseRel(String name) {
			return tableMap.get(name);
		}
		
		public List<RelInfo> getBaseRels() {
			return new ArrayList<RelInfo>(this.tableMap.values());
		}
		
		public List<String> getBaseRelNames() {
			return new ArrayList<String>(tableMap.keySet());
		}
		
		public boolean hasJoinQual() {
			return true;
		}
		
		public boolean hasWhereClause() {
			return this.hasWhereClause;
		}
		
		public Expr getWhereCond() {
			return this.whereCond;
		}
		
		public boolean asterisk() {
			return this.allTarget;
		}
		
		public TargetEntry [] getTargetList() {
			return this.targetList.toArray(new TargetEntry[targetList.size()]);
		}
		
		public boolean hasGroupByClause() {
			return this.hasGroupbyClause;
		}
		
		public TargetEntry [] getGroupbyCols() {
			return this.groupByCols.toArray(new TargetEntry[groupByCols.size()]);
		}		
		
		/**
		 * This method is used as the following statements:
		 * 
		 * DESC TABLE
		 * CREATE TABLE
		 * 
		 * @return
		 */
		public String getTargetTable() {
			return targetTableName;
		}
		
		/**
		 * This method is used as the following statements:
		 * 
		 * CREATE TABLE
		 * 
		 * @return
		 */
		public Schema getTableDef() {
			return this.tableDef;
		}
		
		public String getStoreName() {
			return this.storeName;
		}
		
		public boolean hasStoreType() {
			return this.storeName != null;
		}
		
		/**
		 * This method is used as the following statements:
		 * 
		 * CREATE TABLE
		 * 
		 * @return
		 */		
		public boolean hasSubQuery() {
			return this.subQuery != null;
		}
		
		/**
		 * This method is used as the following statements:
		 * 
		 * CREATE TABLE
		 * 
		 * @return
		 */
		public Query getSubQuery() {
			return this.subQuery;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(cmdType).append("\n");
			if(hasFromClause) {
				sb.append("FROM: ");
				for(RelInfo rinfo: getBaseRels()) {
					sb.append(rinfo.rel);
				}
				sb.append("\n");
			}
			
			if(hasWhereClause) {
				sb.append("WHERE: ");
				sb.append(whereCond).append("\n");
			}
			
			if(hasGroupbyClause) {
				sb.append("GROUP BY:\n");
				for(TargetEntry e : groupByCols)
					sb.append("-"+e.colName).append("\n");
				
				if(hasHavingClause) {
					sb.append("  HAVING:\n  ");
					sb.append(havingCond).append("\n");	
				}
			}
			
			if(storeName != null) {
				sb.append("Store Type: ").append(storeName).append("\n");
			}
			
			return sb.toString();
		}
	}
}
