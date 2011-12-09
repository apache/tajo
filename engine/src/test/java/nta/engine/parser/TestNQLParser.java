package nta.engine.parser;

import static org.junit.Assert.*;

import java.util.List;

import nta.catalog.FieldName;
import nta.engine.exception.NQLSyntaxException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestNQLParser {
	static final String [] selQueries = {
		"select id, name, age, gender from people", // 0
		"select title, ISBN from Books", // 1
		"select studentId from students", // 2
		"session clear", // 3
		"select id, name, age, gender from people as p, students as s", // 4
		"select name, addr, sum(score) from students group by name, addr", // 5
		"select name, addr, age from people where age > 30", // 6
		"select name, addr, age from people where age = 30", // 7
		"select name as n, sum(score, 3+4, 3>4) as total, 3+4 as id from people where age = 30", // 8
		"select ipv4:src_ip from test" // 9
	};
	
	static final String [] insQueries = {
		"insert into people values (1, 'hyunsik', 32)",
		"insert into people (id, name, age) values (1, 'hyunsik', 32)"
	};
	
	public static Tree parseQuery(String query) throws NQLSyntaxException {
		ANTLRStringStream input = new ANTLRStringStream(query);
		NQLLexer lexer = new NQLLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		NQLParser parser = new NQLParser(tokens);
		
		Tree tree = null;
		try {
			tree = ((Tree) parser.statement().getTree());			
		} catch (RecognitionException e) {
			throw new NQLSyntaxException(query);
		}
		
		return tree;
	}
	
	@Test
	public void testSelectClause() throws RecognitionException, NQLSyntaxException {
		Tree tree = parseQuery(selQueries[0]);
		assertEquals(tree.getType(), NQLParser.SELECT);
	}
	
	@Test
	public void testColumnFamily() throws NQLSyntaxException {
		Tree ast = parseQuery(selQueries[9]);
		assertEquals(NQLParser.SELECT, ast.getType());
		assertEquals(NQLParser.SEL_LIST, ast.getChild(1).getType());
		assertEquals(NQLParser.COLUMN, ast.getChild(1).getChild(0).getType());
		assertEquals(NQLParser.FIELD_NAME, ast.getChild(1).getChild(0).getChild(0).
				getType());
		FieldName fieldName = new FieldName(ast.getChild(1).getChild(0).getChild(0));
		assertEquals("ipv4", fieldName.getFamilyName());
		assertEquals("src_ip", fieldName.getSimpleName());
		assertEquals("ipv4:src_ip", fieldName.getName());
	}
	
	@Test
	public void testSessionClear() throws RecognitionException, NQLSyntaxException {
		Tree tree = parseQuery(selQueries[3]);
		assertEquals(tree.getType(), NQLParser.SESSION_CLEAR);
	}
	
	@Test
	public void testWhereClause() throws RecognitionException, NQLSyntaxException {
		Tree tree = parseQuery(selQueries[6]);
		
		assertEquals(tree.getType(), NQLParser.SELECT);		
		tree = tree.getChild(1).getChild(0);
				
		assertEquals(tree.getType(), NQLParser.GTH);
		assertEquals(tree.getChild(0).getType(), NQLParser.FIELD_NAME);
		FieldName fieldName = new FieldName(tree.getChild(0));
		assertEquals(fieldName.getName(), "age");		
		assertEquals(tree.getChild(1).getType(), NQLParser.DIGIT);
		assertEquals(tree.getChild(1).getText(), "30");		
	}
	
	@Test
	public void testInsertClause() throws RecognitionException, NQLSyntaxException {
		Tree tree = parseQuery(insQueries[0]);
		assertEquals(tree.getType(),NQLParser.INSERT);
	}
	
	static String [] schemaStmts = {
		"drop table abc",
		"create table name as select * from test"
	};
	
	@Test
	public void testCreateTable() throws RecognitionException, NQLSyntaxException {
		Tree ast = parseQuery(schemaStmts[1]);
		assertEquals(ast.getType(),NQLParser.CREATE_TABLE);
		assertEquals(ast.getChild(0).getType(),NQLParser.TABLE);
		assertEquals(ast.getChild(1).getType(),NQLParser.SELECT);
	}
	
	@Test
	public void testDropTable() throws RecognitionException, NQLSyntaxException {
		Tree ast = parseQuery(schemaStmts[0]);
		assertEquals(ast.getType(),NQLParser.DROP_TABLE);
		assertEquals(ast.getChild(0).getText(),"abc");
	}
	
	static String [] controlStmts = {
		"\\t",
		"\\t abc"
	};
	
	@Test
	public void testShowTable() throws RecognitionException, NQLSyntaxException {
		Tree ast = parseQuery(controlStmts[0]);
		assertEquals(ast.getType(),NQLParser.SHOW_TABLE);		
		assertEquals(ast.getChildCount(),0);
		
		ast = parseQuery(controlStmts[1]);
		assertEquals(ast.getType(),NQLParser.SHOW_TABLE);		
		assertEquals(ast.getChild(0).getText(),"abc");
	}
}
