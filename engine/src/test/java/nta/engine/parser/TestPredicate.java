package nta.engine.parser;

import static org.junit.Assert.*;

import nta.catalog.FieldName;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestPredicate {
	static String [] exprs = {
			"1 + 2", // 0
			"3 - 4", // 1
			"5 * 6", // 2
			"7 / 8", // 3
			"10 % 2", // 4
			"1 * 2 > 3 / 4", // 5
			"1 * 2 < 3 / 4", // 6
			"1 * 2 = 3 / 4", // 7
			"1 * 2 != 3 / 4", // 8
			"1 * 2 <> 3 / 4", // 9
			"(3, 4)", // 10
			"('male', 'female')", // 11
			"gender in ('male', 'female')", // 12
			"gender not in ('male', 'female')", // 13
			"score > 90 and age < 20", // 14
			"score > 90 and age < 20 and name != 'hyunsik'", // 15
			"score > 90 or age < 20", // 16
			"score > 90 or age < 20 and name != 'hyunsik'", // 17
			"((a+3 > 1) or 1=1) and (3 != (abc + 4) and type in (3,4))", // 18
			"3", // 19
			"1.2", // 20
			"sum(age)", // 21
			"date()" // 22
	};
	
	public static NQLParser parseExpr(String expr) {
		ANTLRStringStream input = new ANTLRStringStream(expr);
		NQLLexer lexer = new NQLLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		NQLParser parser = new NQLParser(tokens);		
		return parser;
	}
	
	@Test
	public void testExpr() throws RecognitionException {
		NQLParser p = parseExpr(exprs[0]);
		CommonTree node = (CommonTree) p.expr().getTree();
		
		assertEquals(node.getText(), "+");
		assertEquals(node.getChild(0).getText(), "1");
		assertEquals(node.getChild(1).getText(), "2");
		
		p = parseExpr(exprs[1]);
		node = (CommonTree) p.expr().getTree();
		assertEquals(node.getText(), "-");
		assertEquals(node.getChild(0).getText(), "3");
		assertEquals(node.getChild(1).getText(), "4");
		
		p = parseExpr(exprs[2]);
		node = (CommonTree) p.expr().getTree();
		assertEquals(node.getText(), "*");
		assertEquals(node.getChild(0).getText(), "5");
		assertEquals(node.getChild(1).getText(), "6");
		
		p = parseExpr(exprs[3]);
		node = (CommonTree) p.expr().getTree();
		assertEquals(node.getText(), "/");
		assertEquals(node.getChild(0).getText(), "7");
		assertEquals(node.getChild(1).getText(), "8");
		
		p = parseExpr(exprs[4]);
		node = (CommonTree) p.expr().getTree();
		assertEquals(node.getText(), "%");
		assertEquals(node.getChild(0).getText(), "10");
		assertEquals(node.getChild(1).getText(), "2");
	}
	
	@Test
	public void testComparison() throws RecognitionException {
		NQLParser p = parseExpr(exprs[5]);
		CommonTree node = (CommonTree) p.comparison_predicate().getTree();
		assertEquals(node.getText(), ">");
		assertEquals("*", node.getChild(0).getText());
		assertEquals("/", node.getChild(1).getText());
		
		p = parseExpr(exprs[6]);
		node = (CommonTree) p.comparison_predicate().getTree();
		assertEquals(node.getText(), "<");
		assertEquals(node.getChild(0).getText(), "*");
		assertEquals(node.getChild(1).getText(), "/");
		
		p = parseExpr(exprs[7]);
		node = (CommonTree) p.comparison_predicate().getTree();
		assertEquals(node.getText(), "=");
		assertEquals(node.getChild(0).getText(), "*");
		assertEquals(node.getChild(1).getText(), "/");
		
		p = parseExpr(exprs[8]);
		node = (CommonTree) p.comparison_predicate().getTree();
		assertEquals(node.getText(), "!=");
		assertEquals(node.getChild(0).getText(), "*");
		assertEquals(node.getChild(1).getText(), "/");
		
		p = parseExpr(exprs[9]);
		node = (CommonTree) p.comparison_predicate().getTree();
		assertEquals(node.getText(), "<>");
		assertEquals(node.getChild(0).getText(), "*");
		assertEquals(node.getChild(1).getText(), "/");
	}
	
	@Test
	public void testAtomArray() throws RecognitionException {
		NQLParser p = parseExpr(exprs[10]);
		CommonTree node = (CommonTree) p.array().getTree();		
		assertEquals(node.getChild(0).getText(), "3");
		assertEquals(node.getChild(1).getText(), "4");
		
		p = parseExpr(exprs[11]);
		node = (CommonTree) p.array().getTree();		
		assertEquals(node.getChild(0).getText(), "'male'");
		assertEquals(node.getChild(1).getText(), "'female'");
	}
	
	@Test
	public void testIn() throws RecognitionException {
		NQLParser p = parseExpr(exprs[12]);
		CommonTree node = (CommonTree) p.in_predicate().getTree();
		assertEquals(node.getType(), NQLParser.IN);
		assertEquals(node.getChild(1).getText(), "'male'");
		assertEquals(node.getChild(2).getText(), "'female'");
		
		p = parseExpr(exprs[13]);
		node = (CommonTree) p.in_predicate().getTree();		
		assertEquals(node.getType(), NQLParser.IN);
		assertEquals(node.getChild(0).getType(), NQLParser.FIELD_NAME);
		FieldName fieldName = new FieldName(node.getChild(0));
		assertEquals(fieldName.getName(), "gender");
		assertEquals(node.getChild(1).getText(), "'male'");
		assertEquals(node.getChild(2).getText(), "'female'");
		assertEquals(node.getChild(3).getType(), NQLParser.NOT);
	}
	
	@Test
	public void testAnd() throws RecognitionException {
		NQLParser p = parseExpr(exprs[14]);
		CommonTree node = (CommonTree) p.and_predicate().getTree();
		assertEquals(node.getText(), "and");
		assertEquals(node.getChild(0).getText(), ">");
		assertEquals(node.getChild(1).getText(), "<");
		
		p = parseExpr(exprs[15]);
		node = (CommonTree) p.and_predicate().getTree();
		assertEquals(node.getText(), "and");
		assertEquals(node.getChild(0).getText(), "and");
		assertEquals(node.getChild(1).getText(), "!=");
	}
	
	@Test
	public void testOr() throws RecognitionException {
		NQLParser p = parseExpr(exprs[16]);
		CommonTree node = (CommonTree) p.bool_expr().getTree();
		assertEquals(node.getText(), "or");
		assertEquals(node.getChild(0).getText(), ">");
		assertEquals(node.getChild(1).getText(), "<");
		
		p = parseExpr(exprs[17]);
		node = (CommonTree) p.bool_expr().getTree();
		assertEquals(node.getText(), "or");
		assertEquals(node.getChild(0).getText(), ">");
		assertEquals(node.getChild(1).getText(), "and");
	}
	
	@Test
	public void testComplex() throws RecognitionException {
		NQLParser p = parseExpr(exprs[18]);
		CommonTree node = (CommonTree) p.search_condition().getTree();
		assertEquals(node.getText(), "and");
		assertEquals(node.getChild(0).getText(), "or");
		assertEquals(node.getChild(1).getText(), "and");
	}
	
	@Test
	public void testFuncCall() throws RecognitionException {
		NQLParser p = parseExpr(exprs[21]);
		CommonTree node = (CommonTree) p.search_condition().getTree();
		System.out.println(node.toStringTree());
		assertEquals(node.getType(), NQLParser.FUNCTION);
		assertEquals(node.getText(), "sum");
		assertEquals(node.getChild(0).getType(), NQLParser.FIELD_NAME);
		FieldName fieldName = new FieldName(node.getChild(0));
		assertEquals(fieldName.getName(), "age");
		
		p = parseExpr(exprs[22]);
		node = (CommonTree) p.search_condition().getTree();
		assertEquals(node.getType(), NQLParser.FUNCTION);
		assertEquals(node.getText(), "date");
		assertNull(node.getChild(1));
	}
}
