/**
 * 
 */
package nta.engine.parser;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import nta.catalog.Catalog;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumType;
import nta.engine.exception.NTAQueryException;
import nta.engine.executor.eval.Expr;
import nta.engine.executor.eval.ExprType;
import nta.engine.parser.NQL.Query;
import nta.storage.CSVFile;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 *
 */
public class TestQueryStmt {
	NtaConf conf;
	Catalog cat = null;
	NQL nql = null;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		this.cat = new Catalog(conf);		
		nql = new NQL(cat);
		
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("age", DataType.INT);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMetaImpl();		
		meta.setSchema(schema);
		meta.setStorageType(StoreType.CSV);
		meta.putOption(CSVFile.DELIMITER, ",");		
		
		TableDesc desc = new TableDescImpl("test", meta);
		desc.setURI(URI.create("/table/test"));
		cat.addTable(desc);
	}
	
	String [] queries = {
			// From
			"select 2+3", // 0
			"select * from test", // 1
			"select * from test as t1", // 2
			
			// Where
			"select * from test where age > 30", // 3
			
			// GroupBy
			"select * from test group by id,age having print(age) > 30", // 4
			
			// CREATE TABLE
			"create table rawtable (id int, age name) using raw" // 5
	};
	
	@Test
	public void testFromClause() throws NTAQueryException {		
		Query stmt = null;
		
		stmt = nql.parse(queries[0]);		
		assertFalse(stmt.hasFromClause);
		
		stmt = nql.parse(queries[1]);		
		assertTrue(stmt.hasFromClause);
		
		assertEquals(1, stmt.numBaseRels);
		assertEquals("test", stmt.getBaseRelNames().get(0));
		assertTrue(cat.getTableId("test") == stmt.getBaseRel("test").getRelation().getId());
		
		stmt = nql.parse(queries[2]);
		assertEquals(1, stmt.numBaseRels);
		assertEquals("t1", stmt.getBaseRelNames().get(0));
		assertTrue(cat.getTableId("test") == stmt.getBaseRel("t1").getRelation().getId());
	}
	
	@Test
	public void testWhereClause() throws NTAQueryException {
		Query stmt = null;
		
		stmt = nql.parse(queries[3]);
		assertTrue(stmt.hasWhereClause);
		assertEquals(stmt.whereCond.getType(), ExprType.GTH);
		System.out.println(stmt);
	}
	
	@Test
	public void testProjectList() throws NTAQueryException {
		Query stmt = null;
		stmt = nql.parse(queries[0]);
		assertEquals(stmt.getTargetList().length,1);
	}
	
	@Test
	public void testGroupByClause() throws NTAQueryException {
		Query stmt = null;
		stmt = nql.parse(queries[4]);
		System.out.println(stmt);
	}
	
	@Test
	public void testbuildExpr() throws NTAQueryException {
		Query stmt = null;
		stmt = nql.parse(queries[0]);
		assertEquals(stmt.getTargetList().length,1);
		Expr expr = stmt.getTargetList()[0].expr;
		assertEquals(expr.getType(), ExprType.PLUS);
		assertEquals(expr.getLeftExpr().getType(), ExprType.CONST);
		assertEquals(expr.getLeftExpr().eval(null).type(), DatumType.INT);
		assertEquals(expr.getLeftExpr().eval(null).asInt(), 2);
		assertEquals(expr.getRightExpr().getType(), ExprType.CONST);
		assertEquals(expr.getRightExpr().eval(null).type(), DatumType.INT);
		assertEquals(expr.getRightExpr().eval(null).asInt(), 3);		
	}
	
	@Test
	public void testCreateTable() throws NTAQueryException {
		Query stmt = null;
		stmt = nql.parse(queries[5]);
		assertEquals(CommandType.CREATE_TABLE,stmt.getCmdType());
		assertEquals("raw",stmt.getStoreName());
	}
}
