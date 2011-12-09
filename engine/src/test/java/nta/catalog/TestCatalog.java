package nta.catalog;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;

import nta.catalog.Catalog;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.executor.eval.Expr;
import nta.engine.executor.eval.FieldExpr;
import nta.engine.function.FuncType;
import nta.engine.function.Function;
import nta.storage.StoreManager;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestCatalog {
	NtaConf conf;
	Catalog cat;
	StoreManager manager;
	
	static final String FieldName1="f1";
	static final String FieldName2="f2";
	static final String FieldName3="f3";	
	
	static final DataType Type1 = DataType.BYTE;
	static final DataType Type2 = DataType.INT;
	static final DataType Type3 = DataType.LONG;
	
	static final int Len2 = 10;
	static final int Len3 = 12;
	
	Column field1;
	Column field2;
	Column field3;	
	
	static final String RelName1="rel1";
	static final String RelName2="rel2";
	
	TableMeta rel1;
	TableMeta rel2;
	
	int fid1;
	int fid2;
	int fid3;
	
	int rid1;
	int rid2;
	
	Schema schema1;
	Schema schema2;
	
	public TestCatalog() throws IOException, URISyntaxException {
		
	}
	
	@Before
	public void setUp() throws IOException {
		conf = new NtaConf();
		cat = new Catalog(conf);
	}
	
	@Test
	public void testGetTable() throws IOException {		
		schema1 = new Schema();
		fid1 = schema1.addColumn(FieldName1, DataType.BYTE);
		fid2 = schema1.addColumn(FieldName2, DataType.INT);
		fid3 = schema1.addColumn(FieldName3, DataType.LONG);
		
		TableMeta meta = new TableMeta("table1");
		meta.setSchema(schema1);
		meta.setStorageType(StoreType.MEM);
		meta.setTableType(TableType.BASETABLE);
		
		assertFalse(cat.existsTable("table1"));
		cat.addTable(meta);
		assertTrue(cat.existsTable("table1"));		
		cat.deleteTable("table1");
		assertFalse(cat.existsTable("table1"));		
	}

/*
	@Test
	public final void testGetRelationString() throws NoSuchTableException {
		assertEquals(catalog.getTableInfo(RelName1).getRelId(),rid1);
		assertEquals(catalog.getTableInfo(RelName2).getRelId(),rid2);
	}

	@Test
	public final void testAddRelation() throws IOException {
		int rid;
		
		Schema s = new Schema();
		s.addField(new Column("age",ColumnType.INT));
		rid = catalog.addRelation("TestCatalog",s, RelationType.BASETABLE, 0, "TestCatalog");
		
		assertEquals(rid,catalog.getTableInfo(rid).getRelId());
		assertEquals("TestCatalog",catalog.getTableInfo(rid).getName());
	}

	@Test
	public final void testDelRelation() throws NoSuchTableException {
		assertNotNull(catalog.getTableInfo(RelName2));
		catalog.deleteRelation(RelName2);
//		assertNull(catalog.getRelation(RelName2));
	}
*/
	
	public static class TestFunc1 extends Function {
		public TestFunc1() {
			super(					
					new Column [] {
							new Column(1, "name", DataType.INT)
					},
					new Expr [] {
							new FieldExpr(DataType.INT, 0, 0)	
					}
			);
		}

		@Override
		public Datum invoke(Datum... datums) {
			return DatumFactory.create(1);
		}

		@Override
		public DataType getResType() {
			return DataType.INT;
		}
	}	

	@Test
	public final void testRegisterFunc() {		
		assertFalse(cat.containFunction("test"));
		FunctionMeta meta = new FunctionMeta("test", TestFunc1.class, FuncType.GENERAL);
		cat.registerFunction(meta);
		assertTrue(cat.containFunction("test"));
		FunctionMeta retrived = cat.getFunctionMeta("test");
		assertEquals(retrived.getName(),"test");
		assertEquals(retrived.getFunctionClass(),TestFunc1.class);
		assertEquals(retrived.getType(),FuncType.GENERAL);
	}

	@Test
	public final void testUnregisterFunc() {
		assertFalse(cat.containFunction("test"));
		FunctionMeta meta = new FunctionMeta("test", TestFunc1.class, FuncType.GENERAL);
		cat.registerFunction(meta);
		assertTrue(cat.containFunction("test"));
		cat.unregisterFunction("test");
		assertFalse(cat.containFunction("test"));
	}
}
