package nta.engine;

import static org.junit.Assert.*;

import java.net.URI;

import nta.catalog.Catalog;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;
import nta.conf.NtaConf;
import nta.engine.exception.NTAQueryException;
import nta.engine.parser.NQL;
import nta.engine.parser.NQL.Query;
import nta.engine.plan.logical.GroupByOp;
import nta.engine.plan.logical.JoinOp;
import nta.engine.plan.logical.LogicalOp;
import nta.engine.plan.logical.LogicalPlan;
import nta.engine.plan.logical.OpType;
import nta.engine.plan.logical.ProjectLO;
import nta.engine.plan.logical.ScanOp;
import nta.engine.plan.logical.SelectionOp;
import nta.engine.query.LogicalPlanner;
import nta.storage.CSVFile;

import org.junit.Before;
import org.junit.Test;

public class TestLogicalPlanner {	
	NtaConf conf;
	Catalog cat;

	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		cat = new Catalog(conf);
		
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("age", DataType.INT);
		schema.addColumn("id", DataType.INT);
		
		TableMeta meta = new TableMetaImpl(schema, StoreType.CSV);		
		meta.putOption(CSVFile.DELIMITER, ",");  
		TableDesc desc = new TableDescImpl("test", meta);
		desc.setURI(URI.create("/table/test"));
		cat.addTable(desc);
	}
	
	String [] SELECTION_TEST = {
			"select * from test",
			"select * from test where id > 10",
			"select * from test as t1, test as t2 where t1.id = t2.id",
			"select * from test as t1, test as t2 where t1.id = t2.id group by t1.age",
			"select t1.age from test as t1, test as t2 where t1.id = t2.id group by t1.age"
	};
	
	public LogicalPlan getLogicalPlan(String query) throws NTAQueryException {
		LogicalPlanner planner = new LogicalPlanner(cat);
		NQL nql = new NQL(cat);		
		
		// "select * from test"
		Query q = nql.parse(query);				
		
		return planner.compile(q);	
	}

	@Test
	public final void testBuildLogicalPlan() throws NTAQueryException {
		LogicalOp inner = null;
		
		// "select * from test"
		inner = getLogicalPlan(SELECTION_TEST[0]).getRoot();		
		assertEquals(inner.getType(),OpType.SCAN);
		
		// "select * from test where id > 10"
		inner = getLogicalPlan(SELECTION_TEST[1]).getRoot();
		assertEquals(inner.getType(),OpType.SELECTION);
	}
	
	@Test
	public void testProduction() throws NTAQueryException {
		// Production Test
		// "select * from test as t1, test as t2 where t1.id = t2.id"
		SelectionOp sOp = (SelectionOp) getLogicalPlan(SELECTION_TEST[2]).getRoot();
		assertEquals(sOp.getType(),OpType.SELECTION);
		
		JoinOp jOp = (JoinOp) sOp.getSubOp();  
		assertEquals(jOp.getType(),OpType.JOIN);
		
		ScanOp outer = (ScanOp) jOp.getOuter();
		ScanOp inner = (ScanOp) jOp.getInner();		
		assertEquals(inner.getType(),OpType.SCAN);
		assertEquals(outer.getType(),OpType.SCAN);
	}
	
	@Test
	public void testGroupBy() throws NTAQueryException {		
		// GroupBy
		// "select * from test as t1, test as t2 where t1.id = t2.id group by age"
		GroupByOp gOp = (GroupByOp) getLogicalPlan(SELECTION_TEST[3]).getRoot();
		assertEquals(gOp.getType(),OpType.GROUP_BY);
		
		SelectionOp sOp = (SelectionOp) gOp.getSubOp();
		assertEquals(sOp.getType(),OpType.SELECTION);
		
		JoinOp jOp = (JoinOp) sOp.getSubOp();  
		assertEquals(jOp.getType(),OpType.JOIN);
		
		ScanOp outer = (ScanOp) jOp.getOuter();
		ScanOp inner = (ScanOp) jOp.getInner();		
		assertEquals(inner.getType(),OpType.SCAN);
		assertEquals(outer.getType(),OpType.SCAN);
	}
	
	@Test
	public void testProject() throws NTAQueryException {
		// GroupBy
		// "select age from test as t1, test as t2 where t1.id = t2.id group by age"
		ProjectLO pOp = (ProjectLO) getLogicalPlan(SELECTION_TEST[4]).getRoot();
		assertEquals(pOp.getType(),OpType.PROJECTION);		
		
		GroupByOp gOp = (GroupByOp) pOp.getSubOp();
		assertEquals(gOp.getType(),OpType.GROUP_BY);
		
		SelectionOp sOp = (SelectionOp) gOp.getSubOp();
		assertEquals(sOp.getType(),OpType.SELECTION);
		
		JoinOp jOp = (JoinOp) sOp.getSubOp();  
		assertEquals(jOp.getType(),OpType.JOIN);
		
		ScanOp outer = (ScanOp) jOp.getOuter();
		ScanOp inner = (ScanOp) jOp.getInner();		
		assertEquals(inner.getType(),OpType.SCAN);
		assertEquals(outer.getType(),OpType.SCAN);
	}
}
