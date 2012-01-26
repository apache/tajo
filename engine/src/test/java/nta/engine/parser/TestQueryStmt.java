/**
 * 
 */
package nta.engine.parser;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import nta.catalog.CatalogServer;
import nta.catalog.Schema;
import nta.catalog.TableDesc;
import nta.catalog.TableDescImpl;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.conf.NtaConf;
import nta.datum.DatumType;
import nta.engine.exception.NTAQueryException;
import nta.engine.exec.eval.EvalNode;
import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.parser.NQL.Query;
import nta.storage.CSVFile2;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Hyunsik Choi
 *
 */
@Deprecated
public class TestQueryStmt {
  NtaConf conf;
  CatalogServer cat = null;
  NQL nql = null;
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    conf = new NtaConf();
    this.cat = new CatalogServer(conf);   
    nql = new NQL(cat);
    
    Schema schema = new Schema();
    schema.addColumn("name", DataType.STRING);
    schema.addColumn("age", DataType.INT);
    schema.addColumn("id", DataType.INT);
    
    TableMeta meta = new TableMetaImpl();   
    meta.setSchema(schema);
    meta.setStorageType(StoreType.CSV);
    meta.putOption(CSVFile2.DELIMITER, ",");    
    
    TableDesc desc = new TableDescImpl("test", meta);
    desc.setPath(new Path("/table/test"));
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
    assertTrue(cat.getTableDesc("test") .getId()== stmt.getBaseRel("test").getRelation().getId());
    
    stmt = nql.parse(queries[2]);
    assertEquals(1, stmt.numBaseRels);
    assertEquals("t1", stmt.getBaseRelNames().get(0));
    assertTrue(cat.getTableDesc("test").getId() == stmt.getBaseRel("t1").getRelation().getId());
  }
  
  @Test
  public void testWhereClause() throws NTAQueryException {
    Query stmt = null;
    
    stmt = nql.parse(queries[3]);
    assertTrue(stmt.hasWhereClause);
    assertEquals(stmt.whereCond.getType(), Type.GTH);
    System.out.println(stmt);
  }
  
  @Test
  public void testProjectList() throws NTAQueryException {
    Query stmt = null;
    stmt = nql.parse(queries[0]);
    assertEquals(stmt.getTargetList().length,1);
  }
  
  @Test
  public void testbuildExpr() throws NTAQueryException {
    Query stmt = null;
    stmt = nql.parse(queries[0]);
    assertEquals(stmt.getTargetList().length,1);
    EvalNode expr = stmt.getTargetList()[0].expr;
    assertEquals(expr.getType(), Type.PLUS);
    assertEquals(expr.getLeftExpr().getType(), Type.CONST);
    assertEquals(expr.getLeftExpr().eval(null, null).type(), DatumType.INT);
    assertEquals(expr.getLeftExpr().eval(null, null).asInt(), 2);
    assertEquals(expr.getRightExpr().getType(), Type.CONST);
    assertEquals(expr.getRightExpr().eval(null, null).type(), DatumType.INT);
    assertEquals(expr.getRightExpr().eval(null, null).asInt(), 3);    
  }
  
  @Test
  public void testCreateTable() throws NTAQueryException {
    Query stmt = null;
    stmt = nql.parse(queries[5]);
    assertEquals(StatementType.CREATE_TABLE,stmt.getCmdType());
    assertEquals("raw",stmt.getStoreName());
  }
}