/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.SubqueryContext;
import nta.engine.planner.logical.CreateTableNode;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.Tuple;

/**
 * This physical operator stores a relation into a table.
 * 
 * @author Hyunsik Choi
 *
 */
public class StoreTableExec extends PhysicalExec {
  private final SubqueryContext ctx;
  private final CreateTableNode annotation;
  private final PhysicalExec subOp;
  private final Appender appender;
  
  private Tuple tuple;
  @SuppressWarnings("unused")
  private final Schema inputSchema;
  private final Schema outputSchema;
  
  /**
   * @throws IOException 
   * 
   */
  public StoreTableExec(SubqueryContext ctx, StorageManager sm,
      CreateTableNode annotation, PhysicalExec subOp) throws IOException {
    this.ctx = ctx;
    this.annotation = annotation;
    this.subOp = subOp;
    this.inputSchema = this.annotation.getInputSchema();
    this.outputSchema = this.annotation.getOutputSchema();
    
    TableMeta meta = TCatUtil.newTableMeta(this.outputSchema, StoreType.CSV);
    this.appender = sm.getAppender(meta,this.annotation.getTableName(),
        ctx.getQueryId().toString());
  }

  /* (non-Javadoc)
   * @see nta.engine.SchemaObject#getSchema()
   */
  @Override
  public Schema getSchema() {
    return this.outputSchema;
  }

  /* (non-Javadoc)
   * @see nta.engine.planner.physical.PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    while((tuple = subOp.next()) != null) {
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    
    // Collect statistics data
    ctx.addStatSet("StoreTable", appender.getStats());
        
    return null;
  }
}
