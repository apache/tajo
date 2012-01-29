/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;

import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.planner.logical.StoreTableNode;
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
  private final StoreTableNode annotation;
  private final PhysicalExec subOp;
  private final Appender appender;
  
  private final Tuple tuple = null;
  private final Schema inputSchema;
  private final Schema outputSchema;
  
  /**
   * @throws IOException 
   * 
   */
  public StoreTableExec(StorageManager sm, int queryId,
      StoreTableNode annotation, PhysicalExec subOp) throws IOException {
    this.annotation = annotation;
    this.subOp = subOp;
    this.inputSchema = annotation.getInputSchema().toSchema();
    this.outputSchema = annotation.getOutputSchema().toSchema();
    
    TableMeta meta = new TableMetaImpl(this.outputSchema, StoreType.CSV);
    sm.initTableBase(meta, annotation.getTableName());
    this.appender = sm.getAppender(meta,annotation.getTableName(),
        annotation.getTableName()+"_"+queryId);
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
    Tuple tuple = null;
    while((tuple = subOp.next()) != null) {
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
    
    return null;
  }
}
