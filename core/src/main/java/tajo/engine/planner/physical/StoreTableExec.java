/**
 * 
 */
package tajo.engine.planner.physical;

import org.apache.hadoop.fs.Path;
import tajo.SubqueryContext;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.engine.planner.logical.StoreTableNode;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.StorageUtil;
import tajo.storage.Tuple;

import java.io.IOException;

/**
 * This physical operator stores a relation into a table.
 * 
 * @author Hyunsik Choi
 *
 */
public class StoreTableExec extends UnaryPhysicalExec {
  private final StoreTableNode plan;
  private final StorageManager sm;
  private Appender appender;
  private Tuple tuple;
  
  /**
   * @throws IOException 
   * 
   */
  public StoreTableExec(SubqueryContext context, StorageManager sm,
      StoreTableNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);

    this.plan = plan;
    this.sm = sm;
  }

  public void init() throws IOException {
    super.init();

    TableMeta meta = TCatUtil.newTableMeta(outSchema, StoreType.CSV);
    if (context.isInterQuery()) {
      Path storeTablePath = new Path(context.getWorkDir().getAbsolutePath(), "out");
      sm.initLocalTableBase(storeTablePath, meta);
      this.appender = sm.getLocalAppender(meta,
          StorageUtil.concatPath(storeTablePath, "data", "0"));
    } else {
      this.appender = sm.getAppender(meta,plan.getTableName(),
          context.getQueryId().toString());
    }
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    while((tuple = child.next()) != null) {
      appender.addTuple(tuple);
    }
        
    return null;
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }

  public void close() throws IOException {
    super.close();

    appender.flush();
    appender.close();

    // Collect statistics data
//    ctx.addStatSet(annotation.getType().toString(), appender.getStats());
    context.setResultStats(appender.getStats());
    context.addRepartition(0, context.getQueryId().toString());
  }
}
