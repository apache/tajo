package tajo.engine.planner.physical;

import tajo.SchemaObject;
import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.storage.Tuple;

import java.io.IOException;

public abstract class PhysicalExec implements SchemaObject {
  protected final SubqueryContext context;
  protected final Schema inSchema;
  protected final Schema outSchema;

  public PhysicalExec(final SubqueryContext context, final Schema inSchema,
                      final Schema outSchema) {
    this.context = context;
    this.inSchema = inSchema;
    this.outSchema = outSchema;
  }

  public final Schema getSchema() {
    return outSchema;
  }

  public abstract void init() throws IOException;

  public abstract Tuple next() throws IOException;

  public abstract void rescan() throws IOException;

  public abstract void close() throws IOException;
}
