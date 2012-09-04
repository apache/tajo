package tajo.engine.planner.physical;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.SubqueryContext;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos;
import tajo.conf.TajoConf;
import tajo.engine.parser.QueryBlock;
import tajo.index.bst.BSTIndex;
import tajo.storage.*;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 *
 */
public class IndexedStoreExec extends PhysicalExec {
  private final SubqueryContext ctx;
  private final StorageManager sm;
  private final PhysicalExec subOp;
  private final Schema inSchema;
  private final Schema outSchema;
  private final QueryBlock.SortSpec [] sortSpecs;
  private int [] indexKeys = null;
  private Schema keySchema;

  private BSTIndex.BSTIndexWriter indexWriter;
  private TupleComparator comp;
  private FileAppender appender;
  private TableMeta meta;

  public IndexedStoreExec(SubqueryContext ctx, StorageManager sm, PhysicalExec subOp, Schema inSchema,
                          Schema outSchema, QueryBlock.SortSpec [] sortSpecs) throws IOException {
    this.ctx = ctx;
    this.sm = sm;
    this.subOp = subOp;
    this.inSchema = inSchema;
    this.outSchema = outSchema;
    this.sortSpecs = sortSpecs;
    open();
  }

  public void open() throws IOException {
    indexKeys = new int[sortSpecs.length];
    keySchema = new Schema();
    Column col;
    for (int i = 0 ; i < sortSpecs.length; i++) {
      col = sortSpecs[i].getSortKey();
      indexKeys[i] = inSchema.getColumnId(col.getQualifiedName());
      keySchema.addColumn(inSchema.getColumn(col.getQualifiedName()));
    }

    QueryBlock.SortSpec [] indexSortSpec = new QueryBlock.SortSpec[keySchema.getColumnNum()];
    for (int i = 0; i < indexSortSpec.length; i++) {
      indexSortSpec[i] = new QueryBlock.SortSpec(keySchema.getColumn(0));
    }

    BSTIndex bst = new BSTIndex(new TajoConf());
    this.comp = new TupleComparator(keySchema, indexSortSpec);
    Path storeTablePath = new Path(ctx.getWorkDir().getAbsolutePath(), "out");
    this.meta = TCatUtil
        .newTableMeta(this.outSchema, CatalogProtos.StoreType.CSV);
    sm.initLocalTableBase(storeTablePath, meta);
    this.appender = (FileAppender) sm.getLocalAppender(meta, new Path(storeTablePath, "data/data"));

    Path indexDir = new Path(storeTablePath, "index");
    FileSystem fs = sm.getFileSystem();
    fs.mkdirs(indexDir);
    this.indexWriter = bst.getIndexWriter(new Path(indexDir, "data.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    this.indexWriter.setLoadNum(100);
    this.indexWriter.open();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    Tuple keyTuple;
    long offset;


    while((tuple = subOp.next()) != null) {
      offset = appender.getOffset();
      appender.addTuple(tuple);
      keyTuple = new VTuple(keySchema.getColumnNum());
      RowStoreUtil.project(tuple, keyTuple, indexKeys);
      indexWriter.write(keyTuple, offset);
    }

    appender.flush();
    appender.close();
    indexWriter.flush();
    indexWriter.close();

    // Collect statistics data
    //    ctx.addStatSet(annotation.getType().toString(), appender.getStats());
    ctx.setResultStats(appender.getStats());
    ctx.addRepartition(0, ctx.getQueryId().toString());

    return null;
  }

  @Override
  public void rescan() throws IOException {
  }

  @Override
  public Schema getSchema() {
    return outSchema;
  }
}
