package nta.engine.planner.physical;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos;
import nta.conf.NtaConf;
import nta.engine.SubqueryContext;
import nta.engine.parser.QueryBlock;
import nta.engine.utils.TupleUtil;
import nta.storage.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.index.bst.BSTIndex;

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
                          Schema outSchema, QueryBlock.SortSpec [] sortSpecs) {
    this.ctx = ctx;
    this.sm = sm;
    this.subOp = subOp;
    this.inSchema = inSchema;
    this.outSchema = outSchema;
    this.sortSpecs = sortSpecs;
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

    BSTIndex bst = new BSTIndex(NtaConf.create());
    this.comp = new TupleComparator(keySchema, indexSortSpec);
    Path storeTablePath = new Path(ctx.getWorkDir().getAbsolutePath(), "out");
    this.meta = TCatUtil.newTableMeta(this.outSchema, CatalogProtos.StoreType.RAW);
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
      TupleUtil.project(tuple, keyTuple, indexKeys);
      indexWriter.write(keyTuple, offset);
    }

    appender.flush();
    appender.close();
    indexWriter.flush();
    indexWriter.close();

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
