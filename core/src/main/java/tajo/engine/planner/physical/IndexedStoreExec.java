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
public class IndexedStoreExec extends UnaryPhysicalExec {
  private final StorageManager sm;
  private final QueryBlock.SortSpec [] sortSpecs;
  private int [] indexKeys = null;
  private Schema keySchema;

  private BSTIndex.BSTIndexWriter indexWriter;
  private TupleComparator comp;
  private FileAppender appender;
  private TableMeta meta;

  public IndexedStoreExec(final SubqueryContext context, final StorageManager sm,
      final PhysicalExec child, final Schema inSchema, final Schema outSchema,
      final QueryBlock.SortSpec [] sortSpecs) throws IOException {
    super(context, inSchema, outSchema, child);
    this.sm = sm;
    this.sortSpecs = sortSpecs;
  }

  public void init() throws IOException {
    super.init();

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
    Path storeTablePath = new Path(context.getWorkDir().getAbsolutePath(), "out");
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


    while((tuple = child.next()) != null) {
      offset = appender.getOffset();
      appender.addTuple(tuple);
      keyTuple = new VTuple(keySchema.getColumnNum());
      RowStoreUtil.project(tuple, keyTuple, indexKeys);
      indexWriter.write(keyTuple, offset);
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
  }

  public void close() throws IOException {
    super.close();

    appender.flush();
    appender.close();
    indexWriter.flush();
    indexWriter.close();

    // Collect statistics data
    context.setResultStats(appender.getStats());
    context.addRepartition(0, context.getQueryId().toString());
  }
}
