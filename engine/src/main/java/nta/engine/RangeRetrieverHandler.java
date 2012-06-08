package nta.engine;

import nta.catalog.Schema;
import nta.conf.NtaConf;
import nta.engine.planner.physical.TupleComparator;
import nta.engine.utils.TupleUtil;
import nta.storage.Tuple;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import tajo.index.bst.BSTIndex;
import tajo.worker.dataserver.retriever.FileChunk;
import tajo.worker.dataserver.retriever.RetrieverHandler;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

/**
 * @author Hyunsik Choi
 *
 * It retrieves the file chunk ranged between start and end keys.
 * The start key is inclusive, but the end key is exclusive.
 */
public class RangeRetrieverHandler implements RetrieverHandler {
  private final File file;
  private final BSTIndex.BSTIndexReader idxReader;
  private final Schema schema;

  public RangeRetrieverHandler(File outDir, Schema schema, TupleComparator comp) throws IOException {
    this.file = outDir;
    BSTIndex index = new BSTIndex(NtaConf.create());
    this.schema = schema;
    this.idxReader =
        index.getIndexReader(new Path(outDir.getAbsolutePath(), "index/data.idx"), this.schema, comp);
    this.idxReader.open();
  }

  @Override
  public FileChunk get(Map<String, String> kvs) throws IOException {
    // nothing to verify the file because AdvancedDataRetriever checks
    // its validity of the file.
    File data = new File(this.file, "data/data");
    byte [] startBytes = Base64.decodeBase64(URLDecoder.decode(kvs.get("start"), "UTF-8"));
    Tuple start = TupleUtil.toTuple(schema, startBytes);
    byte [] endBytes = Base64.decodeBase64(URLDecoder.decode(kvs.get("end"), "UTF-8"));
    Tuple end = TupleUtil.toTuple(schema, endBytes);
    long startOffset = idxReader.find(start);
    long endOffset = idxReader.find(end, true);

    if (endOffset != -1) {
      return new FileChunk(data, startOffset, endOffset - startOffset);
    } else {
      return new FileChunk(data, startOffset, -1);
    }
  }
}
