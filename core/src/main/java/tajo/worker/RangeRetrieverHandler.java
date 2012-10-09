package tajo.worker;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import tajo.catalog.Schema;
import tajo.conf.TajoConf;
import tajo.engine.planner.physical.TupleComparator;
import tajo.index.bst.BSTIndex;
import tajo.storage.RowStoreUtil;
import tajo.storage.Tuple;
import tajo.storage.TupleRange;
import tajo.worker.dataserver.retriever.FileChunk;
import tajo.worker.dataserver.retriever.RetrieverHandler;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Hyunsik Choi
 *
 * It retrieves the file chunk ranged between start and end keys.
 * The start key is inclusive, but the end key is exclusive.
 *
 * Internally, there are four cases:
 * <ul>
 *   <li>out of scope: the index range does not overlapped with the query range.</li>
 *   <li>overlapped: the index range is partially overlapped with the query range. </li>
 *   <li>included: the index range is included in the start and end keys</li>
 *   <li>covered: the index range covers the query range (i.e., start and end keys).</li>
 * </ul>
 */
public class RangeRetrieverHandler implements RetrieverHandler {
  private static final Log LOG = LogFactory.getLog(RangeRetrieverHandler.class);
  private final File file;
  private final BSTIndex.BSTIndexReader idxReader;
  private final Schema schema;
  private final TupleComparator comp;

  public RangeRetrieverHandler(File outDir, Schema schema, TupleComparator comp) throws IOException {
    this.file = outDir;
    BSTIndex index = new BSTIndex(new TajoConf());
    this.schema = schema;
    this.comp = comp;

    this.idxReader =
        index.getIndexReader(new Path(outDir.getAbsolutePath(), "index/data.idx"),
            this.schema, this.comp);
    this.idxReader.open();
    LOG.info("BSTIndex is loaded from disk (" + idxReader.getFirstKey() + ", "
        + idxReader.getLastKey());
  }

  @Override
  public FileChunk get(Map<String, List<String>> kvs) throws IOException {
    // nothing to verify the file because AdvancedDataRetriever checks
    // its validity of the file.
    File data = new File(this.file, "data/data");
    byte [] startBytes = Base64.decodeBase64(kvs.get("start").get(0));
    Tuple start = RowStoreUtil.RowStoreDecoder.toTuple(schema, startBytes);
    byte [] endBytes = null;
    Tuple end = null;
    endBytes = Base64.decodeBase64(kvs.get("end").get(0));
    end = RowStoreUtil.RowStoreDecoder.toTuple(schema, endBytes);
    boolean last = kvs.containsKey("final");

    if(!comp.isAscendingFirstKey()) {
      Tuple tmpKey = start;
      start = end;
      end = tmpKey;
    }

    LOG.info("GET Request for " + data.getAbsolutePath() + " (start="+start+", end="+ end +
        (last ? ", last=true" : "") + ")");

    if (idxReader.getFirstKey() == null && idxReader.getLastKey() == null) { // if # of rows is zero
      LOG.info("There is no contents");
      return null;
    }

    if (comp.compare(end, idxReader.getFirstKey()) < 0 ||
        comp.compare(idxReader.getLastKey(), start) < 0) {
      LOG.info("Out of Scope (indexed data [" + idxReader.getFirstKey() + ", " + idxReader.getLastKey() +
          "], but request start:" + start + ", end: " + end);
      return null;
    }

    long startOffset;
    long endOffset;
    try {
      startOffset = idxReader.find(start);
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
      throw new IOException(ioe);
    }
    try {
      endOffset = idxReader.find(end);
      if (endOffset == -1) {
        endOffset = idxReader.find(end, true);
      }
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
      throw new IOException(ioe);
    }

    // if startOffset == -1 then case 2-1 or case 3
    if (startOffset == -1) { // this is a hack
      // if case 2-1 or case 3
      try {
        startOffset = idxReader.find(start, true);
      } catch (IOException ioe) {
        LOG.error("State Dump (the requested range: "
            + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
            + idxReader.getLastKey());
        throw new IOException(ioe);
      }
    }

    if (startOffset == -1) {
      throw new IllegalStateException("startOffset " + startOffset + " is negative \n" +
          "State Dump (the requested range: "
          + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getFirstKey() + ", idx max: "
          + idxReader.getLastKey());
    }

    // if greater than indexed values
    if (last || (endOffset == -1 && comp.compare(idxReader.getLastKey(), end) < 0)) {
      endOffset = data.length();
    }

    return new FileChunk(data, startOffset, endOffset - startOffset);
  }
}
