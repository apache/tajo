package nta.engine;

import nta.catalog.Schema;
import nta.conf.NtaConf;
import nta.engine.planner.physical.TupleComparator;
import nta.engine.utils.TupleUtil;
import nta.storage.Tuple;
import nta.storage.TupleRange;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    BSTIndex index = new BSTIndex(NtaConf.create());
    this.schema = schema;
    this.comp = comp;

    this.idxReader =
        index.getIndexReader(new Path(outDir.getAbsolutePath(), "index/data.idx"), this.schema, this.comp);
    this.idxReader.open();
  }

  @Override
  public FileChunk get(Map<String, String> kvs) throws IOException {
    // nothing to verify the file because AdvancedDataRetriever checks
    // its validity of the file.
    File data = new File(this.file, "data/data");
    byte [] startBytes = Base64.decodeBase64(URLDecoder.decode(kvs.get("start"), "UTF-8"));
    Tuple start = TupleUtil.toTuple(schema, startBytes);
    byte [] endBytes = null;
    Tuple end = null;
    endBytes = Base64.decodeBase64(URLDecoder.decode(kvs.get("end"), "UTF-8"));
    end = TupleUtil.toTuple(schema, endBytes);
    boolean last = kvs.containsKey("final");

    LOG.info("GET Request for " + data.getAbsolutePath() + " (start="+start+", end="+ end +
        (last ? ", last=true" : "") + ")");

    if (idxReader.getMin() == null && idxReader.getMax() == null) { // if # of rows is zero
      LOG.info("There is no contents");
      return null;
    }
    if (comp.compare(end, idxReader.getMin()) < 0 ||
        comp.compare(idxReader.getMax(), start) < 0) {
      LOG.info("Out of score (indexed data [" + idxReader.getMin() + ", " + idxReader.getMax() +
          "], but request start:" + start + ", end: " + end);
      return null;
    }

    long startOffset;
    long endOffset;
    try {
      startOffset = idxReader.find(start);
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getMin() + ", idx max: "
          + idxReader.getMax());
      throw new IOException(ioe);
    }
    try {
      endOffset = idxReader.find(end);
      if (endOffset == -1) {
        endOffset = idxReader.find(end, true);
      }
    } catch (IOException ioe) {
      LOG.error("State Dump (the requested range: "
          + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getMin() + ", idx max: "
          + idxReader.getMax());
      throw new IOException(ioe);
    }

    // if startOffset == -1 then case 2-1 or case 3
    if (startOffset == -1) { // this is a hack
      // if case 2-1 or case 3
      try {
        startOffset = idxReader.find(start, true);
      } catch (IOException ioe) {
        LOG.error("State Dump (the requested range: "
            + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getMin() + ", idx max: "
            + idxReader.getMax());
        throw new IOException(ioe);
      }
    }

    if (startOffset == -1) {
      throw new IllegalStateException("startOffset " + startOffset + " is negative \n" +
          "State Dump (the requested range: "
          + new TupleRange(schema, start, end) + ", idx min: " + idxReader.getMin() + ", idx max: "
          + idxReader.getMax());
    }

    // if greater than indexed values
    if (last || (endOffset == -1 && comp.compare(idxReader.getMax(), end) < 0)) {
      endOffset = data.length();
    }

    return new FileChunk(data, startOffset, endOffset - startOffset);
  }
}
