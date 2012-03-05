package tajo.index;

import java.io.IOException;

import nta.catalog.Schema;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.physical.TupleComparator;

/**
 * @author Hyunsik Choi
 */
public interface IndexMethod {
  IndexWriter getIndexWriter(int level, Schema keySchema,
      TupleComparator comparator) throws IOException;
  IndexReader getIndexReader(Fragment tablets, Schema keySchema,
      TupleComparator comparator) throws IOException;
}
