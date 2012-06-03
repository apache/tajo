package tajo.index;

import nta.catalog.Schema;
import nta.engine.planner.physical.TupleComparator;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public interface IndexMethod {
  IndexWriter getIndexWriter(final Path fileName, int level, Schema keySchema,
      TupleComparator comparator) throws IOException;
  IndexReader getIndexReader(final Path fileName, Schema keySchema,
      TupleComparator comparator) throws IOException;
}
