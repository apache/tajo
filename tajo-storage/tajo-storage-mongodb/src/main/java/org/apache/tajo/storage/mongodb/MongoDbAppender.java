package org.apache.tajo.storage.mongodb;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.util.List;

/**
 * Created by janaka on 5/21/16.
 */
public class MongoDbAppender implements Appender {
    @Override
    public void init() throws IOException {

    }

    @Override
    public void addTuple(Tuple t) throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public long getEstimatedOutputSize() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void enableStats() {

    }

    @Override
    public void enableStats(List<Column> columnList) {

    }

    @Override
    public TableStats getStats() {
        return null;
    }
}
