/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tajo.storage.mongodb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptId;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created by janaka on 5/21/16.
 */
public class MongoDBAppender implements Appender {

    //Given at constructor
    private final Configuration conf;
    private final Schema schema;
    private final TableMeta meta;
    private final Path stagingDir;
    private final TaskAttemptId taskAttemptId;
    private final URI uri;


    protected boolean inited = false;
    private boolean[] columnStatsEnabled;
    private boolean tableStatsEnabled;

    private MongoDBCollectionWriter mongoDBCollectionWriter;



    public MongoDBAppender(Configuration conf, TaskAttemptId taskAttemptId,
                           Schema schema, TableMeta meta, Path stagingDir, URI uri) {
        this.conf = conf;
        this.schema = schema;
        this.meta = meta;
        this.stagingDir = stagingDir;
        this.taskAttemptId = taskAttemptId;
        this.uri = stagingDir.toUri();
    }

    @Override
    public void init() throws IOException {
        if (inited) {
            throw new IllegalStateException("FileAppender is already initialized.");
        }
        inited = true;
        MongoDBDocumentSerializer md = new MongoDBDocumentSerializer(schema,meta);
        mongoDBCollectionWriter = new MongoDBCollectionWriter(ConnectionInfo.fromURI(stagingDir.toString()),md);
        mongoDBCollectionWriter.init();
    }

    @Override
    public void addTuple(Tuple t) throws IOException {
        mongoDBCollectionWriter.writeTuple(t);
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public long getEstimatedOutputSize() throws IOException {
        throw new IOException(new NotImplementedException());
    }

    @Override
    public void close() throws IOException {
        mongoDBCollectionWriter.close();
    }

    @Override
    public void enableStats() {
        if (inited) {
            throw new IllegalStateException("Should enable this option before init()");
        }

        this.tableStatsEnabled = true;
        this.columnStatsEnabled = new boolean[schema.size()];

    }

    @Override
    public void enableStats(List<Column> columnList) {

    }

    @Override
    public TableStats getStats() {
        TableStats stats = new TableStats();
        return stats;
    }
}
