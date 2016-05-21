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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.*;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.CreateTableNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRuleContext;
import org.apache.tajo.plan.verifier.SyntaxErrorUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.Pair;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class MongoDbTableSpace extends Tablespace {


    public MongoDbTableSpace(String name, URI uri, JSONObject config) {
        super(name, uri, config);
    }

    @Override
    protected void storageInit() throws IOException {

    }

    @Override
    public long getTableVolume(TableDesc table, Optional<EvalNode> filter) throws UnsupportedException {
        return 0;
    }

    @Override
    public URI getTableUri(String databaseName, String tableName) {
        return null;
    }

    @Override
    public List<Fragment> getSplits(String inputSourceId, TableDesc tableDesc, @Nullable EvalNode filterCondition) throws IOException, TajoException {
        return null;
    }

    @Override
    public StorageProperty getProperty() {
        return null;
    }

    @Override
    public FormatProperty getFormatProperty(TableMeta meta) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
        return new TupleRange[0];
    }

    @Override
    public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {

    }

    @Override
    public void createTable(TableDesc tableDesc, boolean ifNotExists) throws TajoException, IOException {

    }

    @Override
    public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {

    }

    @Override
    public void prepareTable(LogicalNode node) throws IOException, TajoException {

    }

    @Override
    public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId, LogicalPlan plan, Schema schema, TableDesc tableDesc) throws IOException {
        return null;
    }

    @Override
    public void rollbackTable(LogicalNode node) throws IOException, TajoException {

    }

    @Override
    public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
        return null;
    }
}
