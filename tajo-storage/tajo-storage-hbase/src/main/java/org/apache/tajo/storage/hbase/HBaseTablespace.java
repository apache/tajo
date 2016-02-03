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

package org.apache.tajo.storage.hbase;

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

/**
 * Tablespace for HBase table.
 */
public class HBaseTablespace extends Tablespace {
  private static final Log LOG = LogFactory.getLog(HBaseTablespace.class);

  public static final StorageProperty HBASE_STORAGE_PROPERTIES =
      new StorageProperty("hbase", false, true, false, false);
  public static final FormatProperty HFILE_FORMAT_PROPERTIES = new FormatProperty(true, false, true);
  public static final FormatProperty PUT_MODE_PROPERTIES = new FormatProperty(true, true, false);
  public static final List<byte []> EMPTY_START_ROW_KEY = Arrays.asList(new byte [0]);
  public static final List<byte []> EMPTY_END_ROW_KEY   = Arrays.asList(new byte [0]);

  private Configuration hbaseConf;

  private final static SortedInsertRewriter REWRITE_RULE = new SortedInsertRewriter();

  private Map<HConnectionKey, HConnection> connMap = new HashMap<>();

  public HBaseTablespace(String spaceName, URI uri, JSONObject config) {
    super(spaceName, uri, config);
  }

  @Override
  public void storageInit() throws IOException {
    this.hbaseConf = HBaseConfiguration.create(conf);
    String zkQuorum = extractQuorum(uri);
    String [] splits = zkQuorum.split(":");
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, splits[0]);
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, splits[1]);
  }

  public Configuration getHbaseConf() {
    return hbaseConf;
  }

  @Override
  public long getTableVolume(TableDesc table, Optional<EvalNode> filter) {
    long totalVolume;
    try {
      totalVolume = getRawSplits("", table, filter.orElse(null)).stream()
          .map(f -> f.getLength())
          .filter(size -> size > 0) // eliminate unknown sizes (-1)
          .reduce(0L, Long::sum);
    } catch (TajoException e) {
      throw new TajoRuntimeException(e);
    } catch (Throwable ioe) {
      throw new TajoInternalError(ioe);
    }
    return totalVolume;
  }

  @Override
  public void close() {
    synchronized (connMap) {
      for (HConnection eachConn: connMap.values()) {
        try {
          eachConn.close();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws TajoException, IOException {
    createTable(tableDesc.getUri(), tableDesc.getMeta(), tableDesc.getSchema(), tableDesc.isExternal(), ifNotExists);
    TableStats stats = new TableStats();
    stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
    tableDesc.setStats(stats);
  }

  private void createTable(URI uri, TableMeta tableMeta, Schema schema,
                           boolean isExternal, boolean ifNotExists) throws TajoException, IOException {
    String hbaseTableName = tableMeta.getProperty(HBaseStorageConstants.META_TABLE_KEY, "");
    if (hbaseTableName == null || hbaseTableName.trim().isEmpty()) {
      throw new MissingTablePropertyException(HBaseStorageConstants.META_TABLE_KEY, "hbase");
    }
    TableName hTableName = TableName.valueOf(hbaseTableName);

    String mappedColumns = tableMeta.getProperty(HBaseStorageConstants.META_COLUMNS_KEY, "");
    if (mappedColumns != null && mappedColumns.split(",").length > schema.size()) {
      throw new InvalidTablePropertyException(HBaseStorageConstants.META_COLUMNS_KEY,
          "mapping column pairs must be more than number of columns in the schema");
    }

    ColumnMapping columnMapping = new ColumnMapping(schema, tableMeta.getPropertySet());
    int numRowKeys = 0;
    boolean[] isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    for (int i = 0; i < isRowKeyMappings.length; i++) {
      if (isRowKeyMappings[i]) {
        numRowKeys++;
      }
    }
    if (numRowKeys > 1) {
      for (int i = 0; i < isRowKeyMappings.length; i++) {
        if (isRowKeyMappings[i] && schema.getColumn(i).getDataType().getType() != Type.TEXT) {
          throw SyntaxErrorUtil.makeSyntaxError("Key field type should be TEXT type.");
        }
      }
    }

    for (int i = 0; i < isRowKeyMappings.length; i++) {
      if (columnMapping.getIsColumnKeys()[i] && schema.getColumn(i).getDataType().getType() != Type.TEXT) {
        throw SyntaxErrorUtil.makeSyntaxError("Column key field('<cfname>:key:') type should be TEXT type.");
      }
      if (columnMapping.getIsColumnValues()[i] && schema.getColumn(i).getDataType().getType() != Type.TEXT) {
        throw SyntaxErrorUtil.makeSyntaxError("Column value field(('<cfname>:value:') type should be TEXT type.");
      }
    }

    try (HBaseAdmin hAdmin = new HBaseAdmin(getHbaseConf())) {
      if (isExternal) {
        // If tajo table is external table, only check validation.
        if (mappedColumns == null || mappedColumns.isEmpty()) {
          throw new MissingTablePropertyException(HBaseStorageConstants.META_COLUMNS_KEY, hbaseTableName);
        }
        if (!hAdmin.tableExists(hTableName)) {
          throw new UnavailableTableLocationException(hbaseTableName, "the table does not exist");
        }
        HTableDescriptor hTableDescriptor = hAdmin.getTableDescriptor(hTableName);
        Set<String> tableColumnFamilies = new HashSet<>();
        for (HColumnDescriptor eachColumn : hTableDescriptor.getColumnFamilies()) {
          tableColumnFamilies.add(eachColumn.getNameAsString());
        }

        Collection<String> mappingColumnFamilies = columnMapping.getColumnFamilyNames();
        if (mappingColumnFamilies.isEmpty()) {
          throw new MissingTablePropertyException(HBaseStorageConstants.META_COLUMNS_KEY, hbaseTableName);
        }

        for (String eachMappingColumnFamily : mappingColumnFamilies) {
          if (!tableColumnFamilies.contains(eachMappingColumnFamily)) {
            throw new IOException("There is no " + eachMappingColumnFamily + " column family in " + hbaseTableName);
          }
        }
      } else {
        if (hAdmin.tableExists(hbaseTableName)) {
          if (ifNotExists) {
            return;
          } else {
            throw new IOException("HBase table [" + hbaseTableName + "] already exists.");
          }
        }
        // Creating hbase table
        HTableDescriptor hTableDescriptor = parseHTableDescriptor(tableMeta, schema);

        byte[][] splitKeys = getSplitKeys(conf, hbaseTableName, schema, tableMeta);
        if (splitKeys == null) {
          hAdmin.createTable(hTableDescriptor);
        } else {
          hAdmin.createTable(hTableDescriptor, splitKeys);
        }
      }
    }
  }

  /**
   * Returns initial region split keys.
   *
   * @param conf
   * @param schema
   * @param meta
   * @return
   * @throws java.io.IOException
   */
  private byte[][] getSplitKeys(TajoConf conf, String hbaseTableName, Schema schema, TableMeta meta)
      throws MissingTablePropertyException, InvalidTablePropertyException, IOException {

    String splitRowKeys = meta.getProperty(HBaseStorageConstants.META_SPLIT_ROW_KEYS_KEY, "");
    String splitRowKeysFile = meta.getProperty(HBaseStorageConstants.META_SPLIT_ROW_KEYS_FILE_KEY, "");

    if ((splitRowKeys == null || splitRowKeys.isEmpty()) &&
        (splitRowKeysFile == null || splitRowKeysFile.isEmpty())) {
      return null;
    }

    ColumnMapping columnMapping = new ColumnMapping(schema, meta.getPropertySet());
    boolean[] isBinaryColumns = columnMapping.getIsBinaryColumns();
    boolean[] isRowKeys = columnMapping.getIsRowKeyMappings();

    boolean rowkeyBinary = false;
    int numRowKeys = 0;
    Column rowKeyColumn = null;
    for (int i = 0; i < isBinaryColumns.length; i++) {
      if (isBinaryColumns[i] && isRowKeys[i]) {
        rowkeyBinary = true;
      }
      if (isRowKeys[i]) {
        numRowKeys++;
        rowKeyColumn = schema.getColumn(i);
      }
    }

    if (rowkeyBinary && numRowKeys > 1) {
      throw new InvalidTablePropertyException("If rowkey is mapped to multi column and a rowkey is binary, " +
          "Multiple region for creation is not support.", hbaseTableName);
    }

    if (splitRowKeys != null && !splitRowKeys.isEmpty()) {
      String[] splitKeyTokens = splitRowKeys.split(",");
      byte[][] splitKeys = new byte[splitKeyTokens.length][];
      for (int i = 0; i < splitKeyTokens.length; i++) {
        if (numRowKeys == 1 && rowkeyBinary) {
          splitKeys[i] = HBaseBinarySerializerDeserializer.serialize(rowKeyColumn, new TextDatum(splitKeyTokens[i]));
        } else {
          splitKeys[i] = HBaseTextSerializerDeserializer.serialize(rowKeyColumn, new TextDatum(splitKeyTokens[i]));
        }
      }
      return splitKeys;
    }

    if (splitRowKeysFile != null && !splitRowKeysFile.isEmpty()) {
      // If there is many split keys, Tajo allows to define in the file.
      Path path = new Path(splitRowKeysFile);
      FileSystem fs = path.getFileSystem(conf);

      if (!fs.exists(path)) {
        throw new MissingTablePropertyException("hbase.split.rowkeys.file=" + path.toString() + " not exists.",
            hbaseTableName);
      }

      SortedSet<String> splitKeySet = new TreeSet<>();
      BufferedReader reader = null;

      try {
        reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = null;
        while ( (line = reader.readLine()) != null ) {
          if (line.isEmpty()) {
            continue;
          }
          splitKeySet.add(line);
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }

      if (splitKeySet.isEmpty()) {
        return null;
      }

      byte[][] splitKeys = new byte[splitKeySet.size()][];
      int index = 0;
      for (String eachKey: splitKeySet) {
        if (numRowKeys == 1 && rowkeyBinary) {
          splitKeys[index++] = HBaseBinarySerializerDeserializer.serialize(rowKeyColumn, new TextDatum(eachKey));
        } else {
          splitKeys[index++] = HBaseTextSerializerDeserializer.serialize(rowKeyColumn, new TextDatum(eachKey));
        }
      }

      return splitKeys;
    }

    return null;
  }

  /**
   * It extracts quorum addresses from a Hbase Tablespace URI.
   * For example, consider an example URI 'hbase:zk://host1:2171,host2:2172,host3:2173/table1'.
   * <code>extractQuorum</code> will extract only 'host1:2171,host2:2172,host3:2173'.
   *
   * @param uri Hbase Tablespace URI
   * @return Quorum addresses
   */
  static String extractQuorum(URI uri) {
    String uriStr = uri.toString();
    int start = uriStr.indexOf("/") + 2;
    int pathIndex = uriStr.lastIndexOf("/");

    if (pathIndex < start) {
      return uriStr.substring(start);
    } else {
      return uriStr.substring(start, pathIndex);
    }
  }

  /**
   * Creates HTableDescription using table meta data.
   *
   * @param tableMeta
   * @param schema
   * @return
   * @throws java.io.IOException
   */
  public static HTableDescriptor parseHTableDescriptor(TableMeta tableMeta, Schema schema)
      throws MissingTablePropertyException, InvalidTablePropertyException {

    String hbaseTableName = tableMeta.getProperty(HBaseStorageConstants.META_TABLE_KEY, "");
    if (hbaseTableName == null || hbaseTableName.trim().isEmpty()) {
      throw new MissingTablePropertyException(HBaseStorageConstants.META_TABLE_KEY, hbaseTableName);
    }
    TableName hTableName = TableName.valueOf(hbaseTableName);

    ColumnMapping columnMapping = new ColumnMapping(schema, tableMeta.getPropertySet());

    HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);

    Collection<String> columnFamilies = columnMapping.getColumnFamilyNames();
    //If 'columns' attribute is empty, Tajo table columns are mapped to all HBase table column.
    if (columnFamilies.isEmpty()) {
      for (Column eachColumn: schema.getRootColumns()) {
        columnFamilies.add(eachColumn.getSimpleName());
      }
    }

    for (String eachColumnFamily: columnFamilies) {
      hTableDescriptor.addFamily(new HColumnDescriptor(eachColumnFamily));
    }

    return hTableDescriptor;
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException, TajoException {

    try (HBaseAdmin hAdmin = new HBaseAdmin(hbaseConf)) {
      HTableDescriptor hTableDesc = parseHTableDescriptor(tableDesc.getMeta(), tableDesc.getSchema());
      LOG.info("Deleting hbase table: " + new String(hTableDesc.getName()));
      hAdmin.disableTable(hTableDesc.getName());
      hAdmin.deleteTable(hTableDesc.getName());
    }
  }

  @Override
  public URI getTableUri(String databaseName, String tableName) {
    return URI.create(uri.toString() + "/" + tableName);
  }

  /**
   * Returns columns which are mapped to the rowkey of the hbase table.
   *
   * @param tableDesc
   * @return
   * @throws java.io.IOException
   */
  private Column[] getIndexableColumns(TableDesc tableDesc) throws
      MissingTablePropertyException, InvalidTablePropertyException {

    ColumnMapping columnMapping = new ColumnMapping(tableDesc.getSchema(), tableDesc.getMeta().getPropertySet());
    boolean[] isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    int[] rowKeyIndexes = columnMapping.getRowKeyFieldIndexes();

    Column indexColumn = null;
    for (int i = 0; i < isRowKeyMappings.length; i++) {
      if (isRowKeyMappings[i]) {
        if (columnMapping.getNumRowKeys() == 1 ||
            rowKeyIndexes[i] == 0) {
          indexColumn = tableDesc.getSchema().getColumn(i);
        }
      }
    }
    return new Column[]{indexColumn};
  }

  private Pair<List<byte []>, List<byte []>> getSelectedKeyRange(
      ColumnMapping columnMap,
      List<IndexPredication> predicates) {

    final List<byte[]> startRows;
    final List<byte[]> stopRows;

    if (predicates != null && !predicates.isEmpty()) {
      // indexPredications is Disjunctive set
      startRows = predicates.stream()
          .map(x -> {
            if (x.getStartValue() != null) {
              return serialize(columnMap, x, x.getStartValue());
            } else {
              return HConstants.EMPTY_START_ROW;
            }
          })
          .collect(Collectors.toList());

      stopRows = predicates.stream()
          .map(x -> {
            if (x.getStopValue() != null) {
              return serialize(columnMap, x, x.getStopValue());
            } else {
              return HConstants.EMPTY_START_ROW;
            }
          })
          .collect(Collectors.toList());

    } else {
      startRows = EMPTY_START_ROW_KEY;
      stopRows  = EMPTY_END_ROW_KEY;
    }

    return new Pair(startRows, stopRows);
  }

  private boolean isEmptyRegion(org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> tableRange) {
    return tableRange == null || tableRange.getFirst() == null || tableRange.getFirst().length == 0;
  }

  private long getRegionSize(RegionSizeCalculator calculator, byte [] regionName) {
    long regionSize = calculator.getRegionSize(regionName);
    if (regionSize == 0) {
      return TajoConstants.UNKNOWN_LENGTH;
    } else {
      return regionSize;
    }
  }

  private List<HBaseFragment> createEmptyFragment(TableDesc table, String sourceId, HTable htable,
                                                  RegionSizeCalculator sizeCalculator) throws IOException {
    HRegionLocation regLoc = htable.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);
    if (null == regLoc) {
      throw new IOException("Expecting at least one region.");
    }

    HBaseFragment fragment = new HBaseFragment(
        table.getUri(),
        sourceId, htable.getName().getNameAsString(),
        HConstants.EMPTY_BYTE_ARRAY,
        HConstants.EMPTY_BYTE_ARRAY,
        regLoc.getHostname());

    fragment.setLength(getRegionSize(sizeCalculator, regLoc.getRegionInfo().getRegionName()));
    return ImmutableList.of(fragment);
  }

  private Collection<HBaseFragment> convertRangeToFragment(
      TableDesc table, String inputSourceId, HTable htable, RegionSizeCalculator sizeCalculator,
      org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> tableRange,
      Pair<List<byte[]>, List<byte[]>> selectedRange) throws IOException {

    final Map<byte[], HBaseFragment> fragmentMap = new HashMap<>();

    for (int i = 0; i < tableRange.getFirst().length; i++) {
      HRegionLocation location = htable.getRegionLocation(tableRange.getFirst()[i], false);
      if (location == null) {
        throw new IOException("Can't find the region of the key: " + Bytes.toStringBinary(tableRange.getFirst()[i]));
      }

      final byte[] regionStartKey = tableRange.getFirst()[i];
      final byte[] regionStopKey = tableRange.getSecond()[i];

      int startRowsSize = selectedRange.getFirst().size();
      for (int j = 0; j < startRowsSize; j++) {
        byte[] startRow = selectedRange.getFirst().get(j);
        byte[] stopRow = selectedRange.getSecond().get(j);
        // determine if the given start an stop key fall into the region
        if ((startRow.length == 0 || regionStopKey.length == 0 || Bytes.compareTo(startRow, regionStopKey) < 0)
            && (stopRow.length == 0 || Bytes.compareTo(stopRow, regionStartKey) > 0)) {
          final byte[] fragmentStart = (startRow.length == 0 || Bytes.compareTo(regionStartKey, startRow) >= 0) ?
              regionStartKey : startRow;

          final byte[] fragmentStop = (stopRow.length == 0 || Bytes.compareTo(regionStopKey, stopRow) <= 0) &&
              regionStopKey.length > 0 ? regionStopKey : stopRow;

          if (fragmentMap.containsKey(regionStartKey)) {
            final HBaseFragment prevFragment = fragmentMap.get(regionStartKey);
            if (Bytes.compareTo(fragmentStart, prevFragment.getStartRow()) < 0) {
              prevFragment.setStartRow(fragmentStart);
            }
            if (Bytes.compareTo(fragmentStop, prevFragment.getStopRow()) > 0) {
              prevFragment.setStopRow(fragmentStop);
            }
          } else {

            final HBaseFragment fragment = new HBaseFragment(table.getUri(),
                inputSourceId,
                htable.getName().getNameAsString(),
                fragmentStart,
                fragmentStop,
                location.getHostname());

            fragment.setLength(getRegionSize(sizeCalculator, location.getRegionInfo().getRegionName()));
            fragmentMap.put(regionStartKey, fragment);

            if (LOG.isDebugEnabled()) {
              LOG.debug("getFragments: fragment -> " + i + " -> " + fragment);
            }
          }
        }
      }
    }

    return fragmentMap.values();
  }

  @Override
  public List<Fragment> getSplits(String inputSourceId,
                                  TableDesc table,
                                  @Nullable EvalNode filterCondition) throws IOException, TajoException {
    return (List<Fragment>) (List) getRawSplits(inputSourceId, table, filterCondition);
  }

  private List<HBaseFragment> getRawSplits(String inputSourceId,
                                           TableDesc table,
                                           @Nullable EvalNode filterCondition) throws IOException, TajoException {
    final ColumnMapping columnMapping = new ColumnMapping(table.getSchema(), table.getMeta().getPropertySet());

    try (final HTable htable = new HTable(hbaseConf, table.getMeta().getProperty(HBaseStorageConstants.META_TABLE_KEY))) {
      final RegionSizeCalculator sizeCalculator = new RegionSizeCalculator(htable);
      final org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> tableRange = htable.getStartEndKeys();
      if (isEmptyRegion(tableRange)) {
        return createEmptyFragment(table, inputSourceId, htable, sizeCalculator);
      }

      final Pair<List<byte []>, List<byte []>> selectedRange = getSelectedKeyRange(
          columnMapping,
          getIndexPredications(columnMapping, table, filterCondition));

      // region startkey -> HBaseFragment
      List<HBaseFragment> fragments = new ArrayList<>(convertRangeToFragment(table, inputSourceId, htable, sizeCalculator, tableRange, selectedRange));
      Collections.sort(fragments);
      if (!fragments.isEmpty()) {
        fragments.get(fragments.size() - 1).setLast(true);
      }

      return ImmutableList.copyOf(fragments);
    }
  }

  private byte[] serialize(ColumnMapping columnMapping,
                           IndexPredication indexPredication, Datum datum) {
    if (columnMapping.getIsBinaryColumns()[indexPredication.getColumnId()]) {
      return HBaseBinarySerializerDeserializer.serialize(indexPredication.getColumn(), datum);
    } else {
      return HBaseTextSerializerDeserializer.serialize(indexPredication.getColumn(), datum);
    }
  }

  @Override
  public Appender getAppenderForInsertRow(OverridableConf queryContext,
                                          TaskAttemptId taskAttemptId,
                                          TableMeta meta,
                                          Schema schema,
                                          Path workDir) throws IOException {
    return new HBasePutAppender(conf, uri, taskAttemptId, schema, meta, workDir);
  }

  @Override
  public Appender getAppender(OverridableConf queryContext,
                              TaskAttemptId taskAttemptId, TableMeta meta, Schema schema, Path workDir)
      throws IOException {
    if ("true".equalsIgnoreCase(queryContext.get(HBaseStorageConstants.INSERT_PUT_MODE, "false"))) {
      return new HBasePutAppender(conf, uri, taskAttemptId, schema, meta, workDir);
    } else {
      return super.getAppender(queryContext, taskAttemptId, meta, schema, workDir);
    }
  }

  public HConnection getConnection() throws IOException {
    synchronized(connMap) {
      HConnectionKey key = new HConnectionKey(hbaseConf);
      HConnection conn = connMap.get(key);
      if (conn == null) {
        conn = HConnectionManager.createConnection(hbaseConf);
        connMap.put(key, conn);
      }

      return conn;
    }
  }

  static class HConnectionKey {
    final static String[] CONNECTION_PROPERTIES = new String[] {
        HConstants.ZOOKEEPER_QUORUM, HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.ZOOKEEPER_CLIENT_PORT,
        HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
        HConstants.HBASE_CLIENT_PAUSE, HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.HBASE_CLIENT_INSTANCE_ID,
        HConstants.RPC_CODEC_CONF_KEY };

    private Map<String, String> properties;
    private String username;

    HConnectionKey(Configuration conf) {
      Map<String, String> m = new HashMap<>();
      if (conf != null) {
        for (String property : CONNECTION_PROPERTIES) {
          String value = conf.get(property);
          if (value != null) {
            m.put(property, value);
          }
        }
      }
      this.properties = Collections.unmodifiableMap(m);

      try {
        UserProvider provider = UserProvider.instantiate(conf);
        User currentUser = provider.getCurrent();
        if (currentUser != null) {
          username = currentUser.getName();
        }
      } catch (IOException ioe) {
        LOG.warn("Error obtaining current user, skipping username in HConnectionKey", ioe);
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      if (username != null) {
        result = username.hashCode();
      }
      for (String property : CONNECTION_PROPERTIES) {
        String value = properties.get(property);
        if (value != null) {
          result = prime * result + value.hashCode();
        }
      }

      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      HConnectionKey that = (HConnectionKey) obj;
      if (this.username != null && !this.username.equals(that.username)) {
        return false;
      } else if (this.username == null && that.username != null) {
        return false;
      }
      if (this.properties == null) {
        if (that.properties != null) {
          return false;
        }
      } else {
        if (that.properties == null) {
          return false;
        }
        for (String property : CONNECTION_PROPERTIES) {
          String thisValue = this.properties.get(property);
          String thatValue = that.properties.get(property);
          // noinspection StringEquality
          if (thisValue == thatValue) {
            continue;
          }
          if (thisValue == null || !thisValue.equals(thatValue)) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public String toString() {
      return "HConnectionKey{ properties=" + properties + ", username='" + username + '\'' + '}';
    }
  }

  public List<IndexPredication> getIndexPredications(ColumnMapping columnMapping,
                                                     TableDesc tableDesc,
                                                     @Nullable EvalNode filterCondition)
      throws IOException, MissingTablePropertyException, InvalidTablePropertyException {

    final Column[] indexableColumns = getIndexableColumns(tableDesc);
    if (indexableColumns == null || indexableColumns.length == 0) {
      return Collections.EMPTY_LIST;
    }

    // Currently supports only single index column.
    return findIndexablePredicateSet(filterCondition, indexableColumns).stream()
        .map(set -> getIndexablePredicateValue(columnMapping, set))
        .filter(value -> value != null)
        .map(value ->
            new IndexPredication(
                indexableColumns[0],
                tableDesc.getLogicalSchema().getColumnId(indexableColumns[0].getQualifiedName()),
                value.getFirst(),
                value.getSecond()))
        .collect(Collectors.toList());
  }

  public List<Set<EvalNode>> findIndexablePredicateSet(@Nullable EvalNode qual,
                                                       Column[] indexableColumns) throws IOException {
    List<Set<EvalNode>> indexablePredicateList = new ArrayList<>();

    // if a query statement has a search condition, try to find indexable predicates
    if (indexableColumns != null && qual != null) {
      EvalNode[] disjunctiveForms = AlgebraicUtil.toDisjunctiveNormalFormArray(qual);

      // add qualifier to schema for qual
      for (Column column : indexableColumns) {
        for (EvalNode disjunctiveExpr : disjunctiveForms) {
          EvalNode[] conjunctiveForms = AlgebraicUtil.toConjunctiveNormalFormArray(disjunctiveExpr);
          Set<EvalNode> indexablePredicateSet = Sets.newHashSet();
          for (EvalNode conjunctiveExpr : conjunctiveForms) {
            if (checkIfIndexablePredicateOnTargetColumn(conjunctiveExpr, column)) {
              indexablePredicateSet.add(conjunctiveExpr);
            }
          }
          if (!indexablePredicateSet.isEmpty()) {
            indexablePredicateList.add(indexablePredicateSet);
          }
        }
      }
    }

    return indexablePredicateList;
  }

  private boolean checkIfIndexablePredicateOnTargetColumn(EvalNode evalNode, Column targetColumn) {
    if (checkIfIndexablePredicate(evalNode) || checkIfConjunctiveButOneVariable(evalNode)) {
      Set<Column> variables = EvalTreeUtil.findUniqueColumns(evalNode);
      // if it contains only single variable matched to a target column
      return variables.size() == 1 && variables.contains(targetColumn);
    } else {
      return false;
    }
  }

  /**
   *
   * @param evalNode The expression to be checked
   * @return true if an conjunctive expression, consisting of indexable expressions
   */
  private boolean checkIfConjunctiveButOneVariable(EvalNode evalNode) {
    if (evalNode.getType() == EvalType.AND) {
      BinaryEval orEval = (BinaryEval) evalNode;
      boolean indexable =
          checkIfIndexablePredicate(orEval.getLeftExpr()) &&
              checkIfIndexablePredicate(orEval.getRightExpr());

      boolean sameVariable =
          EvalTreeUtil.findUniqueColumns(orEval.getLeftExpr())
              .equals(EvalTreeUtil.findUniqueColumns(orEval.getRightExpr()));

      return indexable && sameVariable;
    } else {
      return false;
    }
  }

  /**
   * Check if an expression consists of one variable and one constant and
   * the expression is a comparison operator.
   *
   * @param evalNode The expression to be checked
   * @return true if an expression consists of one variable and one constant
   * and the expression is a comparison operator. Other, false.
   */
  private boolean checkIfIndexablePredicate(EvalNode evalNode) {
    return AlgebraicUtil.containSingleVar(evalNode) && isIndexableOperator(evalNode);
  }

  public static boolean isIndexableOperator(EvalNode expr) {
    return expr.getType() == EvalType.EQUAL ||
        expr.getType() == EvalType.LEQ ||
        expr.getType() == EvalType.LTH ||
        expr.getType() == EvalType.GEQ ||
        expr.getType() == EvalType.GTH ||
        expr.getType() == EvalType.BETWEEN;
  }

  public Pair<Datum, Datum> getIndexablePredicateValue(ColumnMapping columnMapping,
                                                       Set<EvalNode> evalNodes) {
    Datum startDatum = null;
    Datum endDatum = null;
    for (EvalNode evalNode: evalNodes) {
      if (evalNode instanceof BinaryEval) {
        BinaryEval binaryEval = (BinaryEval) evalNode;
        EvalNode left = binaryEval.getLeftExpr();
        EvalNode right = binaryEval.getRightExpr();

        Datum constValue = null;
        if (left.getType() == EvalType.CONST) {
          constValue = ((ConstEval) left).getValue();
        } else if (right.getType() == EvalType.CONST) {
          constValue = ((ConstEval) right).getValue();
        }

        if (constValue != null) {
          if (evalNode.getType() == EvalType.EQUAL ||
              evalNode.getType() == EvalType.GEQ ||
              evalNode.getType() == EvalType.GTH) {
            if (startDatum != null) {
              if (constValue.compareTo(startDatum) > 0) {
                startDatum = constValue;
              }
            } else {
              startDatum = constValue;
            }
          }

          if (evalNode.getType() == EvalType.EQUAL ||
              evalNode.getType() == EvalType.LEQ ||
              evalNode.getType() == EvalType.LTH) {
            if (endDatum != null) {
              if (constValue.compareTo(endDatum) < 0) {
                endDatum = constValue;
              }
            } else {
              endDatum = constValue;
            }
          }
        }
      } else if (evalNode instanceof BetweenPredicateEval) {
        BetweenPredicateEval betweenEval = (BetweenPredicateEval) evalNode;
        if (betweenEval.getBegin().getType() == EvalType.CONST && betweenEval.getEnd().getType() == EvalType.CONST) {
          Datum value = ((ConstEval) betweenEval.getBegin()).getValue();
          if (startDatum != null) {
            if (value.compareTo(startDatum) > 0) {
              startDatum = value;
            }
          } else {
            startDatum = value;
          }

          value = ((ConstEval) betweenEval.getEnd()).getValue();
          if (endDatum != null) {
            if (value.compareTo(endDatum) < 0) {
              endDatum = value;
            }
          } else {
            endDatum = value;
          }
        }
      }
    }

    if (endDatum != null && columnMapping != null && columnMapping.getNumRowKeys() > 1) {
      endDatum = new TextDatum(endDatum.asChars() +
          new String(new char[]{columnMapping.getRowKeyDelimiter(), Character.MAX_VALUE}));
    }
    if (startDatum != null || endDatum != null) {
      return new Pair<>(startDatum, endDatum);
    } else {
      return null;
    }
  }

  @Override
  public Path commitTable(OverridableConf queryContext, ExecutionBlockId finalEbId,
                          LogicalPlan plan, Schema schema,
                          TableDesc tableDesc) throws IOException {
    if (tableDesc == null) {
      throw new IOException("TableDesc is null while calling loadIncrementalHFiles: " + finalEbId);
    }

    Path stagingDir = new Path(queryContext.get(QueryVars.STAGING_DIR));
    Path stagingResultDir = new Path(stagingDir, TajoConstants.RESULT_DIR_NAME);

    Configuration hbaseConf = HBaseConfiguration.create(this.hbaseConf);
    hbaseConf.set("hbase.loadincremental.threads.max", "2");

    JobContextImpl jobContext = new JobContextImpl(hbaseConf,
        new JobID(finalEbId.getQueryId().toString(), finalEbId.getId()));

    FileOutputCommitter committer = new FileOutputCommitter(stagingResultDir, jobContext);
    Path jobAttemptPath = committer.getJobAttemptPath(jobContext);
    FileSystem fs = jobAttemptPath.getFileSystem(queryContext.getConf());
    if (!fs.exists(jobAttemptPath) || fs.listStatus(jobAttemptPath) == null) {
      LOG.warn("No query attempt file in " + jobAttemptPath);
      return stagingResultDir;
    }
    committer.commitJob(jobContext);

    // insert into table
    String tableName = tableDesc.getMeta().getProperty(HBaseStorageConstants.META_TABLE_KEY);

    try (HTable htable = new HTable(hbaseConf, tableName)) {
      LoadIncrementalHFiles loadIncrementalHFiles = null;
      try {
        loadIncrementalHFiles = new LoadIncrementalHFiles(hbaseConf);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new IOException(e.getMessage(), e);
      }
      loadIncrementalHFiles.doBulkLoad(stagingResultDir, htable);

      return stagingResultDir;
    }
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc,
                                          Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange)
      throws IOException {
    try {
      int[] sortKeyIndexes = new int[sortSpecs.length];
      for (int i = 0; i < sortSpecs.length; i++) {
        sortKeyIndexes[i] = inputSchema.getColumnId(sortSpecs[i].getSortKey().getQualifiedName());
      }

      ColumnMapping columnMapping = new ColumnMapping(tableDesc.getSchema(), tableDesc.getMeta().getPropertySet());

      try (HTable htable = new HTable(hbaseConf, columnMapping.getHbaseTableName())) {
        byte[][] endKeys = htable.getEndKeys();
        if (endKeys.length == 1) {
          return new TupleRange[]{dataRange};
        }
        List<TupleRange> tupleRanges = new ArrayList<>(endKeys.length);

        TupleComparator comparator = new BaseTupleComparator(inputSchema, sortSpecs);
        Tuple previousTuple = dataRange.getStart();

        for (byte[] eachEndKey : endKeys) {
          VTuple endTuple = new VTuple(sortSpecs.length);
          byte[][] rowKeyFields;
          if (sortSpecs.length > 1) {
            byte[][] splitValues = BytesUtils.splitPreserveAllTokens(
                eachEndKey, columnMapping.getRowKeyDelimiter(), columnMapping.getNumColumns());
            if (splitValues.length == sortSpecs.length) {
              rowKeyFields = splitValues;
            } else {
              rowKeyFields = new byte[sortSpecs.length][];
              for (int j = 0; j < sortSpecs.length; j++) {
                if (j < splitValues.length) {
                  rowKeyFields[j] = splitValues[j];
                } else {
                  rowKeyFields[j] = null;
                }
              }
            }

          } else {
            rowKeyFields = new byte[1][];
            rowKeyFields[0] = eachEndKey;
          }

          for (int i = 0; i < sortSpecs.length; i++) {
            if (columnMapping.getIsBinaryColumns()[sortKeyIndexes[i]]) {
              endTuple.put(i,
                      HBaseBinarySerializerDeserializer.deserialize(inputSchema.getColumn(sortKeyIndexes[i]),
                              rowKeyFields[i]));
            } else {
              endTuple.put(i,
                      HBaseTextSerializerDeserializer.deserialize(inputSchema.getColumn(sortKeyIndexes[i]),
                              rowKeyFields[i]));
            }
          }
          tupleRanges.add(new TupleRange(sortSpecs, previousTuple, endTuple));
          previousTuple = endTuple;
        }

        // Last region endkey is empty. Tajo ignores empty key, so endkey is replaced with max data value.
        if (comparator.compare(dataRange.getEnd(), tupleRanges.get(tupleRanges.size() - 1).getStart()) >= 0) {
          tupleRanges.get(tupleRanges.size() - 1).setEnd(dataRange.getEnd());
        } else {
          tupleRanges.remove(tupleRanges.size() - 1);
        }
        return tupleRanges.toArray(new TupleRange[tupleRanges.size()]);
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw new IOException(t.getMessage(), t);
    }
  }

  @Override
  public void rewritePlan(OverridableConf context, LogicalPlan plan) throws TajoException {
    if (REWRITE_RULE.isEligible(new LogicalPlanRewriteRuleContext(context, plan))) {
      REWRITE_RULE.rewrite(new LogicalPlanRewriteRuleContext(context, plan));
    }
  }

  @Override
  public StorageProperty getProperty() {
    return HBASE_STORAGE_PROPERTIES;
  }

  @Override
  public FormatProperty getFormatProperty(TableMeta meta) {
    KeyValueSet tableProperty = meta.getPropertySet();
    if (tableProperty.isTrue(HBaseStorageConstants.INSERT_PUT_MODE) ||
        tableProperty.isTrue(StorageConstants.INSERT_DIRECTLY)) {
      return PUT_MODE_PROPERTIES;
    } else {
      return HFILE_FORMAT_PROPERTIES;
    }
  }

  public void prepareTable(LogicalNode node) throws TajoException, IOException {
    if (node.getType() == NodeType.CREATE_TABLE) {
      CreateTableNode cNode = (CreateTableNode)node;
      if (!cNode.isExternal()) {
        TableMeta tableMeta = new TableMeta(cNode.getStorageType(), cNode.getOptions());
        createTable(
            ((CreateTableNode) node).getUri(), tableMeta, cNode.getTableSchema(),
            cNode.isExternal(), cNode.isIfNotExists());
      }
    }
  }

  @Override
  public void rollbackTable(LogicalNode node) throws IOException, TajoException {
    if (node.getType() == NodeType.CREATE_TABLE) {
      CreateTableNode cNode = (CreateTableNode)node;
      if (cNode.isExternal()) {
        return;
      }

      TableMeta tableMeta = new TableMeta(cNode.getStorageType(), cNode.getOptions());
      try (HBaseAdmin hAdmin = new HBaseAdmin(this.hbaseConf)) {
        HTableDescriptor hTableDesc = parseHTableDescriptor(tableMeta, cNode.getTableSchema());
        LOG.info("Delete table cause query failed:" + new String(hTableDesc.getName()));
        hAdmin.disableTable(hTableDesc.getName());
        hAdmin.deleteTable(hTableDesc.getName());
      }
    }
  }

  @Override
  public URI getStagingUri(OverridableConf context, String queryId, TableMeta meta) throws IOException {
    if (meta.getPropertySet().isTrue(HBaseStorageConstants.INSERT_PUT_MODE)) {
      throw new IOException("Staging phase is not supported in this storage.");
    } else {
      return TablespaceManager.getDefault().getStagingUri(context, queryId, meta);
    }
  }

  public URI prepareStagingSpace(TajoConf conf, String queryId, OverridableConf context,
                                 TableMeta meta) throws IOException {
    if (!meta.getPropertySet().isTrue(HBaseStorageConstants.INSERT_PUT_MODE)) {
      return TablespaceManager.getDefault().prepareStagingSpace(conf, queryId, context, meta);
    } else {
      throw new IOException("Staging phase is not supported in this storage.");
    }
  }

  @Override
  public void verifySchemaToWrite(TableDesc tableDesc, Schema outSchema) throws TajoException {
    if (tableDesc != null) {
      Schema tableSchema = tableDesc.getSchema();
      if (tableSchema.size() != outSchema.size()) {
        throw SyntaxErrorUtil.makeSyntaxError("Target columns and projected columns are mismatched to each other");
      }

      for (int i = 0; i < tableSchema.size(); i++) {
        if (!tableSchema.getColumn(i).getDataType().equals(outSchema.getColumn(i).getDataType())) {
          final Column tableColumn = tableSchema.getColumn(i);
          final Column outColumn = outSchema.getColumn(i);
          throw new DataTypeMismatchException(
              tableColumn.getQualifiedName(),
              tableColumn.getDataType().getType().name(),
              outColumn.getQualifiedName(),
              outColumn.getDataType().getType().name());
        }
      }
    }
  }
}
