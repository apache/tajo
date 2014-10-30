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

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryVars;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class HBaseStorageManager extends StorageManager {
  private static final Log LOG = LogFactory.getLog(HBaseStorageManager.class);

  public static final String META_TABLE_KEY = "table";
  public static final String META_COLUMNS_KEY = "columns";
  public static final String META_SPLIT_ROW_KEYS_KEY = "hbase.split.rowkeys";
  public static final String META_SPLIT_ROW_KEYS_FILE_KEY = "hbase.split.rowkeys.file";
  public static final String META_ZK_QUORUM_KEY = "hbase.zookeeper.quorum";
  public static final String ROWKEY_COLUMN_MAPPING = "key";
  public static final String META_ROWKEY_DELIMITER = "hbase.rowkey.delimiter";

  private Map<HConnectionKey, HConnection> connMap = new HashMap<HConnectionKey, HConnection>();

  public HBaseStorageManager (StoreType storeType) {
    super(storeType);
  }

  @Override
  public void storageInit() throws IOException {
  }

  @Override
  public void closeStorageManager() {
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
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {
    createTable(tableDesc.getMeta(), tableDesc.getSchema(), tableDesc.isExternal(), ifNotExists);
    TableStats stats = new TableStats();
    stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
    tableDesc.setStats(stats);
  }

  private void createTable(TableMeta tableMeta, Schema schema,
                           boolean isExternal, boolean ifNotExists) throws IOException {
    String hbaseTableName = tableMeta.getOption(META_TABLE_KEY, "");
    if (hbaseTableName == null || hbaseTableName.trim().isEmpty()) {
      throw new IOException("HBase mapped table is required a '" + META_TABLE_KEY + "' attribute.");
    }
    TableName hTableName = TableName.valueOf(hbaseTableName);

    String columnMapping = tableMeta.getOption(META_COLUMNS_KEY, "");
    if (columnMapping != null && columnMapping.split(",").length > schema.size()) {
      throw new IOException("Columns property has more entry than Tajo table columns");
    }

    Configuration hConf = getHBaseConfiguration(conf, tableMeta);
    HBaseAdmin hAdmin =  new HBaseAdmin(hConf);

    try {
      if (isExternal) {
        // If tajo table is external table, only check validation.
        if (columnMapping == null || columnMapping.isEmpty()) {
          throw new IOException("HBase mapped table is required a '" + META_COLUMNS_KEY + "' attribute.");
        }
        if (!hAdmin.tableExists(hTableName)) {
          throw new IOException("HBase table [" + hbaseTableName + "] not exists. " +
              "External table should be a existed table.");
        }
        HTableDescriptor hTableDescriptor = hAdmin.getTableDescriptor(hTableName);
        Set<String> tableColumnFamilies = new HashSet<String>();
        for (HColumnDescriptor eachColumn : hTableDescriptor.getColumnFamilies()) {
          tableColumnFamilies.add(eachColumn.getNameAsString());
        }

        Collection<String> mappingColumnFamilies = getColumnFamilies(columnMapping);
        if (mappingColumnFamilies.isEmpty()) {
          throw new IOException("HBase mapped table is required a '" + META_COLUMNS_KEY + "' attribute.");
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

        byte[][] splitKeys = getSplitKeys(conf, tableMeta);
        if (splitKeys == null) {
          hAdmin.createTable(hTableDescriptor);
        } else {
          hAdmin.createTable(hTableDescriptor, splitKeys);
        }
      }
    } finally {
      hAdmin.close();
    }
  }

  private byte[][] getSplitKeys(TajoConf conf, TableMeta meta) throws IOException {
    String splitRowKeys = meta.getOption(META_SPLIT_ROW_KEYS_KEY, "");
    String splitRowKeysFile = meta.getOption(META_SPLIT_ROW_KEYS_FILE_KEY, "");

    if ((splitRowKeys == null || splitRowKeys.isEmpty()) &&
        (splitRowKeysFile == null || splitRowKeysFile.isEmpty())) {
      return null;
    }

    if (splitRowKeys != null && !splitRowKeys.isEmpty()) {
      String[] splitKeyTokens = splitRowKeys.split(",");
      byte[][] splitKeys = new byte[splitKeyTokens.length][];
      for (int i = 0; i < splitKeyTokens.length; i++) {
        splitKeys[i] = Bytes.toBytes(splitKeyTokens[i]);
      }
      return splitKeys;
    }

    if (splitRowKeysFile != null && !splitRowKeysFile.isEmpty()) {
      Path path = new Path(splitRowKeysFile);
      FileSystem fs = path.getFileSystem(conf);
      if (!fs.exists(path)) {
        throw new IOException("hbase.split.rowkeys.file=" + path.toString() + " not exists.");
      }

      SortedSet<String> splitKeySet = new TreeSet<String>();
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
        splitKeys[index++] = Bytes.toBytes(eachKey);
      }

      return splitKeys;
    }

    return null;
  }

  private static List<String> getColumnFamilies(String columnMapping) {
    // columnMapping can have a duplicated column name as CF1:a, CF1:b
    List<String> columnFamilies = new ArrayList<String>();

    if (columnMapping == null) {
      return columnFamilies;
    }

    for (String eachToken: columnMapping.split(",")) {
      String[] cfTokens = eachToken.trim().split(":");
      if (cfTokens.length == 2 && cfTokens[1] != null && getRowKeyMapping(cfTokens[0], cfTokens[1].trim()) != null) {
        // rowkey
        continue;
      }
      if (!columnFamilies.contains(cfTokens[0])) {
        String[] binaryTokens = cfTokens[0].split("#");
        columnFamilies.add(binaryTokens[0]);
      }
    }

    return columnFamilies;
  }

  public static Configuration getHBaseConfiguration(Configuration conf, TableMeta tableMeta) throws IOException {
    String zkQuorum = tableMeta.getOption(META_ZK_QUORUM_KEY, "");
    if (zkQuorum == null || zkQuorum.trim().isEmpty()) {
      throw new IOException("HBase mapped table is required a '" + META_ZK_QUORUM_KEY + "' attribute.");
    }

    Configuration hbaseConf = (conf == null) ? HBaseConfiguration.create() : HBaseConfiguration.create(conf);
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);

    for (Map.Entry<String, String> eachOption: tableMeta.getOptions().getAllKeyValus().entrySet()) {
      String key = eachOption.getKey();
      if (key.startsWith(HConstants.ZK_CFG_PROPERTY_PREFIX)) {
        hbaseConf.set(key, eachOption.getValue());
      }
    }
    return hbaseConf;
  }

  public static HTableDescriptor parseHTableDescriptor(TableMeta tableMeta, Schema schema) throws IOException {
    String hbaseTableName = tableMeta.getOption(META_TABLE_KEY, "");
    if (hbaseTableName == null || hbaseTableName.trim().isEmpty()) {
      throw new IOException("HBase mapped table is required a '" + META_TABLE_KEY + "' attribute.");
    }
    TableName hTableName = TableName.valueOf(hbaseTableName);

    String columnMapping = tableMeta.getOption(META_COLUMNS_KEY, "");
    if (columnMapping != null && columnMapping.split(",").length > schema.size()) {
      throw new IOException("Columns property has more entry than Tajo table columns");
    }
    HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);

    Collection<String> columnFamilies = getColumnFamilies(columnMapping);
    //If 'columns' attribute is empty, Tajo table columns are mapped to all HBase table column.
    if (columnFamilies.isEmpty()) {
      for (Column eachColumn: schema.getColumns()) {
        columnFamilies.add(eachColumn.getSimpleName());
      }
    }

    for (String eachColumnFamily: columnFamilies) {
      hTableDescriptor.addFamily(new HColumnDescriptor(eachColumnFamily));
    }

    return hTableDescriptor;
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException {
    HBaseAdmin hAdmin =  new HBaseAdmin(getHBaseConfiguration(conf, tableDesc.getMeta()));

    try {
      HTableDescriptor hTableDesc = parseHTableDescriptor(tableDesc.getMeta(), tableDesc.getSchema());
      hAdmin.disableTable(hTableDesc.getName());
      hAdmin.deleteTable(hTableDesc.getName());
    } finally {
      hAdmin.close();
    }
  }

  private Column[] getIndexableColumns(TableDesc tableDesc) throws IOException {
    ColumnMapping columnMapping = new ColumnMapping(tableDesc.getSchema(), tableDesc.getMeta());
    boolean[] isRowKeyMappings = columnMapping.getIsRowKeyMappings();

    Column indexColumn = null;
    for (int i = 0; i < isRowKeyMappings.length; i++) {
      if (isRowKeyMappings[i]) {
        if (indexColumn != null) {
          //Currently only supports single rowkey.
          return null;
        }
        indexColumn = tableDesc.getSchema().getColumn(i);
      }
    }
    return new Column[]{indexColumn};
  }

  @Override
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc, ScanNode scanNode) throws IOException {
    List<IndexPredication> indexPredications = getIndexPredications(tableDesc, scanNode);
    Configuration hconf = getHBaseConfiguration(conf, tableDesc.getMeta());
    HTable htable = null;
    HBaseAdmin hAdmin = null;

    try {
      htable = new HTable(hconf, tableDesc.getMeta().getOption(META_TABLE_KEY));

      org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
      if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
        HRegionLocation regLoc = htable.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY, false);
        if (null == regLoc) {
          throw new IOException("Expecting at least one region.");
        }
        List<Fragment> fragments = new ArrayList<Fragment>(1);
        Fragment fragment = new HBaseFragment(fragmentId, htable.getName().getNameAsString(),
            HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, regLoc.getHostname());
        fragments.add(fragment);
        return fragments;
      }

      ColumnMapping columnMapping = new ColumnMapping(tableDesc.getSchema(), tableDesc.getMeta());
      List<byte[]> startRows;
      List<byte[]> stopRows;

      if (indexPredications != null && !indexPredications.isEmpty()) {
        // indexPredications is Disjunctive set
        startRows = new ArrayList<byte[]>();
        stopRows = new ArrayList<byte[]>();
        for (IndexPredication indexPredication: indexPredications) {
          byte[] startRow;
          byte[] stopRow;
          if (indexPredication.getStartValue() != null) {
            startRow = serialize(columnMapping, indexPredication, indexPredication.getStartValue());
          } else {
            startRow = HConstants.EMPTY_START_ROW;
          }
          if (indexPredication.getStopValue() != null) {
            stopRow = serialize(columnMapping, indexPredication, indexPredication.getStopValue());
          } else {
            stopRow = HConstants.EMPTY_END_ROW;
          }
          startRows.add(startRow);
          stopRows.add(stopRow);
        }
      } else {
        startRows = TUtil.newList(HConstants.EMPTY_START_ROW);
        stopRows = TUtil.newList(HConstants.EMPTY_END_ROW);
      }

      hAdmin =  new HBaseAdmin(hconf);
      Map<ServerName, ServerLoad> serverLoadMap = new HashMap<ServerName, ServerLoad>();

      // region startkey -> HBaseFragment
      Map<byte[], HBaseFragment> fragmentMap = new HashMap<byte[], HBaseFragment>();
      for (int i = 0; i < keys.getFirst().length; i++) {
        HRegionLocation location = htable.getRegionLocation(keys.getFirst()[i], false);

        byte[] regionStartKey = keys.getFirst()[i];
        byte[] regionStopKey = keys.getSecond()[i];

        int startRowsSize = startRows.size();
        for (int j = 0; j < startRowsSize; j++) {
          byte[] startRow = startRows.get(j);
          byte[] stopRow = stopRows.get(j);
          // determine if the given start an stop key fall into the region
          if ((startRow.length == 0 || regionStopKey.length == 0 || Bytes.compareTo(startRow, regionStopKey) < 0)
              && (stopRow.length == 0 || Bytes.compareTo(stopRow, regionStartKey) > 0)) {
            byte[] fragmentStart = (startRow.length == 0 || Bytes.compareTo(regionStartKey, startRow) >= 0) ?
                regionStartKey : startRow;

            byte[] fragmentStop = (stopRow.length == 0 || Bytes.compareTo(regionStopKey, stopRow) <= 0) &&
                regionStopKey.length > 0 ? regionStopKey : stopRow;

            String regionName = location.getRegionInfo().getRegionNameAsString();

            ServerLoad serverLoad = serverLoadMap.get(location.getServerName());
            if (serverLoad == null) {
              serverLoad = hAdmin.getClusterStatus().getLoad(location.getServerName());
              serverLoadMap.put(location.getServerName(), serverLoad);
            }

            if (fragmentMap.containsKey(regionStartKey)) {
              HBaseFragment prevFragment = fragmentMap.get(regionStartKey);
              if (Bytes.compareTo(fragmentStart, prevFragment.getStartRow()) < 0) {
                prevFragment.setStartRow(fragmentStart);
              }
              if (Bytes.compareTo(fragmentStop, prevFragment.getStopRow()) > 0) {
                prevFragment.setStopRow(fragmentStop);
              }
            } else {
              HBaseFragment fragment = new HBaseFragment(fragmentId, htable.getName().getNameAsString(),
                  fragmentStart, fragmentStop, location.getHostname());

              // get region size
              boolean foundLength = false;
              for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                if (regionName.equals(Bytes.toString(entry.getKey()))) {
                  RegionLoad regionLoad = entry.getValue();
                  long storeFileSize = (regionLoad.getStorefileSizeMB() + regionLoad.getMemStoreSizeMB()) * 1024L * 1024L;
                  fragment.setLength(storeFileSize);
                  foundLength = true;
                  break;
                }
              }

              if (!foundLength) {
                fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
              }

              fragmentMap.put(regionStartKey, fragment);
              if (LOG.isDebugEnabled()) {
                LOG.debug("getFragments: fragment -> " + i + " -> " + fragment);
              }
            }
          }
        }
      }

      List<HBaseFragment> fragments = new ArrayList<HBaseFragment>(fragmentMap.values());
      Collections.sort(fragments);
      if (!fragments.isEmpty()) {
        fragments.get(fragments.size() - 1).setLast(true);
      }
      return (ArrayList<Fragment>) (ArrayList) fragments;
    } finally {
      if (htable != null) {
        htable.close();
      }
      if (hAdmin != null) {
        hAdmin.close();
      }
    }
  }

  public static RowKeyMapping getRowKeyMapping(String cfName, String columnName) {
    if (columnName == null || columnName.isEmpty()) {
      return null;
    }

    String[] tokens = columnName.split("#");
    if (!tokens[0].equalsIgnoreCase(ROWKEY_COLUMN_MAPPING)) {
      return null;
    }

    RowKeyMapping rowKeyMapping = new RowKeyMapping();

    if (tokens.length == 2 && "b".equals(tokens[1])) {
      rowKeyMapping.setBinary(true);
    }

    if (cfName != null && !cfName.isEmpty()) {
      rowKeyMapping.setKeyFieldIndex(Integer.parseInt(cfName));
    }
    return rowKeyMapping;
  }

  private byte[] serialize(ColumnMapping columnMapping,
                           IndexPredication indexPredication, Datum datum) throws IOException {
    if (columnMapping.getIsBinaryColumns()[indexPredication.getColumnId()]) {
      return HBaseBinarySerializerDeserializer.serialize(indexPredication.getColumn(), datum);
    } else {
      return HBaseTextSerializerDeserializer.serialize(indexPredication.getColumn(), datum);
    }
  }

  @Override
  public List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numFragments)
      throws IOException {
    Configuration hconf = getHBaseConfiguration(conf, tableDesc.getMeta());
    HTable htable = null;
    HBaseAdmin hAdmin = null;
    try {
      htable = new HTable(hconf, tableDesc.getMeta().getOption(META_TABLE_KEY));

      org.apache.hadoop.hbase.util.Pair<byte[][], byte[][]> keys = htable.getStartEndKeys();
      if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
        return new ArrayList<Fragment>(1);
      }
      hAdmin =  new HBaseAdmin(hconf);
      Map<ServerName, ServerLoad> serverLoadMap = new HashMap<ServerName, ServerLoad>();

      List<Fragment> fragments = new ArrayList<Fragment>(keys.getFirst().length);

      int start = currentPage * numFragments;
      if (start >= keys.getFirst().length) {
        return new ArrayList<Fragment>(1);
      }
      int end = (currentPage + 1) * numFragments;
      if (end > keys.getFirst().length) {
        end = keys.getFirst().length;
      }
      for (int i = start; i < end; i++) {
        HRegionLocation location = htable.getRegionLocation(keys.getFirst()[i], false);

        String regionName = location.getRegionInfo().getRegionNameAsString();
        ServerLoad serverLoad = serverLoadMap.get(location.getServerName());
        if (serverLoad == null) {
          serverLoad = hAdmin.getClusterStatus().getLoad(location.getServerName());
          serverLoadMap.put(location.getServerName(), serverLoad);
        }

        HBaseFragment fragment = new HBaseFragment(tableDesc.getName(), htable.getName().getNameAsString(),
            location.getRegionInfo().getStartKey(), location.getRegionInfo().getEndKey(), location.getHostname());

        // get region size
        boolean foundLength = false;
        for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
          if (regionName.equals(Bytes.toString(entry.getKey()))) {
            RegionLoad regionLoad = entry.getValue();
            long storeLength = (regionLoad.getStorefileSizeMB() + regionLoad.getMemStoreSizeMB()) * 1024L * 1024L;
            if (storeLength == 0) {
              // If store size is smaller than 1 MB, storeLength is zero
              storeLength = 1 * 1024 * 1024;  //default 1MB
            }
            fragment.setLength(storeLength);
            foundLength = true;
            break;
          }
        }

        if (!foundLength) {
          fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
        }

        fragments.add(fragment);
        if (LOG.isDebugEnabled()) {
          LOG.debug("getFragments: fragment -> " + i + " -> " + fragment);
        }
      }

      if (!fragments.isEmpty()) {
        ((HBaseFragment) fragments.get(fragments.size() - 1)).setLast(true);
      }
      return fragments;
    } finally {
      if (htable != null) {
        htable.close();
      }
      if (hAdmin != null) {
        hAdmin.close();
      }
    }
  }

  public HConnection getConnection(Configuration hbaseConf) throws IOException {
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
        HConstants.HBASE_CLIENT_PREFETCH_LIMIT,
        HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.HBASE_CLIENT_INSTANCE_ID,
        HConstants.RPC_CODEC_CONF_KEY };

    private Map<String, String> properties;
    private String username;

    HConnectionKey(Configuration conf) {
      Map<String, String> m = new HashMap<String, String>();
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
          //noinspection StringEquality
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
      return "HConnectionKey{" +
          "properties=" + properties +
          ", username='" + username + '\'' +
          '}';
    }
  }

  public List<IndexPredication> getIndexPredications(TableDesc tableDesc, ScanNode scanNode) throws IOException {
    List<IndexPredication> indexPredications = new ArrayList<IndexPredication>();
    Column[] indexableColumns = getIndexableColumns(tableDesc);
    if (indexableColumns != null && indexableColumns.length == 1) {
      // Currently supports only single index column.
      List<Set<EvalNode>> indexablePredicateList = findIndexablePredicateSet(scanNode, indexableColumns);
      for (Set<EvalNode> eachEvalSet: indexablePredicateList) {
        Pair<Datum, Datum> indexPredicationValues = getIndexablePredicateValue(eachEvalSet);
        if (indexPredicationValues != null) {
          IndexPredication indexPredication = new IndexPredication();
          indexPredication.setColumn(indexableColumns[0]);
          indexPredication.setColumnId(tableDesc.getLogicalSchema().getColumnId(indexableColumns[0].getQualifiedName()));
          indexPredication.setStartValue(indexPredicationValues.getFirst());
          indexPredication.setStopValue(indexPredicationValues.getSecond());

          indexPredications.add(indexPredication);
        }
      }
    }
    return indexPredications;
  }

  public List<Set<EvalNode>> findIndexablePredicateSet(ScanNode scanNode, Column[] indexableColumns) throws IOException {
    List<Set<EvalNode>> indexablePredicateList = new ArrayList<Set<EvalNode>>();

    // if a query statement has a search condition, try to find indexable predicates
    if (indexableColumns != null && scanNode.getQual() != null) {
      EvalNode[] disjunctiveForms = AlgebraicUtil.toDisjunctiveNormalFormArray(scanNode.getQual());

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

  public Pair<Datum, Datum> getIndexablePredicateValue(Set<EvalNode> evalNodes) {
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

    if (startDatum != null || endDatum != null) {
      return new Pair<Datum, Datum>(startDatum, endDatum);
    } else {
      return null;
    }
  }

  @Override
  public Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId, Schema schema,
                               TableDesc tableDesc) throws IOException {
    if (tableDesc == null) {
      throw new IOException("TableDesc is null while calling loadIncrementalHFiles: " + finalEbId);
    }
    Path finalOutputDir = new Path(queryContext.get(QueryVars.STAGING_DIR));

    Configuration hbaseConf = HBaseStorageManager.getHBaseConfiguration(queryContext.getConf(), tableDesc.getMeta());
    hbaseConf.set("hbase.loadincremental.threads.max", "2");

    JobContextImpl jobContext = new JobContextImpl(hbaseConf,
        new JobID(finalEbId.getQueryId().toString(), finalEbId.getId()));

    FileOutputCommitter committer = new FileOutputCommitter(finalOutputDir, jobContext);
    Path jobAttemptPath = committer.getJobAttemptPath(jobContext);
    FileSystem fs = jobAttemptPath.getFileSystem(queryContext.getConf());
    if (!fs.exists(jobAttemptPath) || fs.listStatus(jobAttemptPath) == null) {
      LOG.warn("No query attempt file in " + jobAttemptPath);
      return finalOutputDir;
    }
    committer.commitJob(jobContext);

    String tableName = tableDesc.getMeta().getOption(HBaseStorageManager.META_TABLE_KEY);

    HTable htable = new HTable(hbaseConf, tableName);
    try {
      LoadIncrementalHFiles loadIncrementalHFiles = null;
      try {
        loadIncrementalHFiles = new LoadIncrementalHFiles(hbaseConf);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new IOException(e.getMessage(), e);
      }
      loadIncrementalHFiles.doBulkLoad(finalOutputDir, htable);

      return finalOutputDir;
    } finally {
      htable.close();
    }
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc,
                                          Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange)
      throws IOException {
    int[] sortKeyIndexes = new int[sortSpecs.length];
    for (int i = 0; i < sortSpecs.length; i++) {
      sortKeyIndexes[i] = inputSchema.getColumnId(sortSpecs[i].getSortKey().getQualifiedName());
    }

    ColumnMapping columnMapping = new ColumnMapping(tableDesc.getSchema(), tableDesc.getMeta());
    Configuration hbaseConf = HBaseStorageManager.getHBaseConfiguration(queryContext.getConf(), tableDesc.getMeta());

    HTable htable = new HTable(hbaseConf, columnMapping.getHbaseTableName());
    try {
      byte[][] endKeys = htable.getEndKeys();
      if (endKeys.length == 1) {
        return new TupleRange[]{dataRange};
      }
      List<TupleRange> tupleRanges = new ArrayList<TupleRange>(endKeys.length);

      TupleComparator comparator = new TupleComparator(inputSchema, sortSpecs);
      Tuple previousTuple = new VTuple(sortSpecs.length);
      for (int i = 0; i < sortSpecs.length; i++) {
        previousTuple.put(i, NullDatum.get());
      }

      for (byte[] eachEndKey: endKeys) {
        Tuple endTuple = new VTuple(sortSpecs.length);
        byte[][] rowKeyFields;
        if (sortSpecs.length > 1) {
          rowKeyFields = BytesUtils.splitPreserveAllTokens(eachEndKey, columnMapping.getRowKeyDelimiter());
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
        if (comparator.compare(dataRange.getStart(), endTuple) < 0) {
          if (comparator.compare(dataRange.getStart(), previousTuple) >= 0) {
            previousTuple = dataRange.getStart();
          }
          tupleRanges.add(new TupleRange(sortSpecs, previousTuple, endTuple));
          previousTuple = endTuple;
        }
      }

      // Last region endkey is empty. Tajo ignores empty key, so endkey is replaced with max data value.
      if (comparator.compare(dataRange.getEnd(), tupleRanges.get(tupleRanges.size() - 1).getStart()) >= 0) {
        tupleRanges.get(tupleRanges.size() - 1).setEnd(dataRange.getEnd());
      } else {
        tupleRanges.remove(tupleRanges.size() - 1);
      }
      return tupleRanges.toArray(new TupleRange[]{});
    } finally {
      htable.close();
    }
  }

  @Override
  public Column[] getIndexColumns(TableDesc tableDesc) throws IOException {
    List<Column> indexColumns = new ArrayList<Column>();

    ColumnMapping columnMapping = new ColumnMapping(tableDesc.getSchema(), tableDesc.getMeta());

    boolean[] isRowKeys = columnMapping.getIsRowKeyMappings();
    for (int i = 0; i < isRowKeys.length; i++) {
      if (isRowKeys[i]) {
        indexColumns.add(tableDesc.getSchema().getColumn(i));
      }
    }

    return indexColumns.toArray(new Column[]{});
  }

  @Override
  public StorageProperty getStorageProperty() {
    StorageProperty storageProperty = new StorageProperty();
    storageProperty.setSortedInsert(true);
    storageProperty.setSupportsInsertInto(true);
    return storageProperty;
  }

  public void verifyTableCreation(LogicalNode node) throws IOException {
    if (node.getType() == NodeType.CREATE_TABLE) {
      CreateTableNode cNode = (CreateTableNode)node;
      if (!cNode.isExternal()) {
        TableMeta tableMeta = new TableMeta(cNode.getStorageType(), cNode.getOptions());
        createTable(tableMeta, cNode.getTableSchema(), cNode.isExternal(), cNode.isIfNotExists());
      }
    }
  }

  @Override
  public void queryFailed(LogicalNode node) throws IOException {
    if (node.getType() == NodeType.CREATE_TABLE) {
      CreateTableNode cNode = (CreateTableNode)node;
      if (cNode.isExternal()) {
        return;
      }
      TableMeta tableMeta = new TableMeta(cNode.getStorageType(), cNode.getOptions());
      HBaseAdmin hAdmin =  new HBaseAdmin(getHBaseConfiguration(conf, tableMeta));

      try {
        HTableDescriptor hTableDesc = parseHTableDescriptor(tableMeta, cNode.getTableSchema());
        LOG.info("Delete table cause query failed:" + hTableDesc.getName());
        hAdmin.disableTable(hTableDesc.getName());
        hAdmin.deleteTable(hTableDesc.getName());
      } finally {
        hAdmin.close();
      }
    }
  }
}
