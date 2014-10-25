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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.IndexPredication;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.Bytes;

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
  public void createTable(TableDesc tableDesc) throws IOException {
    TableMeta tableMeta = tableDesc.getMeta();

    String hbaseTableName = tableMeta.getOption(META_TABLE_KEY, "");
    if (hbaseTableName == null || hbaseTableName.trim().isEmpty()) {
      throw new IOException("HBase mapped table is required a '" + META_TABLE_KEY + "' attribute.");
    }
    TableName hTableName = TableName.valueOf(hbaseTableName);

    String columnMapping = tableMeta.getOption(META_COLUMNS_KEY, "");
    if (columnMapping != null && columnMapping.split(",").length > tableDesc.getSchema().size()) {
      throw new IOException("Columns property has more entry than Tajo table columns");
    }

    Configuration hConf = getHBaseConfiguration(conf, tableMeta);
    HBaseAdmin hAdmin =  new HBaseAdmin(hConf);

    try {
      if (tableDesc.isExternal()) {
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
          throw new IOException("HBase table [" + hbaseTableName + "] already exists.");
        }
        // Creating hbase table
        HTableDescriptor hTableDescriptor = parseHTableDescriptor(tableDesc);

        byte[][] splitKeys = getSplitKeys(conf, tableMeta);
        if (splitKeys == null) {
          hAdmin.createTable(hTableDescriptor);
        } else {
          hAdmin.createTable(hTableDescriptor, splitKeys);
        }
      }

      TableStats stats = new TableStats();
      stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
      tableDesc.setStats(stats);
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

  public static Configuration getHBaseConfiguration(TajoConf tajoConf, TableMeta tableMeta) throws IOException {
    String zkQuorum = tableMeta.getOption(META_ZK_QUORUM_KEY, "");
    if (zkQuorum == null || zkQuorum.trim().isEmpty()) {
      throw new IOException("HBase mapped table is required a '" + META_ZK_QUORUM_KEY + "' attribute.");
    }

    Configuration hbaseConf = (tajoConf == null) ? HBaseConfiguration.create() : HBaseConfiguration.create(tajoConf);
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);

    for (Map.Entry<String, String> eachOption: tableMeta.getOptions().getAllKeyValus().entrySet()) {
      String key = eachOption.getKey();
      if (key.startsWith(HConstants.ZK_CFG_PROPERTY_PREFIX)) {
        hbaseConf.set(key, eachOption.getValue());
      }
    }
    return hbaseConf;
  }

  public static HTableDescriptor parseHTableDescriptor(TableDesc tableDesc) throws IOException {
    TableMeta tableMeta = tableDesc.getMeta();

    String hbaseTableName = tableMeta.getOption(META_TABLE_KEY, "");
    if (hbaseTableName == null || hbaseTableName.trim().isEmpty()) {
      throw new IOException("HBase mapped table is required a '" + META_TABLE_KEY + "' attribute.");
    }
    TableName hTableName = TableName.valueOf(hbaseTableName);

    String columnMapping = tableMeta.getOption(META_COLUMNS_KEY, "");
    if (columnMapping != null && columnMapping.split(",").length > tableDesc.getSchema().size()) {
      throw new IOException("Columns property has more entry than Tajo table columns");
    }
    HTableDescriptor hTableDescriptor = new HTableDescriptor(hTableName);

    Collection<String> columnFamilies = getColumnFamilies(columnMapping);
    //If 'columns' attribute is empty, Tajo table columns are mapped to all HBase table column.
    if (columnFamilies.isEmpty()) {
      for (Column eachColumn: tableDesc.getSchema().getColumns()) {
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
      HTableDescriptor hTableDesc = parseHTableDescriptor(tableDesc);
      hAdmin.disableTable(hTableDesc.getName());
      hAdmin.deleteTable(hTableDesc.getName());
    } finally {
      hAdmin.close();
    }
  }

  @Override
  public Column[] getIndexableColumns(TableDesc tableDesc) throws IOException {
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
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc,
                                     List<IndexPredication> indexPredications) throws IOException {
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
      byte[] startRow = HConstants.EMPTY_START_ROW;
      byte[] stopRow = HConstants.EMPTY_END_ROW;
      IndexPredication indexPredication = null;
      if (indexPredications != null && !indexPredications.isEmpty()) {
        // Currently only supports rowkey
        indexPredication = indexPredications.get(0);

        if (indexPredication.getStartValue() != null) {
          startRow = serialize(columnMapping, indexPredication, indexPredication.getStartValue());
        }
        if (indexPredication.getStopValue() != null) {
          stopRow = serialize(columnMapping, indexPredication, indexPredication.getStopValue());
        }
      }

      hAdmin =  new HBaseAdmin(hconf);
      Map<ServerName, ServerLoad> serverLoadMap = new HashMap<ServerName, ServerLoad>();

      List<Fragment> fragments = new ArrayList<Fragment>(keys.getFirst().length);
      for (int i = 0; i < keys.getFirst().length; i++) {
        HRegionLocation location = htable.getRegionLocation(keys.getFirst()[i], false);

        // determine if the given start an stop key fall into the region
        if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startRow, keys.getSecond()[i]) < 0)
            && (stopRow.length == 0 || Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
          byte[] fragmentStart = (startRow.length == 0 || Bytes.compareTo(keys.getFirst()[i], startRow) >= 0) ?
              keys.getFirst()[i] : startRow;

          byte[] fragmentStop = (stopRow.length == 0 || Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
              keys.getSecond()[i].length > 0 ? keys.getSecond()[i] : stopRow;

          String regionName = location.getRegionInfo().getRegionNameAsString();

          ServerLoad serverLoad = serverLoadMap.get(location.getServerName());
          if (serverLoad == null) {
            serverLoad = hAdmin.getClusterStatus().getLoad(location.getServerName());
            serverLoadMap.put(location.getServerName(), serverLoad);
          }

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

          fragments.add(fragment);
          if (LOG.isDebugEnabled()) {
            LOG.debug("getFragments: fragment -> " + i + " -> " + fragment);
          }
        }
      }

      if (!fragments.isEmpty()) {
        ((HBaseFragment)fragments.get(fragments.size() - 1)).setLast(true);
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

  @Override
  public boolean canCreateAsSelect(StoreType storeType) {
    return false;
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
}
