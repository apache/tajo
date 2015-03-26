/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.elasticsearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hwjeong on 15. 3. 18..
 */
public class ElasticsearchStorageManager extends StorageManager {
  private static final Log LOG = LogFactory.getLog(ElasticsearchStorageManager.class);
  private Map<String, Client> clientMap = new HashMap<String, Client>();
  private int fragmentLength = 0;

  public ElasticsearchStorageManager(StoreType storeType) {
    super(storeType);
  }

  @Override
  protected void storageInit() throws IOException {
  }

  @Override
  public StorageProperty getStorageProperty() {
    return null;
  }

  @Override
  public void closeStorageManager() {
    synchronized (clientMap) {
      for ( Map.Entry<String, Client> eachClient : clientMap.entrySet() ) {
        try {
          eachClient.getValue().close();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        } finally {
        }
      }
    }
  }

  @Override
  public void rollbackOutputCommit(LogicalNode node) throws IOException {
  }

  @Override
  public void beforeInsertOrCATS(LogicalNode node) throws IOException {
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
    return new TupleRange[0];
  }

  @Override
  public List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numFragments) throws IOException {
    if ( 0 < currentPage && fragmentLength <= currentPage ) { // check condition of the end.
      return new ArrayList<Fragment>(1);
    }

    ElasticsearchWithOptionInfo opt = new ElasticsearchWithOptionInfo(tableDesc.getMeta());
    Client client = null;

    List<Fragment> fragments = new ArrayList<Fragment>();

    try {
      client = getClient(opt);
      ClusterAdminClient cluster = client.admin().cluster();
      IndicesAdminClient indices = client.admin().indices();
      ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
      ClusterStateResponse clusterStateResponse;
      IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
      IndicesStatsResponse indicesStatsResponse;

      indicesStatsRequest.indices(opt.index()).types(opt.type());
      clusterStateResponse = cluster.state(clusterStateRequest).actionGet();
      indicesStatsResponse = indices.stats(indicesStatsRequest).actionGet();

      // have to reduce a method call.
      String index = opt.index();
      String type = opt.type();
      String nodes = opt.nodes();
      int fetchSize = opt.fetchSize();

      for (ShardRouting shard : clusterStateResponse.getState().routingTable().allShards()) {
        CommonStats shardStats = indicesStatsResponse.asMap().get(shard);

        if ( shard.primary() && shard.assignedToNode() ) {
          int shardId = shard.id();
          long docCount = shardStats.getDocs().getCount();

          if ( docCount <= fetchSize ) {
            ElasticsearchFragment fragment = new ElasticsearchFragment(tableDesc.getName(), index, type, nodes, shardId, 0, fetchSize);
            fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
            fragments.add(fragment);
          } else {
            for ( int offset=0; offset<docCount; offset+=fetchSize ) {
              ElasticsearchFragment fragment = new ElasticsearchFragment(tableDesc.getName(), index, type, nodes, shardId, offset, fetchSize);
              fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
              fragments.add(fragment);
            }
          }
        }
      }

      fragmentLength = fragments.size();

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
    }

    return fragments;
  }

  @Override
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc, ScanNode scanNode) throws IOException {
    ElasticsearchWithOptionInfo opt = new ElasticsearchWithOptionInfo(tableDesc.getMeta());
    Client client = null;

    List<Fragment> fragments = new ArrayList<Fragment>();

    try {
      client = getClient(opt);
      ClusterAdminClient cluster = client.admin().cluster();
      IndicesAdminClient indices = client.admin().indices();
      ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
      ClusterStateResponse clusterStateResponse;
      IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
      IndicesStatsResponse indicesStatsResponse;

      indicesStatsRequest.indices(opt.index()).types(opt.type());
      clusterStateResponse = cluster.state(clusterStateRequest).actionGet();
      indicesStatsResponse = indices.stats(indicesStatsRequest).actionGet();

      String index = opt.index();
      String type = opt.type();
      String nodes = opt.nodes();
      int fetchSize = opt.fetchSize();

      for (ShardRouting shard : clusterStateResponse.getState().routingTable().allShards()) {
        CommonStats shardStats = indicesStatsResponse.asMap().get(shard);

        if ( shard.primary() && shard.assignedToNode() ) {
          int shardId = shard.id();
          long docCount = shardStats.getDocs().getCount();

          if ( docCount <= fetchSize ) {
            ElasticsearchFragment fragment = new ElasticsearchFragment(tableDesc.getName(), index, type, nodes, shardId, 0, fetchSize);
            fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
            fragments.add(fragment);
          } else {
            for ( int offset=0; offset<docCount; offset+=fetchSize ) {
              ElasticsearchFragment fragment = new ElasticsearchFragment(tableDesc.getName(), index, type, nodes, shardId, offset, fetchSize);
              fragment.setLength(TajoConstants.UNKNOWN_LENGTH);
              fragments.add(fragment);
            }
          }
        }
      }

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
    }

    return fragments;
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException {
  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {
    ElasticsearchWithOptionInfo opt = new ElasticsearchWithOptionInfo(tableDesc.getMeta());
    Client client = null;
    long numRows = TajoConstants.UNKNOWN_ROW_NUMBER;

    try {
      client = getClient(opt);
      IndicesAdminClient indices = client.admin().indices();
      IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
      IndicesStatsResponse indicesStatsResponse;

      indicesStatsRequest.indices(opt.index()).types(opt.type()).docs(true);
      indicesStatsResponse = indices.stats(indicesStatsRequest).actionGet();

      String index = opt.index();

      for (Map.Entry<String, IndexStats> eachIndex : indicesStatsResponse.getIndices().entrySet() ) {
        String key = eachIndex.getKey();
        IndexStats indexStats = eachIndex.getValue();
        CommonStats commonStats = indexStats.getTotal();

        if ( index.equals(key) ) {
          numRows = commonStats.docs.getCount();
          break;
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
    }

    TableStats stats = new TableStats();
    stats.setNumRows(numRows);
    tableDesc.setStats(stats);
  }

  public Client getClient(ElasticsearchWithOptionInfo opt) throws Exception {
    synchronized (clientMap) {
      Client client = clientMap.get(opt.cluster());

      if ( client == null ) {
        Settings settings = getSettings(opt);
        client = buildClient(settings, opt.nodes());
        clientMap.put(opt.cluster(), client);
      }

      return client;
    }
  }

  public Settings getSettings(ElasticsearchWithOptionInfo opt) throws Exception {
    Settings settings;

    settings = ImmutableSettings
        .settingsBuilder()
        .put("cluster.name",  opt.cluster())
        .put("client.transport.sniff", true)
        .put("network.tcp.blocking", false)
        .put("client.transport.ping_timeout", opt.pingTimeout())
        .put("transport.tcp.connect_timeout", opt.connectTimeout())
        .put("transport.connections_per_node.recovery", opt.threadPoolRecovery())
        .put("transport.connections_per_node.bulk", opt.threadPoolBulk())
        .put("transport.connections_per_node.reg", opt.threadPoolReg())
        .build();

    return settings;
  }

  protected Client buildClient(Settings settings, String nodes) throws Exception {
    TransportClient client = new TransportClient(settings);

    String[] nodeList = nodes.split(ElasticsearchConstants.NODES_DELIMITER);
    int nodeSize = nodeList.length;

    for (int i = 0; i < nodeSize; i++) {
      client.addTransportAddress(toAddress(nodeList[i]));
    }

    return client;
  }

  protected InetSocketTransportAddress toAddress(String address) {
    if (address == null) return null;

    String[] splitted = address.split(ElasticsearchConstants.HOST_DELIMITER);
    int port = ElasticsearchConstants.THRANSPORT_PORT;

    if (splitted.length > 1) {
      port = Integer.parseInt(splitted[1]);
    }

    return new InetSocketTransportAddress(splitted[0], port);
  }
}
