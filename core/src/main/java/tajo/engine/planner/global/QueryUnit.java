/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner.global;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import tajo.QueryIdFactory;
import tajo.QueryUnitAttemptId;
import tajo.QueryUnitId;
import tajo.catalog.Schema;
import tajo.catalog.statistics.TableStat;
import tajo.engine.MasterWorkerProtos.Partition;
import tajo.engine.MasterWorkerProtos.QueryStatus;
import tajo.ipc.protocolrecords.Fragment;
import tajo.engine.planner.logical.*;
import tajo.master.AbstractQuery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Map.Entry;

public class QueryUnit extends AbstractQuery {

	private QueryUnitId id;
	private StoreTableNode store = null;
	private LogicalNode plan = null;
	private List<ScanNode> scan;
	
	private Map<String, Fragment> fragMap;
	private Map<String, Set<URI>> fetchMap;
	
  private List<Partition> partitions;
	private TableStat stats;

  private Map<Integer, QueryUnitAttempt> attemptMap;
  private Integer lastAttemptId;
  private QueryStatus status;

	public QueryUnit(QueryUnitId id) {
		this.id = id;
		scan = new ArrayList<ScanNode>();
    fetchMap = Maps.newHashMap();
    fragMap = Maps.newHashMap();
    partitions = new ArrayList<Partition>();
    attemptMap = Maps.newConcurrentMap();
    lastAttemptId = -1;
	}
	
	public void setLogicalPlan(LogicalNode plan) {
    Preconditions.checkArgument(plan.getType() == ExprType.STORE ||
        plan.getType() == ExprType.CREATE_INDEX);
    
	  this.plan = plan;
	  if (plan instanceof StoreTableNode) {
      store = (StoreTableNode) plan;      
    } else {
      store = (StoreTableNode) ((IndexWriteNode)plan).getSubNode();
    }
	  LogicalNode node = plan;
	  ArrayList<LogicalNode> s = new ArrayList<LogicalNode>();
	  s.add(node);
	  while (!s.isEmpty()) {
	    node = s.remove(s.size()-1);
	    if (node instanceof UnaryNode) {
	      UnaryNode unary = (UnaryNode) node;
	      s.add(s.size(), unary.getSubNode());
	    } else if (node instanceof BinaryNode) {
	      BinaryNode binary = (BinaryNode) node;
	      s.add(s.size(), binary.getOuterNode());
	      s.add(s.size(), binary.getInnerNode());
	    } else if (node instanceof ScanNode) {
	      scan.add((ScanNode)node);
	    }
	  }
	}

  public void setFragment(String tableId, Fragment fragment) {
    this.fragMap.put(tableId, fragment);
  }
	
	public void addFetch(String tableId, String uri) throws URISyntaxException {
	  this.addFetch(tableId, new URI(uri));
	}
	
	public void addFetch(String tableId, URI uri) {
	  Set<URI> uris;
	  if (fetchMap.containsKey(tableId)) {
	    uris = fetchMap.get(tableId);
	  } else {
	    uris = Sets.newHashSet();
	  }
	  uris.add(uri);
    fetchMap.put(tableId, uris);
	}
	
	public void addFetches(String tableId, List<URI> urilist) {
	  Set<URI> uris;
    if (fetchMap.containsKey(tableId)) {
      uris = fetchMap.get(tableId);
    } else {
      uris = Sets.newHashSet();
    }
    uris.addAll(urilist);
    fetchMap.put(tableId, uris);
	}
	
	public void setFetches(Map<String, Set<URI>> fetches) {
	  this.fetchMap.clear();
	  this.fetchMap.putAll(fetches);
	}
	
  public Fragment getFragment(String tableId) {
    return this.fragMap.get(tableId);
  }

  public Collection<Fragment> getAllFragments() {
    return fragMap.values();
  }
	
	public LogicalNode getLogicalPlan() {
	  return this.plan;
	}
	
	public QueryUnitId getId() {
		return id;
	}
	
	public Collection<URI> getFetchHosts(String tableId) {
	  return fetchMap.get(tableId);
	}
	
	public Collection<Set<URI>> getFetches() {
	  return fetchMap.values();
	}
	
	public Collection<URI> getFetch(ScanNode scan) {
	  return this.fetchMap.get(scan.getTableId());
	}

	public String getOutputName() {
		return this.store.getTableName();
	}
	
	public Schema getOutputSchema() {
	  return this.store.getOutSchema();
	}
	
	public StoreTableNode getStoreTableNode() {
	  return this.store;
	}
	
	public ScanNode[] getScanNodes() {
	  return this.scan.toArray(new ScanNode[scan.size()]);
	}
	
	@Override
	public String toString() {
		String str = new String(plan.getType() + " \n");
		for (Entry<String, Fragment> e : fragMap.entrySet()) {
		  str += e.getKey() + " : ";
      str += e.getValue() + " ";
		}
		for (Entry<String, Set<URI>> e : fetchMap.entrySet()) {
      str += e.getKey() + " : ";
      for (URI t : e.getValue()) {
        str += t + " ";
      }
    }
		
		return str;
	}
	
	public void setStats(TableStat stats) {
	  this.stats = stats;
	}
	
	public void setPartitions(List<Partition> partitions) {
	  this.partitions = Collections.unmodifiableList(partitions);
	}
	
	public TableStat getStats() {
	  return this.stats;
	}
	
	public List<Partition> getPartitions() {
	  return this.partitions;
	}
	
	public int getPartitionNum() {
	  return this.partitions.size();
	}

  public QueryUnitAttempt newAttempt() {
    QueryUnitAttempt attempt = new QueryUnitAttempt(
        QueryIdFactory.newQueryUnitAttemptId(this.getId(),
            ++lastAttemptId), this);
    attempt.setStatus(QueryStatus.QUERY_NEW);
    this.attemptMap.put(attempt.getId().getId(), attempt);
    return attempt;
  }

  public QueryUnitAttempt getAttempt(QueryUnitAttemptId attemptId) {
    return this.getAttempt(attemptId.getId());
  }

  public QueryUnitAttempt getAttempt(int attempt) {
    return this.attemptMap.get(attempt);
  }

  public QueryUnitAttempt getLastAttempt() {
    return this.attemptMap.get(this.lastAttemptId);
  }

  public int getRetryCount () {
    return this.lastAttemptId;
  }
}
