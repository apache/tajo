/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package tajo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.conf.TajoConf;
import tajo.engine.utils.JVMClusterUtil;
import tajo.engine.utils.JVMClusterUtil.WorkerThread;
import tajo.master.TajoMaster;
import tajo.worker.Worker;

import java.io.IOException;
import java.util.List;

public class MiniTajoCluster {
	static final Log LOG = LogFactory.getLog(MiniTajoCluster.class);
	private TajoConf conf;
	public LocalTajoCluster engineCluster;
	
	public MiniTajoCluster(TajoConf conf, int numLeafServers) throws Exception {
		this.conf = conf;		
		init(numLeafServers);
	}
	
	private void init(int numLeafServers) throws Exception {
		try {
		engineCluster = new LocalTajoCluster(conf,numLeafServers);
		
		engineCluster.startup();
		} catch (IOException e) {
			shutdown();
			throw e;
		}
	}
	
	public WorkerThread startWorkers() throws IOException {
		final TajoConf newConf = new TajoConf(conf);
		
		WorkerThread t;
		
		t = engineCluster.addLeafServer(newConf, engineCluster.getWorkers().size());
		t.start();
		t.waitForServerOnline();
		
		return t;
	}
	
	public String abortWorker(int index) {
		Worker server = getWorker(index);
		LOG.info("Aborting " + server.toString());
		server.abort("Aborting for tests", new Exception("Trace info"));
		return server.toString();
	}
	
	public WorkerThread stopLeafServer(int index) {
		return stopLeafServer(index);
	}
	
	public WorkerThread stopLeafServer(int index, final boolean shutdownFS) {
		WorkerThread server = engineCluster.getWorkers().get(index);
		LOG.info("Stopping " +  server.toString());
		server.getWorker().shutdown("Stopping ls " + index);
		return server;
	}
	
	public JVMClusterUtil.MasterThread startMaster() throws Exception {
		TajoConf c = new TajoConf(conf);
		
		JVMClusterUtil.MasterThread t;
		
		
		t = engineCluster.addMaster(c, 0);
		t.start();
		t.waitForServerOnline();
		
		return t;		
	}
	
	public TajoMaster getMaster() {
		return this.engineCluster.getMaster();
	}
	
	public void join() {
		this.engineCluster.join();
	}
	
	public void shutdown() {
		if(this.engineCluster != null) {
			this.engineCluster.shutdown();
		}
	}
	
	public List<WorkerThread> getWorkerThreads() {
		return this.engineCluster.getWorkers();
	}
	
	public List<WorkerThread> getLiveWorkerThreads() {
		return this.engineCluster.getLiveLeafServers();
	}
	
	public Worker getWorker(int index) {
		return engineCluster.getLeafServer(index);
	}
	
	public WorkerThread addWorker() throws IOException {
	  return engineCluster.addLeafServer(conf, engineCluster.getClusterSize());
	}
	
	public void shutdownWorker(int idx) {
	  engineCluster.getLeafServer(idx).shutdown("Shutting down Normally");
	}
}
