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

package org.apache.tajo.pullserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.CompositeService;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.pullserver.PullServerAuxService.PullServer;
import org.apache.tajo.util.StringUtils;

public class TajoPullServer extends CompositeService {
  private static final Log LOG = LogFactory.getLog(TajoPullServer.class);

  private TajoPullServerService pullService;
  private TajoConf systemConf;

  public TajoPullServer() {
    super(TajoPullServer.class.getName());
  }

  @Override
  public void init(Configuration conf) {
    this.systemConf = (TajoConf)conf;
    pullService = new TajoPullServerService();
    addService(pullService);

    super.init(conf);
  }

  public void startPullServer(TajoConf systemConf) {
    init(systemConf);
    start();
  }

  public void start() {
    super.start();

  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(PullServer.class, args, LOG);

    if (!TajoPullServerService.isStandalone()) {
      LOG.fatal("TAJO_PULLSERVER_STANDALONE env variable is not 'true'");
      return;
    }

    TajoConf tajoConf = new TajoConf();
    tajoConf.addResource(new Path(TajoConstants.SYSTEM_CONF_FILENAME));

    (new TajoPullServer()).startPullServer(tajoConf);
  }
}
