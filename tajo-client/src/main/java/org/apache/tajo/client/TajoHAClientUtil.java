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

package org.apache.tajo.client;

import com.google.protobuf.ServiceException;
import org.apache.tajo.cli.tsql.TajoCli.TajoCliContext;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ha.HAServiceUtil;

import java.io.IOException;

public class TajoHAClientUtil {
  /**
   * In TajoMaster HA mode, if TajoCli can't connect existing active master,
   * this should try to connect new active master.
   *
   * @param conf
   * @param client
   * @return
   * @throws IOException
   * @throws ServiceException
   */
  public static TajoClient getTajoClient(TajoConf conf, TajoClient client)
      throws IOException, ServiceException {
    return getTajoClient(conf, client, null);
  }

  public static TajoClient getTajoClient(TajoConf conf, TajoClient client,
      TajoCliContext context) throws IOException, ServiceException {

    if (conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {

      if (!HAServiceUtil.isMasterAlive(conf.getVar(
        TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS), conf)) {
        TajoClient tajoClient = null;
        String baseDatabase = client.getBaseDatabase();
        conf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS,
          HAServiceUtil.getMasterClientName(conf));
        client.close();
        tajoClient = new TajoClientImpl(conf, baseDatabase);

        if (context != null && context.getCurrentDatabase() != null) {
          tajoClient.selectDatabase(context.getCurrentDatabase());
        }
        return tajoClient;
      } else {
        return client;
      }
    } else {
      return client;
    }
  }
}
