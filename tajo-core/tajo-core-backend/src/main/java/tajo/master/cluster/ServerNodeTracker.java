/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.master.cluster;

import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkNodeTracker;

/**
 * This class is an utility class for a server mapping to
 * a specific znode in zookeeper.*
 */
public class ServerNodeTracker extends ZkNodeTracker {

  public ServerNodeTracker(ZkClient client, String znodePath) {
    super(client, znodePath);
  }

  public ServerName getServerAddress() {
    byte[] data = super.getData();
    return data == null ? null : new ServerName(new String(data));
  }

  public synchronized ServerName waitForServer(long timeout)
      throws InterruptedException {
    byte[] data = super.blockUntilAvailable();
    return data == null ? null : new ServerName(new String(data));
  }
}
