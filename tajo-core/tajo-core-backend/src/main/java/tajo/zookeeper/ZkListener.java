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

package tajo.zookeeper;

public interface ZkListener {
  /**
   * Called when a new node has been created.
   * @param path full path of the new node
   */
  public void nodeCreated(String path);

  /**
   * Called when a node has been deleted
   * @param path full path of the deleted node
   */
  public void nodeDeleted(String path);

  /**
   * Called when an existing node has changed data.
   * @param path full path of the updated node
   */
  public void nodeDataChanged(String path);

  /**
   * Called when an existing node has a child node added or removed.
   * @param path full path of the node whose children have changed
   */
  public void nodeChildrenChanged(String path);
}
