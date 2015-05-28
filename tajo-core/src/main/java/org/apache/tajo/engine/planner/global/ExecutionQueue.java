/**
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

package org.apache.tajo.engine.planner.global;

import org.apache.tajo.ExecutionBlockId;

// Retrieves execution blocks to run real works
public interface ExecutionQueue {

  /**
   * remaining blocks in queue
   *
   * @return number of blocks
   */
  int size();

  /**
   * return initial blocks to be run
   *
   * @return blocks to be run
   */
  ExecutionBlock[] first();

  /**
   * get next execution blocks to be run
   *
   * @param blockId currently finished id of execution block
   * @return null for finished, can return empty array
   */
  ExecutionBlock[] next(ExecutionBlockId blockId);
}
