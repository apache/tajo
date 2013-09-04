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

package org.apache.tajo.engine.planner;

import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * It provides a graph data structure for query blocks and their relations.
 */
public class QueryBlockGraph {
  private Map<String, List<BlockEdge>> blockEdges = new HashMap<String, List<BlockEdge>>();

  public QueryBlockGraph() {
  }

  public int size() {
    return blockEdges.size();
  }

  public void connectBlocks(String srcBlock, String targetBlock, BlockType type) {
    BlockEdge newBlockEdge = new BlockEdge(targetBlock, type);
    if (blockEdges.containsKey(srcBlock)) {
      blockEdges.get(srcBlock).add(newBlockEdge);
    } else {
      blockEdges.put(srcBlock, TUtil.newList(newBlockEdge));
    }
  }

  public Collection<BlockEdge> getBlockEdges(String srcBlock) {
    return blockEdges.get(srcBlock);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, List<BlockEdge>> entry : blockEdges.entrySet()) {
      sb.append(entry.getKey()).append("\n");
      for (BlockEdge edge : entry.getValue()) {
        sb.append(" |- ");
        sb.append(edge.getTargetBlock()).append(" (").append(edge.getBlockType()).append(")");
      }
      sb.append("\n");
    }

    return sb.toString();
  }

  public static enum BlockType {
    TableSubQuery,
    ScalarSubQuery
  }

  public static class BlockEdge {
    private String targetBlock;
    private BlockType blockType;

    public BlockEdge(String targetBlock, BlockType blockType) {
      this.targetBlock = targetBlock;
      this.blockType = blockType;
    }

    public String getTargetBlock() {
      return targetBlock;
    }

    public BlockType getBlockType() {
      return blockType;
    }
  }
}
