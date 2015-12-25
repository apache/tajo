/*
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

package org.apache.tajo.plan.rewrite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.visitor.BasicLogicalPlanVisitor;
import org.apache.tajo.storage.StorageService;

import java.util.Stack;

public class TableStatUpdateRewriter implements LogicalPlanRewriteRule {
  private static final Log LOG = LogFactory.getLog(TableStatUpdateRewriter.class);

  private static final String NAME = "Table Stat Updater";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlanRewriteRuleContext context) {
    return true;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlanRewriteRuleContext context) throws TajoException {
    LogicalPlan plan = context.getPlan();
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();

    Rewriter r = new Rewriter(context.getStorage());
    r.visit(rootBlock, plan, rootBlock, rootBlock.getRoot(), new Stack<>());
    return plan;
  }

  private final class Rewriter extends BasicLogicalPlanVisitor<Object, Object> {
    private final StorageService storage;


    private Rewriter(StorageService storage) {
      this.storage = storage;
    }

    @Override
    public Object visitScan(Object object, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode scanNode,
                            Stack<LogicalNode> stack) throws TajoException {
      updatePhysicalInfo(scanNode.getTableDesc());
      return scanNode;
    }

    private void updatePhysicalInfo(TableDesc desc) {
      // FAKEFILE is used for test
      if (!desc.getMeta().getDataFormat().equals("SYSTEM") && !desc.getMeta().getDataFormat().equals("FAKEFILE")) {
        try {
          if (desc.getStats() != null) {
            desc.getStats().setNumBytes(storage.getTableVolumn(desc.getUri()));
          }
        } catch (UnsupportedException t) {
          LOG.warn(desc.getName() + " does not support Tablespace::getTableVolume()");
          // -1 means unknown volume size.
          desc.getStats().setNumBytes(-1);
        }
      }
    }
  }
}
