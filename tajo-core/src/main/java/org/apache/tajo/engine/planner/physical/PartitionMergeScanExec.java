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

package org.apache.tajo.engine.planner.physical;

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/**
 * A Scanner that reads multiple partitions
 */
public class PartitionMergeScanExec extends ScanExec {
  private final ScanNode plan;
  private SeqScanExec currentScanner = null;

  private CatalogProtos.FragmentProto [] fragments;

  private List<SeqScanExec> scanners = Lists.newArrayList();
  private Iterator<SeqScanExec> iterator;

  private float progress;
  protected TableStats inputStats;

  public PartitionMergeScanExec(TaskAttemptContext context,
                                ScanNode plan, CatalogProtos.FragmentProto[] fragments) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());

    this.plan = plan;
    this.fragments = fragments;

    inputStats = new TableStats();
  }

  @Override
  public void init() throws IOException {
    for (CatalogProtos.FragmentProto fragment : fragments) {
      SeqScanExec scanExec = new SeqScanExec(context, (ScanNode) PlannerUtil.clone(null, plan),
          new CatalogProtos.FragmentProto[]{fragment});
      scanners.add(scanExec);
    }
    progress = 0.0f;
    initScanExecutors();
    super.init();
  }

  private void initScanExecutors() throws IOException {
    if (scanners.size() > 0) {
      iterator = scanners.iterator();
      currentScanner = iterator.next();
      currentScanner.init();
    }
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    while (!context.isStopped() && currentScanner != null) {
      tuple = currentScanner.next();

      if (tuple != null) {
        return tuple;
      }

      // since read tuple is null, close the current scanner.
      if (currentScanner != null) {
        currentScanner.close();
        currentScanner = null;
      }

      if (iterator.hasNext()) {
        currentScanner = iterator.next();
        currentScanner.init();
      } else {
        break;
      }
    }
    return null;
  }

  @Override
  public void rescan() throws IOException {
    for (SeqScanExec scanner : scanners) {
      scanner.close();
    }
    initScanExecutors();
  }

  @Override
  public void close() throws IOException {
    inputStats.reset();
    for (SeqScanExec scanner : scanners) {
      scanner.close();
      TableStats scannerTableStsts = scanner.getInputStats();
      if (scannerTableStsts != null) {
        inputStats.merge(scannerTableStsts);
      }
    }
    scanners.clear();
    iterator = null;
    progress = 1.0f;
  }

  @Override
  public String getTableName() {
    return plan.getTableName();
  }

  @Override
  public String getCanonicalName() {
    return plan.getCanonicalName();
  }

  @Override
  public CatalogProtos.FragmentProto[] getFragments() {
    return fragments;
  }

  @Override
  public float getProgress() {
    if (iterator != null) {
      float progressSum = 0.0f;
      for (SeqScanExec scanner : scanners) {
        progressSum += scanner.getProgress();
      }
      if (progressSum > 0) {
        // get a average progress - divide progress summary by the number of scanners
        return progressSum / (float)(scanners.size());
      } else {
        return 0.0f;
      }
    } else {
      return progress;
    }
  }

  @Override
  public TableStats getInputStats() {
    if (iterator != null) {
      inputStats.reset();
      for (SeqScanExec scanner : scanners) {
        TableStats scannerTableStats = scanner.getInputStats();
        if (scannerTableStats != null) {
          inputStats.merge(scannerTableStats);
        }
      }
    }
    return inputStats;
  }
}
