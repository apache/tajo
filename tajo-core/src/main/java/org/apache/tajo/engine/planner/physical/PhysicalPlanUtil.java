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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.PhysicalPlanningException;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.NodeType;
import org.apache.tajo.plan.logical.PersistentStoreNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.storage.BaseTupleComparator;
import org.apache.tajo.storage.FileTablespace;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

public class PhysicalPlanUtil {
  public static <T extends PhysicalExec> T findExecutor(PhysicalExec plan, Class<? extends PhysicalExec> clazz)
      throws PhysicalPlanningException {
    return (T) new FindVisitor().visit(plan, new Stack<>(), clazz);
  }

  public static TupleComparator [] getComparatorsFromJoinQual(EvalNode joinQual, Schema leftSchema, Schema rightSchema) {
    SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(joinQual, leftSchema, rightSchema);
    BaseTupleComparator[] comparators = new BaseTupleComparator[2];
    comparators[0] = new BaseTupleComparator(leftSchema, sortSpecs[0]);
    comparators[1] = new BaseTupleComparator(rightSchema, sortSpecs[1]);
    return comparators;
  }

  /**
   * Listing table data file which is not empty.
   * If the table is a partitioned table, return file list which has same partition key.
   * @param tajoConf
   * @param tableDesc
   * @param fileIndex
   * @param numResultFiles
   * @return
   * @throws java.io.IOException
   */
  public static CatalogProtos.FragmentProto[] getNonZeroLengthDataFiles(TajoConf tajoConf,TableDesc tableDesc,
                                                          int fileIndex, int numResultFiles) throws IOException {
    Path path = new Path(tableDesc.getUri());
    FileSystem fs = path.getFileSystem(tajoConf);

    //In the case of partitioned table, we should return same partition key data files.
    int partitionDepth = 0;
    if (tableDesc.hasPartition()) {
      partitionDepth = tableDesc.getPartitionMethod().getExpressionSchema().getRootColumns().size();
    }

    List<FileStatus> nonZeroLengthFiles = new ArrayList<>();
    if (fs.exists(path)) {
      getNonZeroLengthDataFiles(fs, path, nonZeroLengthFiles, fileIndex, numResultFiles,
          new AtomicInteger(0), tableDesc.hasPartition(), 0, partitionDepth);
    }

    List<FileFragment> fragments = new ArrayList<>();


    String[] previousPartitionPathNames = null;
    for (FileStatus eachFile: nonZeroLengthFiles) {
      FileFragment fileFragment = new FileFragment(tableDesc.getName(), eachFile.getPath(), 0, eachFile.getLen(), null);

      if (partitionDepth > 0) {
        // finding partition key;
        Path filePath = fileFragment.getPath();
        Path parentPath = filePath;
        String[] parentPathNames = new String[partitionDepth];
        for (int i = 0; i < partitionDepth; i++) {
          parentPath = parentPath.getParent();
          parentPathNames[partitionDepth - i - 1] = parentPath.getName();
        }

        // If current partitionKey == previousPartitionKey, add to result.
        if (previousPartitionPathNames == null) {
          fragments.add(fileFragment);
        } else if (previousPartitionPathNames != null && Arrays.equals(previousPartitionPathNames, parentPathNames)) {
          fragments.add(fileFragment);
        } else {
          break;
        }
        previousPartitionPathNames = parentPathNames;
      } else {
        fragments.add(fileFragment);
      }
    }
    return FragmentConvertor.toFragmentProtoArray(tajoConf, fragments.toArray(new Fragment[fragments.size()]));
  }

  /**
   *
   * @param fs
   * @param path The table path
   * @param result The final result files to be used
   * @param startFileIndex
   * @param numResultFiles
   * @param currentFileIndex
   * @param partitioned A flag to indicate if this table is partitioned
   * @param currentDepth Current visiting depth of partition directories
   * @param maxDepth The partition depth of this table
   * @throws IOException
   */
  private static void getNonZeroLengthDataFiles(FileSystem fs, Path path, List<FileStatus> result,
                                         int startFileIndex, int numResultFiles,
                                         AtomicInteger currentFileIndex, boolean partitioned,
                                         int currentDepth, int maxDepth) throws IOException {
    // Intermediate directory
    if (fs.isDirectory(path)) {
      FileStatus[] files = fs.listStatus(path, FileTablespace.hiddenFileFilter);
      if (files != null && files.length > 0) {

        for (FileStatus eachFile : files) {

          // checking if the enough number of files are found
          if (result.size() >= numResultFiles) {
            return;
          }

          if (eachFile.isDirectory()) {
            getNonZeroLengthDataFiles(
                fs,
                eachFile.getPath(),
                result,
                startFileIndex,
                numResultFiles,
                currentFileIndex,
                partitioned,
                currentDepth + 1, // increment a visiting depth
                maxDepth);


            // if partitioned table, we should ignore files located in the intermediate directory.
            // we can ensure that this file is in leaf directory if currentDepth == maxDepth.
          } else if (eachFile.isFile() && eachFile.getLen() > 0 && (!partitioned || currentDepth == maxDepth)) {
            if (currentFileIndex.get() >= startFileIndex) {
              result.add(eachFile);
            }
            currentFileIndex.incrementAndGet();
          }
        }
      }

      // Files located in leaf directory
    } else {
      FileStatus fileStatus = fs.getFileStatus(path);
      if (fileStatus != null && fileStatus.getLen() > 0) {
        if (currentFileIndex.get() >= startFileIndex) {
          result.add(fileStatus);
        }
        currentFileIndex.incrementAndGet();
        if (result.size() >= numResultFiles) {
          return;
        }
      }
    }
  }

  private static class FindVisitor extends BasicPhysicalExecutorVisitor<Class<? extends PhysicalExec>, PhysicalExec> {
    public PhysicalExec visit(PhysicalExec exec, Stack<PhysicalExec> stack, Class<? extends PhysicalExec> target)
        throws PhysicalPlanningException {
      if (target.isAssignableFrom(exec.getClass())) {
        return exec;
      } else {
        return super.visit(exec, stack, target);
      }
    }
  }

  /**
   * Set nullChar to TableMeta according to data format
   *
   * @param meta TableMeta
   * @param nullChar A character for NULL representation
   */
  private static void setNullCharForTextSerializer(TableMeta meta, String nullChar) {
    String dataFormat = meta.getDataFormat();
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.TEXT)) {
      meta.putProperty(StorageConstants.TEXT_NULL, nullChar);
    }  else if (dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE)) {
      meta.putProperty(StorageConstants.RCFILE_NULL, nullChar);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) {
      meta.putProperty(StorageConstants.TEXT_NULL, nullChar);
    }
  }

  /**
   * Check if TableMeta contains NULL char property according to data format
   *
   * @param meta Table Meta
   * @return True if TableMeta contains NULL char property according to data format
   */
  public static boolean containsNullChar(TableMeta meta) {
    String dataFormat = meta.getDataFormat();
    if (dataFormat.equalsIgnoreCase(BuiltinStorages.TEXT)) {
      return meta.containsProperty(StorageConstants.TEXT_NULL);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE)) {
      return meta.containsProperty(StorageConstants.RCFILE_NULL);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) {
      if (!meta.containsProperty(StorageConstants.TEXT_NULL)
        && meta.containsProperty(StorageConstants.SEQUENCEFILE_NULL)) {
        return meta.containsProperty(StorageConstants.SEQUENCEFILE_NULL);
      } else {
        return meta.containsProperty(StorageConstants.TEXT_NULL);
      }
    } else {
      return false;
    }
  }

  /**
   * Set session variable null char TableMeta if necessary
   *
   * @param context QueryContext
   * @param plan StoreTableNode
   * @param meta TableMeta
   */
  public static void setNullCharIfNecessary(QueryContext context, PersistentStoreNode plan, TableMeta meta) {
    if (plan.getType() != NodeType.INSERT) {
      // table property in TableMeta is the first priority, and session is the second priority
      if (!containsNullChar(meta) && context.containsKey(SessionVars.NULL_CHAR)) {
        setNullCharForTextSerializer(meta, context.get(SessionVars.NULL_CHAR));
      }
    }
  }

  /**
   * A pretty print string of a physical operator and its descendant operators.
   *
   */
  public static void printPhysicalPlan(PhysicalExec exec, StringBuilder builder, int depth) {

    String pad = "  ";
    if (exec instanceof UnaryPhysicalExec) {
      UnaryPhysicalExec operator = TUtil.checkTypeAndGet(exec, UnaryPhysicalExec.class);

      builder.append(StringUtils.repeat(pad, depth)).append(operator.toString());
      builder.append("\n");

      if (operator.getChild() != null) {
        printPhysicalPlan(operator.getChild(), builder, ++depth);
      }
    } else if (exec instanceof BinaryPhysicalExec) {
      BinaryPhysicalExec operator = TUtil.checkTypeAndGet(exec, BinaryPhysicalExec.class);

      builder.append(StringUtils.repeat(pad, depth)).append(operator.getClass().getSimpleName());
      builder.append("\n");

      if (operator.getLeftChild() != null) {
        printPhysicalPlan(operator.getLeftChild(), builder, depth + 1);
      }

      if (operator.getRightChild() != null) {
        printPhysicalPlan(operator.getRightChild(), builder, depth + 1);
      }
    } else if (exec == null) {
      return;
    } else {
      builder.append(StringUtils.repeat(pad, depth)).append(exec.toString());
      builder.append("\n");
    }
  }
}
