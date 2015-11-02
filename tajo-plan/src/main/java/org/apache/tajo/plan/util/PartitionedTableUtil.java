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

package org.apache.tajo.plan.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.*;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class PartitionedTableUtil {
  private static final Log LOG = LogFactory.getLog(PartitionedTableUtil.class);

  public static FilteredPartitionInfo findFilteredPartitionInfo(CatalogService catalog, TajoConf conf,
    ScanNode scanNode) throws IOException, UndefinedDatabaseException, UndefinedTableException,
    UndefinedPartitionMethodException, UndefinedOperatorException, UnsupportedException {
    TableDesc table = scanNode.getTableDesc();
    PartitionMethodDesc partitionDesc = scanNode.getTableDesc().getPartitionMethod();

    Schema paritionValuesSchema = new Schema();
    for (Column column : partitionDesc.getExpressionSchema().getRootColumns()) {
      paritionValuesSchema.addColumn(column);
    }

    Set<EvalNode> indexablePredicateSet = Sets.newHashSet();

    // if a query statement has a search condition, try to find indexable predicates
    if (scanNode.hasQual()) {
      EvalNode [] conjunctiveForms = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());
      // add qualifier to schema for qual
      paritionValuesSchema.setQualifier(scanNode.getCanonicalName());
      for (Column column : paritionValuesSchema.getRootColumns()) {
        for (EvalNode simpleExpr : conjunctiveForms) {
          if (checkIfIndexablePredicateOnTargetColumn(simpleExpr, column)) {
            indexablePredicateSet.add(simpleExpr);
          }
        }
      }
    }

    if (indexablePredicateSet.size() > 0) { // There are at least one indexable predicates
      return findFilteredPartitionInfo(catalog, conf, table.getName(), paritionValuesSchema,
        indexablePredicateSet.toArray(new EvalNode[indexablePredicateSet.size()]), new Path(table.getUri()), scanNode);
    } else { // otherwise, we will get all partition paths.
      return findFilteredPartitionInfo(catalog, conf, table.getName(), paritionValuesSchema, null,
        new Path(table.getUri()));
    }
  }

  private static boolean checkIfIndexablePredicateOnTargetColumn(EvalNode evalNode, Column targetColumn) {
    if (checkIfIndexablePredicate(evalNode) || checkIfDisjunctiveButOneVariable(evalNode)) {
      Set<Column> variables = EvalTreeUtil.findUniqueColumns(evalNode);
      // if it contains only single variable matched to a target column
      return variables.size() == 1 && variables.contains(targetColumn);
    } else {
      return false;
    }
  }

  /**
   * Check if an expression consists of one variable and one constant and
   * the expression is a comparison operator.
   *
   * @param evalNode The expression to be checked
   * @return true if an expression consists of one variable and one constant
   * and the expression is a comparison operator. Other, false.
   */
  private static boolean checkIfIndexablePredicate(EvalNode evalNode) {
    // TODO - LIKE with a trailing wild-card character and IN with an array can be indexable
    return AlgebraicUtil.containSingleVar(evalNode) && AlgebraicUtil.isIndexableOperator(evalNode);
  }

  /**
   *
   * @param evalNode The expression to be checked
   * @return true if an disjunctive expression, consisting of indexable expressions
   */
  private static boolean checkIfDisjunctiveButOneVariable(EvalNode evalNode) {
    if (evalNode.getType() == EvalType.OR) {
      BinaryEval orEval = (BinaryEval) evalNode;
      boolean indexable =
        checkIfIndexablePredicate(orEval.getLeftExpr()) &&
          checkIfIndexablePredicate(orEval.getRightExpr());

      boolean sameVariable =
        EvalTreeUtil.findUniqueColumns(orEval.getLeftExpr())
          .equals(EvalTreeUtil.findUniqueColumns(orEval.getRightExpr()));

      return indexable && sameVariable;
    } else {
      return false;
    }
  }

  private static FilteredPartitionInfo findFilteredPartitionInfo(CatalogService catalog, TajoConf conf,
    String tableName, Schema partitionColumns, EvalNode [] conjunctiveForms, Path tablePath) throws IOException,
    UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException, UndefinedOperatorException,
    UnsupportedException {
    return findFilteredPartitionInfo(catalog, conf, tableName, partitionColumns, conjunctiveForms, tablePath, null);
  }

  /**
   * It assumes that each conjunctive form corresponds to one column.
   *
   * @param partitionColumns
   * @param conjunctiveForms search condition corresponding to partition columns.
   *                         If it is NULL, it means that there is no search condition for this table.
   * @param tablePath
   * @return
   * @throws IOException
   */
  private static FilteredPartitionInfo findFilteredPartitionInfo(CatalogService catalog, TajoConf conf,
    String tableName, Schema partitionColumns, EvalNode [] conjunctiveForms, Path tablePath, ScanNode scanNode)
    throws IOException, UndefinedDatabaseException, UndefinedTableException, UndefinedPartitionMethodException,
    UndefinedOperatorException, UnsupportedException {

    FilteredPartitionInfo filteredPartitionInfo = null;
    Path[] filteredPaths = null;
    FileSystem fs = tablePath.getFileSystem(conf);
    String [] splits = CatalogUtil.splitFQTableName(tableName);
    List<CatalogProtos.PartitionDescProto> partitions = null;

    try {
      if (conjunctiveForms == null) {
        partitions = catalog.getPartitionsOfTable(splits[0], splits[1]);
        if (partitions.isEmpty()) {
          filteredPaths = findFilteredPathsFromFileSystem(partitionColumns, conjunctiveForms, fs, tablePath);
          filteredPartitionInfo = new FilteredPartitionInfo(filteredPaths);
          setFilteredPartitionInfo(filteredPartitionInfo, fs, partitionColumns);
        } else {
          filteredPartitionInfo = findFilteredPartitionInfoByPartitionDesc(partitions);
        }
      } else {
        if (catalog.existPartitions(splits[0], splits[1])) {
          CatalogProtos.PartitionsByAlgebraProto request = getPartitionsAlgebraProto(splits[0], splits[1],
            conjunctiveForms);
          partitions = catalog.getPartitionsByAlgebra(request);
          filteredPartitionInfo = findFilteredPartitionInfoByPartitionDesc(partitions);
        } else {
          filteredPaths = findFilteredPathsFromFileSystem(partitionColumns, conjunctiveForms, fs, tablePath);
          filteredPartitionInfo = new FilteredPartitionInfo(filteredPaths);
          setFilteredPartitionInfo(filteredPartitionInfo, fs, partitionColumns);
        }
      }
    } catch (UnsupportedException ue) {
      // Partial catalog might not allow some filter conditions. For example, HiveMetastore doesn't In statement,
      // regexp statement and so on. Above case, Tajo need to build filtered path by listing hdfs directories.
      LOG.warn(ue.getMessage());
      partitions = catalog.getPartitionsOfTable(splits[0], splits[1]);
      if (partitions.isEmpty()) {
        filteredPaths = findFilteredPathsFromFileSystem(partitionColumns, conjunctiveForms, fs, tablePath);
        filteredPartitionInfo = new FilteredPartitionInfo(filteredPaths);
        setFilteredPartitionInfo(filteredPartitionInfo, fs, partitionColumns);
      } else {
        filteredPartitionInfo = findFilteredPartitionInfoByPartitionDesc(partitions);
      }
      scanNode.setQual(AlgebraicUtil.createSingletonExprFromCNF(conjunctiveForms));
    }

    LOG.info("### Filtered directory or files: " + filteredPartitionInfo.getPartitionPaths().length +
      ", totalVolume:" + filteredPartitionInfo.getTotalVolume());

    return filteredPartitionInfo;
  }

  /**
   * Build list of partition path by PartitionDescProto which is generated from CatalogStore.
   *
   * @param partitions
   * @return
   */
  private static FilteredPartitionInfo findFilteredPartitionInfoByPartitionDesc(List<CatalogProtos.PartitionDescProto> 
    partitions) {
    long totalVolume = 0L;
    Path[] filteredPaths = new Path[partitions.size()];
    String[] partitionNames = new String[partitions.size()];
    for (int i = 0; i < partitions.size(); i++) {
      CatalogProtos.PartitionDescProto partition = partitions.get(i);
      filteredPaths[i] = new Path(partition.getPath());
      partitionNames[i] = partition.getPartitionName();
      totalVolume += partition.getNumBytes();
    }
    return new FilteredPartitionInfo(filteredPaths, partitionNames, totalVolume);
  }

  /**
   * Build list of partition path by filtering directories in the given table path.
   *
   *
   * @param partitionColumns
   * @param conjunctiveForms
   * @param fs
   * @param tablePath
   * @return
   * @throws IOException
   */
  private static Path[] findFilteredPathsFromFileSystem(Schema partitionColumns, EvalNode [] conjunctiveForms,
                                                  FileSystem fs, Path tablePath) throws IOException{
    Path[] filteredPaths = null;
    PathFilter[] filters;

    if (conjunctiveForms == null) {
      filters = buildAllAcceptingPathFilters(partitionColumns);
    } else {
      filters = buildPathFiltersForAllLevels(partitionColumns, conjunctiveForms);
    }

    // loop from one to the number of partition columns
    filteredPaths = toPathArray(fs.listStatus(tablePath, filters[0]));

    for (int i = 1; i < partitionColumns.size(); i++) {
      // Get all file status matched to a ith level path filter.
      filteredPaths = toPathArray(fs.listStatus(filteredPaths, filters[i]));
    }
    return filteredPaths;
  }

  /**
   * Build algebra expressions for querying partitions and partition keys by using EvalNodeToExprConverter.
   *
   * @param databaseName the database name
   * @param tableName the table name
   * @param conjunctiveForms EvalNode which contains filter conditions
   * @return
   */
  private static CatalogProtos.PartitionsByAlgebraProto getPartitionsAlgebraProto(
    String databaseName, String tableName, EvalNode [] conjunctiveForms) {

    CatalogProtos.PartitionsByAlgebraProto.Builder request = CatalogProtos.PartitionsByAlgebraProto.newBuilder();
    request.setDatabaseName(databaseName);
    request.setTableName(tableName);

    if (conjunctiveForms != null) {
      EvalNode evalNode = AlgebraicUtil.createSingletonExprFromCNF(conjunctiveForms);
      EvalNodeToExprConverter convertor = new EvalNodeToExprConverter(databaseName + "." + tableName);
      convertor.visit(null, evalNode, new Stack<>());
      request.setAlgebra(convertor.getResult().toJson());
    } else {
      request.setAlgebra("");
    }

    return request.build();
  }

  /**
   * Build path filters for all levels with a list of filter conditions.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Then, this methods will create three path filters for (col1), (col1, col2), (col1, col2, col3).
   *
   * Corresponding filter conditions will be placed on each path filter,
   * If there is no corresponding expression for certain column,
   * The condition will be filled with a true value.
   *
   * Assume that an user gives a condition WHERE col1 ='A' and col3 = 'C'.
   * There is no filter condition corresponding to col2.
   * Then, the path filter conditions are corresponding to the followings:
   *
   * The first path filter: col1 = 'A'
   * The second path filter: col1 = 'A' AND col2 IS NOT NULL
   * The third path filter: col1 = 'A' AND col2 IS NOT NULL AND col3 = 'C'
   *
   * 'IS NOT NULL' predicate is always true against the partition path.
   *
   * @param partitionColumns
   * @param conjunctiveForms
   * @return
   */
  private static PathFilter [] buildPathFiltersForAllLevels(Schema partitionColumns,
                                                            EvalNode [] conjunctiveForms) {
    // Building partition path filters for all levels
    Column target;
    PathFilter [] filters = new PathFilter[partitionColumns.size()];
    List<EvalNode> accumulatedFilters = Lists.newArrayList();
    for (int i = 0; i < partitionColumns.size(); i++) { // loop from one to level
      target = partitionColumns.getColumn(i);

      for (EvalNode expr : conjunctiveForms) {
        if (EvalTreeUtil.findUniqueColumns(expr).contains(target)) {
          // Accumulate one qual per level
          accumulatedFilters.add(expr);
        }
      }

      if (accumulatedFilters.size() < (i + 1)) {
        accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));
      }

      EvalNode filterPerLevel = AlgebraicUtil.createSingletonExprFromCNF(
        accumulatedFilters.toArray(new EvalNode[accumulatedFilters.size()]));
      filters[i] = new PartitionPathFilter(partitionColumns, filterPerLevel);
    }

    return filters;
  }

  /**
   * Build an array of path filters for all levels with all accepting filter condition.
   * @param partitionColumns The partition columns schema
   * @return The array of path filter, accpeting all partition paths.
   */
  public static PathFilter [] buildAllAcceptingPathFilters(Schema partitionColumns) {
    Column target;
    PathFilter [] filters = new PathFilter[partitionColumns.size()];
    List<EvalNode> accumulatedFilters = Lists.newArrayList();
    for (int i = 0; i < partitionColumns.size(); i++) { // loop from one to level
      target = partitionColumns.getColumn(i);
      accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));

      EvalNode filterPerLevel = AlgebraicUtil.createSingletonExprFromCNF(
        accumulatedFilters.toArray(new EvalNode[accumulatedFilters.size()]));
      filters[i] = new PartitionPathFilter(partitionColumns, filterPerLevel);
    }
    return filters;
  }

  private static Path[] toPathArray(FileStatus[] fileStatuses) {
    Path[] paths = new Path[fileStatuses.length];
    for (int i = 0; i < fileStatuses.length; i++) {
      FileStatus fileStatus = fileStatuses[i];
      paths[i] = fileStatus.getPath();
    }
    return paths;
  }

  private static void setFilteredPartitionInfo(FilteredPartitionInfo filteredPartitionInfo, FileSystem fs,
    Schema partitionColumnSchema) {
    long totalVolume = 0L;
    String[] partitionNames = null;
    if (filteredPartitionInfo.getPartitionPaths().length > 0) {
      try {
        partitionNames = new String[filteredPartitionInfo.getPartitionPaths().length];
        for (int i = 0; i < filteredPartitionInfo.getPartitionPaths().length; i++) {
          Path input = filteredPartitionInfo.getPartitionPaths()[i];
          int startIdx = input.toString().indexOf(getColumnPartitionPathPrefix(partitionColumnSchema));
          ContentSummary summary = fs.getContentSummary(input);
          partitionNames[i] = input.toString().substring(startIdx);
          totalVolume += summary.getLength();
        }
      } catch (Throwable e) {
        throw new TajoInternalError(e);
      }
    }
    filteredPartitionInfo.setPartitionNames(partitionNames);
    filteredPartitionInfo.setTotalVolume(totalVolume);
  }

  private static class PartitionPathFilter implements PathFilter {

    private Schema schema;
    private EvalNode partitionFilter;
    public PartitionPathFilter(Schema schema, EvalNode partitionFilter) {
      this.schema = schema;
      this.partitionFilter = partitionFilter;
      partitionFilter.bind(null, schema);
    }

    @Override
    public boolean accept(Path path) {
      Tuple tuple = buildTupleFromPartitionPath(schema, path, true);
      if (tuple == null) { // if it is a file or not acceptable file
        return false;
      }

      return partitionFilter.eval(tuple).asBool();
    }

    @Override
    public String toString() {
      return partitionFilter.toString();
    }
  }


  /**
   * Take a look at a column partition path. A partition path consists
   * of a table path part and column values part. This method transforms
   * a partition path into a tuple with a given partition column schema.
   *
   * hdfs://192.168.0.1/tajo/warehouse/table1/col1=abc/col2=def/col3=ghi
   *                   ^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^
   *                      table path part        column values part
   *
   * When a file path is given, it can perform two ways depending on beNullIfFile flag.
   * If it is true, it returns NULL when a given path is a file.
   * Otherwise, it returns a built tuple regardless of file or directory.
   *
   * @param partitionColumnSchema The partition column schema
   * @param partitionPath The partition path
   * @param beNullIfFile If true, this method returns NULL when a given path is a file.
   * @return The tuple transformed from a column values part.
   */
  public static Tuple buildTupleFromPartitionPath(Schema partitionColumnSchema, Path partitionPath,
                                                  boolean beNullIfFile) {
    int startIdx = partitionPath.toString().indexOf(getColumnPartitionPathPrefix(partitionColumnSchema));

    if (startIdx == -1) { // if there is no partition column in the patch
      return null;
    }
    String columnValuesPart = partitionPath.toString().substring(startIdx);

    String [] columnValues = columnValuesPart.split("/");

    // true means this is a file.
    if (beNullIfFile && partitionColumnSchema.size() < columnValues.length) {
      return null;
    }

    Tuple tuple = new VTuple(partitionColumnSchema.size());
    int i = 0;
    for (; i < columnValues.length && i < partitionColumnSchema.size(); i++) {
      String [] parts = columnValues[i].split("=");
      if (parts.length != 2) {
        return null;
      }
      int columnId = partitionColumnSchema.getColumnIdByName(parts[0]);
      Column keyColumn = partitionColumnSchema.getColumn(columnId);
      tuple.put(columnId, DatumFactory.createFromString(keyColumn.getDataType(),
        StringUtils.unescapePathName(parts[1])));
    }
    for (; i < partitionColumnSchema.size(); i++) {
      tuple.put(i, NullDatum.get());
    }
    return tuple;
  }

  /**
   * Get a prefix of column partition path. For example, consider a column partition (col1, col2).
   * Then, you will get a string 'col1='.
   *
   * @param partitionColumn the schema of column partition
   * @return The first part string of column partition path.
   */
  public static String getColumnPartitionPathPrefix(Schema partitionColumn) {
    StringBuilder sb = new StringBuilder();
    sb.append(partitionColumn.getColumn(0).getSimpleName()).append("=");
    return sb.toString();
  }

  /**
   * This transforms a partition name into a tupe with a given partition column schema. When a file path
   * Assume that an user gives partition name 'country=KOREA/city=SEOUL'.
   *
   * The first datum of tuple : KOREA
   * The second datum of tuple : SEOUL
   *
   * @param partitionColumnSchema The partition column schema
   * @param partitionName The partition name
   * @return The tuple transformed from a column values part.
   */
  public static Tuple buildTupleFromPartitionName(Schema partitionColumnSchema, String partitionName) {
    Preconditions.checkNotNull(partitionColumnSchema);
    Preconditions.checkNotNull(partitionName);

    String [] columnValues = partitionName.split("/");
    Preconditions.checkArgument(partitionColumnSchema.size() < columnValues.length, "Invalid Partition Name");

    Tuple tuple = new VTuple(partitionColumnSchema.size());

    for (int i = 0; i < tuple.size(); i++) {
      tuple.put(i, NullDatum.get());
    }

    for (int i = 0; i < columnValues.length; i++) {
      String [] parts = columnValues[i].split("=");
      if (parts.length == 2) {
        int columnId = partitionColumnSchema.getColumnIdByName(parts[0]);
        Column keyColumn = partitionColumnSchema.getColumn(columnId);
        tuple.put(columnId, DatumFactory.createFromString(keyColumn.getDataType(),
          StringUtils.unescapePathName(parts[1])));
      }
    }

    return tuple;
  }

}