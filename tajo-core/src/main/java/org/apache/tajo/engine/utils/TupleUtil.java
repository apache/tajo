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

package org.apache.tajo.engine.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.RowStoreUtil.RowStoreEncoder;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.VTuple;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TupleUtil {
  private static final Log LOG = LogFactory.getLog(TupleUtil.class);

  public static String rangeToQuery(Schema schema, TupleRange range, boolean last)
      throws UnsupportedEncodingException {
    return rangeToQuery(range, last, RowStoreUtil.createEncoder(schema));
  }

  public static String rangeToQuery(TupleRange range, boolean last, RowStoreEncoder encoder)
      throws UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder();
    byte [] firstKeyBytes = encoder.toBytes(range.getStart());
    byte [] endKeyBytes = encoder.toBytes(range.getEnd());

    String firstKeyBase64 = new String(Base64.encodeBase64(firstKeyBytes));
    String lastKeyBase64 = new String(Base64.encodeBase64(endKeyBytes));

    sb.append("start=")
        .append(URLEncoder.encode(firstKeyBase64, "utf-8"))
        .append("&")
        .append("end=")
        .append(URLEncoder.encode(lastKeyBase64, "utf-8"));

    if (last) {
      sb.append("&final=true");
    }

    return sb.toString();
  }

  /**
   * if max value is null, set ranges[last]
   * @param sortSpecs
   * @param sortSchema
   * @param colStats
   * @param ranges
   */
  public static void setMaxRangeIfNull(SortSpec[] sortSpecs, Schema sortSchema,
                                       List<ColumnStats> colStats, TupleRange[] ranges) {
    Map<Column, ColumnStats> statMap = Maps.newHashMap();
    for (ColumnStats stat : colStats) {
      statMap.put(stat.getColumn(), stat);
    }

    int i = 0;
    for (Column col : sortSchema.getColumns()) {
      ColumnStats columnStat = statMap.get(col);
      if (columnStat == null) {
        continue;
      }
      if (columnStat.hasNullValue()) {
        int rangeIndex = sortSpecs[i].isAscending() ? ranges.length - 1 : 0;
        VTuple rangeTuple = sortSpecs[i].isAscending() ? (VTuple) ranges[rangeIndex].getEnd() : (VTuple) ranges[rangeIndex].getStart();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Set null into range: " + col.getQualifiedName() + ", previous tuple is " + rangeTuple);
        }
        rangeTuple.put(i, NullDatum.get());
        LOG.info("Set null into range: " + col.getQualifiedName() + ", current tuple is " + rangeTuple);
      }
      i++;
    }
  }

  public static TupleRange columnStatToRange(SortSpec [] sortSpecs, Schema target, List<ColumnStats> colStats,
                                             boolean checkNull) {

    Map<Column, ColumnStats> statSet = Maps.newHashMap();
    for (ColumnStats stat : colStats) {
      statSet.put(stat.getColumn(), stat);
    }

    for (Column col : target.getColumns()) {
      Preconditions.checkState(statSet.containsKey(col),
          "ERROR: Invalid Column Stats (column stats: " + colStats + ", there exists not target " + col);
    }

    Tuple startTuple = new VTuple(target.size());
    Tuple endTuple = new VTuple(target.size());
    int i = 0;

    // In outer join, empty table could be searched.
    // As a result, min value and max value would be null.
    // So, we should put NullDatum for this case.
    for (Column col : target.getColumns()) {
      if (sortSpecs[i].isAscending()) {
        if (statSet.get(col).getMinValue() != null)
          startTuple.put(i, statSet.get(col).getMinValue());
        else
          startTuple.put(i, DatumFactory.createNullDatum());

        if (checkNull) {
          if (statSet.get(col).hasNullValue() || statSet.get(col).getMaxValue() == null)
            endTuple.put(i, DatumFactory.createNullDatum());
          else
            endTuple.put(i, statSet.get(col).getMaxValue());
        } else {
          if (statSet.get(col).getMaxValue() != null)
            endTuple.put(i, statSet.get(col).getMaxValue());
          else
            endTuple.put(i, DatumFactory.createNullDatum());
        }
      } else {
        if (checkNull) {
          if (statSet.get(col).hasNullValue() || statSet.get(col).getMaxValue() == null)
            startTuple.put(i, DatumFactory.createNullDatum());
          else
            startTuple.put(i, statSet.get(col).getMaxValue());
        } else {
          if (statSet.get(col).getMaxValue() != null)
            startTuple.put(i, statSet.get(col).getMaxValue());
          else
            startTuple.put(i, DatumFactory.createNullDatum());
        }
        if (statSet.get(col).getMinValue() != null)
          endTuple.put(i, statSet.get(col).getMinValue());
        else
          endTuple.put(i, DatumFactory.createNullDatum());
      }
      i++;
    }
    return new TupleRange(sortSpecs, startTuple, endTuple);
  }

  /**
   * It creates a tuple of a given size filled with NULL values in all fields
   * It is usually used in outer join algorithms.
   *
   * @param size The number of columns of a creating tuple
   * @return The created tuple filled with NULL values
   */
  public static Tuple createNullPaddedTuple(int size){
    VTuple aTuple = new VTuple(size);
    int i;
    for(i = 0; i < size; i++){
      aTuple.put(i, DatumFactory.createNullDatum());
    }
    return aTuple;
  }

  @SuppressWarnings("unused")
  public static Collection<Tuple> filterTuple(Schema schema, Collection<Tuple> tupleBlock, EvalNode filterCondition) {
    TupleBlockFilterScanner filter = new TupleBlockFilterScanner(schema, tupleBlock, filterCondition);
    return filter.nextBlock();
  }

  private static class TupleBlockFilterScanner {
    private EvalNode qual;
    private Iterator<Tuple> iterator;
    private Schema schema;

    public TupleBlockFilterScanner(Schema schema, Collection<Tuple> tuples, EvalNode qual) {
      this.schema = schema;
      this.qual = qual;
      this.iterator = tuples.iterator();
    }

    public List<Tuple> nextBlock() {
      List<Tuple> results = Lists.newArrayList();

      Tuple tuple;
      while (iterator.hasNext()) {
        tuple = iterator.next();
        if (qual.eval(schema, tuple).isTrue()) {
          results.add(tuple);
        }
      }
      return results;
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
      tuple.put(columnId, DatumFactory.createFromString(keyColumn.getDataType(), parts[1]));
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
  private static String getColumnPartitionPathPrefix(Schema partitionColumn) {
    StringBuilder sb = new StringBuilder();
    sb.append(partitionColumn.getColumn(0).getSimpleName()).append("=");
    return sb.toString();
  }
}
