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

package org.apache.tajo.storage.hbase;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;

public class IndexPredication {
  final private Column column;
  final private int columnId;
  final private Datum startValue;
  final private Datum stopValue;

  public IndexPredication(Column c, int columnId, Datum startValue, Datum stopValue) {
    this.column     = c;
    this.columnId   = columnId;
    this.startValue = startValue;
    this.stopValue  = stopValue;
  }

  public Column getColumn() {
    return column;
  }

  public int getColumnId() {
    return columnId;
  }

  public Datum getStartValue() {
    return startValue;
  }

  public Datum getStopValue() {
    return stopValue;
  }
}
