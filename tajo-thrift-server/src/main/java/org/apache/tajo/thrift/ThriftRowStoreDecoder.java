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

package org.apache.tajo.thrift;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.exception.UnknownDataTypeException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.thrift.generated.TRowData;

import java.nio.ByteBuffer;
import java.util.List;

public class ThriftRowStoreDecoder {
    private Schema schema;

    public ThriftRowStoreDecoder(Schema schema) {
        this.schema = schema;
    }

    public Tuple toTuple(TRowData rowData) {
        Tuple tuple = new VTuple(schema.size());
        Column col;
        TajoDataTypes.DataType type;

        List<Boolean> nullFlags = rowData.getNullFlags();
        List<ByteBuffer> colDatas = rowData.getColumnDatas();
        for (int i = 0; i < schema.size(); i++) {
            if (nullFlags.get(i)) {
                tuple.put(i, DatumFactory.createNullDatum());
                continue;
            }

            col = schema.getColumn(i);
            type = col.getDataType();
            byte[] colDataBytes = colDatas.get(i).array();
            String colData = new String(colDataBytes);
            switch (type.getType()) {
                case BOOLEAN:
                    tuple.put(i, DatumFactory.createBool(new Boolean(colData)));
                    break;
                case BIT:
                    tuple.put(i, DatumFactory.createBit(colDataBytes[0]));
                    break;

                case CHAR:
                    tuple.put(i, DatumFactory.createChar(colDataBytes[0]));
                    break;

                case INT2:
                    tuple.put(i, DatumFactory.createInt2(colData));
                    break;

                case INT4:
                case DATE:
                    tuple.put(i, DatumFactory.createInt4(colData));
                    break;

                case INT8:
                case TIME:
                case TIMESTAMP:
                    tuple.put(i, DatumFactory.createInt8(colData));
                    break;

                case INTERVAL:
                    tuple.put(i, new IntervalDatum(colData));
                    break;

                case FLOAT4:
                    tuple.put(i, DatumFactory.createFloat4(colData));
                    break;

                case FLOAT8:
                    tuple.put(i, DatumFactory.createFloat8(colData));
                    break;

                case VARCHAR:
                case TEXT:
                    tuple.put(i, DatumFactory.createText(colData));
                    break;

                default:
                    throw new RuntimeException(new UnknownDataTypeException(type.getType().name()));
            }
        }
        return tuple;
    }
}
