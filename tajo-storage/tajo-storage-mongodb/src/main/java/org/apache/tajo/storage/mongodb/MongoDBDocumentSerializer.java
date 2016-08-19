/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tajo.storage.mongodb;

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.NestedPathUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.storage.Tuple;
import org.bson.Document;

import java.io.IOException;
import java.util.Map;

/**
 * Serialize tuples into Mongo Documents
 */
public class MongoDBDocumentSerializer {

    private final Schema schema;
    private final TableMeta meta;
    private final Map<String, Type> types;
    String[] paths;

    public MongoDBDocumentSerializer(Schema schema, TableMeta meta) {
        this.schema = schema;
        this.meta = meta;

        paths = SchemaUtil.convertColumnsToPaths(Lists.newArrayList(schema.getAllColumns()), true);
        types = SchemaUtil.buildTypeMap(schema.getAllColumns(), paths);
    }

    public void setValue(Tuple inputTuple, String fullPath, String[] paths, int depth, int index, Document outputDoc) {


        String simpleColumnName = paths[depth];
        switch (types.get(fullPath)) {
            case INT1:
            case UINT1:
            case INT2:
            case UINT2:
                outputDoc.put(simpleColumnName, inputTuple.getInt2(index));
                break;

            case INT4:
            case UINT4:
                outputDoc.put(simpleColumnName, inputTuple.getInt4(index));
                break;

            case INT8:
            case UINT8:
                outputDoc.put(simpleColumnName, inputTuple.getInt8(index));
                break;

            case FLOAT4:
                outputDoc.put(simpleColumnName, inputTuple.getFloat4(index));
                break;

            case FLOAT8:
                outputDoc.put(simpleColumnName, inputTuple.getFloat8(index));
                break;

            case CHAR:
                outputDoc.put(simpleColumnName, inputTuple.getChar(index));
                break;

            case BOOLEAN:
                outputDoc.put(simpleColumnName, inputTuple.getBool(index));
                break;

            default:
                outputDoc.put(simpleColumnName, inputTuple.getText(index));
                break;
        }
    }


    public void serialize(Tuple inputTuple, Document outputDoc) throws IOException {
        for (int i = 0; i < inputTuple.size(); i++) {
            String[] newPaths = paths[i].split(NestedPathUtil.PATH_DELIMITER);
            setValue(inputTuple, newPaths[0], newPaths, 0, i, outputDoc);
        }
    }

}
