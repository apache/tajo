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
package org.apache.tajo.storage.thirdparty.orc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.orc.CompressionCodec;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SnappyCodec;
import org.apache.orc.impl.ZlibCodec;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TypeDesc;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedDataTypeException;

public class OrcUtils {
  private static final Log LOG = LogFactory.getLog(OrcUtils.class);

  public static org.apache.orc.CompressionCodec createCodec(org.apache.orc.CompressionKind kind) {
    switch (kind) {
      case NONE:
        return null;
      case ZLIB:
        return new ZlibCodec();
      case SNAPPY:
        return new SnappyCodec();
      case LZO:
        try {
          ClassLoader loader = Thread.currentThread().getContextClassLoader();
          if (loader == null) {
            throw new RuntimeException("error while getting a class loader");
          }
          @SuppressWarnings("unchecked")
          Class<? extends org.apache.orc.CompressionCodec> lzo =
              (Class<? extends CompressionCodec>)
                  loader.loadClass("org.apache.hadoop.hive.ql.io.orc.LzoCodec");
          return lzo.newInstance();
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("LZO is not available.", e);
        } catch (InstantiationException e) {
          throw new IllegalArgumentException("Problem initializing LZO", e);
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException("Insufficient access to LZO", e);
        }
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
            kind);
    }
  }

  public static TypeDescription convertSchema(Schema schema) {
    TypeDescription description = TypeDescription.createStruct();

    for (Column eachColumn : schema.getRootColumns()) {
      description.addField(eachColumn.getQualifiedName(),
          convertTypeInfo(eachColumn.getTypeDesc()));
    }
    return description;
  }

  public static TypeDescription convertTypeInfo(TypeDesc desc) {
    switch (desc.getDataType().getType()) {
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case BIT:
        return TypeDescription.createByte();
      case INT2:
        return TypeDescription.createShort();
      case INT4:
        return TypeDescription.createInt();
      case INT8:
        return TypeDescription.createLong();
      case FLOAT4:
        return TypeDescription.createFloat();
      case FLOAT8:
        return TypeDescription.createDouble();
      case TEXT:
        return TypeDescription.createString();
      case DATE:
        return TypeDescription.createDate();
      case TIMESTAMP:
        return TypeDescription.createTimestamp();
      case BLOB:
        return TypeDescription.createBinary();
      case CHAR:
        return TypeDescription.createChar()
            .withMaxLength(desc.getDataType().getLength());
      case RECORD: {
        TypeDescription result = TypeDescription.createStruct();
        for (Column eachColumn : desc.getNestedSchema().getRootColumns()) {
          result.addField(eachColumn.getQualifiedName(),
              convertTypeInfo(eachColumn.getTypeDesc()));
        }
        return result;
      }
      default:
        throw new TajoRuntimeException(new UnsupportedDataTypeException(desc.getDataType().getType().name()));
    }
  }
}
