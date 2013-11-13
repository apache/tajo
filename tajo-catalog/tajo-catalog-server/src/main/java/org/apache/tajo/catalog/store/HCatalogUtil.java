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
package org.apache.tajo.catalog.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InternalException;

import java.io.IOException;

public class HCatalogUtil {
  protected final Log LOG = LogFactory.getLog(getClass());

  public static void validateHCatTableAndTajoSchema(HCatSchema tblSchema) throws InternalException {
    for (HCatFieldSchema hcatField : tblSchema.getFields()) {
      validateHCatFieldAndTajoSchema(hcatField);
    }
  }

  private static void validateHCatFieldAndTajoSchema(HCatFieldSchema fieldSchema) throws
      InternalException {
    try {
      HCatFieldSchema.Type fieldType = fieldSchema.getType();
      switch (fieldType) {
        case ARRAY:
          throw new HCatException("Tajo cannot support array field type.");
        case STRUCT:
          throw new HCatException("Tajo cannot support struct field type.");
        case MAP:
          throw new HCatException("Tajo cannot support map field type.");
      }
    } catch (HCatException e) {
      throw new InternalException("incompatible hcatalog types when assigning to tajo type. - " +
          "HCatFieldSchema:" + fieldSchema, e);
    }
  }

  public static HiveMetaStoreClient getHiveMetaClient(String metaStoreUri,
                                                      String metaStoreKerberosPrincipal)
                                                      //Class<?> cls)
  throws Exception {
//    HiveConf hiveConf = new HiveConf(cls);

    HiveConf hiveConf = new HiveConf();

    if (metaStoreUri != null) {
      hiveConf.set("hive.metastore.local", "false");
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUri.trim());
    }

    if (metaStoreKerberosPrincipal != null) {
      hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, metaStoreKerberosPrincipal);
    }

    try {
      return HCatUtil.getHiveClient(hiveConf);
    } catch (Exception e) {
      throw new InternalException("Tajo cannot connect Hive metastore. - serverUri:" +
          metaStoreUri, e);
    }
  }

  public static TajoDataTypes.Type getTajoFieldType(String fieldType) throws IOException {
    if(fieldType == null) {
      throw new InternalException("Hive field type is null.");
    }
    String typeStr = null;

    if(fieldType.equalsIgnoreCase(serdeConstants.INT_TYPE_NAME))
      typeStr = "INT4";
    else if(fieldType.equalsIgnoreCase(serdeConstants.TINYINT_TYPE_NAME))
      typeStr = "INT1";
    else if(fieldType.equalsIgnoreCase(serdeConstants.SMALLINT_TYPE_NAME))
      typeStr = "INT2";
    else if(fieldType.equalsIgnoreCase(serdeConstants.BIGINT_TYPE_NAME))
      typeStr = "INT8";
    else if(fieldType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME))
      typeStr = "BOOLEAN";
    else if(fieldType.equalsIgnoreCase(serdeConstants.FLOAT_TYPE_NAME))
      typeStr = "FLOAT4";
    else if(fieldType.equalsIgnoreCase(serdeConstants.DOUBLE_TYPE_NAME))
      typeStr = "FLOAT8";
    else if(fieldType.equalsIgnoreCase(serdeConstants.STRING_TYPE_NAME))
      typeStr = "TEXT";
    else if(fieldType.equalsIgnoreCase(serdeConstants.BINARY_TYPE_NAME))
      typeStr = "BLOB";

    try {
      return Enum.valueOf(TajoDataTypes.Type.class, typeStr);
    } catch (IllegalArgumentException iae) {
      System.out.println("Cannot find a matched type aginst from '" + typeStr + "'");
      return null;
    }
  }

  public static String getHiveFieldType(String fieldType) throws IOException {
    if(fieldType == null) {
      throw new InternalException("Tajo field type is null.");
    }
    String typeStr = null;

    if(fieldType.equalsIgnoreCase("INT4"))
      typeStr = serdeConstants.INT_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("INT1"))
      typeStr = serdeConstants.TINYINT_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("INT2"))
      typeStr = serdeConstants.SMALLINT_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("INT8"))
      typeStr = serdeConstants.BIGINT_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("BOOLEAN"))
      typeStr = serdeConstants.BOOLEAN_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("FLOAT4"))
      typeStr = serdeConstants.FLOAT_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("FLOAT8"))
      typeStr = serdeConstants.DOUBLE_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("TEXT"))
      typeStr = serdeConstants.STRING_TYPE_NAME;
    else if(fieldType.equalsIgnoreCase("BLOB"))
      typeStr = serdeConstants.BINARY_TYPE_NAME;

    return typeStr;
  }

  public static String getStoreType(String fileFormat, String delimiter) throws IOException{
    if(fileFormat == null) {
      throw new InternalException("Hive file output format is null.");
    }

    String[] fileFormatArrary = fileFormat.split("\\.");
    if(fileFormatArrary.length < 1) {
      throw new InternalException("Hive file output format is wrong. - file output format:" + fileFormat);
    }

    String inputFormatClass = fileFormatArrary[fileFormatArrary.length-1];

    if(inputFormatClass.equals("HiveIgnoreKeyTextOutputFormat")) {
      return CatalogProtos.StoreType.CSV.name();
    } else {
      //TODO: other file format
      return null;
    }
  }

}