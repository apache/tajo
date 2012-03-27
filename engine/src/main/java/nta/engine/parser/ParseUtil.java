package nta.engine.parser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.IndexMethod;
import nta.catalog.proto.CatalogProtos.StoreType;

/**
 * @author Hyunsik Choi
 */
public class ParseUtil {
  private static final Log LOG = LogFactory.getLog(ParseUtil.class);
  
  public static StoreType getStoreType(final String typeStr) {
    if (typeStr.equalsIgnoreCase("csv")) {
      return StoreType.CSV;
    } else if (typeStr.equalsIgnoreCase("raw")) {
      return StoreType.RAW;
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }
  
  public static DataType getDataTypeFromEnum(final String typeStr) {
    if (typeStr.equals(DataType.BOOLEAN.toString())) {
      return DataType.BOOLEAN;
    } else if (typeStr.equals(DataType.BYTE.toString())) {
      return DataType.BYTE;
    } else if (typeStr.equals(DataType.SHORT.toString())) {
      return DataType.SHORT;
    } else if (typeStr.equals(DataType.INT.toString())) {
      return DataType.INT;
    } else if (typeStr.equals(DataType.LONG.toString())) {
      return DataType.LONG;
    } else if (typeStr.equals(DataType.FLOAT.toString())) {
      return DataType.FLOAT;
    } else if (typeStr.equals(DataType.DOUBLE.toString())) {
      return DataType.DOUBLE;
    } else if (typeStr.equals(DataType.STRING.toString())) {
      return DataType.STRING;
    } else if (typeStr.equals(DataType.IPv4.toString())) {
      return DataType.IPv4;
    } else if (typeStr.equals(DataType.IPv6.toString())) {
      return DataType.IPv6;
    } else if (typeStr.equals(DataType.BYTES.toString())) {
      return DataType.BYTES;
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }
  
  public static IndexMethod getIndexMethod(final String typeStr) {
    if (typeStr.equals(IndexMethod.TWO_LEVEL_BIN_TREE.toString())) {
      return IndexMethod.TWO_LEVEL_BIN_TREE;
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }
}
