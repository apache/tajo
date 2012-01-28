package nta.catalog;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.engine.query.exception.InvalidQueryException;

public class CatalogUtil {
  public static String getCanonicalName(String signature,
      DataType...paramTypes) {
    StringBuilder sb = new StringBuilder(signature);
    sb.append("(");
    int i = 0;
    for (DataType type : paramTypes) {
      sb.append(type);
      if(i < paramTypes.length - 1) {
        sb.append(",");
      }
      
      i++;
    }
    sb.append(")");
    return sb.toString();
  }
  
  public static char getTypeCode(DataType type) {
    switch(type) {
    case BOOLEAN: return 'Z';
    case BYTE: return 'B';
    case SHORT: return 'S';
    case INT: return 'I';
    case LONG: return 'J';
    case FLOAT: return 'F';
    case DOUBLE: return 'D';
    case BYTES: return 'N';
    case IPv4: return '4';
    case IPv6: return '6';
    default: throw new InvalidQueryException("Unsupported type exception");
    }
  }
}
