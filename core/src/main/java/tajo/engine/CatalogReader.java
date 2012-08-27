package tajo.engine;

import tajo.catalog.FunctionDesc;
import tajo.catalog.TableDesc;
import tajo.catalog.proto.CatalogProtos.DataType;

public interface CatalogReader {
  boolean existsTable(String tableId);
  
  TableDesc getTableDesc(String tableId);
  
  FunctionDesc getFunctionMeta(String signature, DataType...paramTypes);
  
  boolean containFunction(String signature, DataType...paramTypes);
}
