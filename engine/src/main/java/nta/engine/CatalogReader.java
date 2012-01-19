package nta.engine;

import nta.catalog.FunctionDesc;
import nta.catalog.TableDesc;

public interface CatalogReader {
  boolean existsTable(String tableId);
  
  TableDesc getTableDesc(String tableId);
  
  FunctionDesc getFunctionMeta(String signature);
  
  boolean containFunction(String signature);
}
