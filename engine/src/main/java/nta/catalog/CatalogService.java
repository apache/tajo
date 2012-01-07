package nta.catalog;

import nta.catalog.exception.CatalogException;

/**
 * 
 * @author hyunsik
 *
 */
public interface CatalogService {
  
  /**
   * Get a table description by name
   * @param name table name
   * @return a table description
   * @see TableDescImpl
   * @throws Throwable
   */
  TableDesc getTableDesc(String name) throws CatalogException;
  
  /**
   * Add a table via table description
   * @param meta table meta
   * @see TableDescImpl
   * @throws Throwable
   */
  void addTable(TableDesc desc) throws CatalogException;
  
  /**
   * Drop a table by name
   * @param name table name
   * @throws Throwable
   */
  void deleteTable(String name) throws CatalogException;
}
