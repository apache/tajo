/**
 * 
 */
package nta.catalog.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nta.catalog.FunctionDesc;
import nta.catalog.TableDesc;
import nta.catalog.proto.CatalogProtos.IndexDescProto;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

/**
 * @author Hyunsik Choi
 */
public class MemStore implements CatalogStore {
  private final Map<String,TableDesc> tables
    = Maps.newHashMap();
  private final Map<String, FunctionDesc> functions
    = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexes
    = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexesByColumn
  = Maps.newHashMap();
  
  public MemStore(Configuration conf) {
  }

  /* (non-Javadoc)
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    tables.clear();
    functions.clear();
    indexes.clear();
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#addTable(nta.catalog.TableDesc)
   */
  @Override
  public void addTable(TableDesc desc) throws IOException {
    synchronized(tables) {
      tables.put(desc.getId(), desc);
    }
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#existTable(java.lang.String)
   */
  @Override
  public boolean existTable(String name) throws IOException {
    synchronized(tables) {
      return tables.containsKey(name);
    }
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#deleteTable(java.lang.String)
   */
  @Override
  public void deleteTable(String name) throws IOException {
    synchronized(tables) {
      tables.remove(name);
    }
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#getTable(java.lang.String)
   */
  @Override
  public TableDesc getTable(String name) throws IOException {
    return tables.get(name);
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#getAllTableNames()
   */
  @Override
  public List<String> getAllTableNames() throws IOException {
    return new ArrayList<String>(tables.keySet());
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#addIndex(nta.catalog.proto.CatalogProtos.IndexDescProto)
   */
  @Override
  public void addIndex(IndexDescProto proto) throws IOException {
    synchronized(indexes) {
      indexes.put(proto.getName(), proto);
      indexesByColumn.put(proto.getTableId() + "." 
          + proto.getColumn().getColumnName(), proto);
    }
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#delIndex(java.lang.String)
   */
  @Override
  public void delIndex(String indexName) throws IOException {
    synchronized(indexes) {
      indexes.remove(indexName);
    }
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#getIndex(java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String indexName) throws IOException {
    return indexes.get(indexName);
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#getIndex(java.lang.String, java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String tableName, String columnName)
      throws IOException {
    return indexesByColumn.get(tableName+"."+columnName);
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#existIndex(java.lang.String)
   */
  @Override
  public boolean existIndex(String indexName) throws IOException {
    return indexes.containsKey(indexName);
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#existIndex(java.lang.String, java.lang.String)
   */
  @Override
  public boolean existIndex(String tableName, String columnName)
      throws IOException {
    return indexesByColumn.containsKey(tableName + "." + columnName);
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#getIndexes(java.lang.String)
   */
  @Override
  public IndexDescProto[] getIndexes(String tableName) throws IOException {
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    for (IndexDescProto proto : indexesByColumn.values()) {
      if (proto.getTableId().equals(tableName)) {
        protos.add(proto);
      }
    }
    return protos.toArray(new IndexDescProto[protos.size()]);
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#addFunction(nta.catalog.FunctionDesc)
   */
  @Override
  public void addFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#deleteFunction(nta.catalog.FunctionDesc)
   */
  @Override
  public void deleteFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#existFunction(nta.catalog.FunctionDesc)
   */
  @Override
  public void existFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see nta.catalog.store.CatalogStore#getAllFunctionNames()
   */
  @Override
  public List<String> getAllFunctionNames() throws IOException {
    // to be implemented
    return null;
  }

}
