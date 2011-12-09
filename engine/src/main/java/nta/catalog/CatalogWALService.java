/**
 * 
 */
package nta.catalog;

import java.io.IOException;

/**
 * @author hyunsik
 *
 */
public interface CatalogWALService {
	public void appendAddTable(TableMeta meta) throws IOException;
	
	public void appendDelTable(String name) throws IOException;
	
	public void appendAddFunction(FunctionMeta meta) throws IOException;
	
	public void appendDelFunction(String name) throws IOException;
}
