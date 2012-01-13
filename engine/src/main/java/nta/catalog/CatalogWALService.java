/**
 * 
 */
package nta.catalog;

import java.io.IOException;

/**
 * @author hyunsik
 *
 */
interface CatalogWALService {
	public void appendAddTable(TableDesc meta) throws IOException;
	
	public void appendDelTable(String name) throws IOException;
	
	public void appendAddFunction(FunctionDesc meta) throws IOException;
	
	public void appendDelFunction(String name) throws IOException;
}
