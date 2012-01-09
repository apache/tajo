/**
 * 
 */
package nta.storage;

import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

/**
 * @author hyunsik
 *
 */
public class MemStores {
	private Configuration conf;
	
	private Map<URI, MemTable> slots = Maps.newHashMap();
	
	public MemStores(Configuration conf) {
		this.conf = conf;
	}
	
	public MemTable getMemStore(Store store) {
		return slots.get(store.getURI());
	}
	
	public void addMemStore(Store store, MemTable slot) {
		if(slots.containsKey(store.getURI())) {
			
		}		
		slots.put(store.getURI(), slot);
	}
	
	public void dropMemStore(Store store) {
		slots.remove(store.getURI());
	}
}
