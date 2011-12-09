/**
 * 
 */
package nta.storage;

import java.net.URI;
import java.util.Map;
import com.google.common.collect.Maps;

import nta.conf.NtaConf;

/**
 * @author hyunsik
 *
 */
public class MemStores {
	private NtaConf conf;
	
	private Map<URI, MemTable> slots = Maps.newHashMap();
	
	public MemStores(NtaConf conf) {
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
