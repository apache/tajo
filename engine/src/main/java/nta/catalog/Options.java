/**
 * 
 */
package nta.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import nta.catalog.proto.TableProtos.KeyValueProto;
import nta.catalog.proto.TableProtos.OptionsProto;
import nta.catalog.proto.TableProtos.OptionsProtoOrBuilder;
import nta.catalog.proto.TableProtos.SchemaProto;
import nta.catalog.proto.TableProtos.TableProtoOrBuilder;

/**
 * @author hyunsik
 *
 */
public class Options implements ProtoObject<OptionsProto> {
	private OptionsProto proto = OptionsProto.getDefaultInstance();
	private OptionsProto.Builder builder = null;
	private boolean viaProto = false;
	
	private Map<String,String> keyVals;
	
	/**
	 * 
	 */
	public Options() {
		builder = OptionsProto.newBuilder();		
	}
	
	public Options(OptionsProto proto) {
		this.proto = proto;
		viaProto = true;
	}
	
	public void put(String key, String val) {
		initOptions();
		this.keyVals.put(key, val);
	}
	
	public String get(String key) {
		initOptions();
		return this.keyVals.get(key);
	}
	
	public String delete(String key) {
		initOptions();		
		return keyVals.remove(key);
	}
	
	@Override
	public boolean equals(Object object) {
		if(object instanceof Options) {
			Options other = (Options)object;
			initOptions();
			other.initOptions();
			for(Entry<String, String> entry : other.keyVals.entrySet()) {
				if(!keyVals.get(entry.getKey()).equals(entry.getValue()))
					return false;
			}
			return true;
		}
		
		return false;
	}
	
	@Override
	public OptionsProto getProto() {
		mergeLocalToProto();
		proto = viaProto ? proto : builder.build();
		viaProto = true;
		return proto;
	}
	
	private void initOptions() {
		if (this.keyVals != null) {
			return;
		}
		OptionsProtoOrBuilder p = viaProto ? proto : builder;
		this.keyVals = new HashMap<String, String>();
		for(KeyValueProto keyval:p.getKeyvalList()) {
			this.keyVals.put(keyval.getKey(), keyval.getValue());
		}
	}
	
	private void maybeInitBuilder() {
		if (viaProto || builder == null) {
			builder = OptionsProto.newBuilder(proto);
		}
		viaProto = false;
	}
	
	private void mergeLocalToBuilder() {
		KeyValueProto.Builder kvBuilder = null;
		if(this.keyVals != null) {
			for(Entry<String,String> kv : keyVals.entrySet()) {
				kvBuilder = KeyValueProto.newBuilder();
				kvBuilder.setKey(kv.getKey());
				kvBuilder.setValue(kv.getValue());
				builder.addKeyval(kvBuilder.build());
			}
		}
	}
	
	private void mergeLocalToProto() {
		if(viaProto) {
			maybeInitBuilder();
		}
		mergeLocalToBuilder();
		proto = builder.build();
		viaProto = true;
	}
}
