package nta.storage;

import nta.catalog.Column;
import nta.catalog.Schema;

public class StorageUtils {
	public static int getRowByteSize(Schema schema) {
		int sum = 0;
		for(Column col : schema.getColumns()) {
			sum += StorageUtils.getColByteSize(col);
		}
		
		return sum;
	}
	
	public static int getColByteSize(Column col) {
		switch(col.getDataType()) {
		case BOOLEAN: return 1;
		case BYTE: return 1;
		case SHORT: return 2;
		case INT: return 4;
		case LONG: return 8;
		case FLOAT: return 4;
		case DOUBLE: return 8;
		case IPv4: return 4;
		case IPv6: return 32;
		case STRING: return 256;
		default: return 0;
		}
	}
}
