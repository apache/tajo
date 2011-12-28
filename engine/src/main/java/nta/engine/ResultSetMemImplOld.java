/**
 * 
 */
package nta.engine;

import java.net.InetAddress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.common.type.IPv4;
import nta.engine.exception.NTAQueryException;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 *
 */
public class ResultSetMemImplOld implements ResultSetOld, SchemaObject {
	private static Log LOG = LogFactory.getLog(ResultSetMemImplOld.class);
	
	List<Tuple> rows = new ArrayList<Tuple>();	
	volatile int cursor;
	int cur = -1;	
	Map<String,Integer> columnMap = new HashMap<String, Integer>(); 
	private final Schema meta; 
	
	/**
	 * 
	 */
	public ResultSetMemImplOld(Schema schema) {
		this.meta = schema;
		
		int i=0;
		for(Column col: schema.getColumns()) {
			columnMap.put(col.getName(), i);
			i++;
		}
	}
	

	@Override
	public Schema getSchema() {	
		return this.meta;
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#next()
	 */
	public boolean next() {
		if(cur+1 < rows.size()) {
			cur++;
			return true;
		} else {
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#previous()
	 */
	public boolean previous() {
		if(cur-1 > -1) {
			cur--;
			return true;
		} else {
			return false;
		}
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#first()
	 */
	public boolean first() {
		cur=-1;
		
		return true;
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#last()
	 */
	public boolean last() {
		cur = rows.size();
		
		return true;
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getRow()
	 */
	public int getRow() {		
		return cur;
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#close()
	 */
	public void close() {
		rows.clear();
		rows = null;
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getString(int)
	 */
	public String getString(int columnIndex) {		
		return rows.get(cur).getString(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getBoolean(int)
	 */
	public boolean getBoolean(int columnIndex) {
		return true;
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getByte(int)
	 */
	public byte getByte(int columnIndex) {
		return rows.get(cur).getByte(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getShort(int)
	 */
	public short getShort(int columnIndex) {
		return rows.get(cur).getShort(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getInt(int)
	 */
	public int getInt(int columnIndex) {
		return rows.get(cur).getInt(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getLong(int)
	 */
	public long getLong(int columnIndex) {
		return rows.get(cur).getLong(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getFloat(int)
	 */
	public float getFloat(int columnIndex) {
		return rows.get(cur).getFloat(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getDouble(int)
	 */
	public double getDouble(int columnIndex) {
		return rows.get(cur).getDouble(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getBytes(int)
	 */
	public byte[] getBytes(int columnIndex) {
		return rows.get(cur).getBytes(columnIndex);
	}
	
	public InetAddress getIPv4(int columnIndex) {
		return rows.get(cur).getIPv4(columnIndex);
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getString(java.lang.String)
	 */
	public String getString(String columnLabel) {
		return rows.get(cur).getString(columnMap.get(columnLabel));
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getBoolean(java.lang.String)
	 */
	public boolean getBoolean(String columnLabel) {
		return rows.get(cur).getBoolean(columnMap.get(columnLabel));
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getByte(java.lang.String)
	 */
	public byte getByte(String columnLabel) {
		return rows.get(cur).getByte(columnMap.get(columnLabel));
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getShort(java.lang.String)
	 */
	public short getShort(String columnLabel) {
		return rows.get(cur).getShort(columnMap.get(columnLabel));
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getInt(java.lang.String)
	 */
	public int getInt(String columnLabel) throws NTAQueryException {
		try {
			return rows.get(cur).getInt(columnMap.get(columnLabel));
		} catch (NullPointerException npe) {
			throw new NTAQueryException();
		}		
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getLong(java.lang.String)
	 */
	public long getLong(String columnLabel) {
		return rows.get(cur).getLong(columnMap.get(columnLabel));
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getFloat(java.lang.String)
	 */
	public float getFloat(String columnLabel) {
		return rows.get(cur).getFloat(columnMap.get(columnLabel));
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getDouble(java.lang.String)
	 */
	public double getDouble(String columnLabel) {
		return rows.get(cur).getDouble(columnMap.get(columnLabel));
	}

	/* (non-Javadoc)
	 * @see nta.query.ResultSet#getBytes(java.lang.String)
	 */
	public byte[] getBytes(String columnLabel) {
		return rows.get(cur).getBytes(columnMap.get(columnLabel));
	}
	
	public InetAddress getIPv4(String columnLabel) {
		return rows.get(cur).getIPv4(columnMap.get(columnLabel));
	}
	
	public void addTuple(Tuple tuple) {
		rows.add(tuple);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(Column desc : this.meta.getColumns()) {
			sb.append(desc.getName()+"\t");
		}
		sb.append("\n----------------------------------\n");
		first();
		while (next()) {
			for (Column cd : meta.getColumns()) {
				switch (cd.getDataType()) {
				case BOOLEAN:
					sb.append(rows.get(cur).getBoolean(cd.getId()));
					break;
				case DOUBLE:
					sb.append(rows.get(cur).getDouble(cd.getId()));
					break;
				case INT:
					sb.append(rows.get(cur).getInt(cd.getId()));
					break;
				case LONG:
					sb.append(rows.get(cur).getLong(cd.getId()));
					break;
				case SHORT:
					sb.append(rows.get(cur).getShort(cd.getId()));
					break;
				case STRING:
					sb.append(rows.get(cur).getString(cd.getId()));
					break;
				case IPv4:
					sb.append(new IPv4(rows.get(cur).getIPv4Bytes(cd.getId())).toString());
					break;
				default:
					if(LOG.isDebugEnabled())
						LOG.debug("Type " + cd.getDataType() + " is not implemented!!!!!!!!!!!!!!");
				
					break;
				}
				sb.append("\t");
			}
			sb.append("\n");
		}
		
		return sb.toString();
	}
}
