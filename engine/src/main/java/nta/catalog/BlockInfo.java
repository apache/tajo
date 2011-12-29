package nta.catalog;

public class BlockInfo {

	private String hostName;
	private long offset;
	private long length;
	
	public BlockInfo() {
		
	}
	
	public BlockInfo(String hostName, long offset, long length) {
		this.set(hostName, offset, length);
	}
	
	public void set(String hostName, long offset, long length) {
		this.hostName = hostName;
		this.offset = offset;
		this.length = length;
	}
	
	public String getHostName() {
		return this.hostName;
	}
	
	public long getOffset() {
		return this.offset;
	}
	
	public long getLength() {
		return this.length;
	}
	
	public String toString() {
		return new String("HostName: " + hostName + " offset: " + offset + " length: " + length);
	}
}
