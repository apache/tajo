package nta.engine.ipc.protocolrecords;

public class Tablet implements Comparable<Tablet> {
	
	private long startOffset;
	private long length;
	
	public Tablet() {
		
	}
	
	public Tablet(long start, long length) {
		this.set(start, length);
	}
	
	public void set(long start, long length) {
		this.startOffset = start;
		this.length = length;
	}
	
	public long getStartOffset() {
		return this.startOffset;
	}
	
	public long getLength() {
		return this.length;
	}
	
	/**
	 * The offset range of tablets <b>MUST NOT</b> be overlapped.
	 * @param t
	 * @return
	 */
	@Override
	public int compareTo(Tablet t) {
		return (int)(this.startOffset - t.getStartOffset());
	}
}
