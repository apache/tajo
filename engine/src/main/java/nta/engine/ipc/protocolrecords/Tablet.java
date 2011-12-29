package nta.engine.ipc.protocolrecords;

import org.apache.hadoop.fs.Path;


public class Tablet implements Comparable<Tablet> {
	
	private long startOffset;
	private long length;
	private Path path;
	
	public Tablet() {
		
	}
	
	public Tablet(Path path, long start, long length) {
		this.set(path, start, length);
	}
	
	public void set(Path path, long start, long length) {
		this.path = path;
		this.startOffset = start;
		this.length = length;
	}
	
	public Path getPath() {
		return this.path;
	}
	
	public long getStartOffset() {
		return this.startOffset;
	}
	
	public long getLength() {
		return this.length;
	}
	
	/**
	 * 
	 * The offset range of tablets <b>MUST NOT</b> be overlapped.
	 * @param t
	 * @return If the table paths are not same, return -1. 
	 */
	@Override
	public int compareTo(Tablet t) {
		if (t.getPath().equals(this.path)) {
			return (int)(this.startOffset - t.getStartOffset());
		} else {
			return -1;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Tablet) {
			Tablet t = (Tablet)o;
			if (t.getPath().equals(this.path)
					&& t.getStartOffset() == this.startOffset
					&& t.getLength() == this.length) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public String toString() {
		return new String("(" + path + " " + startOffset + " " + length + ")");
	}
}
