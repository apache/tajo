package nta.engine.ipc.protocolrecords;

import org.apache.hadoop.fs.Path;


public class Tablet implements Comparable<Tablet> {
	
	private long startOffset;
	private long length;
	private Path path;
	private String filename;
	
	public Tablet() {
		
	}
	
	public Tablet(Path tablePath, String filename, long start, long length) {
		this.set(tablePath, filename, start, length);
	}
	
	public void set(Path path, String filename, long start, long length) {
		this.path = path;
		this.filename = filename;
		this.startOffset = start;
		this.length = length;
	}
	
	public Path getTablePath() {
		return this.path;
	}
	
	public String getFileName() {
	  return this.filename;
	}
	
	public Path getFilePath() {
	  return new Path(this.path, "data/"+this.filename);
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
		if (getFilePath().equals(t.getFilePath())) {
			return (int)(this.startOffset - t.getStartOffset());
		} else {
			return -1;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Tablet) {
			Tablet t = (Tablet)o;
			if (getFilePath().equals(t.getFilePath()) 			    
					&& t.getStartOffset() == this.startOffset
					&& t.getLength() == this.length) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
	  return getFilePath().hashCode();
	}
	
	@Override
	public String toString() {
		return new String("(" + getFilePath() + " " + startOffset + " " + length + ")");
	}
}
