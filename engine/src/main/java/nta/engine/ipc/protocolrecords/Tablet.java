package nta.engine.ipc.protocolrecords;

import nta.catalog.proto.TableProtos.TabletProto;
import nta.catalog.proto.TableProtos.TabletProtoOrBuilder;

import org.apache.hadoop.fs.Path;

/**
 * 
 * @author jihoon
 *
 */
public class Tablet implements Comparable<Tablet> {
	
	protected TabletProto proto = TabletProto.getDefaultInstance();
	protected TabletProto.Builder builder = null;
	protected boolean viaProto = false;
	
	private long startOffset;
	private long length;
	private Path path;
	private String filename;
	
	public Tablet() {
		builder = TabletProto.newBuilder();
		startOffset = length = -1;
	}
	
	public Tablet(Path tablePath, String filename, long start, long length) {
		this();
		this.set(tablePath, filename, start, length);
	}
	
	public Tablet(Path file, long start, long length) {
		this();
	  set(file.getParent().getParent(), file.getName(), start, length);
	}
	
	public Tablet(TabletProto proto) {
		startOffset = length = -1;
		this.proto = proto;
		this.viaProto = true;
	}
	
	public void set(Path path, String filename, long start, long length) {
		this.path = path;
		this.filename = filename;
		this.startOffset = start;
		this.length = length;
	}
	
	public Path getTablePath() {
		TabletProtoOrBuilder p = viaProto ? proto : builder;
		
		if (this.path != null) {
			return this.path;			
		}
		if (!proto.hasPath()) {
			return null;
		}
		this.path = new Path(p.getPath());
		
		return this.path;
	}
	
	public String getFileName() {
		TabletProtoOrBuilder p = viaProto ? proto : builder;
		
		if (this.filename != null) {
			return this.filename;	
		}
		if (!proto.hasFilename()) {
			return null;
		}
		this.filename = p.getFilename();
		return this.filename;
	}
	
	public Path getFilePath() {
		Path path = this.getTablePath();
		String filename = this.getFileName();
	  return new Path(path, "data/"+filename);
	}
	
	public long getStartOffset() {
		TabletProtoOrBuilder p = viaProto ? proto : builder;
		
		if (this.startOffset > -1) {
			return this.startOffset;
		}
		if (!proto.hasStartOffset()) {
			return -1;
		}
		
		this.startOffset = p.getStartOffset();
		return this.startOffset;
	}
	
	public long getLength() {
		TabletProtoOrBuilder p = viaProto ? proto : builder;
		
		if (this.length > -1) {
			return this.length;
		} 
		if (!proto.hasLength()) {
			return -1;
		}
		this.length = p.getLength();
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
			return (int)(this.getStartOffset() - t.getStartOffset());
		} else {
			return -1;
		}
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Tablet) {
			Tablet t = (Tablet)o;
			if (getFilePath().equals(t.getFilePath()) 
					&& t.getStartOffset() == this.getStartOffset()
					&& t.getLength() == this.getLength()) {
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
		return new String("(" + getFilePath() + " " + this.getStartOffset() + " " + getLength() + ")");
	}
	
	public TabletProto getProto() {
	    mergeLocalToProto();
	    
	    proto = viaProto ? proto : builder.build();
	    viaProto = true;
	    return proto;
	  }
	  
	  private void maybeInitBuilder() {
	    if (viaProto || builder == null) {
	      builder = TabletProto.newBuilder(proto);
	    }
	    viaProto = false;
	  }
	  
	  protected void mergeLocalToBuilder() {    
	    if (this.startOffset > -1) {
	      builder.setStartOffset(this.startOffset);
	    }
	    
	    if (this.length > -1) {
	      builder.setLength(this.length);
	    }
	    
	    if (this.path != null) {
	      builder.setPath(this.path.toString());
	    }
	    
	    if (this.filename != null) {
	      builder.setFilename(this.filename);
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
