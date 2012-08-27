package tajo.util;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class FileUtil {
	public static void writeProto(File file, Message proto) throws IOException {
		FileOutputStream stream = new FileOutputStream(file);
		stream.write(proto.toByteArray());
		stream.close();		
	}
	public static void writeProto(OutputStream out, Message proto) throws IOException {
		out.write(proto.toByteArray());
	}
	
	public static void writeProto(Configuration conf, Path path, Message proto) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataOutputStream stream = fs.create(path);
		stream.write(proto.toByteArray());
		stream.close();
	}
	
	public static Message loadProto(File file, Message proto) throws IOException {
		FileInputStream in = new FileInputStream(file);
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);		
		return builder.build();
	}
	
	public static Message loadProto(InputStream in, Message proto) throws IOException {
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
		return builder.build();
	}
	
	public static Message loadProto(Configuration conf, Path path, Message proto) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream in = new FSDataInputStream(fs.open(path));
		Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
		return builder.build();
	}
	
	public static File getFile(String path) {
		return new File(path);
	}
	
	public static String readTextFile(File file) throws IOException {
    StringBuilder fileData = new StringBuilder(1000);
    BufferedReader reader = new BufferedReader(
            new FileReader(file));
    char[] buf = new char[1024];
    int numRead = 0;
    while((numRead=reader.read(buf)) != -1){
        String readData = String.valueOf(buf, 0, numRead);
        fileData.append(readData);
        buf = new char[1024];
    }
    reader.close();
    return fileData.toString();
  }

  public static String humanReadableByteCount(long bytes, boolean si) {
    int unit = si ? 1000 : 1024;
    if (bytes < unit) return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }
}
