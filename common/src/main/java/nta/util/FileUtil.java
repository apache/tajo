package nta.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.protobuf.Message;

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
}
