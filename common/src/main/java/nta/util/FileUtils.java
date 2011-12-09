package nta.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.protobuf.Message;

public class FileUtils {
	public static void writeProto(File file, Message proto) throws IOException {
		FileOutputStream stream = new FileOutputStream(file);
		stream.write(proto.toByteArray());
		stream.close();		
	}
	public static void writeProto(OutputStream out, Message proto) throws IOException {
		out.write(proto.toByteArray());
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
}
