package nta.util;

public class NumberUtil {

	public static long unsigned32(int n) {
		return n & 0xFFFFFFFFL;
	}
	
	public static int unsigned16(short n) {
		return n & 0xFFFF;
	}
}
