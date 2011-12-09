package nta.util;

public class ReflectionUtils {
	public static Object newInstance(Class<?> clazz) 
			throws InstantiationException, IllegalAccessException {         
		return clazz.newInstance();
	}
}
