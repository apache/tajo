package tajo.util;

public class ReflectionUtil {
	public static Object newInstance(Class<?> clazz) 
			throws InstantiationException, IllegalAccessException {         
		return clazz.newInstance();
	}
}
