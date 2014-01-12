package stork.dls.util;

import java.lang.reflect.Field;


public class DLSLog {
	private final String className;
	private boolean debugEnable;
	private boolean initFlag = false;
	private DLSLog(String className){
		this.className = className;
	}
	
	public static DLSLog getLog(String className){

		return new DLSLog(className);
	}
	
	public void debug(String debugInfo){
		if(false == initFlag){
			initFlag = true;
			Class c = null;
			Field field = null;
			try{
				c = Class.forName(className);
				field = c.getDeclaredField("DEBUG_PRINT");
				debugEnable = field.getBoolean(field);
			}catch (Exception ex){
			}
		}
		if(debugEnable){
			System.out.println(this.className + ": " + debugInfo);
		}
		//save to physical file?
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
