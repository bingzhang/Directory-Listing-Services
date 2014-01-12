package stork.dls.util;

import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.Date;
/**
 * time unit: milliseconds
 * thread safety
 * @author bing
 *
 */
public class DLSLogTime {
	final private long start;
	final private long MICROLLEVEL = 1000;
	final private long MILLILLEVEL = MICROLLEVEL*1000;
	final private long SECONDLEVEL = MILLILLEVEL*1000;
	private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
	private final Date date = new Date();
	
	public DLSLogTime(){
		start = System.nanoTime();
	}
	//return milliseconds
	public long getTime(){
		return this.date.getTime();
	}
	public String toString(){
		return df.format(date).toString();
	}
	public long timeElapse(){
		long end = System.nanoTime();
		return end - this.start;
	}
	public double timeElapseStr(boolean flag){
		long end = System.nanoTime();
		double ret = (end-this.start)*1.0f/SECONDLEVEL;
		return ret;
		
	}
	public String timeElapseStr(){
		long end = System.nanoTime();
		long servicetime = (end - this.start); 
		String servicetimeElapse = null;
		if( servicetime <= MICROLLEVEL){
			servicetimeElapse = servicetime + ") ns; ";
		}else if(servicetime <= MILLILLEVEL){
			double microsprecison = servicetime*1.0f / MICROLLEVEL;
			servicetimeElapse = String.format("%1$.4f ) μs; ", microsprecison);
		}else if(servicetime <= SECONDLEVEL){
			float msprecison = servicetime*1.0f / MILLILLEVEL;
			servicetimeElapse = String.format("%1$.4f ) ms; ", msprecison);
		}else {
			float secprecison = servicetime*1.0f / SECONDLEVEL;
			servicetimeElapse = String.format("%1$.4f ) s; ", secprecison);
		}
		return servicetimeElapse;
	}

	public String timeStamp(){
		return new Timestamp(date.getTime()).toString();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
