package stork.dls.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeConverter {
	static SimpleDateFormat sdf1 = new SimpleDateFormat("MMM DD HH:mm");
	static SimpleDateFormat sdf2 = new SimpleDateFormat("MMM DD YYYY");
	public static long getTimeStamp(String datestring) {
		Date date = null;
		try {
			date = sdf1.parse(datestring);
		} catch (ParseException e1) {
			try {
				date = sdf2.parse(datestring);
			} catch (ParseException e2) {
			}
		}
		if (null != date) {
			System.out.println(datestring + " converted to: " + date.getTime());
		}
		return (null == date) ? -1 : date.getTime();
	}
	
	public static void main(String[] args) {

	}

}

