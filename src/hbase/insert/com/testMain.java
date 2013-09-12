package hbase.insert.com;

import org.apache.log4j.Logger;


public class testMain {
	
	private static Logger logg = Logger.getLogger(testMain.class);
	
	public static void main(String[] args) {
		ConfigFactory.init(null);
		String a = ConfigFactory.getString("fileBack.cycle");
		System.out.println(a);
		logg.error("sdafasdfdsaf");
		logg.info("111111111111");
	}
}
