package hbase.insert.com;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class LoadDeptFile {
	
	private static HashMap<String,String> hm = new HashMap<String,String>();
	
	public static void load(String path){
		BufferedReader br = null;
		InputStreamReader isr = null;
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(path);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			String line = null;
			while((line = br.readLine()) != null){
				String[] lines = line.split(" |	");
				hm.put(lines[0], lines[3] + ";" + lines[4]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		load("E:\\unicom\\Èë¿â×ÊÁÏ\\hh.txt");
		/*int count = 0;
		for (Iterator iterator = hm.entrySet().iterator(); iterator.hasNext();) {
			Entry type = (Entry) iterator.next();
			System.out.println(type.getKey() + " " + type.getValue());
			count++;
		}
		System.out.println(count);*/
	}
}
