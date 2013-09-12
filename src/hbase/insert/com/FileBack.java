package hbase.insert.com;

import java.io.File;

public class FileBack {
	
	public static void delZipFile(File[] listFiles){
		for (int i = 0; i < listFiles.length; i++) {
			File file = new File(listFiles[i].getPath());
			if (file.isFile()) {
				file.delete();
			}
		}
	}
	
	public static void main(String[] args) {
		File file = new File("E:\\unicom\\store_back");
		delZipFile(file.listFiles());
	}
}
