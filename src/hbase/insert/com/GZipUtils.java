package hbase.insert.com;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

/**
 * GZip类型文件解压
 * 
 * @author yaolei
 * @since 2013.09.03
 */
public class GZipUtils {

	/**
	 * get logger
	 */
	private static Logger logger = Logger.getLogger(GZipUtils.class);

	/**
	 * 缓存大小
	 */
	public static final int BUFFER = 1024;

	/**
	 * 扩展名
	 */
	public static final String EXT = ".gz";

	/**
	 * 待解压文件所在目录
	 */
	private static final String INPUT_DIRECTORY = "/home/xdr/store/hq_fdr";

	/**
	 * 输出至目录
	 */
	private static final String OUTPUT_DIRECTORY = "/home/xdr/hbase-test/hq_hdf_back";

	/**
	 * 解压文件
	 * 
	 * @param file
	 *            待解压文件
	 * @param outputPath
	 *            解压至
	 * 
	 */
	public static void decompress(File file, String outputPath) {
		// 文件输入流
		FileInputStream fis = null;
		// 文件输出流
		FileOutputStream fos = null;
		// 文件解压流
		GZIPInputStream gis = null;
		try {
			fis = new FileInputStream(file);
			fos = new FileOutputStream(outputPath + File.separator
					+ file.getName().replace(EXT, ""));

			// 解压文件中的数据
			gis = new GZIPInputStream(fis);
			int count;
			byte data[] = new byte[BUFFER];
			while ((count = gis.read(data, 0, BUFFER)) != -1) {
				fos.write(data, 0, count);
			}
		} catch (FileNotFoundException e) {
			logger.error("decompress error!" + e.getMessage());
		} catch (IOException e) {
			logger.error("decompress error!" + e.getMessage());
		} finally {
			// 释放资源
			try {
				gis.close();
				fis.close();
				fos.flush();
				fos.close();
			} catch (IOException e) {
				logger.error("close resource error!" + e.getMessage());
			}
		}

	}

	/**
	 * 遍历目录下文件
	 * 
	 * @param directory
	 *            目标路径
	 */
	public static void iteratorFile(String directory) {
		File d = new File(directory);
		// list file
		File[] list = d.listFiles();
		for (File f : list) {
			// 如果是文件，并且后缀名为.gz
			if (f.isFile() && f.getPath().endsWith(EXT)) {
				decompress(f, OUTPUT_DIRECTORY);
				//System.out.println("file path:" + f.getPath());
				//System.out.println("file name:" + f.getName());
			}
		}

	}

	public static void main(String[] args) {
		iteratorFile(INPUT_DIRECTORY);
	}

}
