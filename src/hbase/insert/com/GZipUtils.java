package hbase.insert.com;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

/**
 * GZip�����ļ���ѹ
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
	 * �����С
	 */
	public static final int BUFFER = 1024;

	/**
	 * ��չ��
	 */
	public static final String EXT = ".gz";

	/**
	 * ����ѹ�ļ�����Ŀ¼
	 */
	private static final String INPUT_DIRECTORY = "/home/xdr/store/hq_fdr";

	/**
	 * �����Ŀ¼
	 */
	private static final String OUTPUT_DIRECTORY = "/home/xdr/hbase-test/hq_hdf_back";

	/**
	 * ��ѹ�ļ�
	 * 
	 * @param file
	 *            ����ѹ�ļ�
	 * @param outputPath
	 *            ��ѹ��
	 * 
	 */
	public static void decompress(File file, String outputPath) {
		// �ļ�������
		FileInputStream fis = null;
		// �ļ������
		FileOutputStream fos = null;
		// �ļ���ѹ��
		GZIPInputStream gis = null;
		try {
			fis = new FileInputStream(file);
			fos = new FileOutputStream(outputPath + File.separator
					+ file.getName().replace(EXT, ""));

			// ��ѹ�ļ��е�����
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
			// �ͷ���Դ
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
	 * ����Ŀ¼���ļ�
	 * 
	 * @param directory
	 *            Ŀ��·��
	 */
	public static void iteratorFile(String directory) {
		File d = new File(directory);
		// list file
		File[] list = d.listFiles();
		for (File f : list) {
			// ������ļ������Һ�׺��Ϊ.gz
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
