package hbase.insert.com;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
//import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * ���̶߳�ȡ�ļ�,����hbase��
 * @author dengyongbin
 * @version 2013-09-03
 */
public class InsertDataSource implements Runnable{
	// hbase conf
	static Configuration config;
	// Դ�ļ����λ��
	private static String sourceFile = "/home/xdr/hbase-test/hq_hdf_back";
	// ����ѹ�ļ�����Ŀ¼
	private static final String INPUT_DIRECTORY = "/home/xdr/store/hq_fdr";
	// �����ı���
	private static String tableName = "CDR_201201_TEST";
	// htable
	HTable htable;
	CountDownLatch latch = null;
	// �ܼ���
	private int count = 0;
	// �̶��������߳�����
	//private static int threadNum = 80;
	// ��ʼС��
	private int start = 0;
	// �����±�
	private int end = 0;
	// �ļ���������
	private File[] files;
	
	private static final long WRITE_BUFFER_SIZE = 1024 * 1024 * 5;
	private static final long MAX_FILE_SIZE = 1024 * 1024 * 20;
	//private static final int COREPOOLSIZE = 2;
	//private static final int MAXINUMPOOLSIZE = 5;
	//private static final long KEEPALIVETIME = 4;
	//private static final TimeUnit UNIT = TimeUnit.SECONDS;
	//private static final BlockingQueue<Runnable> WORKQUEUE = new ArrayBlockingQueue<Runnable>(3);
	//private static final AbortPolicy HANDLER = new ThreadPoolExecutor.AbortPolicy();
	
	static{
		config = new Configuration();
		config.addResource("hbase-site.xml");
		config = HBaseConfiguration.create(config);
	}
	
	/**
	 * ���캯��
	 * @param fileNames	�ļ���������
	 * @param index		��ǰ���߳�
	 * @param fileNumber	�ļ��ĸ���
	 */
	public InsertDataSource(File[] listFiles) {
		//this.latch = cdl;
		try {
			htable = new HTable(config, Bytes.toBytes(tableName));
			htable.setAutoFlush(false);
			htable.setWriteBufferSize(1024 * 1024 * 5);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//int size = (fileNumber < threadNum) ? fileNumber : fileNumber / threadNum;
		long fileSize = 0;
		for (int i = end; i < listFiles.length; i++) {
			if (listFiles[i].length() > MAX_FILE_SIZE) {
				start = end;
				end = i;
				break;
			} else {
				fileSize += listFiles[i].length();
				if (fileSize > MAX_FILE_SIZE) {
					fileSize = 0;
					start = end;
					end = i + 1;
					break;
				}
			}
		}
		this.files = listFiles;
		System.out.println("start = " + start + " and end = " + end);
	}
	
	/**
	 * ��ȡ
	 * Դ�ļ�,����hbase
	 * @param fileNames
	 * @param start
	 * @param end
	 */
	private void readFile (int start,int end) {
		Calendar cal = Calendar.getInstance();
		BufferedReader br = null;
		InputStreamReader isr = null;
		FileInputStream fis = null;
		Vector<Put> vector = new Vector<Put>();
		for (int i = start; i < end; i++) {
			try {
				fis = new FileInputStream(sourceFile + "/" + files[i].getName());
				isr = new InputStreamReader(fis);
				br = new BufferedReader(isr);
				String line = null;
				while((line = br.readLine()) != null){
					StringBuffer sbRow = new StringBuffer();
					StringBuffer sbValue = new StringBuffer();
					String[] lines = line.split("\\|");
					long ts = cal.getTimeInMillis();
					for (int j = 0; j < lines.length; j++) {
						if (j < 3) {
							sbRow.append(lines[j]).append("|");
						} else {
							if (j == lines.length-1) {
								sbValue.append(lines[j]);
							} else {
								sbValue.append(lines[j]).append("|");
							}
						}
					}
					Put put = new Put(Bytes.toBytes(sbRow.toString()));
					put.add(Bytes.toBytes("cdr"), Bytes.toBytes("null"), ts, Bytes.toBytes(sbValue.toString()));
					vector.add(put);
					count++;
					if (count % 1000 == 0) {
						htable.put(vector);
						vector.clear();
						if (htable.getWriteBufferSize() == WRITE_BUFFER_SIZE) {
							System.out.println("upload data is 5m");
						}
					}
				}
				htable.put(vector);
				vector.clear();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					fis.close();
					isr.close();
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			//htable.flushCommits();
			
			htable.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		// ���̶߳�д
		readFile(start, end);
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private static File[] sortListFile(File[] listFiles) {
		for (int i = 0; i < listFiles.length; i++) {
			for (int j = i+1; j < listFiles.length; j++) {
				if (listFiles[i].length() > listFiles[j].length()) {
					File temp = listFiles[i];
					listFiles[i] = listFiles[j];
					listFiles[j] = temp;
				}
			}
		}
		return listFiles;
	}
	
	private static int taskAssign(File[] listFiles) {
		int sumTask = 0;
		long fileSize = 0;
		for (int i = 0; i < listFiles.length; i++) {
			if (listFiles[i].length() > MAX_FILE_SIZE) {
				sumTask++;
			} else {
				fileSize += listFiles[i].length();
				if (fileSize > MAX_FILE_SIZE) {
					fileSize = 0;
					sumTask++;
				}
			}
		}
		return sumTask;
	}
	
	/**
	 * �������
	 * @param args
	 */
	public static void main(String[] args) {
		// ��ѹ
		GZipUtils.iteratorFile(INPUT_DIRECTORY);
		// Դ�ļ�Ŀ¼
		File file = new File(sourceFile);
		// �ļ�����
		//int fileNumber = file.listFiles().length;
		// �ļ���������
		//String[] fileNames = file.list();
		// �ļ���С����
		File[] listFiles = sortListFile(file.listFiles());
		// �ܷ����������
		int sumTask = taskAssign(listFiles);
		//ThreadPoolExecutor pool = new ThreadPoolExecutor(fileNumber, fileNumber + 50, KEEPALIVETIME, UNIT, WORKQUEUE, HANDLER);
		// �̳߳�
		ExecutorService pool = Executors.newFixedThreadPool(sumTask);
		for (int i = 0; i < sumTask; i++) {
			InsertDataSource r = new InsertDataSource(listFiles);
			Thread thread = new Thread(r);
			pool.submit(thread);
			System.out.println(i + " is run ...");
		}
		pool.shutdown();
		System.out.println(Thread.currentThread().getName() + " is run ...");
	}
	
	
}
