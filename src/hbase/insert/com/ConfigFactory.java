package hbase.insert.com;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;

public final class ConfigFactory {

	private static final String CONFIG_FILE_DEFAULT_PATH = "config.xml";
	private static XMLConfiguration config = null;

	public static void init(String configFilePath) {
		if (configFilePath == null) {
			configFilePath = CONFIG_FILE_DEFAULT_PATH;
		}

		try {
			config = new XMLConfiguration(configFilePath);
			config.setReloadingStrategy(new FileChangedReloadingStrategy());
		} catch (ConfigurationException e) {
			System.out.println("Fatal:Create Config Object Error!!!");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static String getString(String configXPath) {
		return config.getString(configXPath, null);
	}

	public static String getString(String configXPath, String defaultValue) {
		return config.getString(configXPath, defaultValue);
	}

	public static int getInt(String configXPath) {
		return config.getInt(configXPath);
	}

	public static int getInt(String configXPath, int defaultValue) {
		return config.getInt(configXPath, defaultValue);
	}

	public static float getFloat(String configXPath) {
		return config.getFloat(configXPath, 1.0F);
	}

	public static boolean getBoolean(String configXPath) {
		return config.getBoolean(configXPath);
	}

	public static boolean getBoolean(String configXPath, boolean defaultValue) {
		return config.getBoolean(configXPath, defaultValue);
	}

	/*public static List<String> getList(String configXPath) {
		return config.getList(configXPath);
	}*/
}
