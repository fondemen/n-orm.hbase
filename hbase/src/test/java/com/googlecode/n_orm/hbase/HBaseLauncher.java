package com.googlecode.n_orm.hbase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;

import com.googlecode.n_orm.DatabaseNotReachedException;
import com.googlecode.n_orm.PersistingElement;
import com.googlecode.n_orm.StoreSelector;
import com.googlecode.n_orm.StoreTestLauncher;
import com.googlecode.n_orm.hbase.Store;


public class HBaseLauncher extends StoreTestLauncher {
	private static Map<String, Object> hbaseProperties = null;

	public static String hbaseHost;
	public static Integer hbasePort = HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
	public static Integer hbaseMaxRetries = 1;
	/**
	 * To launch a map/reduce minicluster.
	 * Only works for localhost
	 */
	public static boolean withMapReduce = false;

	public static HBaseTestingUtility hBaseServer = null;
	public static com.googlecode.n_orm.hbase.Store hbaseStore;
	
	static {
		try {
			hbaseHost = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			hbaseHost = "localhost";
		}
	}
	
	public static Map<String, Object> prepareHBase() {
		try {
			Store s = Store.getStore(hbaseHost, hbasePort, hbaseMaxRetries);
			s.start();
			hbaseStore = s;
		} catch (Exception x) {}
		Properties p = new Properties();
		p.setProperty("host", hbaseHost);
		p.setProperty("port", Integer.toString(hbasePort));
		if (hbaseMaxRetries != null)
			p.setProperty("maxRetries", hbaseMaxRetries.toString());
		return prepareHBase(p);
	}
	
	public static Map<String, Object> prepareHBase(Class<? extends PersistingElement> clazz) throws IOException {
		try {
			Store s = (Store) StoreSelector.getInstance().getStoreFor(clazz);
			s.start();
			hbaseStore = s;
		} catch (Exception x) {}
		Map<String, Object> p = StoreSelector.getInstance().findProperties(clazz);
		Properties prop = new Properties();
		for (Entry<String, Object> e : p.entrySet()) {
			if (! (e.getValue() instanceof String)) throw new IllegalArgumentException("Cannot use class " + clazz.getSimpleName() + " as property " + e.getKey() + " is not a String (actually " + e.getValue() + ")");
			prop.put(e.getKey(), (String)e.getValue());
		}
		return prepareHBase(prop);
	}

	public static Map<String, Object> prepareHBase(Properties properties) {
		Map<String, Object> p = new TreeMap<String, Object>();
		p.put(StoreSelector.STORE_DRIVERCLASS_PROPERTY, com.googlecode.n_orm.hbase.Store.class.getName());
		p.put(StoreSelector.STORE_DRIVERCLASS_STATIC_ACCESSOR, "getStore");

		if (hbaseStore == null && hBaseServer == null) {
			if (hbaseMaxRetries == null) {
				String retries = properties.getProperty("maxRetries");
				if (retries != null) hbaseMaxRetries = Integer.parseInt(retries);
			}
			
			// Starting HBase server
			try {
				hBaseServer = new HBaseTestingUtility();
				hBaseServer.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
				hBaseServer.getConfiguration().setInt("hbase.client.pause", 250);
				hBaseServer.getConfiguration().setInt("test.hbase.zookeeper.property.clientPort", hbasePort);
				if (hbaseMaxRetries != null) hBaseServer.getConfiguration().setInt("hbase.client.retries.number", hbaseMaxRetries);
				
				hBaseServer.startMiniCluster(1);
				if (withMapReduce) hBaseServer.startMiniMapReduceCluster();
				hbaseHost = hBaseServer.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM);
				hbasePort = hBaseServer.getConfiguration().getInt(HConstants.ZOOKEEPER_CLIENT_PORT, HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
				
				hbaseStore = com.googlecode.n_orm.hbase.Store.getStore(hbaseHost, hbasePort, hbaseMaxRetries);
				hbaseStore.setConf(hBaseServer.getConfiguration());
				hbaseStore.setConnection(hBaseServer.getHBaseAdmin().getConnection());
				hbaseStore.start();
				com.googlecode.n_orm.hbase.Store.setKnownStore(properties, hbaseStore);
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		p.put("1", hbaseHost);
		p.put("2", hbasePort.toString());
		if (hbaseMaxRetries != null) p.put("3", Integer.toString(hbaseMaxRetries));
			
		return p;
	}

	public static void setConf(Store store) {
		store.setConf(hBaseServer.getConfiguration());
	}

	@Override
	public Map<String, Object> prepare(Class<?> testClass) {
		if (hbaseProperties == null)
			hbaseProperties = prepareHBase();
		return hbaseProperties;
	}

//	@AfterClass
//	public static void shutdownHBase() {
//		try {
//			hBaseServer.shutdownMiniCluster();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
}
