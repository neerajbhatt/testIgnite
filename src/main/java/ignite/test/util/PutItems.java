package ignite.test.util;

import java.io.ByteArrayInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import ignite.test.core.*;
public class PutItems {
	public static void main(String[] args) throws IgniteException {
		int maxItemCount = args.length == 1 ? Integer.valueOf(args[0]) : 10;

		putItemsInIgnite(maxItemCount);
	}

	private static void putItemsInIgnite(int itemCount) {
		Ignition.setClientMode(true);

		IgniteConfiguration conf = new IgniteConfiguration();
		conf.setPeerClassLoadingEnabled(true);
		TcpDiscoverySpi discovery = new TcpDiscoverySpi();

		TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

		ipFinder.setAddresses(Arrays.asList("172.29.65.249", "172.29.65.250", "172.29.65.251"));

		discovery.setIpFinder(ipFinder);

		conf.setDiscoverySpi(discovery);

		Ignite ignite = Ignition.start(conf);

		IgniteCluster cluster = ignite.cluster();

		CacheConfiguration<String, TestItem> itemCfg = new CacheConfiguration<String, TestItem>("TESTCACHE");
		itemCfg.setCacheMode(CacheMode.PARTITIONED);
		itemCfg.setIndexedTypes(String.class, TestItem.class);
		itemCfg.setBackups(1);
		itemCfg.setQueryParallelism(Runtime.getRuntime().availableProcessors() - 1);
		cluster.ignite().destroyCache("TESTCACHE");
		
		IgniteCache<String, TestItem> itemCache = cluster.ignite().getOrCreateCache(itemCfg);

		System.out.println("putting data");
		Map<String, TestItem> itemMap = new HashMap<String, TestItem>();
		for (int counter=0;counter < itemCount;counter++){
			TestItem item=new TestItem();
			item.setId(counter);
			item.setField1("field1"+counter);
			item.setField2("field2"+counter);
			itemMap.put(String.valueOf(counter), item);
			
		}
		itemCache.putAll(itemMap);
		Ignition.stop(false);
	}
	
}