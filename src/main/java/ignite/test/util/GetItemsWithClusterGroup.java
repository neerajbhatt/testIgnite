package ignite.test.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;


public class GetItemsWithClusterGroup {


	public static void main(String args[]) {
			int queryType =Integer.parseInt(args[0]);
			int maxNumberOfQueries = Integer.parseInt(args[1]);
			Ignition.setClientMode(true);
			IgniteConfiguration conf = new IgniteConfiguration();
			conf.setPeerClassLoadingEnabled(true);
			TcpDiscoverySpi discovery = new TcpDiscoverySpi();
			TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
			String i = (args.length >= 3 ? args[2] : "249");
			String ip = "a.b.c."+ i; 
			ipFinder.setAddresses(Arrays.asList(ip));
			discovery.setIpFinder(ipFinder);
			conf.setDiscoverySpi(discovery);
			Ignite ignite = Ignition.start(conf);
			
			System.out.println("IP=> " + i);
			
			IgniteCluster cluster = ignite.cluster();
			List<String> groups = Arrays.asList("i-"+i);
			List<IgniteCompute> computes = new ArrayList<IgniteCompute>();
	 
		ClusterGroup cg = cluster.forAttribute("ROLE","i-"+i );
		//computes.add(ignite.compute(ignite.cluster().forNodes(cg.nodes())).withAsync().withTimeout(5000));
		computes.add(ignite.compute(cg).withAsync().withTimeout(5000));
		
		int numberOfQueries = 0;
		
		while (numberOfQueries < maxNumberOfQueries) {
			numberOfQueries++;
			process(ignite, computes, queryType, cluster);
		}

	}

	private static void process(Ignite ignite, List<IgniteCompute> computes, 
			int queryType, IgniteCluster cluster) {
	 
		List<IgniteFuture<List<String>>> futures = new ArrayList<IgniteFuture<List<String>>>();
		StringBuilder buff = new StringBuilder();
		final List<Object> args = new ArrayList<Object>();
		if (queryType == 0) {
			buff = new StringBuilder("SELECT T.id FROM " + "TESTCache.TestItem as T LIMIT 3");
		}
		if (queryType == 1) {
			buff = new StringBuilder("SELECT T.id FROM " + "TESTCache.TestItem as T order by field1 desc LIMIT 3");
	
		}
		
	SqlFieldsQuery qry = new SqlFieldsQuery(buff.toString());
	 	long t0 = System.currentTimeMillis();
		for (IgniteCompute async : computes) {
			
			async.call(new TestIgniteCallable(buff.toString()));
			

			IgniteFuture<List<String>> future = async.future();
			futures.add(future);
		}

		List<String> list = new ArrayList<String>();

		for (IgniteFuture<List<String>> future : futures) {
			try {
				List<String> returnList = future.get(500, TimeUnit.MILLISECONDS);
			if (returnList != null)
					list.addAll(returnList);
			} catch (Exception e) {
				System.out.println("got exception1 " + e.getMessage());
				e.printStackTrace();
			}
		}
		long t1 = System.currentTimeMillis();
		String date = new SimpleDateFormat("HH:mm:ss").format(new Date());
		Set<String> set = new HashSet<String>(list);
		System.out.println(date + "-" + (t1 - t0) + "-   " + list.size() + "#" + set.size() + "  -" 
				+ (list.size() > 0 ? list : "empty") + "-" + Thread.currentThread().getName());
	}

 
 
}