package ignite.test.util;

import java.util.ArrayList;

import java.util.Collection;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

public class TestIgniteCallable implements IgniteCallable<List<String>> {

	private static final long serialVersionUID = 1L;

	private Ignite ignite;

	private String query;

	public TestIgniteCallable(String query) {
		super();
		this.query = query;

	}

	@IgniteInstanceResource
	public void setIgnite(Ignite ignite) {
		this.ignite = ignite;
	}

	private List<String> convert(List<List<?>> res) {
		List<String> items = new ArrayList<String>();
		if (res != null) {
			for (List<?> list : res) {
				items.addAll((Collection<? extends String>) list);
			}
		}
		return items;
	}

	public List<String> call() throws Exception {

		try {
			SqlFieldsQuery qry = new SqlFieldsQuery(this.query);
			qry.setEnforceJoinOrder(true);
			IgniteCache<Integer, ignite.test.core.TestItem> cache = ignite.getOrCreateCache("TESTCACHE");
			List<List<?>> res = cache.query(qry.setArgs().setLocal(true)).getAll();
			List<String> items = convert(res);
			System.out.println("size=>" + res.size() + " query=>" + qry.getSql() + items);
			return items;
		} catch (Exception e) {
			System.out.println("exceptionx: " + e.getMessage());
			e.printStackTrace();
		}

		return null;

	}

}
