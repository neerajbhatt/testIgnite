package ignite.test.core;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class TestItem implements Serializable {

	/**
	* 
	*/
	private static final long serialVersionUID = 1L;


	@QuerySqlField(index=true,descending=true)
	private String field1;
	@QuerySqlField(index=true)
	private String field2;
	
	@QuerySqlField(index=true)
	private int id;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
 
	public String getField1() {
		return field1;
	}
	public void setField1(String field1) {
		this.field1 = field1;
	}
	public String getField2() {
		return field2;
	}
	public void setField2(String field2) {
		this.field2 = field2;
	}
	
	 
	 
}
