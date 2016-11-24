package com.caijun.em;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class Props {
	private JdbcTemplate jdbc; 
	private ConcurrentHashMap<String, String> propsCache;
	private Logger logger = Logger.getRootLogger();
	
	
	
	public Props(JdbcTemplate jdbc) {
		super();
		this.jdbc = jdbc;
		init();
	}

	public String get(String k){
		return propsCache.get(k);
	}
	
	public String get(String k,String defaultv){
		String v = propsCache.get(k);
		if(v==null){
			addOrModify(k,defaultv);
			return defaultv;
		}
		return v;
	}
	
	public String getInPattern(String patternK){
		return patternK;
	}
		
	public int get(String k,int defaultv){
		return (int) getnum(k,defaultv,Integer.MIN_VALUE,Integer.MAX_VALUE);
	}
	
	public int getInMinLimit(String k,int defaultv,int min){
		return (int) getnum(k,defaultv,min,Integer.MAX_VALUE);
	}
	
	public int getInMaxLimit(String k,int defaultv,int max){
		return (int) getnum(k,defaultv,Integer.MIN_VALUE,max);
	}
	
	public int get(String k,int defaultv,int min,int max){
		return (int) getnum(k,defaultv,min,max);
	}
	
	public long get(String k,long defaultv){
		return getnum(k,defaultv,Long.MIN_VALUE,Long.MAX_VALUE);
	}
	
	public long getInMinLimit(String k,long defaultv,long min){
		return getnum(k,defaultv,min,Long.MAX_VALUE);
	}
	
	public long getInMaxLimit(String k,long defaultv,long max){
		return getnum(k,defaultv,Long.MIN_VALUE,max);
	}
	
	public long getInMaxLimit(String k,long defaultv,long min,long max){
		return getnum(k,defaultv,min,max);
	}
	
	private long getnum(String k,long defaultv,long min,long max){
		String v = propsCache.get(k);
		if(v==null){
			addOrModify(k,Long.toString(defaultv));
			return defaultv;
		}
		long iv=0;
		try{
			iv = Long.valueOf(v);
		}catch(Exception e){
			addOrModify(k,Long.toString(defaultv));
			return defaultv;
		}
		if(iv<min||iv>max){
			addOrModify(k,Long.toString(defaultv));
			return defaultv;
		}
		return iv;
	}
	
	public void addOrModify(String k,String v){
		String ov = propsCache.get(k);
		if(v==ov){
			return;
		}
		if(propsCache.containsKey(k)){
			jdbc.update("update props set v = ? where k = ?", v,k);
		}else{
			jdbc.update("insert into props(k,v) values(?,?)", k,v);
		}
		propsCache.put(k, v);
	}
	
	private  void init(){
		propsCache = new ConcurrentHashMap<String,String>();
		load_propsCache();
		
	}
	
	private void load_propsCache(){
		propsCache.clear();
		jdbc.query("select k,v from props",new ResultSetExtractor<Map<String,String>>(){
			@Override
			public Map<String,String> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				String k;
				String v;
				while(rs.next()){
					k = rs.getString(1);
					v = rs.getString(2);
					k = k==null?null:k.trim();
					v = v==null?null:v.trim();
					propsCache.put(k, v);
				}
				return propsCache;
			}
		});
	}
}
