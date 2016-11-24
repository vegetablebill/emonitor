package com.caijun.em;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class IDGen {
	private final static String ID_CACHE ="IDGen.cache";
	private JdbcTemplate jdbc;
	private Props props;
	private  Lock lock = new ReentrantLock();
	private  long uplimit;
	private  AtomicLong cur;
	private Logger logger = Logger.getRootLogger();
	
	
	
	public IDGen(JdbcTemplate jdbc, Props props) {
		super();
		this.jdbc = jdbc;
		this.props = props;
		init();
	}

	private void init(){
		long curvalue = jdbc.query("select curvalue from idgen", new ResultSetExtractor<Long>(){
			@Override
			public Long extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				if(!rs.next()){
					return 0L;
				}
				return rs.getLong("curvalue");
			}
		});
		uplimit= curvalue+props.getInMinLimit(ID_CACHE, 1000, 1);
		cur = new AtomicLong(curvalue);
		if(curvalue==0){
			jdbc.update("insert into idgen(curvalue) values("+uplimit+")");
		}else{
			jdbc.update("update idgen set curvalue="+uplimit);
		}
	}
	
	public long getCur(){
		return cur.get();
	}
	
	public long getNext(){
		long next = cur.addAndGet(1);
		if(next>=uplimit){
			lock.tryLock();
			try{
				if(next<uplimit){
					return next;
				}
			uplimit+=props.getInMinLimit(ID_CACHE, 1000, 1);
			jdbc.update("update idgen set curvalue="+uplimit);
			}finally{
				lock.unlock();
			}
		}
		return next;
		
		
	}

}
