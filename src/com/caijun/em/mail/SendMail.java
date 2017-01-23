package com.caijun.em.mail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import com.caijun.db.DBUtil;
import com.caijun.em.Systore;
import com.caijun.mail.Email;

public abstract class SendMail {
	protected static final String MAIL_USER = "Mail.User";
	protected static int M_UNDEALED = 0;
	protected static int M_SENDED = 1;
	protected static int M_SKIP = 2;

	protected Systore systore;
	protected Logger logger;

	public SendMail(Systore systore, Logger logger) {
		super();
		this.systore = systore;
		this.logger = logger;
	}

	public abstract void beforeSend();

	public abstract Email send();

	public abstract void afterSend(String emailID);

	protected List<Msg> getUndealMS(String type) {
		String sql = "select id,mt,dc,mc,ms,ct from mailmessage t where mt='" + type + "' and ms=" + M_UNDEALED;
		List<Msg> msgs = systore.jdbc.query(sql, new MsgRowMapper());
		return msgs;
	}

	protected List<Msg> getLatestSendMS(String type, Date begin) {
		String sql = "select t1.id,t1.mt,t1.dc,t1.mc,t1.ms,t1.ct from mailmessage t1,"
				+ "(select max(id) id from mailmessage t where mt='" + type + "' and ms=" + M_SENDED
				+ " and ct>=? group by dc) t2 where t1.id=t2.id";
		List<Msg> msgs = systore.jdbc.query(sql, new Object[] { begin }, new MsgRowMapper());
		return msgs;
	}

	protected Map<String, List<Msg>> groupByDigest(List<Msg> msgs) {
		Map<String, List<Msg>> map = new HashMap<String, List<Msg>>();
		for (Msg msg : msgs) {
			if (!map.containsKey(msg.digest)) {
				map.put(msg.digest, new ArrayList<Msg>());

			}
			map.get(msg.digest).add(msg);
		}
		return map;
	}

	protected Msg getOldestOne(List<Msg> msgs) {
		Msg oldest = null;
		for (Msg msg : msgs) {
			if (oldest == null || oldest.ct.getTime() > msg.ct.getTime()) {
				oldest = msg;
			}

		}
		return oldest;
	}

	protected Msg getNewestOne(List<Msg> msgs) {
		Msg newest = null;
		for (Msg msg : msgs) {
			if (newest == null || msg.ct.getTime() > newest.ct.getTime()) {
				newest = msg;
			}
		}
		return newest;
	}

	protected void skipMsgs(final List<Msg> msgs) {
		setFlag(msgs, M_SKIP);
	}

	protected void setSended(final List<Msg> msgs) {
		setFlag(msgs, M_SENDED);
	}

	protected String serializeMsgByID(Collection<Msg> msgs) {
		List<Long> ids = new ArrayList<Long>();
		for (Msg msg : msgs) {
			ids.add(msg.id);
		}
		return ids.toString();
	}

	private void setFlag(final List<Msg> msgs, final int flag) {
		systore.jdbc.batchUpdate("update mailmessage set ms=" + flag + " where id =?",
				new BatchPreparedStatementSetter() {
					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						msgs.get(i).flag = flag;
						ps.setLong(1, msgs.get(i).id);
					}

					@Override
					public int getBatchSize() {
						return msgs.size();
					}
				});
	}

	protected class Msg {
		long id;
		String type;
		String digest;
		String content;
		int flag;
		Date ct;
	}

	final protected class MsgRowMapper implements RowMapper<Msg> {

		@Override
		public Msg mapRow(ResultSet rs, int i) throws SQLException {
			Msg msg = new Msg();
			msg.id = rs.getLong(1);
			msg.type = rs.getString(2);
			msg.digest = rs.getString(3);
			msg.content = DBUtil.readBlob(rs, 4, "utf-8");
			msg.flag = rs.getInt(5);
			msg.ct = rs.getTimestamp(6);
			return msg;
		}

	}

}
