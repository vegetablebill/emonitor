package com.caijun.em.mail;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import com.caijun.em.Systore;
import com.caijun.utils.str.StringUtil;

public class MSender {
	private static Systore systore;

	public static void setSystore(Systore systore) {
		MSender.systore = systore;
	}

	public static void sendMsg(String msgType, String content) {

		systore.jdbc
				.update("insert into mailmessage(id,mt,dc,mc,ms,ct) values(?,?,?,?,?,?)",
						systore.id.getNext(), msgType,
						StringUtil.toHexString(StringUtil.md5(content)),
						content, 0,
						new java.sql.Timestamp(new Date().getTime()));
	}

	public static void sendMsgs(String msgType, String... contents) {
		final class MsgBatchPreparedStatementSetter implements
				BatchPreparedStatementSetter {
			private String msgType;
			private String[] contents;
			private java.sql.Timestamp timestamp;

			public MsgBatchPreparedStatementSetter(String msgType,
					String[] contents) {
				this.contents = contents;
				this.msgType = msgType;
				timestamp = new java.sql.Timestamp(new Date().getTime());

			}

			@Override
			public int getBatchSize() {
				return contents.length;
			}

			@Override
			public void setValues(PreparedStatement ps, int i)
					throws SQLException {
				ps.setLong(1, systore.id.getNext());
				ps.setString(2, msgType);
				ps.setString(3,
						StringUtil.toHexString(StringUtil.md5(contents[i])));
				ps.setString(4, contents[i]);
				ps.setInt(5, 0);
				ps.setTimestamp(6, timestamp);

			}

		}
		systore.jdbc
				.batchUpdate(
						"insert into mailmessage(id,mt,dc,mc,ms,ct) values(?,?,?,?,?,?)",
						new MsgBatchPreparedStatementSetter(msgType, contents));
	}

}
