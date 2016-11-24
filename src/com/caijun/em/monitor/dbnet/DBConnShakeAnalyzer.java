package com.caijun.em.monitor.dbnet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.caijun.em.Systore;
import com.caijun.utils.str.StringUtil;
import com.caijun.utils.xml.XMLHandler;

public class DBConnShakeAnalyzer {
	private final static String DETECT_TIME_INTERVAL = "DBNet.ConnDetect.TimeInterval";
	private final static String TIME_INTERVAL = "DBNet.connShake.TimeInterval";
	private Systore systore;

	public DBConnShakeAnalyzer(Systore systore) {
		super();
		this.systore = systore;
	}

	public List<DBShakeAnalysisRS> doing() {
		int time_interval = systore.props.getInMinLimit(TIME_INTERVAL, 60, 1);
		int detect_time_interval = systore.props.getInMinLimit(
				DETECT_TIME_INTERVAL, 5, 1);
		int disconnCount = time_interval / detect_time_interval - 1;
		if (disconnCount < 1) {
			disconnCount = 1;
		}
		String sql = "select dbid,ct from"
				+ "(select dbid,count(dbid) c,min(ct) ct from dbstatus t where t.disconn = 1 and t.ct > sysdate-?/24/60 group by dbid) "
				+ "where c>=?";
		return systore.jdbc.query(sql, new Object[] { time_interval,
				disconnCount }, new DBShakeAnalysisRSMapper());
	}

	private final class DBShakeAnalysisRSMapper implements
			RowMapper<DBShakeAnalysisRS> {
		public DBShakeAnalysisRS mapRow(ResultSet rs, int rowNum)
				throws SQLException {
			return new DBShakeAnalysisRS(rs.getLong("dbid"),
					rs.getTimestamp("ct"));
		}
	}

	static public class DBShakeAnalysisRS {
		private long dbId;
		private Date ct;

		public DBShakeAnalysisRS(long dbId, Date ct) {
			super();
			this.dbId = dbId;
			this.ct = ct;
		}

		public DBShakeAnalysisRS(String xml) throws Exception {
			super();
			Document doc = com.caijun.utils.xml.XMLHandler.loadXMLString(xml);
			Node dbshake = XMLHandler.getSubNode(doc, "dbshake");
			this.dbId = Long.parseLong(XMLHandler.getTagValue(dbshake, "i"));
			this.ct = StringUtil.parseToDate("yyyy-MM-dd HH:mm",
					(XMLHandler.getTagValue(dbshake, "ct")));
		}

		public String toXML() {
			StringBuffer sb = new StringBuffer();
			sb.append("<dbshake><i>" + this.dbId + "</i><ct>"
					+ StringUtil.formatDate("yyyy-MM-dd HH:mm", this.ct)
					+ "</ct></dbshake>");
			return sb.toString();
		}

		public long getDbId() {
			return dbId;
		}

		public Date getCt() {
			return ct;
		}

	}
}
