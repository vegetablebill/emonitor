package com.caijun.em.monitor.dbnet;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.caijun.em.Systore;
import com.caijun.utils.str.StringUtil;
import com.caijun.utils.xml.XMLHandler;

public class DBDisConnAnalyzer {
	private final static String DISCONNTIME = "DBNet.disconnTime";
	private Systore systore;

	public DBDisConnAnalyzer(Systore systore) {
		super();
		this.systore = systore;
	}

	public List<DBDisConnAnalysisRS> doing(List<DBStatus> list) {
		List<DBDisConnAnalysisRS> result = new ArrayList<DBDisConnAnalysisRS>();
		long curTime = new Date().getTime();
		long time = systore.props.getInMinLimit(DISCONNTIME, 30, 0) * 6000;
		List<DBInfo> allDBInfo = systore.dbNet.getDBInfos();
		for (DBInfo dbInfo : allDBInfo) {
			for (DBStatus dbstatus : list) {
				if (dbInfo.getId() == dbstatus.getDbid()
						&& dbstatus.isDisconn()
						&& curTime - dbstatus.getCt().getTime() >= time) {
					result.add(new DBDisConnAnalysisRS(dbstatus.getDbid(),
							dbstatus.getCt()));
					break;
				}
			}
		}
		return result;
	}

	static public class DBDisConnAnalysisRS {
		private long dbId;
		private Date ct;

		public DBDisConnAnalysisRS(long dbId, Date ct) {
			super();
			this.dbId = dbId;
			this.ct = ct;
		}

		public DBDisConnAnalysisRS(String xml) throws Exception {
			super();
			Document doc = com.caijun.utils.xml.XMLHandler.loadXMLString(xml);
			Node dbshake = XMLHandler.getSubNode(doc, "dbdisconn");
			this.dbId = Long.parseLong(XMLHandler.getTagValue(dbshake, "i"));
			this.ct = StringUtil.parseToDate("yyyy-MM-dd HH:mm",
					(XMLHandler.getTagValue(dbshake, "ct")));
		}

		public String toXML() {
			StringBuffer sb = new StringBuffer();
			sb.append("<dbdisconn><i>" + this.dbId + "</i><ct>"
					+ StringUtil.formatDate("yyyy-MM-dd HH:mm", this.ct)
					+ "</ct></dbdisconn>");
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
