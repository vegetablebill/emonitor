package com.caijun.em.monitor.dbschema;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.caijun.em.Systore;
import com.caijun.utils.str.StringUtil;
import com.caijun.utils.xml.XMLHandler;
import com.tongtech.cj.dbMeta.TrigMetaComparer;

public class TrigMetaAnalyzer {
	private Systore systore;

	public TrigMetaAnalyzer(Systore systore) {
		super();
		this.systore = systore;
	}

	public List<TrigMetaAnalysisRS> doing(List<TrigStatus> list,
			boolean withStruct) {
		List<TrigMetaAnalysisRS> result = new ArrayList<TrigMetaAnalysisRS>();
		List<TrigInfo> trigInfos = systore.dbSchema.getTrigInfos();

		for (TrigInfo trigInfo : trigInfos) {
			for (TrigStatus tristatus : list) {
				if (trigInfo.getId() == tristatus.getTrigid()) {

					if (tristatus.isDroped()) {
						result.add(new TrigMetaAnalysisRS(systore.dbNet
								.getDBInfo(trigInfo.getDbid()).getAid(),
								trigInfo.getDbid(), trigInfo.getSchema() + "."
										+ trigInfo.getTrigName(), true, true,
								trigInfo.getStrcut().getContent(), null,
								tristatus.getCt()));
						break;
					} else if (tristatus.isDisabled()) {
						result.add(new TrigMetaAnalysisRS(systore.dbNet
								.getDBInfo(trigInfo.getDbid()).getAid(),
								trigInfo.getDbid(), trigInfo.getSchema() + "."
										+ trigInfo.getTrigName(), false, true,
								trigInfo.getStrcut().getContent(), null,
								tristatus.getCt()));
						break;
					} else if (withStruct
							&& !TrigMetaComparer.compare(trigInfo.getStrcut(),
									tristatus.getStrcut())) {
						result.add(new TrigMetaAnalysisRS(systore.dbNet
								.getDBInfo(trigInfo.getDbid()).getAid(),
								trigInfo.getDbid(), trigInfo.getSchema() + "."
										+ trigInfo.getTrigName(), false, false,
								trigInfo.getStrcut().getContent(), tristatus
										.getStrcut().getContent(), tristatus
										.getCt()));
						break;
					}
				}
			}
		}

		return result;
	}

	static public class TrigMetaAnalysisRS {
		private long areaId;
		private long dbId;
		private String name;
		private boolean droped = false;
		private boolean disabled = false;
		private String stdStrc;
		private String newStrc;
		private Date ct;

		public TrigMetaAnalysisRS(long areaId, long dbId, String name,
				boolean droped, boolean disabled, String stdStrc,
				String newStrc, Date ct) {
			super();
			this.areaId = areaId;
			this.dbId = dbId;
			this.name = name;
			this.droped = droped;
			this.disabled = disabled;
			this.stdStrc = stdStrc;
			this.newStrc = newStrc;
			this.ct = ct;
		}

		public TrigMetaAnalysisRS(String xml) throws Exception {
			super();
			Document doc = com.caijun.utils.xml.XMLHandler.loadXMLString(xml);
			Node tgmcrs = XMLHandler.getSubNode(doc, "tgmcrs");
			this.areaId = Long.parseLong(XMLHandler.getTagValue(tgmcrs, "aid"));
			this.dbId = Long.parseLong(XMLHandler.getTagValue(tgmcrs, "dbid"));
			this.name = XMLHandler.getTagValue(tgmcrs, "n");
			this.droped = Boolean.parseBoolean(XMLHandler.getTagValue(tgmcrs,
					"drop"));
			this.disabled = Boolean.parseBoolean(XMLHandler.getTagValue(tgmcrs,
					"disable"));
			this.stdStrc = XMLHandler.getTagValue(tgmcrs, "std");
			this.newStrc = XMLHandler.getTagValue(tgmcrs, "new");
			this.ct = StringUtil.parseToDate("yyyy-MM-dd HH:mm",
					(XMLHandler.getTagValue(tgmcrs, "ct")));
		}

		public String toXML() {
			StringBuffer sb = new StringBuffer();
			sb.append("<tgmcrs><aid>").append(this.areaId)
					.append("</aid><dbid>").append(this.dbId)
					.append("</dbid><n>").append(this.name)
					.append("</n><drop>").append(this.droped)
					.append("</drop><disable>").append(this.disabled)
					.append("</disable>").append("<std><![CDATA[")
					.append(this.stdStrc == null ? "" : this.stdStrc)
					.append("]]></std><new><![CDATA[")
					.append(this.newStrc == null ? "" : this.newStrc)
					.append("]]></new><ct>")
					.append(StringUtil.formatDate("yyyy-MM-dd HH:mm", this.ct));
			sb.append("</ct></tgmcrs>");
			return sb.toString();
		}

		public long getAreaId() {
			return areaId;
		}

		public long getDbId() {
			return dbId;
		}

		public String getName() {
			return name;
		}

		public boolean isDroped() {
			return droped;
		}

		public boolean isDisabled() {
			return disabled;
		}

		public String getStdStrc() {
			return stdStrc;
		}

		public String getNewStrc() {
			return newStrc;
		}

		public Date getCt() {
			return ct;
		}

	}
}
