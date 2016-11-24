package com.caijun.em.monitor.dbschema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.caijun.em.Systore;
import com.caijun.utils.collection.SortedArrayList;
import com.caijun.utils.str.StringUtil;
import com.tongtech.cj.dbMeta.TBMeta.ColMeta;
import com.tongtech.cj.dbMeta.TBMetaComparer;
import com.tongtech.cj.dbMeta.TBMetaComparer.CompareCOLSRS;

public class TBMetaAnalyzer {
	private Systore systore;

	public TBMetaAnalyzer(Systore systore) {
		super();
		this.systore = systore;
	}

	public List<TBMetaAnalysisRS> doing(List<TBStatus> list) {
		List<TBMetaAnalysisRS> result = new ArrayList<TBMetaAnalysisRS>();
		List<TBInfo> tbInfos = systore.dbSchema.getTBInfos();
		for (TBInfo tbInfo : tbInfos) {
			for (TBStatus tbstatus : list) {
				if (tbInfo.getId() == tbstatus.getTbid()) {
					CompareCOLSRS rs = TBMetaComparer.compareCols(tbInfo.getStrcut(), tbstatus.getStrcut(), true, true);
					if (tbstatus.isDroped() || rs.getDiff().size() > 0 || rs.getOnlyleft().size() > 0
							|| rs.getOnlyRight().size() > 0) {
						result.add(new TBMetaAnalysisRS(tbInfo.getDbid(), tbInfo.getSchema() + "." + tbInfo.getName(),
								tbstatus.isDroped(), rs, tbstatus.getCt()));
					}
					break;
				}
			}
		}

		return result;
	}

	static public class TBMetaAnalysisRS {
		private long dbId;
		private String name;
		private boolean droped;
		private CompareCOLSRS colrs;
		private Date ct;

		public TBMetaAnalysisRS(long dbId, String name, boolean droped, CompareCOLSRS colrs, Date ct) {
			super();
			this.dbId = dbId;
			this.name = name;
			this.droped = droped;
			this.colrs = colrs;
			this.ct = ct;
		}

		public String toXML() {
			StringBuffer sb = new StringBuffer();
			sb.append("<tbmcrs><i>").append(this.dbId).append("</i><n>").append(this.name).append("</n>").append("<d>")
					.append(this.droped).append("</d>");
			SortedArrayList<String> cns = new SortedArrayList<String>(new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}
			});
			Map<String, ColMeta> onlyLeft = new HashMap<String, ColMeta>();
			Map<String, ColMeta> onlyRight = new HashMap<String, ColMeta>();
			Map<String, ColMeta[]> diff = new HashMap<String, ColMeta[]>();
			Map<String, ColMeta> jointly = new HashMap<String, ColMeta>();
			for (ColMeta colMeta : colrs.getOnlyleft()) {
				cns.add(colMeta.getName());
				onlyLeft.put(colMeta.getName(), colMeta);
			}
			for (ColMeta colMeta : colrs.getOnlyRight()) {
				cns.add(colMeta.getName());
				onlyRight.put(colMeta.getName(), colMeta);
			}
			for (ColMeta colMeta : colrs.getJointly()) {
				cns.add(colMeta.getName());
				jointly.put(colMeta.getName(), colMeta);
			}
			for (ColMeta[] colMeta : colrs.getDiff()) {
				cns.add(colMeta[0].getName());
				diff.put(colMeta[0].getName(), colMeta);
			}

			sb.append("<crs>");
			for (String cn : cns) {
				sb.append("<cr>");
				if (onlyLeft.containsKey(cn)) {
					sb.append("<c1>").append(onlyLeft.get(cn).toString()).append("</c1><c2/>");
				} else if (onlyRight.containsKey(cn)) {
					sb.append("<c1/><c2>").append(onlyRight.get(cn).toString()).append("</c2>");
				} else if (jointly.containsKey(cn)) {
					String tmp = jointly.get(cn).toString();
					sb.append("<c1>").append(tmp).append("</c1><c2>").append(tmp).append("</c2>");
				} else if (diff.containsKey(cn)) {
					sb.append("<c1>").append(diff.get(cn)[0].toString()).append("</c1><c2>")
							.append(diff.get(cn)[1].toString()).append("</c2>");
				}
				sb.append("</cr>");
			}
			sb.append("</crs>");

			sb.append("<ct>" + StringUtil.formatDate("yyyy-MM-dd HH:mm", this.ct) + "</ct>");
			sb.append("</tbmcrs>");
			return sb.toString();
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

		public Date getCt() {
			return ct;
		}

		public CompareCOLSRS getColrs() {
			return colrs;
		}

	}
}
