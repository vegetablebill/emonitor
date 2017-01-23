package com.caijun.em.mail;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.em.monitor.dbnet.DBInfo;
import com.caijun.em.monitor.dbschema.TrigMetaAnalyzer.TrigMetaAnalysisRS;
import com.caijun.mail.Email;
import com.caijun.utils.str.StringUtil;

public class DBSchemaTrigSendMail extends SendMail {
	private static String MAIL_TYPE_TRIG = "DBSchema.trig";
	private static String MAIL_TO = "Mail.DBSchema.ToUser";
	private static String MAIL_REPEAT_INTERVAL = "Mail.DBSchema.Repeat_Interval";
	private Map<String, Msg> trig_LSMS;
	private String mailContent;
	private List<Msg> msgs;

	public DBSchemaTrigSendMail(Systore systore, Logger logger) {
		super(systore, logger);
		trig_LSMS = new HashMap<String, Msg>();
		Date begin = new Date(new Date().getTime()
				- systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L)
				* 60000);
		List<Msg> msgs = this.getLatestSendMS(MAIL_TYPE_TRIG, begin);
		for (Msg msg : msgs) {
			trig_LSMS.put(msg.digest, msg);
		}
	}

	@Override
	public void beforeSend() {
		if (logger != null) {
			logger.info("上次处理过数据库触发器异常消息"
					+ trig_LSMS.values().size()
					+ "条"
					+ (!logger.isDebugEnabled() ? "." : ",消息ID为:"
							+ serializeMsgByID(trig_LSMS.values())));
		}
		msgs = getNeedSendMSG();
		if (logger != null) {
			logger.info("需要发送邮件的数据库触发器异常消息"
					+ msgs.size()
					+ "条"
					+ (!logger.isDebugEnabled() ? "." : ",消息ID为:"
							+ serializeMsgByID(msgs)));
		}

		if (msgs == null || msgs.size() == 0) {
			return;
		}
		mailContent = createMailContent(msgs);
	}

	@Override
	public Email send() {
		if (msgs == null || msgs.size() == 0) {
			return null;
		}
		String user = systore.props.get(MAIL_USER);
		String to = systore.props.get(MAIL_TO);
		Email email = Email.create().from(user).to(to);
		email.subject("数据库触发器异常提醒");
		email.addHtml(mailContent);
		return email;

	}

	@Override
	public void afterSend(String emailID) {
		if (msgs.size() == 0 && logger != null) {
			logger.info("没有发送数据库触发器异常邮件.");
		} else if (logger != null) {
			logger.info("发送数据库触发器异常邮件成功,邮件编号[" + emailID + "]");
		}
		setSended(msgs);
		long timeFlag = new Date().getTime()
				- systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L)
				* 60000;
		for (Msg msg : trig_LSMS.values()) {
			if (timeFlag > msg.ct.getTime()) {
				trig_LSMS.remove(msg.digest);
			}
		}
		for (Msg msg : msgs) {
			trig_LSMS.put(msg.digest, msg);
		}

	}

	private List<Msg> getNeedSendMSG() {
		boolean sendFlag = false;
		List<Msg> rs = new ArrayList<Msg>();
		List<Msg> skipMsgs = this.getUndealMS(MAIL_TYPE_TRIG);
		logger.info("本次需要处理的数据库触发器异常消息"
				+ skipMsgs.size()
				+ "条"
				+ (!logger.isDebugEnabled() ? "." : ",消息ID为:"
						+ serializeMsgByID(skipMsgs)));
		long time = systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000;
		Map<String, List<Msg>> msgs_map = this.groupByDigest(skipMsgs);
		for (String key : msgs_map.keySet()) {
			Msg msg = this.getNewestOne(msgs_map.get(key));
			rs.add(msg);
			if (!trig_LSMS.containsKey(key)
					|| msg.ct.getTime() - trig_LSMS.get(key).ct.getTime() >= time) {
				sendFlag = true;
				skipMsgs.remove(msg);
			}
		}
		if (!sendFlag) {
			rs.clear();
		}
		this.skipMsgs(skipMsgs);
		return rs;
	}

	private String createMailContent(List<Msg> msgs) {
		StringBuffer sb = new StringBuffer();
		List<Area> areas = systore.getAreas();
		List<DBInfo> dbinfos = systore.dbNet.getDBInfos();
		Map<Long, StringBuffer> db_htmls = new HashMap<Long, StringBuffer>();
		List<Msg> skips = new ArrayList<Msg>();
		for (Msg msg : msgs) {
			TrigMetaAnalysisRS trigMetaARS = null;
			try {
				trigMetaARS = new TrigMetaAnalysisRS(msg.content);
			} catch (Exception e) {
				logger.error("消息[" + msg.id + "]解析异常,丢弃", e);
				skips.add(msg);
				continue;
			}
			StringBuffer tmp = db_htmls.get(trigMetaARS.getDbId());
			if (tmp == null) {
				tmp = new StringBuffer();
				db_htmls.put(trigMetaARS.getDbId(), tmp);
			}

			tmp.append("<div><span class='font20'>");
			tmp.append(trigMetaARS.getName());
			tmp.append("</span><span class='font12'>&nbsp;&nbsp;");
			tmp.append(StringUtil.formatDate("yyyy-MM-dd HH:mm",
					trigMetaARS.getCt()));
			tmp.append("&nbsp;&nbsp;</span><span class='font12'>");
			if (trigMetaARS.isDroped()) {
				;
				tmp.append("被删除");
			} else if (trigMetaARS.isDisabled()) {
				tmp.append("被禁用");
			} else {
				tmp.append("被修改");
			}
			tmp.append("</span>");
			if (trigMetaARS.isDisabled()) {
				continue;
			}
			tmp.append("<table><tr><td class='td1'>原结构:</td><td>现结构:</td></tr><tr><td class='td1'>");
			tmp.append(StringUtil.replace(trigMetaARS.getStdStrc(),
					StringUtil.NEWLINE, "<br/>" + StringUtil.NEWLINE));
			tmp.append("</td><td>");
			if (!trigMetaARS.isDroped()) {
				tmp.append(StringUtil.replace(trigMetaARS.getNewStrc(),
						StringUtil.NEWLINE, "<br/>" + StringUtil.NEWLINE));
			}
			tmp.append("</td></tr></table></div>");
		}

		this.skipMsgs(skips);
		msgs.removeAll(skips);
		if (msgs.size() == 0) {
			return null;
		}

		for (DBInfo dbinfo : dbinfos) {
			StringBuffer tmp = db_htmls.get(dbinfo.getId());
			if (tmp != null) {
				tmp.insert(
						0,
						"<div><div><span class='font20'>["
								+ dbinfo.getUrl().replaceFirst(".*@", "")
										.replaceAll(":.*", "")
								+ "]</span><span class='font12'>数据库:</span></div><div class='trigs'>");
				tmp.append("</div></div>");
			}
		}

		sb.append("<html content='text/html; charset=gb2312'>");
		sb.append("<style type='text/css'>body {word-wrap:break-word;word-break:normal;} .area  {clear:both;} .area>div{float:left;} .font12 ,table{font-size:12px;} .font20 {font-size:20px;font-weight:bold} .trigs {margin-left:25px;width:850px;} .trigs>div {margin-top:5px;margin-bottom:10px;} table {width:100%;table-layout:fixed;border-collapse:collapse;border: 1px solid #ff00ff;} ");
		sb.append("td {padding:2px;text-align:left;vertical-align:top;} .td1 {border-right:2px solid #ff00ff;}</style>");
		sb.append("<body><H1>以下区域存在数据库触发器异常,请及时处理!!!</H1>");
		Map<Long, StringBuffer> area_htmls = new HashMap<Long, StringBuffer>();
		for (Area area : areas) {
			for (DBInfo dbInfo : dbinfos) {
				if (area.getId() != dbInfo.getAid()) {
					continue;
				}
				StringBuffer tmp_db = db_htmls.get(dbInfo.getId());
				if (tmp_db == null) {
					continue;
				}
				StringBuffer tmp_area = area_htmls.get(dbInfo.getAid());
				if (tmp_area == null) {
					tmp_area = new StringBuffer();
					area_htmls.put(dbInfo.getAid(), tmp_area);
				}
				tmp_area.append(tmp_db);
			}
		}

		for (Area area : areas) {
			StringBuffer tmp = area_htmls.get(area.getId());
			if (tmp != null) {
				sb.append("<div class='area'><div><div class='font20'>[");
				sb.append(area.getName());
				sb.append("]&nbsp;-&nbsp;</div></div><div>");
				sb.append(tmp);
				sb.append("</div></div>");
			}
		}
		sb.append("</body></html>");
		return sb.toString();
	}
}
