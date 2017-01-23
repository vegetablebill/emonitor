package com.caijun.em.mail;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.em.monitor.dbnet.DBInfo;
import com.caijun.mail.Email;
import com.caijun.utils.xml.XMLHandler;

public class DBSchemaTBSendMail extends SendMail {
	private static String MAIL_TYPE_TB = "DBSchema.tb";
	private static String MAIL_TO = "Mail.DBSchema.ToUser";
	private static String MAIL_REPEAT_INTERVAL = "Mail.DBSchema.Repeat_Interval";
	private Map<String, Msg> tb_LSMS;
	private String mailContent;
	private List<Msg> msgs;

	public DBSchemaTBSendMail(Systore systore, Logger logger) {
		super(systore, logger);
		tb_LSMS = new HashMap<String, Msg>();
		Date begin = new Date(
				new Date().getTime() - systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000);
		List<Msg> msgs = this.getLatestSendMS(MAIL_TYPE_TB, begin);
		for (Msg msg : msgs) {
			tb_LSMS.put(msg.digest, msg);
		}
	}

	@Override
	public void beforeSend() {
		if (logger != null) {
			logger.info("上次处理过数据库表结构异常消息" + tb_LSMS.values().size() + "条"
					+ (!logger.isDebugEnabled() ? "." : ",消息ID为:" + serializeMsgByID(tb_LSMS.values())));
		}
		msgs = getNeedSendMSG();
		if (logger != null) {
			logger.info("需要发送邮件的数据库表结构异常消息" + msgs.size() + "条"
					+ (!logger.isDebugEnabled() ? "." : ",消息ID为:" + serializeMsgByID(msgs)));
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
		email.subject("数据库表结构异常提醒");
		email.addHtml(mailContent);
		return email;

	}

	@Override
	public void afterSend(String emailID) {
		if (msgs.size() == 0 && logger != null) {
			logger.info("没有发送数据库表结构异常邮件.");
		} else if (logger != null) {
			logger.info("发送数据库表结构异常邮件成功,邮件编号[" + emailID + "]");
		}
		setSended(msgs);
		long timeFlag = new Date().getTime() - systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000;
		for (Msg msg : tb_LSMS.values()) {
			if (timeFlag > msg.ct.getTime()) {
				tb_LSMS.remove(msg.digest);
			}
		}
		for (Msg msg : msgs) {
			tb_LSMS.put(msg.digest, msg);
		}

	}

	private List<Msg> getNeedSendMSG() {
		boolean sendFlag = false;
		List<Msg> rs = new ArrayList<Msg>();
		List<Msg> skipMsgs = this.getUndealMS(MAIL_TYPE_TB);
		logger.info("本次需要处理的数据库表结构异常消息" + skipMsgs.size() + "条"
				+ (!logger.isDebugEnabled() ? "." : ",消息ID为:" + serializeMsgByID(skipMsgs)));
		long time = systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000;
		Map<String, List<Msg>> msgs_map = this.groupByDigest(skipMsgs);
		for (String key : msgs_map.keySet()) {
			Msg msg = this.getNewestOne(msgs_map.get(key));
			rs.add(msg);
			if (!tb_LSMS.containsKey(key) || msg.ct.getTime() - tb_LSMS.get(key).ct.getTime() >= time) {
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
			Long dbid = null;
			try {
				Node tbmcrs = XMLHandler.getSubNode(com.caijun.utils.xml.XMLHandler.loadXMLString(msg.content),
						"tbmcrs");
				dbid = Long.parseLong(XMLHandler.getTagValue(tbmcrs, "i"));
				StringBuffer tmp = db_htmls.get(dbid);
				if (tmp == null) {
					tmp = new StringBuffer();
					db_htmls.put(dbid, tmp);
				}
				tmp.append("<div><span class='font20'>");
				tmp.append(XMLHandler.getTagValue(tbmcrs, "n"));
				tmp.append("</span><span class='font12'>&nbsp;&nbsp;");
				tmp.append(XMLHandler.getTagValue(tbmcrs, "ct"));
				tmp.append("&nbsp;&nbsp;</span><span class='font12'>");
				if (Boolean.parseBoolean(XMLHandler.getTagValue(tbmcrs, "d"))) {
					tmp.append("被删除");
				}else{
					tmp.append("被修改");
				}
				tmp.append("</span>");
				tmp.append("<table><tr><td>原字段:</td><td>现字段:</td></tr>");
				for (Node cr : XMLHandler.getNodes(XMLHandler.getSubNode(tbmcrs, "crs"), "cr")) {
					String left = XMLHandler.getTagValue(cr, "c1");
					String right = XMLHandler.getTagValue(cr, "c2");
					if (left == null || right == null) {
						tmp.append("<tr class='bold'><td>");

					}else if (!left.equals(right)) {
						tmp.append("<tr class='gred'><td>");
					} else {
						tmp.append("<tr><td>");
					}
					tmp.append(left == null ? "" : left);
					tmp.append("</td><td>");
					tmp.append(right == null ? "" : right);
					tmp.append("</td></tr>");
				}
				tmp.append("</table></div>");
			} catch (Exception e) {
				logger.error("消息[" + msg.id + "]解析异常,丢弃", e);
				db_htmls.remove(dbid);
				skips.add(msg);
			}
		}

		this.skipMsgs(skips);
		msgs.removeAll(skips);
		if (msgs.size() == 0) {
			return null;
		}

		for (DBInfo dbinfo : dbinfos) {
			StringBuffer tmp = db_htmls.get(dbinfo.getId());
			if (tmp != null) {
				tmp.insert(0,
						"<div><div><span class='font20'>["
								+ dbinfo.getUrl().replaceFirst(".*@", "").replaceAll(":.*", "")
								+ "]</span><span class='font12'>数据库:</span></div><div class='tbs'>");
				tmp.append("</div></div>");
			}
		}

		sb.append("<html content='text/html; charset=gb2312'>");
		sb.append(
				"<style type='text/css'>body {word-wrap:break-word;word-break:normal;} .area {clear:both;} .area>div {float:left;} .font12 ,table{font-size:12px;} .font20 {font-size:20px;font-weight:bold} .tbs {margin-left:25px;width:850px;} .tbs>div {margin-top:5px;margin-bottom:10px;} table {width:100%;table-layout:fixed;border-collapse:collapse;border: 1px solid #ff00ff;} td {padding:2px;border: 1px solid #ff00ff;text-align:left;vertical-align:top;} .gred {background-color:yellow;font-weight:bold;} .bold {font-weight:bold;}</style>");
		sb.append("<body><H1>以下区域存在数据库表结构异常,请及时处理!!!</H1>");
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
