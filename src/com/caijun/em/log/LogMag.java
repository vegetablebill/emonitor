package com.caijun.em.log;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class LogMag implements BeanPostProcessor {
	private Logger logger = Logger.getRootLogger();
	// private static final String FORMAT = "%d [%t] %-5p %l - %m%n";
	private static final String FORMAT = "%d [%t] %-5p - %m%n";
	private static final String ROLLINGFILE_PARAM_P = "fileSize=([1-9][0-9]*);maxBackup=([1-9][0-9]*)";
	private JdbcTemplate jdbc;
	private Map<String, LogConfig> logConfig_cache;
	private Pattern rollingFile_param_p;

	public LogMag(JdbcTemplate jdbc) {
		super();
		long begin = new Date().getTime();
		logger.debug("[LogMag]开始加载...");
		this.jdbc = jdbc;
		logConfig_cache = new HashMap<String, LogConfig>();
		rollingFile_param_p = Pattern.compile(ROLLINGFILE_PARAM_P);
		init();
		long end = new Date().getTime();
		double time = (end - begin) / 1000.0;
		logger.debug("[LogMag]加载完成,用时:" + time + "s");

	}

	private void init() {
		Pattern type_p = Pattern.compile("(rollingfile)|(dailyRollingFile)");
		Pattern level_p = Pattern
				.compile("(off)|(fatal)|(error)|(warn)|(info)|(debug)|(trace)|(all)");

		List<LogConfig> list = jdbc.query(
				"select lname,ltype,llevel,filename,params from logconfig",
				new LogConfigMapper());
		final List<LogConfig> temp = new ArrayList<LogConfig>();
		boolean valid = true;
		for (LogConfig logConfig : list) {
			if (logConfig.type == null
					|| !type_p.matcher(logConfig.type).matches()) {
				logConfig.type = "rollingfile";
				valid = false;
			}
			if (logConfig.level == null
					|| !level_p.matcher(logConfig.level).matches()) {
				logConfig.level = "info";
				valid = false;
			}
			if (logConfig.type.equals("rollingfile")) {
				if (logConfig.params == null
						|| !rollingFile_param_p.matcher(logConfig.params)
								.matches()) {
					logConfig.params = "fileSize=10485760;maxBackup=10";
					valid = false;
				}
			}
			if (!valid) {
				temp.add(logConfig);
			}
			valid = true;
		}

		jdbc.batchUpdate(
				"update logconfig set ltype=?,llevel=?,filename=?,params=? where lname=?",
				new BatchPreparedStatementSetter() {
					@Override
					public int getBatchSize() {
						return temp.size();
					}

					@Override
					public void setValues(PreparedStatement ps, int i)
							throws SQLException {
						LogConfig logConfig = temp.get(i);
						ps.setString(1, logConfig.type);
						ps.setString(2, logConfig.level);
						ps.setString(3, logConfig.fileName);
						ps.setString(4, logConfig.params);
						ps.setString(5, logConfig.name);
					}
				});

		for (LogConfig logConfig : list) {
			logConfig_cache.put(logConfig.name, logConfig);
		}

		for (LogConfig logConfig : logConfig_cache.values()) {
			if ("rollingfile".equals(logConfig.type)) {
				createRollingFileLogger(logConfig);
			} else if ("dailyRollingFile".equals(logConfig.type)) {
				createDailyRollingFileLogger(logConfig);
			} else {
				createDailyRollingFileLogger(logConfig);
			}
		}

	}

	private void createRollingFileLogger(LogConfig logConfig) {
		if (!"rollingfile".equals(logConfig.type)) {
			return;
		}
		String name = logConfig.name;
		Level level = getLevel(logConfig.level);
		String fileName = logConfig.fileName;
		Matcher m = rollingFile_param_p.matcher(logConfig.params);
		String fileSize = "10485760";
		int maxBackup = 10;
		if (m.find()) {
			fileSize = m.group(1);
			maxBackup = Integer.parseInt(m.group(2));
		}
		Logger logger = null;
		if (name.equals("rootlog")) {
			logger = Logger.getRootLogger();
		} else {
			logger = Logger.getLogger(name);
		}
		// logger.removeAllAppenders();
		// logger.setAdditivity(false);
		logger.setLevel(level);
		PatternLayout layout = new PatternLayout();
		layout.setConversionPattern(FORMAT);
		try {
			RollingFileAppender appender = new RollingFileAppender(layout,
					fileName, true);
			appender.setEncoding("UTF-8");
			appender.setMaxFileSize(fileSize);
			appender.setMaxBackupIndex(maxBackup);
			logger.addAppender(appender);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void createDailyRollingFileLogger(LogConfig logConfig) {
		if (!"dailyRollingFile".equals(logConfig.type)) {
			return;
		}
		String name = logConfig.name;
		Level level = getLevel(logConfig.level);
		String fileName = logConfig.fileName;
		Logger logger = null;
		if (name.equals("rootlog")) {
			logger = Logger.getRootLogger();
		} else {
			logger = Logger.getLogger(name);
		}
		// logger.removeAllAppenders();
		// logger.setAdditivity(false);
		logger.setLevel(level);
		PatternLayout layout = new PatternLayout();
		layout.setConversionPattern(FORMAT);
		DailyRollingFileAppender appender;
		try {
			appender = new DailyRollingFileAppender(layout, fileName,
					"yyyyMMdd");
			appender.setEncoding("UTF-8");
			logger.addAppender(appender);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private class LogConfig {
		private String name;
		private String type;
		private String level;
		private String fileName;
		private String params;

	}

	private final class LogConfigMapper implements RowMapper<LogConfig> {
		public LogConfig mapRow(ResultSet rs, int rowNum) throws SQLException {
			LogConfig logConfig = new LogConfig();
			String lname = rs.getString("lname");
			String ltype = rs.getString("ltype");
			String level = rs.getString("llevel");
			String filename = rs.getString("filename");
			String params = rs.getString("params");
			logConfig.name = lname == null ? null : lname.trim();
			logConfig.type = ltype == null ? null : ltype.trim();
			logConfig.level = level == null ? null : level.trim();
			logConfig.fileName = filename == null ? null : filename.trim();
			logConfig.params = params == null ? null : params.trim();
			return logConfig;
		}
	}

	private Level getLevel(String level) {
		if ("off".equals(level)) {
			return Level.OFF;
		} else if ("fatal".equals(level)) {
			return Level.FATAL;
		} else if ("error".equals(level)) {
			return Level.ERROR;
		} else if ("warn".equals(level)) {
			return Level.WARN;
		} else if ("info".equals(level)) {
			return Level.INFO;
		} else if ("debug".equals(level)) {
			return Level.DEBUG;
		} else if ("trace".equals(level)) {
			return Level.TRACE;
		} else if ("all".equals(level)) {
			return Level.ALL;
		}
		return Level.OFF;
	}

	@Override
	public Object postProcessAfterInitialization(Object arg0, String arg1)
			throws BeansException {
		// TODO Auto-generated method stub
		return arg0;
	}

	@Override
	public Object postProcessBeforeInitialization(Object arg0, String arg1)
			throws BeansException {
		// TODO Auto-generated method stub
		return arg0;
	}

}
