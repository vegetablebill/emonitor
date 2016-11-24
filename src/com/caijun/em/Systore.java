package com.caijun.em;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.caijun.em.monitor.dbnet.DBNetMag;
import com.caijun.em.monitor.dbschema.DBSchemaMag;

public class Systore {
	public JdbcTemplate jdbc;
	public Props props;
	public IDGen id;
	public DBNetMag dbNet;
	public DBSchemaMag dbSchema;
	private ConcurrentHashMap<Long, Area> areasCache;

	private Logger logger = Logger.getRootLogger();

	public Systore(JdbcTemplate jdbc, Props props, IDGen id, DBNetMag dbNet,
			DBSchemaMag dbSchema) {
		super();
		this.jdbc = jdbc;
		this.props = props;
		this.id = id;
		this.dbNet = dbNet;
		this.dbSchema = dbSchema;
		areasCache = new ConcurrentHashMap<Long, Area>();
		load_areasCache();
	}

	public List<Area> getAreas() {
		List<Area> list = new ArrayList<Area>(areasCache.values());
		Collections.sort(list, new Comparator<Area>() {
			@Override
			public int compare(Area o1, Area o2) {
				if (o1.getPos() > o2.getPos()) {
					return 1;
				}
				return -1;
			}
		});
		return list;

	}

	public Area getArea(long id) {
		return areasCache.get(id);
	}

	private void load_areasCache() {
		areasCache.clear();
		List<Area> areas = jdbc.query("select id,aname,aid,pos from areas",
				new AreaMapper());
		for (Area area : areas) {
			areasCache.put(area.getId(), area);
		}
	}

	private final class AreaMapper implements RowMapper<Area> {
		public Area mapRow(ResultSet rs, int rowNum) throws SQLException {
			Area area = new Area();
			area.setId(rs.getLong("id"));
			String aname = rs.getString("aname");
			aname = aname == null ? null : aname.trim();
			area.setName(aname);
			String aid = rs.getString("aid");
			aid = aid == null ? null : aid.trim();
			area.setAid(aid);
			area.setPos(rs.getInt("pos"));
			return area;
		}
	}

}
