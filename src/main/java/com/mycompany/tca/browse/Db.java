package com.mycompany.tca.browse;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author zendle.joe
 */
public class Db {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	// static final String JDBC_DRIVER = "com.vertica.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/npm_tca_iot";
	// static final String DB_URL = "jdbc:vertica://invertidcplp301.twtelecom.com:5433/NPMP";

	//  Database credentials
	static final String USER = "jzendle";
	// static final String USER = "npm_batch";
	static final String PASS = "jzendle";
	// static final String PASS = "batch3";

	static final String SELECT_RESOURCES
		= "select guid from resource where circuit = ?  and  virtual_circuit = ? and bclli = ? ";

	static final String SELECT_TCA_INSTANCES
		= "select * from tca_instance where resource = ? ";

	static final String SELECT_METRIC
		= "SELECT\n"
		+ "    metric.guid,\n"
		+ "    threshold_type.name,\n"
		+ "    element_subtype.name AS level,\n"
		+ "    metric.threshold,\n"
		+ "    threshold_type.operator,\n"
		+ "    element.name,\n"
		+ "    element.info        AS units,\n"
		+ "    element.description AS threshold_name\n"
		+ "FROM\n"
		+ "    metric\n"
		+ "LEFT JOIN\n"
		+ "    threshold_type\n"
		+ "ON\n"
		+ "    (\n"
		+ "        metric.threshold_type = threshold_type.id)\n"
		+ "LEFT JOIN\n"
		+ "    element\n"
		+ "ON\n"
		+ "    (\n"
		+ "        metric.metric = element.id)\n"
		+ "LEFT JOIN\n"
		+ "    element_subtype\n"
		+ "ON\n"
		+ "    (\n"
		+ "        metric.level = element_subtype.id) "
		+ "        where metric.guid = ? ";

	static final String SELECT_ALERT
		= "SELECT\n"
		+ "    alert.guid AS guid,\n"
		+ "    element_subtype_alias2.name AS timezone,\n"
		+ "    element_subtype_alias1.name AS period\n"
		+ "FROM\n"
		+ "    alert\n"
		+ "INNER JOIN\n"
		+ "    element_subtype element_subtype_alias1\n"
		+ "ON\n"
		+ "    (\n"
		+ "        alert.period = element_subtype_alias1.id)\n"
		+ "INNER JOIN\n"
		+ "    element_subtype element_subtype_alias2\n"
		+ "ON\n"
		+ "    (\n"
		+ "        alert.timezone = element_subtype_alias2.id) "
		+ "        where alert.metric = ? ";

	static final String SELECT_ACTION = "SELECT\n"
		+ "    alert_action_parameter.value,\n"
		+ "    action_parameter.name\n"
		+ "FROM\n"
		+ "    action\n"
		+ "INNER JOIN\n"
		+ "    alert_action_parameter\n"
		+ "ON\n"
		+ "    (\n"
		+ "        action.guid = alert_action_parameter.action)\n"
		+ "INNER JOIN\n"
		+ "    action_parameter\n"
		+ "ON\n"
		+ "    (\n"
		+ "        alert_action_parameter.action_parameter = action_parameter.id)"
		+ "        where action.alert = ? ";

	public List getResources(String circuit, String vcircuit, String clli) throws SQLException, ClassNotFoundException {

		try (Connection conn = Db.createConnection()) {
			return processQuery(conn,SELECT_RESOURCES,circuit,vcircuit,clli);
		}
	}
	public List getTCAs(String guid) throws SQLException, ClassNotFoundException {

		try (Connection conn = Db.createConnection()) {
			return processQuery(conn,SELECT_TCA_INSTANCES, guid);
		}
	}
	public List getMetrics(String metricGuid) throws SQLException, ClassNotFoundException {

		try (Connection conn = Db.createConnection()) {
			return processQuery(conn,SELECT_METRIC, metricGuid);
		}
	}
	public List getAlerts(String alertGuid) throws SQLException, ClassNotFoundException {

		try (Connection conn = Db.createConnection()) {
			return processQuery(conn,SELECT_ALERT, alertGuid);
		}
	}
	public List getActions(String actionGuid) throws SQLException, ClassNotFoundException {

		try (Connection conn = Db.createConnection()) {
			return processQuery(conn,SELECT_ACTION, actionGuid);
		}
	}

	public List processQuery(Connection conn, String sql, Object... args) throws SQLException, ClassNotFoundException {
		try (PreparedStatement stmt = conn.prepareStatement(sql)) {
			int idx = 1;
			for (Object obj : args) {
				stmt.setObject(idx++, obj);
			}
			try (ResultSet rs = stmt.executeQuery()) {
				return convertResultSetToList(rs);
			}
		}

	}

	public List<Map<String, Object>> convertResultSetToList(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int columns = md.getColumnCount();
		List<Map<String, Object>> list = new ArrayList<>();

		while (rs.next()) {
			Map<String, Object> row = new HashMap<>(columns);
			for (int i = 1; i <= columns; ++i) {
				row.put(md.getColumnLabel(i), rs.getObject(i));
			}
			list.add(row);
		}

		return list;
	}

	static Connection createConnection() throws ClassNotFoundException, SQLException {
		Class.forName(JDBC_DRIVER);
		return DriverManager.getConnection(DB_URL, USER, PASS);

	}

	public static void main(String [] args) throws SQLException, ClassNotFoundException {
		Db db = new Db();

		Connection conn = Db.createConnection();

		List ret = db.getResources("25/KFFN/000000/DEMO","","");
		System.out.println("ret: "+ ret);
		String guid = (String) ((Map) ret.get(0)).get("guid");

		ret = db.getTCAs(guid);
		System.out.println("ret: "+ ret);
		guid = (String) ((Map) ret.get(0)).get("metric");
		
		System.out.println("metric guid: "+ guid);
		ret = db.getMetrics(guid);
		System.out.println("ret: "+ ret);

		guid = (String) ((Map) ret.get(0)).get("guid");
		System.out.println("alert guid: "+ guid);
		ret = db.getAlerts(guid);
		System.out.println("ret: "+ ret);

		guid = (String) ((Map) ret.get(0)).get("guid");
		System.out.println("action guid: "+ guid);
		ret = db.getActions(guid);
		System.out.println("ret: "+ ret);



	}
}
