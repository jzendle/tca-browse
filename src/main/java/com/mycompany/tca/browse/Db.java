package com.mycompany.tca.browse;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;

/**
 *
 * @author zendle.joe
 */
public class Db {

   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
   // static final String JDBC_DRIVER = "com.vertica.jdbc.Driver";
   static final String DB_URL = "jdbc:mysql://localhost/npm_dba";
   // static final String DB_URL = "jdbc:vertica://invertidcplp301.twtelecom.com:5433/NPMP";

   //  Database credentials
   static final String USER = "jzendle";
   // static final String USER = "npm_batch";
   static final String PASS = "jzendle";
   // static final String PASS = "batch3";

   static final String SELECT_RESOURCES
           = "select guid from resource where circuitid = ?  and  vcircuit = ? and bclli = ? ";

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
           + "INNER JOIN\n"
           + "    threshold_type\n"
           + "ON\n"
           + "    (\n"
           + "        metric.threshold_type = threshold_type.id)\n"
           + "INNER JOIN\n"
           + "    element\n"
           + "ON\n"
           + "    (\n"
           + "        metric.metric = element.id)\n"
           + "INNER JOIN\n"
           + "    element_subtype\n"
           + "ON\n"
           + "    (\n"
           + "        metric.level = element_subtype.id) "
           + "        where metric = ? ";

   static final String SELECT_ALERT
           = "SELECT\n"
           + "    alert.guid AS guid,\n"
           + "    element_subtype_alias2.name AS timezone,\n"
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
           + "        where metric = ?) ";

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
           + "        where action = ? ";

   private final static String RESOURCES_SQL = " select distinct circuit_id, uuid from circuit_alert ";

   public static  Collection<String> getRespources(String circuit, String vcircuit, String clli) throws SQLException, ClassNotFoundException {

      try (Connection conn = Db.createConnection()) {

         try (PreparedStatement stmt = conn.prepareStatement(SELECT_RESOURCES)) {
            stmt.setString(0, circuit);
            stmt.setString(1, vcircuit);
            stmt.setString(2, clli);
            try (ResultSet rs = stmt.executeQuery()) {
               ResultSetMetaData rsmd = rs.getMetaData();
               int columnsNumber = rsmd.getColumnCount();

               while (rs.next()) {
                  for (int i = 1; i <= columnsNumber; i++) {
                     if (i > 1) {
                        System.out.print(",  ");
                     }
                     String columnValue = rs.getString(i);
                     System.out.print(columnValue + " " + rsmd.getColumnName(i));
                  }
                  System.out.println("");
               }
            }
         }
      }
      return null;
   }
   static Connection createConnection() throws ClassNotFoundException, SQLException {
      Class.forName(JDBC_DRIVER);
      return DriverManager.getConnection(DB_URL, USER, PASS);

   }
}
