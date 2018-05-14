package com.qingmin.hive.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJDBC {
	//private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	  public static void main(String[] args) throws SQLException {
	      try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      e.printStackTrace();
	      System.exit(1);
	    }
	    
	    Connection con = DriverManager.getConnection("jdbc:hive2://hadoop-spark-kafka-slave.test.com:10001/default","hadoop","");//this the username to logon the hiveserver2 service's server
	    Statement stmt = con.createStatement();
	    String tableName = "testHiveDriverTable";
	    stmt.execute("drop table if exists " + tableName);
	    stmt.execute("create table " + tableName + " (key int, value string)");
	   
	    String sql = "show tables '" + tableName + "'";
	    System.out.println("Running: " + sql);
	    ResultSet res = stmt.executeQuery(sql);
	    if (res.next()) {
	      System.out.println(res.getString(1));
	    }
	     
	    sql = "describe " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1) + "\t" + res.getString(2));
	    }
	 
	    String filepath = "/home/hadoop/app/apache-hive-1.1.0-cdh5.7.5-bin/data/hiveJdbcClientTest.txt";
	    sql = "load data local inpath '" + filepath + "' into table " + tableName;
	    System.out.println("Running: " + sql);
	    stmt.execute(sql);
	 
	    //sql = "select * from " + tableName;
	    sql = "select addradomnum(ename) from emp";
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      //System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
	      System.out.println(String.valueOf(res.getString(1)) + "\t");
	    }
	 
	    sql = "select count(1) from " + tableName;
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1));
	    }
	  }
}
