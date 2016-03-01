package com.newland;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class HiveJDBC {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		//nn1	10.238.155.40
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://10.238.155.40:10000/default", "hive", "");
		System.out.println("=====getConnection success!=====");
		Statement stmt = con.createStatement();
		String tableName = "testHiveDriverTable";
		stmt.execute("drop table if exists " + tableName);
		stmt.execute("create table " + tableName + " (key int, value string) row format delimited fields terminated by '|'");
		System.out.println("Create table success!");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}

		// load data into table
	    // NOTE: filepath has to be local to the hive server
	    // NOTE: /tmp/a.txt is a '|' separated file with two fields per line
	    String filepath = "/tmp/a.txt";
	    sql = "load data local inpath '" + filepath + "' into table " + tableName;
	    System.out.println("Running: " + sql);
	    stmt.execute(sql);
	    System.out.println("=====load data success!=====");
	    
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
		System.out.println("=====describe table success!=====");
		
		sql = "select * from " + tableName;
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t"
					+ res.getString(2));
		}
		System.out.println("=====select table success!=====");
		
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
		System.out.println("=====count table success!=====");
	}
}
