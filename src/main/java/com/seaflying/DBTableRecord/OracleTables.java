package com.seaflying.DBTableRecord;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.ListIterator;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by songqq on 2017/10/20.
 */
public class OracleTables {
    public ArrayList<Long> getTablesCount(ArrayList<String> tables){
        ArrayList<Long> oracleCount = new ArrayList<Long>();
        Properties props = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream("resources/config.properties"));
        props.load(in);
        String oracleUrl = props.getProperty("OracleJDBCUrl");
        String oracleUser = props.getProperty("OracleUser");
        String oraclePwd = props.getProperty("OraclePasswd");
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection con = DriverManager.getConnection(oracleUrl, oracleUser,
                   oraclePwd);
            java.sql.Statement stmt = con.createStatement();
            ListIterator<String> iter = tables.listIterator();
            while (iter.hasNext()) {
                String sql = "select count(*) from " + iter.next() + ";";
                ResultSet set = stmt.executeQuery(sql);
                if (set.next()) {
                    oracleCount.add(set.getLong(1));
                } else {
                    oracleCount.add(-1);
                }
            }
            con.close();
            return oracleCount;
        } catch(Exception e){
            e.printStackTrace();
            return -1;
        }
    }

}
