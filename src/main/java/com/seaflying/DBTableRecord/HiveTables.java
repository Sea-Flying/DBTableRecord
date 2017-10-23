package com.seaflying.DBTableRecord;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by songqq on 2017/10/20.
 */
public class HiveTables {
    public ArrayList<Long> getTablesCount(ArrayList<String> tables){
        ArrayList<Long> hiveCount = new ArrayList<Long>();
        Properties props = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream("resources/config.properties"));
        props.load(in);
        String hiveUrl = props.getProperty("HiveJDBCUrl");
        String hiveUser = props.getProperty("HiveUser");
        String hivePwd = props.getProperty("HivePasswd");
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection con = DriverManager.getConnection(hiveUrl, hiveUser,
                    hivePwd);
            java.sql.Statement stmt = con.createStatement();
            ListIterator<String> iter = tables.listIterator();
            while (iter.hasNext()) {
                String sql = "select count(*) from " + iter.next() + ";";
                ResultSet set = stmt.executeQuery(sql);
                if(set.next()) {
                    hiveCount.add(Long.parseLong(set.getString(1)));
                }
                else {
                    hiveCount.add(-1);
                }
            }
            stm
            con.close();
        } catch(Exception e){
            e.printStackTrace();
            hiveCount.add(-1);
        }
        return hiveCount;
    }
}
