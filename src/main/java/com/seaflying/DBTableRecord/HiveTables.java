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
    public ArrayList<Long> getTablesCount(ArrayList<String> tables)throws Exception{
        ArrayList<Long> hiveCount = new ArrayList<Long>();
        Properties props = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream("config.properties"));
        props.load(in);
        String hiveUrl = props.getProperty("HiveJDBCUrl");
        String hiveUser = props.getProperty("HiveUser");
        String hivePwd = props.getProperty("HivePasswd");

            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection con = DriverManager.getConnection(hiveUrl, hiveUser,
                    hivePwd);
            java.sql.Statement stmt = con.createStatement();
            ListIterator<String> iter = tables.listIterator();
            while (iter.hasNext()) {
                String str1 = iter.next();
                System.out.println(str1);
                String sql = "select count(*) from test." + str1 + ";";
                ResultSet set = stmt.executeQuery(sql);
                if(set.next()) {
                    hiveCount.add(set.getLong(1));
                }
                else {
                    hiveCount.add(-1L);
                    System.err.println("Empty Table: "+str1);
                }
            }
            stmt.close();
            con.close();

        return hiveCount;
    }
}
