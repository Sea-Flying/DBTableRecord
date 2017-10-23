package com.seaflying.DBTableRecord;

import javax.naming.CompositeName;
import java.util.ArrayList;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
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
public class TablesIOImpl implements TablesIO {

    private Connection con;
    private String inTable;
    private String outTable;

    public TablesIOImpl(){
        Properties props = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream("resources/config.properties"));
        props.load(in);
        String mysqlUrl = props.getProperty("MysqlJDBCUrl");
        String mysqlUser = props.getProperty("MysqlUser");
        String mysqlPwd = props.getProperty("MysqlPasswd");

        Class.forName("com.mysql.jdbc.Driver");
        this.con = DriverManager.getConnection(mysqlUrl, mysqlUser,
                mysqlPwd);
        this.inTable=props.getProperty("MysqlInTable");
        this.outTable=props.getProperty("MysqlOutTable");
    }

    final public ArrayList<String> getTablesname() {
        java.sql.Statement stmt = con.createStatement();
        String sql = "select * from "+this.inTable+";";

    }

    final public Void setTabelsCount(String table) {

    }
}
