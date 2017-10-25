package com.seaflying.DBTableRecord;

import javax.naming.CompositeName;
import java.util.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

    public Connection con;
    private Properties props;
    private String inTable;
    private String outTable;

    public TablesIOImpl() throws Exception{
        this.props = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream("D:\\Workspace\\project\\DBTableRecord\\target\\classes\\config.properties"));
        this.props.load(in);
        String mysqlUrl = this.props.getProperty("MysqlJDBCUrl");
        String mysqlUser = this.props.getProperty("MysqlUser");
        String mysqlPwd = this.props.getProperty("MysqlPasswd");

        Class.forName("com.mysql.jdbc.Driver");
        this.con = DriverManager.getConnection(mysqlUrl, mysqlUser,
                mysqlPwd);
        this.inTable=props.getProperty("MysqlInTable");
        this.outTable=props.getProperty("MysqlOutTable");
    }


    final public Map<String, ArrayList<String>> getTablesname() throws Exception {
        Map<String,ArrayList<String>> re = new HashMap<String, ArrayList<String>>();
        ArrayList<String> oracle = new ArrayList<String>();
        ArrayList<String> hive = new ArrayList<String>();
        java.sql.Statement stmt = this.con.createStatement();
        String sql = "select count(*) from "+this.inTable+";";
        ResultSet set = stmt.executeQuery(sql);
        Long count;
        if(set.next()){
            count = set.getLong(1);
        }
        else{
            count = -1L;
        }
        this.props.setProperty("TablePairsCount",Long.toString(count));
        sql = "select oracle_table,hive_table from "+this.inTable+";";
        set = stmt.executeQuery(sql);
        while(set.next()){
            oracle.add(set.getString("oracle_table"));
            hive.add(set.getString("hive_table"));
        }
        re.put("oracle",oracle);
        re.put("hive",hive);
        set.close();
        stmt.close();
        return re;
    }


    final public void  setTabelsCount(ArrayList<Long> orcl_count, ArrayList<Long> hive_count) throws Exception {
        java.sql.Statement stmt = this.con.createStatement();
        Integer length = orcl_count.size();

        String sql;
        for (Integer i = 0 ; i < length; i++ ){
            sql = "insert into "+this.outTable+" (pair_id, oracle_len, hive_len, len_equal) values ("+(i+1)+","+orcl_count.get(i)+","+hive_count.get(i)+","+ (orcl_count.get(i) == hive_count.get(i) ? 1 :0 ) +");";
            stmt.execute(sql);
        }
        stmt.close();
        return ;
    }
}
