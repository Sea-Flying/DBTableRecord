package com.seaflying.DBTableRecord;

import javax.naming.CompositeName;
import java.io.*;
import java.util.*;

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
        System.out.println(new File(".").getCanonicalPath());
        InputStream in = new BufferedInputStream(new FileInputStream("config.properties"));
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
        ArrayList<String> pair_id = new ArrayList<String>();
        ArrayList<String> oracle = new ArrayList<String>();
        ArrayList<String> hive = new ArrayList<String>();
        java.sql.Statement stmt = this.con.createStatement();
        String sql = "select * from "+this.inTable+";";
        ResultSet set = stmt.executeQuery(sql);
        while(set.next()){
            pair_id.add(set.getString("tab_pair_id"));
            oracle.add(set.getString("oracle_table"));
            hive.add(set.getString("hive_table"));
        }
        re.put("pair_id",pair_id);
        re.put("oracle",oracle);
        re.put("hive",hive);
        set.close();
        stmt.close();
        return re;
    }


    final public void  setTabelsCount(ArrayList<String> id, ArrayList<Long> orcl_count, ArrayList<Long> hive_count) throws Exception {
        java.sql.Statement stmt = this.con.createStatement();
        int length = orcl_count.size();
        String sql;
        for (int i = 0 ; i < length; i++ ){
            sql = "insert into "+this.outTable+" (pair_id, oracle_len, hive_len, len_equal) values ("+id.get(i)+","+orcl_count.get(i)+","+hive_count.get(i)+","+ (orcl_count.get(i).longValue() == hive_count.get(i).longValue() ? 1 :0 ) +");";
            stmt.execute(sql);
        }
        stmt.close();
        return ;
    }
}
