import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.seaflying.DBTableRecord.HiveTables;
import com.seaflying.DBTableRecord.OracleTables;
import com.seaflying.DBTableRecord.TablesIOImpl;

public class RunMain {
    /**
     * Created by songqq on 2017/10/19.
     */
    public static void main(String[] args)throws Exception {
        TablesIOImpl run = new TablesIOImpl();
        OracleTables f1 = new OracleTables();
        HiveTables f2 = new HiveTables();
        Map<String,ArrayList<String>> tblist = run.getTablesname();
        run.setTabelsCount(f1.getTablesCount(tblist.get("oracle")) ,f2.getTablesCount(tblist.get("hive")) );
        run.con.close();
    }

}