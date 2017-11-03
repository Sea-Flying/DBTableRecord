
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import com.seaflying.DBTableRecord.*;


public class RunMain {
    /**
     * Created by songqq on 2017/10/19.
     */
    public static void main(String[] args)throws Exception {
        TablesIOImpl run = new TablesIOImpl();
//        OracleTables f1 = new OracleTables();
//        HiveTables f2 = new HiveTables();
        Map<String,ArrayList<String>> tblist = run.getTablesname();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Callable c1 = new TRWorker(tblist.get("oracle"),1);
        Callable c2 = new TRWorker(tblist.get("hive"),2);
        Future f1 = pool.submit(c1);
        Future f2 = pool.submit(c2);
        run.setTabelsCount(tblist.get("pair_id"),(ArrayList<Long>)(f1.get()),(ArrayList<Long>)(f2.get()));
        pool.shutdown();
//      run.setTabelsCount(tblist.get("pair_id"),f1.getTablesCount(tblist.get("oracle")) ,f2.getTablesCount(tblist.get("hive")) );
        run.con.close();
    }

}