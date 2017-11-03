package com.seaflying.DBTableRecord;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import com.seaflying.DBTableRecord.*;
import org.apache.commons.lang.ObjectUtils;

/**
 * Created by songqq on 2017/11/3.
 */
public class TRWorker implements Callable< ArrayList<Long> >{
    private ArrayList<String> tabList;
    private int type;
    public TRWorker(ArrayList<String> t, int type){
        this.tabList = t;
        this.type = type;
    }

    public ArrayList<Long> call() throws Exception{
        ArrayList<Long> re = new ArrayList<Long>();
        try {
            switch (type) {
                case 1: {
                    OracleTables c = new OracleTables();
                    re = c.getTablesCount(tabList);
                    break;
                }
                case 2: {
                    HiveTables c = new HiveTables();
                    re = c.getTablesCount(tabList);
                    break;
                }
            }
        }catch (Exception e){
            re = null;
            e.printStackTrace();
        }
        return re;
    }

}
