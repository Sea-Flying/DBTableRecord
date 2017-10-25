package com.seaflying.DBTableRecord;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by songqq on 2017/10/20.
 */
public interface TablesIO {
    Map<String, ArrayList<String>> getTablesname ()throws Exception;
    void setTabelsCount( ArrayList<Long> a, ArrayList<Long> b ) throws Exception;
}
