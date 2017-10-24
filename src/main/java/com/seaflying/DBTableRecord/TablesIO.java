package com.seaflying.DBTableRecord;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by songqq on 2017/10/20.
 */
public interface TablesIO {
    Map<String, ArrayList<String>> getTablesname();
    Void setTabelsCount();
}
