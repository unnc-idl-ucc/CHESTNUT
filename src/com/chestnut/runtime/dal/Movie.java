package com.chestnut.runtime.dal;

import com.chestnut.runtime.core.ZPZItem;
import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;

public class Movie extends ZPZItem{
    
    public Movie(String id, ConfigManager cfgManager, String tableName, String primeField) {
        super(id, cfgManager);
        super.loadDataFromMySQL(_cfgManager.GetMySQLHelper("dbName_moive"), tableName, primeField);
    }
    
    public DataSession GetData() {
        return super._atomData;
    }
}
