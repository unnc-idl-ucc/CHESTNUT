package com.chestnut.runtime.dal.agent;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class ConfigManager {
    private Map<String, String> _configs;
    private Map<String, MySQLHelper> _mysqlHps;
    private String _configName;
    
    public ConfigManager(String configName) {
        _configName = configName;
        _configs = new HashMap<String, String>();
        _mysqlHps = new HashMap<String, MySQLHelper>();
    }
    
    public String GetConfigVal(String configKey) {
        if(_configs.containsKey(configKey)) {
            return _configs.get(configKey);
        }else {
            return null;
        }
    }
    
    public String GetConfigName() {
        return _configName;
    }
    
    public void SetConfig(String configKey, String configVal) {
        _configs.put(configKey, configVal);
    }
    
    public void SetMySQLHelper(String helperName, String dbName, String dbAddress, String dbPort, String dbUser, String dbPwds) {
        _mysqlHps.put(helperName, new MySQLHelper(dbAddress, dbPort, dbName, dbUser, dbPwds));
    }
    
    public MySQLHelper GetMySQLHelper(String helperName) {
        return _mysqlHps.get(helperName);
    }
    
    public void CloseAllMySQLHelper() {
        for(Map.Entry<String, MySQLHelper> sqlHp : _mysqlHps.entrySet()) {
            try {
                sqlHp.getValue().CloseConnection();
            } catch (SQLException e) {
                System.out.println("[WARN] ConfigManager.CloseAllMySQLHelper: " + sqlHp.getValue().GetDBName() + " helper failed closing connection.");
                e.printStackTrace();
            }
        }
    }
}
