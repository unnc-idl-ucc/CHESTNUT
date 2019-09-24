package com.chestnut.runtime.booster;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.ESVParser;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class DataAgent {

    private Map<String, String> _globalSeriesMap, _topXMap, _servedUserRatings;
    private String[] _servedUserItems;
    private String[] _allUserTableId, _allUserId;
    private String[] _recommendUserList;
    private Map<String, List<String>> _recommendUserItems;
    private Map<String, List<String>> _recommendUserIntersectItems;
    private ESVParser _GlobalKNHandler;
    
    public DataAgent() {
        _recommendUserItems = new HashMap<String, List<String>>();
        _recommendUserIntersectItems = new HashMap<String, List<String>>();
    }
    
    public void StoreRecommendUsers(Map<String, Double> recommendUsers) {
        _recommendUserList = new String[recommendUsers.size()];
        int userListPtr = 0;
        for(Map.Entry<String, Double> ru : recommendUsers.entrySet()) {
            _recommendUserList[userListPtr] = ru.getKey();
            userListPtr++;
        }
    }
    
    /** Store **/
    
    public void StoreServedUserItems(String[] servedUserItems) {
        _servedUserItems = servedUserItems;
    }
    
    public void StoreServedUserRatings(Map<String, String> servedUserRatings) {
        _servedUserRatings = servedUserRatings;
    }
    
    public void StoreGlobalSeriesMap(Map<String, String> globalSeriesMap) {
        _globalSeriesMap = globalSeriesMap;
    }
    
    public void StoreTopXMap(Map<String, String> topXMap) {
        _topXMap = topXMap;
    }
    
    public void StoreRecommendUserItems(String userId, List<String> userItems) {
        //System.out.println("[Tracing] DataAgent.StoreRecommendUserItems: userId is " + userId + ", userItems size is " + userItems.size());
        _recommendUserItems.put(userId, userItems);
    }
    
    public void StoreRecommendUserIntersectItems(String userId, List<String> userIntersectItems) {
        //System.out.println("[Tracing] DataAgent.StoreRecommendUserItems: userId is " + userId + ", userItems size is " + userItems.size());
        _recommendUserIntersectItems.put(userId, userIntersectItems);
    }
    
    /** Get **/
    
    public String[] GetRecommendUsers() {
        return _recommendUserList;
    }
    
    public String[] GetServedUserItems() {
        return _servedUserItems;
    }
    
    public Map<String, String> GetServedUserRatings() {
        return _servedUserRatings;
    }
    
    public Map<String, String> GetGlobalSeriesMap() {
        return _globalSeriesMap;
    }
    
    public Map<String, String> GetTopXMap() {
        return _topXMap;
    }
    
    public List<String> GetRecommendUserItems(String userId) {
        return _recommendUserItems.get(userId);
    }
    
    public List<String> GetRecommendUserIntersectItems(String userId) {
        return _recommendUserIntersectItems.get(userId);
    }
    
    public String[] GetAllUserTableIds() {
        return _allUserTableId;
    }
    
    public String[] GetAllUserIds() {
        return _allUserId;
    }
    
    public ESVParser GetGlobalKNHandler() {
        return _GlobalKNHandler;
    }
    
    
    /** Load **/
    
    public void LoadGlobalSeriesMapFromFile(String globalSeriesFileDir) {
        try {
            _globalSeriesMap = LoadFileToMap(globalSeriesFileDir, "Series");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void LoadTopXMapFromFile(String topXFileDir) {
        try {
            _topXMap = LoadFileToMap(topXFileDir, "Series");
        } catch (IOException e) {
           // TODO Auto-generated catch block
           e.printStackTrace();
        }
    }
    
    public void LoadAllUserTableId(ConfigManager dbConfigManager) {
        MySQLHelper sqlhp = dbConfigManager.GetMySQLHelper("dbName_user");
        try {
            _allUserTableId = sqlhp.QueryAllTables();
            
            _allUserId = new String[_allUserTableId.length];
            String tableIdHolder;
            for(int i=0; i<_allUserId.length; i++) {
                tableIdHolder = _allUserTableId[i];
                _allUserId[i] = tableIdHolder.substring(tableIdHolder.indexOf("_")+1);
            }
            sqlhp.CloseStatement();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void LoadGlobalKN(String GlobalKNFileDir) {
        _GlobalKNHandler = new ESVParser("data/ProductEnv/HelperSet/k_nearest_comb.csv");
    }
    
    private Map<String, String> LoadFileToMap(String fileDir, String loadType) throws IOException {
        String temp;
        String[] tempSplit;
        
        BufferedReader loadedFile;
        Map<String, String> loadedMap;
        
        loadedFile = new BufferedReader(new FileReader(fileDir));
        loadedMap = new HashMap<String, String>();
        
        switch(loadType) {
        case "Series":
            while((temp = loadedFile.readLine())!=null) {
                tempSplit = temp.split(",");
                loadedMap.put(tempSplit[0], tempSplit[1]);
            }
            break;
            
        case "TopX":
            while((temp = loadedFile.readLine())!=null) {
                tempSplit = temp.split(",");
                loadedMap.put(tempSplit[0], tempSplit[0]);
            }
            break;
        }
        
        loadedFile.close();
        
        return loadedMap;
    }
    
    
    
}
