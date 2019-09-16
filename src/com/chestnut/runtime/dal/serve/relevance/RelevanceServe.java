package com.chestnut.runtime.dal.serve.relevance;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.Level;
import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.csv.DataBuilder;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.ma.ESVParser;

public class RelevanceServe {

    private String _servedUserId;
    private Map<String, Double> _recommendUserList;
    private int _finalLevel = 0;
    
    private ConfigManager _cfgManager;
    private ESVParser _globalPearsonHandler;
    
    public RelevanceServe(String servedUserId, ConfigManager cfgManager) {
        _servedUserId = servedUserId;
        _cfgManager = cfgManager;
        _recommendUserList = new HashMap<String, Double>();
    }
    
    public Map<String, Double> MostRelevanceByThreshold(Double PSThreshold) {
        String[] ServedList = new String[1];
        ServedList[0] = _servedUserId;
        
        Map<String, Double> recommendUserWithPS = new HashMap<String, Double>();
        Map<String, String> existList = new HashMap<String, String>();
        recommendUserWithPS.put(_servedUserId, 1.0);
        existList.put(_servedUserId, _servedUserId);
        
      //Build log session
        String[] logFields = new String[9];
        logFields[0] = "logId";
        logFields[1] = "StartUser";
        logFields[2] = "EffectiveDirector";
        logFields[3] = "RecommedUser";
        logFields[4] = "Level";
        logFields[5] = "PSOrigin";
        logFields[6] = "PSParent";
        logFields[7] = "JCOrigin";
        logFields[8] = "JCParent";
        
        DataSession logSession = new DataSession("userId_" + _servedUserId + "_log");
        logSession.BuildFields(logFields, "logId");
        
        int levelPtr = 1;
        while(!recommendUserWithPS.isEmpty()){
            
            Level recommendLevel = new Level(ServedList, logSession, levelPtr, 1, _servedUserId, existList, _cfgManager);
            if(_globalPearsonHandler!=null) {
                recommendLevel.SetGlobalPearsonHandler(_globalPearsonHandler);
            }
            logSession = recommendLevel.RunLevel("userId_", "directorId_",
                                                 "rating", "rating", 
                                                 "directorId", "userId", 
                                                 "movieId", "rating",
                                                 4.0, 4.0,
                                                 existList);
            
            // get exist list
            existList = recommendLevel.GetExistList();
            // filtered by threshold
            recommendUserWithPS = FilterRecommendListByThreshold(PSThreshold, recommendLevel.GetRecommendUserList());
            
            System.out.println("[Debug] ====================");
            System.out.println("[Debug] level to " + levelPtr);
            for(Map.Entry<String, Double> e : recommendUserWithPS.entrySet()) {
                System.out.println("[Debug] User " + e.getKey() + "; PS = " + e.getValue());
            }
            
            ServedList = GetFilteredServedList(recommendUserWithPS);
            levelPtr++;
            
        }
        _finalLevel = levelPtr-1;
        GenerateLogFile(logSession);
        return _recommendUserList;
    }
    
    public Map<String, Double> runWithLevelSpNoEffectiveFilter(int LevelNum, int EffAmount) {
        String[] ServedList = new String[1];
        ServedList[0] = _servedUserId;
        
        Map<String, Double> recommendUserWithPS = new HashMap<String, Double>();
        Map<String, String> existList = new HashMap<String, String>();
        recommendUserWithPS.put(_servedUserId, 1.0);
        existList.put(_servedUserId, _servedUserId);
        
      //Build log session
        String[] logFields = new String[9];
        logFields[0] = "logId";
        logFields[1] = "StartUser";
        logFields[2] = "EffectiveDirector";
        logFields[3] = "RecommedUser";
        logFields[4] = "Level";
        logFields[5] = "PSOrigin";
        logFields[6] = "PSParent";
        logFields[7] = "JCOrigin";
        logFields[8] = "JCParent";
        
        DataSession logSession = new DataSession("userId_" + _servedUserId + "_log");
        logSession.BuildFields(logFields, "logId");
        
        int levelPtr = 1;
        while(levelPtr<=LevelNum){
            
            Level recommendLevel = new Level(ServedList, logSession, levelPtr, EffAmount, _servedUserId, existList, _cfgManager);
            logSession = recommendLevel.RunLevel("userId_", "directorId_",
                                                 "rating", "rating", 
                                                 "directorId", "userId", 
                                                 "movieId", "rating",
                                                 4.0, 4.0,
                                                 existList);
            
            _recommendUserList = recommendUserWithPS;
            // get exist list
            existList = recommendLevel.GetExistList();
            // filtered by threshold
            recommendUserWithPS = recommendLevel.GetRecommendUserList();
            ServedList = GetFilteredServedList(recommendUserWithPS);
            levelPtr++;
        }
        _finalLevel = levelPtr-1;
        GenerateLogFile(logSession);
        return _recommendUserList;
    }
    
    public void runWithOneRoundTest(){
        String[] ServedList = new String[1];
        ServedList[0] = _servedUserId;
        
        Map<String, Double> recommendUserWithPS = new HashMap<String, Double>();
        Map<String, String> existList = new HashMap<String, String>();
        recommendUserWithPS.put(_servedUserId, 1.0);
        existList.put(_servedUserId, _servedUserId);
        
      //Build log session
        String[] logFields = new String[7];
        logFields[0] = "logId";
        logFields[1] = "StartUser";
        logFields[2] = "EffectiveDirector";
        logFields[3] = "RecommedUser";
        logFields[4] = "Level";
        logFields[5] = "PSOrigin";
        logFields[6] = "PSParent";
        logFields[7] = "JCOrigin";
        logFields[8] = "JCParent";
        
        DataSession logSession = new DataSession(_servedUserId + "_log");
        logSession.BuildFields(logFields, "logId");
        
        int levelPtr = 1;
        Level recommendLevel = new Level(ServedList, logSession, levelPtr, 1, _servedUserId, existList, _cfgManager);
        logSession = recommendLevel.RunLevel("userId_", "directorId_",
                                             "rating", "rating", 
                                             "directorId", "userId", 
                                             "movieId", "rating",
                                             4.0, 4.0,
                                             existList);
        
        // get exist list
        existList = recommendLevel.GetExistList();
        // filtered by threshold
        recommendUserWithPS = FilterRecommendListByThreshold(0.3, recommendLevel.GetRecommendUserList());
        ServedList = GetFilteredServedList(recommendUserWithPS);
        
        System.out.println("[Tracing] RecommendServe.runWithOneRoundTest(), ===== Log file generator part =====");
        System.out.println("[Tracing] RecommendServe.runWithOneRoundTest(), final log session size report as " + logSession.dataRecordSize);
        
        _finalLevel = levelPtr;
        GenerateLogFile(logSession);
        
    }
    
    public void SetGlobalPearsonHandler(ESVParser globalPearsonHandler) {
        _globalPearsonHandler = globalPearsonHandler;
    }
    
    public int GetFinalLevel(){
        return _finalLevel;
    }
    
    private Map<String, Double> FilterRecommendListByThreshold(Double Threadhold, Map<String, Double> recommendList){
        for(Map.Entry<String, Double> entry : recommendList.entrySet()){
            if(Math.abs(entry.getValue())<=Threadhold){
                recommendList.remove(entry.getKey());
                _recommendUserList.put(entry.getKey(), entry.getValue());
            }
        }
        return recommendList;
    }
    
    private String[] GetFilteredServedList(Map<String, Double> recommendList){
        String[] serverdList = new String[recommendList.size()];
        int serverdListPtr = 0;
        for(Map.Entry<String, Double> entry : recommendList.entrySet()){
            serverdList[serverdListPtr] = entry.getKey();
            serverdListPtr++;
        }
        return serverdList;
    }
    
    public void GenerateLogFile(DataSession log){
        try {
            DataBuilder.GenFileFromDataSession(log, "data/logs/all/" + log.sessionName + ".csv");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}


