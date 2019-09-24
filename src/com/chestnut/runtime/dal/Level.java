package com.chestnut.runtime.dal;

import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.ma.ESVParser;
import com.chestnut.runtime.dal.math.similarity.JaccardSimilarity;
import com.chestnut.runtime.dal.math.similarity.PearsonCorrelationSimilarity;

public class Level {

    private int _levelNum, _effectAmount;
    private String _originUserId;
    private String[] _startUserIds;
    private Map<String, Double> _recommendUserWithPS;
    private Map<String, String> _existList;
    private DataSession _logSession;
    private ESVParser _globalPearsonHandler;
    
    private ConfigManager _cfgManager;
    
    public Level(String[] startUserIds, DataSession logSession, int levelNum, int effectAmount, String originUserId, Map<String, String> existList, ConfigManager cfgManager){
        _levelNum = levelNum;
        _effectAmount = effectAmount;
        _originUserId = originUserId;
        _startUserIds = startUserIds;
        _logSession = logSession;
        _recommendUserWithPS = new HashMap<String, Double>();
        _existList = existList;
        _cfgManager = cfgManager;
    }
    
    public DataSession RunLevel(String userTableNamePrefix, String directorTableNamePrefix,
                                String userValuedField, String directorValuedField,
                                String userGroupField, String directorGroupField,
                                String userPSCoField, String userPSValField,
                                double userThreshold, double directorThreshold,
                                Map<String, String> existList)
    {
        String[] logRecords = new String[9];
        int logPtr = _logSession.dataRecordSize;
        
        for(int i=0; i<_startUserIds.length; i++){// Start user part
            String[] tempEffectiveDirectors;
            User tempStartUser = new User(_startUserIds[i], _cfgManager, userTableNamePrefix + _startUserIds[i], userTableNamePrefix + _startUserIds[i] + "_id");
            if(!tempStartUser.isIdenticalValuedUser(userValuedField)){
                tempEffectiveDirectors = tempStartUser.GetMostEffectiveDirector(userValuedField, userGroupField, userThreshold, existList);
                /*
                System.out.println("[Tracing] Level.RunLevel(), ===== Start user part =====");
                System.out.println("[Tracing] Level.RunLevel(), Start user is " 
                                    + tempStartUser.GetData().sessionName 
                                    + " whose effective director list size is " 
                                    + tempEffectiveDirectors.length
                                    + ". Print as below:");
                for(String director : tempEffectiveDirectors){
                    System.out.println("[Tracing] Level.RunLevel(), " + director);
                }
                */
                for(int j=0; j<tempEffectiveDirectors.length; j++){// Director part
                    String[] tempRecommendUsers;
                    Director tempDirector = new Director(tempEffectiveDirectors[j], _cfgManager, directorTableNamePrefix + tempEffectiveDirectors[j], directorTableNamePrefix + tempEffectiveDirectors[j] + "_id");
                    tempRecommendUsers = tempDirector.GetEffectiveList(directorValuedField, directorGroupField, _effectAmount, directorThreshold, existList);
                    /*
                    System.out.println("[Tracing] Level.RunLevel(), ===== Director part =====");
                    System.out.println("[Tracing] Level.RunLevel(), Present effective director is " 
                            + tempDirector.GetData().sessionName 
                            + " whose effective user list size is " 
                            + tempRecommendUsers.length
                            + ". Print as below:");
                    for(String user : tempRecommendUsers){
                        System.out.println("[Tracing] Level.RunLevel(), " + user);
                    }
                    */
                    for(int k=0; k<tempRecommendUsers.length; k++){// Recommend user list part
                        
                        double PSorigin, PSparent;
                        double JCorigin, JCparent;
                        User tempRecommendUser = new User(tempRecommendUsers[k], _cfgManager, userTableNamePrefix + tempRecommendUsers[k], userTableNamePrefix + tempRecommendUsers[k] + "_id");
                        User tempOriginUser = new User(_originUserId, _cfgManager, userTableNamePrefix + _originUserId, userTableNamePrefix + _originUserId + "_id");
                        PSorigin = CalcPearson(tempRecommendUser.GetData(), tempOriginUser.GetData(), userPSCoField, userPSValField);
                        PSparent = CalcPearson(tempRecommendUser.GetData(), tempStartUser.GetData(), userPSCoField, userPSValField);
                        JCorigin = CalcJaccard(tempRecommendUser.GetData(), tempOriginUser.GetData(), userPSCoField, userPSValField);
                        JCparent = CalcJaccard(tempRecommendUser.GetData(), tempStartUser.GetData(), userPSCoField, userPSValField);
                        /*
                        System.out.println("[Tracing] Level.RunLevel(), ===== Recommend user list part =====");
                        System.out.println("[Tracing] Level.RunLevel(), Present recommend user is " 
                                            + tempRecommendUser.GetData().sessionName 
                                            + " whose Pearson Similarity with original user " 
                                            + tempOriginUser.GetData().sessionName 
                                            + " is " + PSorigin);
                        System.out.println("[Tracing] Level.RunLevel(), JCorigin = " + JCorigin + ", JCparent = " + JCparent);
                        */
                        logRecords[0] = String.valueOf(logPtr++);
                        logRecords[1] = _startUserIds[i];
                        logRecords[2] = tempEffectiveDirectors[j];
                        logRecords[3] = tempRecommendUsers[k];
                        logRecords[4] = String.valueOf(_levelNum);
                        logRecords[5] = String.valueOf(PSorigin);
                        logRecords[6] = String.valueOf(PSparent);
                        logRecords[7] = String.valueOf(JCorigin);
                        logRecords[8] = String.valueOf(JCparent);
                        _logSession.SetARow(logRecords);
                        /*
                        System.out.println("[Tracing] Level.RunLevel(), ===== Log file generator part =====");
                        System.out.println("[Tracing] Level.RunLevel(), logSession size report as " + _logSession.dataRecordSize + "\n");
                        */
                        _recommendUserWithPS.put(tempRecommendUsers[k], PSorigin);
                        _existList.put(tempRecommendUsers[k], tempRecommendUsers[k]);
                    }
                }
            }else{
                logRecords[0] = String.valueOf(logPtr++);
                logRecords[1] = _startUserIds[i];
                logRecords[2] = "NA";
                logRecords[3] = "NA";
                logRecords[4] = String.valueOf(_levelNum);
                logRecords[5] = "0.0";
                logRecords[6] = "0.0";
                logRecords[7] = "0.0";
                logRecords[8] = "0.0";
                _logSession.SetARow(logRecords);
            }
            
        }
        
        return _logSession;
    }
    
    public void SetGlobalPearsonHandler(ESVParser globalPearsonHandler) {
        _globalPearsonHandler = globalPearsonHandler;
    }
    
    public Map<String, Double> GetRecommendUserList(){
        return _recommendUserWithPS;
    }
    
    public Map<String, String> GetExistList(){
        return _existList;
    }
    
    private double CalcPearson(DataSession dataV, DataSession dataU, String CorrFieldName, String ValueFieldName){
        PearsonCorrelationSimilarity PScalc = new PearsonCorrelationSimilarity(dataV, dataU);
        
        if(_globalPearsonHandler!=null) {
            PScalc.SetGlobalPearsonHandler(_globalPearsonHandler);
        }
        
        Double psHandler = PScalc.ExecuteSimilarity(CorrFieldName, ValueFieldName);
        if(psHandler.isNaN()) {
            return 0.0;
        }else {
            return Math.abs(psHandler);
        }
    }
    
    private double CalcJaccard(DataSession dataV, DataSession dataU, String CorrFieldName, String ValueFieldName){
        JaccardSimilarity JCcalc = new JaccardSimilarity(dataV, dataU);
        return JCcalc.ExecuteSimilarity(CorrFieldName, ValueFieldName);
    }
}
