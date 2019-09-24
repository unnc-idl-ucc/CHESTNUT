package com.chestnut.runtime.dal.serve.useful;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.log.LogSession;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.ma.ESVParser;
import com.chestnut.runtime.dal.math.similarity.DataSessionComparator;

public class UsefulnessServe {
    
    private String _servedUser, _recommendUser;
    private List<String> _recommendItems;
    private Map<String, DataSession> _predictUserPool;
    private Map<String, String> _predictUserPS, _servedUserRatings;
    private Map<String, Double> _predictRecommendRatings;
    private ESVParser _globalKNHandler;
    
    private LogSession _sysLogSession;
    
    public UsefulnessServe(String servedUser, String recommendUser, List<String> recommendItems, ESVParser globalKNHandler, LogSession sysLogSession) {
        _servedUser = servedUser;
        _recommendUser = recommendUser;
        _recommendItems = recommendItems;
        _globalKNHandler = globalKNHandler;
        _predictUserPool = new HashMap<String, DataSession>();
        _predictUserPS = new HashMap<String, String>();
        _sysLogSession = sysLogSession;
    }
    
    public void SetServedUserRatings(Map<String, String> servedUserRatings) {
        _servedUserRatings = servedUserRatings;
    }
    
    public void BuildPredictUsersByRKN(String[] allUserTbs, ConfigManager cfgManager) {
        KNearest rknHandler = new KNearest(5, cfgManager);
        rknHandler.SetGlobalKNs(_globalKNHandler);
        
        System.out.println("[Tracing] UsefulnessServe.BuildPredictUsersByRKN: recommend user is " + _recommendUser);
        System.out.println("[Tracing] UsefulnessServe.BuildPredictUsersByRKN: before filter, _recommendItems size is " + _recommendItems.size());
        
        _recommendItems = rknHandler.GetKNByItem(_servedUser, _recommendUser, allUserTbs, _recommendItems, 0.5);
        
        System.out.println("[Tracing] UsefulnessServe.BuildPredictUsersByRKN: after filter, _recommendItems size is " + _recommendItems.size());
        
        _predictUserPool = rknHandler.GetAllSelectedUserData();
        _predictUserPS = rknHandler.GetAllSelectedUserPS();
        
        if(_sysLogSession!=null) {
            //_sysLogSession.SetRowKeyValue(String.valueOf(rknHandler.GetGlobalKNSelectedCounts()), "KN_GlobalCounts");
            //_sysLogSession.SetRowKeyValue(String.valueOf(rknHandler.GetRandomKNSelectedCounts()), "KN_RandomCounts");
        }
        
    }
    
    public LogSession GetLog() {
        return _sysLogSession;
    }
    
    public void BuildPredictUsersByKN(BufferedReader KNBuffer) {
        
    }
    
    public Map<String, Double> PredictAllItems(int logType, double ratingThreshold) {
        
        //System.out.println("\n---------------------------------------------------------------------------------------");
        _predictRecommendRatings = new HashMap<String, Double>();
        
        Double predictRatingHolder = 0.0;
        
        int exceptCount = 0;
        
        for(int i=0; i<_recommendItems.size(); i++) {
            predictRatingHolder = PredictOne(_recommendItems.get(i));
            //System.out.println("[Tracing] UsefulnessServe.PredictAllItems: item-" + _recommendItems.get(i) + " estimate rating => " + predictRatingHolder);
            if(predictRatingHolder>ratingThreshold) {
                _predictRecommendRatings.put(_recommendItems.get(i), predictRatingHolder);
            }
            
            // debug
            /*
            if(predictRatingHolder>5.0||predictRatingHolder<=0.0){
                exceptCount++;
                System.out.println("[Tracing] UsefulnessServe.PredictAllItems: predict rating for item " + _recommendItems.get(i) + " is " + predictRatingHolder);
            }
            */
        }
        System.out.println("[Tracing] there are " + exceptCount + " exception founded.");
        System.out.println("[UsefulnessServe] PredictAllItems: the size of predicted is " + _predictRecommendRatings.size());
        
        
        Map<String, String> useful_item_log = new HashMap<String, String>();
        
        
        if(_sysLogSession!=null) {
            if(logType == 1) {
                //_sysLogSession.SetRowKeyValue(String.valueOf(_predictRecommendRatings.size()), "N_useful_target_user_items");
                
                for(Map.Entry<String, Double> recommend : _predictRecommendRatings.entrySet()) {
                    useful_item_log.put(recommend.getKey(), String.valueOf(recommend.getValue()));
                }
                
                if(!useful_item_log.isEmpty()) {
                    _sysLogSession.SetRowKeyValueMap(useful_item_log, "all_items_useful");
                }else {
                    _sysLogSession.SetRowKeyValue("Empty", "all_items_useful");
                }
                
            }
            if(logType == 2) {
                
                for(Map.Entry<String, Double> recommend : _predictRecommendRatings.entrySet()) {
                    useful_item_log.put(recommend.getKey(), (recommend.getValue() + "_" + _servedUserRatings.get(recommend.getKey())));
                }
                
                if(!useful_item_log.isEmpty()) {
                    _sysLogSession.SetRowKeyValueMap(useful_item_log, "final_items_accuracyUse");
                }else {
                    _sysLogSession.SetRowKeyValue("No useful prediction found", "final_items_accuracyUse");
                }
                
            }
            
        }
        return _predictRecommendRatings;
    }
    
    private Double PredictOne(String ItemId) {
        Map<String, DataSession> contributedUsers = new HashMap<String, DataSession>();
        
        DataSession servedUserSession = _predictUserPool.get(_servedUser);
        
        contributedUsers = ContributionFilter(ItemId);
        
        PredictionSet predictPreData = new PredictionSet(_servedUser);
        String[] predictSetRow = new String[4];
        
        for(Map.Entry<String, DataSession> contributedUser : contributedUsers.entrySet()) {
            predictSetRow[0] = contributedUser.getKey();
            predictSetRow[1] = CalcSpcItemRatingAvg(contributedUser.getValue(), ItemId).toString();
            predictSetRow[2] = CalcRatingAvg(contributedUser.getValue(), ItemId).toString();
            predictSetRow[3] = _predictUserPS.get(contributedUser.getKey());
            predictPreData.SetARow(predictSetRow);
            
            //System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
            //System.out.println("[Tracing] UsefulnessServe.PredictRecommendRating: userId is " + predictSetRow[0]);
            //System.out.println("[Tracing] UsefulnessServe.PredictRecommendRating: user_item rating is " + predictSetRow[1]);
            //System.out.println("[Tracing] UsefulnessServe.PredictRecommendRating: userId_items rating avg is " + predictSetRow[2]);
            //System.out.println("[Tracing] UsefulnessServe.PredictRecommendRating: user " + contributedUser.getKey() + " userId_weight is " + predictSetRow[3]);
            
        }
        
        PredictionCalc predictonCalcHandler = new PredictionCalc();
        
        Double predictedRating = predictonCalcHandler.CalcWSOR(predictPreData, CalcRatingAvg(servedUserSession, "NA"));
        //System.out.println("[Tracing] UsefulnessServe.PredictRecommendRating: contributedUserSize is " + contributedUsers.size());
        
        return predictedRating;
    }
    
    private Map<String, DataSession> ContributionFilter(String ItemId) {
        Map<String, DataSession> filteredData = new HashMap<String, DataSession>();
        for(Map.Entry<String, DataSession> userData : _predictUserPool.entrySet()) {
            if(!userData.getKey().equals(_servedUser)) {
                if(userData.getValue().ContainsRecord("movieId", ItemId)) {
                    filteredData.put(userData.getKey(), userData.getValue());
                }
            }
        }
        return filteredData;
    }
    
    private Double CalcSpcItemRatingAvg(DataSession dataSet, String spcItemId) {
        String[] ratings = dataSet.GetAColum("rating");
        Map<Integer, String> spcRatings = dataSet.GetARecordAllIndex(spcItemId, "movieId");
        Double sum = 0.0;
        for(Map.Entry<Integer, String> spcRating : spcRatings.entrySet()) {
            sum = sum + Double.valueOf(ratings[spcRating.getKey()]);
        }
        return sum/spcRatings.size();
    }
    
    private Double CalcRatingAvg(DataSession dataSet, String uncontributedItemId) {
        
        DataSessionComparator dsc = new DataSessionComparator(_predictUserPool.get(_servedUser), dataSet, "movieId", "rating");
        List<Integer> intersect = dsc.GetIntersection()._sameRecordIndexU;
        String[] ratings = dataSet.GetAColum("rating");
        Map<Integer, String> uncontributedRatings;
        int size = 1;
        Double sum = 0.0;
        if(uncontributedItemId.equals("NA")) {
            uncontributedRatings = null;
            size = ratings.length;
            for(int i=0; i<ratings.length; i++) {
                sum = sum + Double.valueOf(ratings[i]);
            }
            return sum/size;
        }else {
            uncontributedRatings = dataSet.GetARecordAllIndex(uncontributedItemId, "movieId");
            size = intersect.size() - uncontributedRatings.size();
            for(int i=0; i<intersect.size(); i++) {
                if(!uncontributedRatings.containsKey(intersect.get(i))) {
                    sum = sum + Double.valueOf(ratings[intersect.get(i)]);
                }
            }
            return sum/size;
        }
        
        
        
    }

}
