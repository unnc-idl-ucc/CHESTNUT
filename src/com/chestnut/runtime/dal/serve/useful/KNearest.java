package com.chestnut.runtime.dal.serve.useful;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.ma.ESVParser;
import com.chestnut.runtime.dal.math.similarity.PearsonCorrelationSimilarity;
import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class KNearest {

    private Map<String, DataSession> _allSelectedData;
    private Map<String, String> _allSelectedPS;
    private int _KCValue;
    private ESVParser _globalKNHandler;
    private boolean _globalKNToggle;
    private Map<String, Integer> _recommendItemFlags;
    private Map<String, List<String>> _candidateItemContributors;
    private ConfigManager _cfgManager;
    
    private int _logGlobalKNSelectedCount, _logKNSelectedCount;
    
    public KNearest(int KValue, ConfigManager cfgManager) {
        _KCValue = KValue;
        _cfgManager = cfgManager;
        _globalKNToggle = false;
        _allSelectedData = new HashMap<String, DataSession>();
        _allSelectedPS = new HashMap<String, String>();
        
        _logGlobalKNSelectedCount = 0;
        _logKNSelectedCount = 0;
    }
    
    public void SetGlobalKNs(ESVParser globalKNHandler) {
        if(globalKNHandler != null) {
            _globalKNHandler = globalKNHandler;
            _globalKNToggle = true;
        }
    }
    
    public int GetGlobalKNSelectedCounts() {
        return _logGlobalKNSelectedCount;
    }
    
    public int GetRandomKNSelectedCounts() {
        return _logKNSelectedCount;
    }
    
    public Map<String, Integer> GetRecommendItemContributions() {
        return _recommendItemFlags;
    }
    
    public Map<String, List<String>> GetRecommendItemContributors() {
    	return _candidateItemContributors;
    }
    
    /**
     * Find the suitable neighbors start from items.
     * @param centerUserId
     * @param recommendUser
     * @param allUserTbList
     * @param recommendItems The items is used to find the neighbors.
     * @param pearsonThreashold The border to accept an user as a neighbor.
     * @return
     */
    public List<String> GetKNByItem(String centerUserId, String recommendUserId, String[] allUserTbList, List<String> recommendItems, Double pearsonThreashold){
        
        // Instance the map to record the number of neighbors contributing to each items.
        _recommendItemFlags = new HashMap<String, Integer>();
        
        // Instance the map to hold all the found neighbor binding to their contributing items.
        _candidateItemContributors = new HashMap<String, List<String>>();
        
        // Initialize the default value for each map.
        for(int i=0; i<recommendItems.size(); i++) {
            _recommendItemFlags.put(recommendItems.get(i), 0);
            _candidateItemContributors.put(recommendItems.get(i), new ArrayList<String>());
        }
        
        // Instance the map to hold each compared user as their full table name during searching of the neighbors.
        Map<String, String> calcedUserIds = new HashMap<String, String>();
        calcedUserIds.put(centerUserId, BuildUserTbName(centerUserId));
        
        // Put the recommend user into the calculated user map.
        if(!recommendUserId.equals("NA")) {
            calcedUserIds.put(recommendUserId, BuildUserTbName(recommendUserId));
        }
        
        //System.out.println("--------------------------------------------------------");
        System.out.println("[Tracing] KNearest.GetKNByItem: centerUserTbId is " + BuildUserTbName(centerUserId));
        DataSession centerUserSession = loadDataFromMySQL(BuildUserTbName(centerUserId), BuildUserTbName(centerUserId) + "_id", _cfgManager.GetMySQLHelper("dbName_user"));
        _allSelectedData.put(centerUserId, centerUserSession);
        
        String[] candidateItemUsersId;
        
        for(int i=0; i<recommendItems.size(); i++) {
            int countBreak = _recommendItemFlags.get(recommendItems.get(i));
            if(countBreak<_KCValue) {
                //System.out.println("[Tracing] KNearest.GetKNByItem: search neighbor from the " + i + " item with id " + recommendItems.get(i));
                DataSession currentItem = loadDataFromMySQL("movieId_" + recommendItems.get(i), "movieId_" + recommendItems.get(i) + "_id", _cfgManager.GetMySQLHelper("dbName_movie"));
                candidateItemUsersId = currentItem.GetAColum("userId");
                int rdSize = candidateItemUsersId.length;
                
                //System.out.println("[Tracing] KNearest.GetKNRandom: ------- Item " + recommendItems.get(i) + " -------");
                //System.out.println("[Tracing] KNearest.GetKNRandom: current start users from this item with size -> " + rdSize);
                
                if(rdSize > (_KCValue - countBreak)) {
                    HashMap<Integer, String> calcedItemUsersId = new HashMap<Integer, String>();
                    Random pickIdGen = new Random();
                    while((_KCValue - countBreak) > 0) {
                        int pickIdIndex = pickIdGen.nextInt(rdSize);
                        if(!calcedItemUsersId.containsKey(pickIdIndex)) {
                            if(scanOneItemUser(calcedUserIds, candidateItemUsersId[pickIdIndex], centerUserSession, centerUserId, pearsonThreashold)) {
                                countBreak++;
                            }
                            calcedItemUsersId.put(pickIdIndex, candidateItemUsersId[pickIdIndex]);
                        }
                        
                        if(calcedItemUsersId.size() == rdSize) {
                            countBreak = _KCValue;
                        }
                    }
                    
                }else {
                    for(int j=0; j<rdSize; j++) {
                        
                        scanOneItemUser(calcedUserIds, candidateItemUsersId[j], centerUserSession, centerUserId, pearsonThreashold);
                        
                    }
                }
                
                
            }else {
                //System.out.println("[Tracing] KNearest.GetKNByItem: search neighbor from the " + i + " item, and this item has satisfied the required number of contributor.");
            }
            
        }
        
        //printFlags();
        return FilterItemsByUserContribution(recommendItems);
        
    }
    
    private boolean scanOneItemUser(Map<String, String> calcedUserIds, String itemUserId, DataSession centerUserSession, String centerUserId, Double pearsonThreashold) {
     // Check if the user has been compared.
        if(!calcedUserIds.containsKey(itemUserId)) {
            String surroundUserId = itemUserId;
            calcedUserIds.put(surroundUserId, BuildUserTbName(surroundUserId));
            
            //System.out.println("[Tracing] KNearest.GetKNRandom: counter j -> " + j + ", surroundUserTbId is " + surroundUserTbId + "\n");
            
            // Load neighbor user data
            DataSession surroundUserSession = loadDataFromMySQL(BuildUserTbName(surroundUserId), BuildUserTbName(surroundUserId) + "_id", _cfgManager.GetMySQLHelper("dbName_user"));
            
            // Check if the system loaded the global Pearson similarities.
            if(_globalKNToggle) {
                String globalKNHolder;
                globalKNHolder = GetGlobalTwoUsersPS(surroundUserId, centerUserId, 0, 1);
                if(!globalKNHolder.equals("NA")) {
                    if(Math.abs(Double.valueOf(globalKNHolder))>pearsonThreashold && EvalueateContributeOnItem(surroundUserSession, surroundUserId)) {
                        //System.out.println("[Tracing] KNearest.GetKNRandom: KPtr " + KPtr + "; found a suitable surround user " + surroundUserId + " for center user " + centerUserId + " with PS = " + pearsonHolder);
                        _allSelectedData.put(surroundUserId, surroundUserSession);
                        _allSelectedPS.put(surroundUserId, globalKNHolder);
                        _logGlobalKNSelectedCount++;
                        return true;
                    }else {
                        return false;
                    }
                }else {
                    return TrackPSCalcContribution(centerUserSession, surroundUserSession, surroundUserId, pearsonThreashold);
                }
            }else {
                return TrackPSCalcContribution(centerUserSession, surroundUserSession, surroundUserId, pearsonThreashold);
            }
            
        }else {
            return false;
        }
    }
    
    public String GetGlobalTwoUsersPS(String userA, String userB, int knUIDColIndex, int psColIndex) {
        if(!_globalKNToggle) {
            return "NA";
        }
        
        List<List<String>> userAKNList;
        if((userAKNList = _globalKNHandler.GetARecordSet(userA))!=null) {
            List<String> userListKN = userAKNList.get(knUIDColIndex);
            List<String> psListKN = userAKNList.get(psColIndex);
            for(int i=0; i<userListKN.size(); i++) {
                if(userB.equals(userListKN.get(i))) {
                    return psListKN.get(i);
                }
            }
        }
        
        return "NA";
    }
    
    public Map<String, String> GetAllSelectedUserPS() {
        return _allSelectedPS;
    }
    
    public Map<String, DataSession> GetAllSelectedUserData() {
        return _allSelectedData;
    }
    
    private List<String> FilterItemsByUserContribution(List<String> recommendItems) {
        List<String> recordPassed = new ArrayList<String>();
        for(int i=0; i<recommendItems.size(); i++) {
            if(_recommendItemFlags.get(recommendItems.get(i))>=2) {
                recordPassed.add(recommendItems.get(i));
            }else {
                //System.out.println("[Tracing] KNearest.FilterItemsByUserContribution: found an uncontributed item --> " + recommendItems.get(i));
            }
        }
        return recordPassed;
    }
    
    /**
     * If find a proper neighbor, it will be passed to this function to search all its possible items that is contribute to the items used to find neighbors.
     * @param userData A suitable found neighbor.
     * @param userId
     * @return
     */
    private Boolean EvalueateContributeOnItem(DataSession userData, String userId) {
        String[] items = userData.GetAColum("movieId");
        String[] ratings = userData.GetAColum("rating");
        //System.out.println("[Tracing] KNearest.EvalueateContributeOnItem: flag map size is " + recommendItemFlag.size());
        /*
        List<String> recordRemovings = new ArrayList<String>();
        for(Map.Entry<String, Integer> itemFlag : recommendItemFlag.entrySet()) {
            if(itemFlag.getValue()>1) {
                System.out.println("[Tracing] KNearest.EvalueateContributeOnItem: item " + itemFlag.getKey() + " reached 5 contributor, removing");
                recordRemovings.add(itemFlag.getKey());
            }
        }
        for(int i=0; i<recordRemovings.size(); i++) {
            recommendItemFlag.remove(recordRemovings.get(i));
        }
        */
        
        // If counts holder is changed, it means the neighbor is contributing to the recommend items.
        int countsHolder = 0;
        for(int i=0; i<items.length; i++) {
            if(_recommendItemFlags.containsKey(items[i])) {
                countsHolder = _recommendItemFlags.get(items[i]) + 1;
                _recommendItemFlags.put(items[i], countsHolder);
            }
            
            if(_candidateItemContributors.containsKey(items[i])) {
            	_candidateItemContributors.get(items[i]).add(userId + "_" + ratings[i]);
            }
        }
        
        if(countsHolder!=0) {
            return true;
        }else {
            return false;
        }
    }
    
    private DataSession loadDataFromMySQL(String tableName, String primeField, MySQLHelper sqlhp){
        
        DataLoader dl = new DataLoader(sqlhp);
        DataSession queryData;
        
        try {
            queryData = dl.MySQLToDRS(tableName, dl.GetField(tableName, sqlhp.GetDBName()), primeField);
            dl = null;
            return queryData;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    @SuppressWarnings("unused")
    private String[] BuildUsersTbId(String[] userIds) {
        String[] userTbIds = new String[userIds.length];
        for(int i=0; i<userIds.length; i++) {
            userTbIds[i] = BuildUserTbName(userIds[i]);
        }
        return userTbIds;
    }
    
    private String BuildUserTbName(String userId) {
        return "userId_" + userId;
    }
    
    private boolean TrackPSCalcContribution(DataSession centerUserSession, DataSession surroundUserSession, String surroundUserId, Double pearsonThreashold) {
        PearsonCorrelationSimilarity PScalc = new PearsonCorrelationSimilarity(centerUserSession, surroundUserSession, _cfgManager);
        
        Double pearsonHolder = PScalc.ExecuteSimilarity("movieId", "rating");
        //if(Math.abs(pearsonHolder)>0.5) System.out.println("[Tracing] KNearest.GetKNRandom: pearsonHolder is " + pearsonHolder);
        if(Math.abs(pearsonHolder)>pearsonThreashold) {
            
            if(EvalueateContributeOnItem(surroundUserSession, surroundUserId)) {
                //System.out.println("[Tracing] KNearest.GetKNRandom: found a suitable surround user " + surroundUserId + " for center user " + centerUserSession.sessionName + " with PS = " + pearsonHolder);
                //_KNUserTbList[KPtr] = surroundUserTbId;
                _allSelectedData.put(surroundUserId, surroundUserSession);
                _allSelectedPS.put(surroundUserId, pearsonHolder.toString());
                _logKNSelectedCount++;
                //KPtr++;
            }
            
            return true;
        }else {
            return false;
        }
    }
    
    
    @SuppressWarnings("unused")
    private void printFlags() {
        int sum = 0;
        int uncontributeSum = 0;
        for(Map.Entry<String, Integer> itemFlag : _recommendItemFlags.entrySet()) {
            System.out.println("[Flags] Item " + itemFlag.getKey() + ", flag = " + itemFlag.getValue());
            sum = sum + itemFlag.getValue();
            if(itemFlag.getValue()==0) {
                uncontributeSum = uncontributeSum + 1;
            }
        }
        System.out.println("\n[Flags] size of items = " + _recommendItemFlags.size());
        System.out.println("[Flags] sum for flag = " + sum);
        System.out.println("[Flags] sum for uncontributed flag = " + uncontributeSum);
    }
    
}
