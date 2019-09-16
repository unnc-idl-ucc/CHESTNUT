package com.chestnut.runtime.booster;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.log.LogSession;
import com.chestnut.runtime.dal.ma.BinaryNode;
import com.chestnut.runtime.dal.ma.BinarySearchTree;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;
import com.chestnut.runtime.dal.serve.expectedness.ExpectednessServe;
import com.chestnut.runtime.dal.serve.relevance.RelevanceServe;
import com.chestnut.runtime.dal.serve.useful.UsefulnessServe;

public class RecommendServe {

    private String _servedUserId;
    private int _finalLevel = 0;
    private LogSession _sysLogSession;
    
    private DataAgent _midDataAgent;
    
    private ConfigManager _cfgManager;
    
    /**
     * A RecommendServe is used to handle all the functions and data used to provide a one time serve of recommendation to an user.
     * @param servedUserId The ID of the user who request the serve.
     * @param systemDataAgent A specified data agent used globally by the system to provide repeated access to the data.
     * @param sysLog A Log Session of the system records return values from runtime functions if needed.
     * @param cfgManager A globally used manager handling all the static parameters used to access the database or run the functions.
     */
    public RecommendServe(String servedUserId, DataAgent systemDataAgent, LogSession sysLog, ConfigManager cfgManager){
        _servedUserId = servedUserId;
        _midDataAgent = systemDataAgent;
        _cfgManager = cfgManager;
        
        //_midDataAgent.StoreGlobalSeriesMap(globalSeriesMap);
        //_midDataAgent.StoreTopXMap(topXMap);
        
        
        _sysLogSession = sysLog;
        _sysLogSession.SetRowKeyValue(servedUserId, "ServedUID");
        
        _midDataAgent.StoreServedUserItems(LoadServedUserItems());
    }
    
    /**
     * Providing serendipitous recommendations. Contains all the functions used to find and filter a list of items as the candidates of recommendations.
     * @param PSThreshold The Pearson Similarity threshold used to determined a target user to provide the candidates of recommendation under an acceptable weight of relevance with the served user. 
     * @param recommendSize The size of the final recommendations provided to the served user.
     * @return A list of recommendations in the order from highest estimated rating to the lowest.
     */
    public List<String> RecommendBySerendipity(Double PSThreshold, int recommendSize){
        /** --------- **/
        /** Relevance **/
        /** --------- **/
        long starTime=System.currentTimeMillis();
        
        RelevanceServe(PSThreshold);
        
        long endTime=System.currentTimeMillis();
        long Time=endTime-starTime;
        _sysLogSession.SetRowKeyValue(String.valueOf(Time), "serve_one_time_consuming");
        
        /** -------------- **/
        /** Unexpectedness **/
        /** -------------- **/
        starTime=System.currentTimeMillis();
        
        String[] RecommendUsersHolder = _midDataAgent.GetRecommendUsers();
        UnexpectedFiltering(RecommendUsersHolder);
        
        endTime=System.currentTimeMillis();
        Time=endTime-starTime;
        _sysLogSession.SetRowKeyValue(String.valueOf(Time), "serve_two_time_consuming");
        
        /** ---------- **/
        /** Usefulness **/
        /** ---------- **/
        List<String> finalTopRecommends = UsefulFiltering(RecommendUsersHolder, recommendSize);
        
        
        return finalTopRecommends;
        
        //return null;
    }
    
    /**
     * 
     * @param rawRdItems
     */
    public void EvalRecommend(List<String> rawRdItems) {
        List<String> recommendsHandler;
        LoadServedUserItems();
        // Unexpected Filtering
        ExpectednessServe expectednessHandler = new ExpectednessServe(_midDataAgent.GetGlobalSeriesMap(), _midDataAgent.GetTopXMap(), _midDataAgent.GetServedUserItems(), _sysLogSession);
        recommendsHandler = expectednessHandler.BuildUnexpectedList(rawRdItems, true);
        if(recommendsHandler.isEmpty()) {
            _sysLogSession.SetRowKeyValue("Empty", "all_items_useful");
        }else {
            if(!(recommendsHandler.get(0).equals("Empty"))) {
                // Useful Filtering
                UsefulnessServe usefulnessHandler = new UsefulnessServe(_servedUserId, "NA", recommendsHandler, _midDataAgent.GetGlobalKNHandler(), _sysLogSession);
                usefulnessHandler.BuildPredictUsersByRKN(_midDataAgent.GetAllUserTableIds(), _cfgManager);
                usefulnessHandler.PredictAllItems(1, Double.valueOf(_cfgManager.GetConfigVal("usefulFilteringThreshold")));
                
                System.out.println("[RecommendServe] usefulness ---- accuracy uses:");
                recommendsHandler = GetIntersection(_midDataAgent.GetServedUserItems(), rawRdItems);
                UsefulnessServe accuracyUsefullnessHandler = new UsefulnessServe(_servedUserId, "NA", recommendsHandler, _midDataAgent.GetGlobalKNHandler(), _sysLogSession);
                accuracyUsefullnessHandler.SetServedUserRatings(_midDataAgent.GetServedUserRatings());
                accuracyUsefullnessHandler.BuildPredictUsersByRKN(_midDataAgent.GetAllUserTableIds(), _cfgManager);
                accuracyUsefullnessHandler.PredictAllItems(2, Double.valueOf(_cfgManager.GetConfigVal("usefulFilteringThreshold")));
                
            }else {
                _sysLogSession.SetRowKeyValue("Empty", "all_items_useful");
            }
        }
        
        
        
        
    }
    
    /**
     * Give the number of levels iterated in the serendipitous recommender to find a target user through a proper insight property.
     * @return An integer to denote the number of levels.
     */
    public int getLevel() {
        return _finalLevel;
    }
    
    /**
     * Give the Log Session passed through this recommend serve.
     * @return A LogSession of the whole system.
     */
    public LogSession GetLog() {
        return _sysLogSession;
    }
    
    private void RelevanceServe(Double PSThreshold) {
        System.out.println("\n[RecommendServe] relevance serve start...");
        RelevanceServe getRecommendUsers = new RelevanceServe(_servedUserId, _cfgManager);
        if(_midDataAgent.GetGlobalKNHandler()!=null) {
            getRecommendUsers.SetGlobalPearsonHandler(_midDataAgent.GetGlobalKNHandler());
        }
        _midDataAgent.StoreRecommendUsers(getRecommendUsers.MostRelevanceByThreshold(PSThreshold));
        _midDataAgent.StoreServedUserItems(LoadServedUserItems());
        _finalLevel = getRecommendUsers.GetFinalLevel();
        System.out.println("[RecommendServe] found " + _midDataAgent.GetRecommendUsers().length + " recommend users.");
    }
    
    private void UnexpectedFiltering(String[] targetUsers) {
        System.out.println("\n[RecommendServe] unexpectedness serve start...");
        ExpectednessServe expectednessHandler = new ExpectednessServe(_midDataAgent.GetGlobalSeriesMap(), _midDataAgent.GetTopXMap(), _midDataAgent.GetServedUserItems(), _sysLogSession);
        
        
        for(int i=0; i<targetUsers.length; i++) {
            _midDataAgent.StoreRecommendUserItems(targetUsers[i], 
                                                  expectednessHandler.BuildUnexpectedList(LoadRecommendUserItems(targetUsers[i]), true));
            _midDataAgent.StoreRecommendUserIntersectItems(targetUsers[i], expectednessHandler.getIntersectRecommendList());
            
            
            /** Items relative users count **/
            
            //List<String> intersectRecommendedItems = _midDataAgent.GetRecommendUserIntersectItems(RecommendUsersHolder[i]);
            /*
            String itemCountsHandler = "";
            for(int j=0; j<intersectRecommendedItems.size(); j++) {
                DataSession movie = loadDataFromMySQL(_serveItemSQLHelper, "movieId_" + intersectRecommendedItems.get(j), "movieId_" + intersectRecommendedItems.get(j) + "_id");
                if(movie.dataRecordSize>100) itemCountsHandler = itemCountsHandler + "|" +intersectRecommendedItems.get(j) + "_" + movie.dataRecordSize;
            }
            */
            //_sysLogSession.SetRowKeyValue(String.valueOf(intersectRecommendedItems.size()), "N_intersect_items");
            
            /** Items relative users count **/
            
            
        }
        
        _sysLogSession = expectednessHandler.GetLog();
    }
    
    private List<String> UsefulFiltering(String[] targetUsers, int recommendSize) {
        System.out.println("\n[RecommendServe] usefulness serve start...");
        
        List<String> finalTopRecommends = null;
        
        for(int i=0; i<targetUsers.length; i++) {
            UsefulnessServe usefulnessHandler = new UsefulnessServe(_servedUserId, targetUsers[i], _midDataAgent.GetRecommendUserItems(targetUsers[i]), null, _sysLogSession);
            usefulnessHandler.BuildPredictUsersByRKN(_midDataAgent.GetAllUserTableIds(), _cfgManager);
            finalTopRecommends = GetTopXUsefulRecommend(usefulnessHandler.PredictAllItems(1, Double.valueOf(_cfgManager.GetConfigVal("usefulFilteringThreshold"))), recommendSize);
            /*
            System.out.println("[RecommendServe] usefulness ---- accuracy uses: \n");
            UsefulnessServe accuracyUsefullnessHandler = new UsefulnessServe(_servedUserId, targetUsers[i], _midDataAgent.GetRecommendUserIntersectItems(targetUsers[i]), _midDataAgent.GetGlobalKNHandler(), _sysLogSession);
            accuracyUsefullnessHandler.SetServedUserRatings(_midDataAgent.GetServedUserRatings());
            accuracyUsefullnessHandler.BuildPredictUsersByRKN(_midDataAgent.GetAllUserTableIds(), _cfgManager);
            accuracyUsefullnessHandler.PredictAllItems(2, Double.valueOf(_cfgManager.GetConfigVal("usefulFilteringThreshold")));
            */
            
        }
        System.out.println("[RecommendServe] usefulness serve ended...\n");
        
        return finalTopRecommends;
    }
    
    /**
     * Select the request number of recommendations and sort them by descend order.
     * @param usefulRecommends 
     * @param topSize
     * @return
     */
    private List<String> GetTopXUsefulRecommend(Map<String, Double> usefulRecommends, int topSize) {
        List<String> TopXRecommends = new ArrayList<String>();
        
        int initCount = 0;
        for(Map.Entry<String, Double> recommmend : usefulRecommends.entrySet()) {
            if(initCount < topSize) {
                TopXRecommends.add(recommmend.getKey());
                initCount++;
            }else {
                TopXRecommends.add(recommmend.getKey());
                TopXRecommends = SortRecommendRatingDESC(TopXRecommends, usefulRecommends);
                TopXRecommends.remove(topSize);
            }
        }
        
        
        return TopXRecommends;
    }
    
    private List<String> SortRecommendRatingDESC(List<String> TopXRecommends, Map<String, Double> usefulRecommends) {
        Double[] arr = new Double[TopXRecommends.size()];
        for(int i=0; i<TopXRecommends.size(); i++) {
            arr[i] = usefulRecommends.get(TopXRecommends.get(i));
        }
        BinaryNode root = new BinaryNode(arr[0], 0, null, null);
        BinarySearchTree bst = new BinarySearchTree(root, TopXRecommends.size(), "DESC");
        for(int i=1; i<arr.length; i++) {
            bst.AddNewNode(arr[i], i);  
        }
        
        List<Integer> sortIndexHolder = bst.GetSortedArrByAmount();
        List<String> sortedRecommend = new ArrayList<String>();
        for(int i=0; i<sortIndexHolder.size(); i++) {
            sortedRecommend.add(TopXRecommends.get(sortIndexHolder.get(i)));
        }
        return sortedRecommend;
    }
    
    private String[] LoadServedUserItems() {
        DataSession servedUserItems = loadDataFromMySQL(_cfgManager.GetMySQLHelper("dbName_user"), "userId_"+_servedUserId, "userId_"+_servedUserId+"_id");
        
        /** All the items of the active user mapping with rating **/
        Map<String, String> itemRatingMapHandler = new HashMap<String, String>();
        String[] movieList = servedUserItems.GetAColum("movieId");
        String[] ratingList = servedUserItems.GetAColum("rating");
        for(int i=0; i<movieList.length; i++) {
            itemRatingMapHandler.put(movieList[i], ratingList[i]);
        }
        _midDataAgent.StoreServedUserRatings(itemRatingMapHandler);
        _midDataAgent.StoreServedUserItems(movieList);
        //_sysLogSession.SetRowKeyValueMap(itemRatingMapHandler, "all_active_user_items");
        
        
        return movieList;
    }
    
    private List<String> LoadRecommendUserItems(String userId) {
        DataSession recommendUserItems = loadDataFromMySQL(_cfgManager.GetMySQLHelper("dbName_user"), "userId_"+userId, "userId_"+userId+"_id");
        String[] itemsHolder = recommendUserItems.GetAColum("movieId");
        
        List<String> itemsList = new ArrayList<String>();
        for(int i=0; i<itemsHolder.length; i++) {
            //System.out.println("[LoadRecommendUserItems] recommend item: " + itemsHolder[i]);
            itemsList.add(itemsHolder[i]);
        }
        
        return itemsList;
    }
    
    private DataSession loadDataFromMySQL(MySQLHelper sqlhp, String tableName, String primeField) {
        DataLoader dl = new DataLoader(sqlhp);
        DataSession queryData;
        
        try {
            queryData = dl.MySQLToDRS(tableName, dl.GetField(tableName, sqlhp.GetDBName()), primeField);
            return queryData;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private List<String> GetIntersection(String[] activeUserItems, List<String> rawRtItems) {
        
        System.out.println("[Tracing] RecommendServe.GetIntersection: active user items size = " + activeUserItems.length);
        
        List<String> results = new ArrayList<String>();
        Map<String, String> activeUserItemsCheck = new HashMap<String, String>();
        for(int i=0; i<activeUserItems.length; i++) {
            activeUserItemsCheck.put(activeUserItems[i], activeUserItems[i]);
        }
        
        String itemHolder;
        for(int i=0; i<rawRtItems.size(); i++) {
            itemHolder = rawRtItems.get(i);
            if(activeUserItemsCheck.containsKey(itemHolder)) {
                results.add(itemHolder);
            }
        }
        
        System.out.println("[Tracing] RecommendServe.GetIntersection: result size = " + results.size());
        
        return results;
    }
}
