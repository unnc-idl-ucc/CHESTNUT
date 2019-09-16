package com.chestnut.runtime.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.log.LogSession;
import com.chestnut.runtime.booster.DataAgent;
import com.chestnut.runtime.booster.RecommendServe;

public class RunEvaluation {

    public static void main(String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        
        LogSession systemLogSession = new LogSession("log_sys", "data/logs/critical/log_" + (df.format(new Date())) + ".csv");
        String[] logFields = new String[6];
        logFields[0] = "ServedUID";
        logFields[1] = "all_items";
        logFields[2] = "all_items_unexpected";
        logFields[3] = "all_items_useful";
        logFields[4] = "serve_time_consuming";
        logFields[5] = "final_items_accuracyUse";
        systemLogSession.BuildLogFields(logFields, "ServedUID");
        
        //"localhost", "3306", dbName, "root", "DZ2175362zhz", "dbName_movie"
        ConfigManager sysConfigManager = new ConfigManager("dbConfigManager");
        
        sysConfigManager.SetConfig("tbUser_itemFieldName", "movieId");
        sysConfigManager.SetConfig("tbMovie_ratingFieldName", "rating");
        sysConfigManager.SetConfig("pearsonSignificanceWeightBoarder", "50");
        sysConfigManager.SetConfig("usefulFilteringThreshold", "3.0");
        
        sysConfigManager.SetMySQLHelper("dbName_user", "ZPZRecommendSystem_user_sm", "localhost", "3306", "root", "DZ2175362zhz");
        sysConfigManager.SetMySQLHelper("dbName_movie", "ZPZRecommendSystem_movie_sm", "localhost", "3306", "root", "DZ2175362zhz");
        sysConfigManager.SetMySQLHelper("dbName_director", "ZPZRecommendSystem_director_sm", "localhost", "3306", "root", "DZ2175362zhz");
        
        DataAgent systemDataAgent = new DataAgent();
        System.out.println("[System] start building system data agent...");
        systemDataAgent.LoadGlobalSeriesMapFromFile("data/ProductEnv/ExpectServe/movies_series.csv");
        systemDataAgent.LoadTopXMapFromFile("data/ProductEnv/ExpectServe/movies_topx.csv");
        systemDataAgent.LoadGlobalKN("data/ProductEnv/HelperSet/k_nearest_comb.csv");
        systemDataAgent.LoadAllUserTableId(sysConfigManager);
        System.out.println("[System] system data agent built. size of two global maps:\n"
                         + "[System] GlobalSeries: " + systemDataAgent.GetGlobalSeriesMap().size() + "\n"
                         + "[System] TopX: " + systemDataAgent.GetTopXMap().size() + "\n");
        
        
        
        try {
            BufferedReader IUBasedRD = new BufferedReader(new FileReader("data/ItemUserBased.csv"));
            IUBasedRD.readLine();
            
            String lineHolder, servedUID;
            String[] splitHolder, IBItems, UBItems;
            
            long starTime, endTime, Time;
            
            while((lineHolder = IUBasedRD.readLine())!=null) {
                splitHolder = lineHolder.split(",");
                
                servedUID = splitHolder[0];
                
                RecommendServe newServe = new RecommendServe(servedUID, systemDataAgent, systemLogSession, sysConfigManager);
                
                
                
                if(!splitHolder[6].equals("Empty")) {
                 // Item-Based
                    systemLogSession.SetRowKeyValue(servedUID, "ServedUID");
                    IBItems = splitHolder[6].split("\\|");
                    System.out.println("[System] start recommends filtering for IBItems from user " + servedUID + "...");
                 
                    //System.out.println("[System] size of IBItems: " + IBItems.length);
                    
                    starTime=System.currentTimeMillis();
                    newServe.EvalRecommend(ArrayToList(IBItems));
                    endTime=System.currentTimeMillis();
                    Time=endTime-starTime;
                    systemLogSession.SetRowKeyValue(String.valueOf(Time), "serve_time_consuming");
                    systemLogSession.SetARow();
                    
                 // User-Based
                    systemLogSession.SetRowKeyValue(servedUID, "ServedUID");
                    UBItems = splitHolder[7].split("\\|");
                    System.out.println("[System] start recommends filtering for UBItems from user " + servedUID + "...");
                    
                    starTime=System.currentTimeMillis();
                    newServe.EvalRecommend(ArrayToList(UBItems));
                    endTime=System.currentTimeMillis();
                    Time=endTime-starTime;
                    systemLogSession.SetRowKeyValue(String.valueOf(Time), "serve_time_consuming");
                    systemLogSession.SetARow();
                }else {
                    systemLogSession.SetRowKeyValue("empty", "all_items");
                    systemLogSession.SetRowKeyValue("empty", "all_items_unexpected");
                    systemLogSession.SetRowKeyValue("empty", "all_items_useful");
                    systemLogSession.SetRowKeyValue("NA", "useful_serve_time_consuming");
                    systemLogSession.SetRowKeyValue("NA", "serve_time_consuming");
                    systemLogSession.SetARow();
                }
                
            }
            
            IUBasedRD.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
        
        sysConfigManager.CloseAllMySQLHelper();

    }
    
    private static List<String> ArrayToList(String[] inArray) {
        List<String> outList = new ArrayList<String>();
        for(int i=0; i<inArray.length; i++) {
            outList.add(inArray[i]);
        }
        return outList;
    }
}
