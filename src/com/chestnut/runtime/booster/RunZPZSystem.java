package com.chestnut.runtime.booster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.HTML.RecommendationExporter;
import com.chestnut.runtime.IMDb.IMDbExportsLoader;
import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.csv.DataBuilder;
import com.chestnut.runtime.dal.log.LogSession;
import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;


public class RunZPZSystem {
	
    /**
     * This program needs to run with three MySQL databases deployed before the system.
     * The three databases are established by the program BuildDataBase in the same package.
     * If you have not established those three databases, please refer to BuildDataBase to establish the databases first.
     * This program is start with a very simple command line interface to boost a recommendation of a user.<BR/>
     * The regulations of the input are:<BR/>
     * If you want do a single recommendation for a user with a specific user id, just input the user id in the first line and then input 0.<BR/>
     * If you want do a batch of recommendations, input a start point in the first line such as 1 means start from second user. And then input an end point where to stop the batch.
     * @param args
     */
	public static void main(String[] args){
		
		int StartUser = 0, BatchScale = 0, RecommendScale = 0;
		
		// 杈撳叆鎿嶄綔
		BufferedReader bufferedInput = new BufferedReader(new InputStreamReader(System.in));
		try {
			System.out.println("How many recommendations to display:");
			RecommendScale = Integer.valueOf(bufferedInput.readLine());
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//System.out.println("[System] Start at:"  + StartUser);
		//int endPos = StartUser + BatchScale;
		//System.out.println("[System] End at:"  + endPos);
		
		// 鍒濆鍖栨棩蹇楃郴缁�?
	    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	    LogSession systemLogSession = new LogSession("log_sys", "data/logs/critical/log_" + (df.format(new Date())) + ".csv");
	    String[] logFields = new String[9];
	    logFields[0] = "ServedUID";
        logFields[1] = "all_items";
        logFields[2] = "all_items_unexpected";
        logFields[3] = "all_items_useful";
        logFields[4] = "serve_time_consuming";
        logFields[5] = "final_items_accuracyUse";
        logFields[6] = "serve_one_time_consuming";
        logFields[7] = "serve_two_time_consuming";
        logFields[8] = "serve_all_time_consuming";
        systemLogSession.BuildLogFields(logFields, "ServedUID");
	    
        //"localhost", "3306", dbName, "root", "DZ2175362zhz", "dbName_movie"
        ConfigManager sysConfigManager = new ConfigManager("dbConfigManager");
        
        // 鍒濆鍖栫郴缁熼厤缃�?
        sysConfigManager.SetConfig("tbUser_itemFieldName", "movieId");          // 鐢ㄦ埛琛ㄤ腑鐗╁搧�?�楁鍚嶇�?
        sysConfigManager.SetConfig("tbMovie_ratingFieldName", "rating");        // 鐢靛奖琛ㄤ腑璇勫垎�?�楁鍚嶇�?
        sysConfigManager.SetConfig("pearsonSignificanceWeightBoarder", "50");   // Significance鏉冮噸杈圭晫
        sysConfigManager.SetConfig("usefulFilteringThreshold", "3.0");          // 鏈夋剰涔夎瘎鍒嗛槇鍊�?
        sysConfigManager.SetConfig("weightToggle", "ON");                       // 鏄惁鍔犳潈寮�鍏筹紝閽堝SW
        // 閰嶇疆鐢熸垚鏁版嵁搴撹繛鎺ュ疄浣�?
        sysConfigManager.SetMySQLHelper("dbName_user", "ZPZRecommendSystem_user", "localhost", "3306", "root", "DZ2175362zhz");
        sysConfigManager.SetMySQLHelper("dbName_movie", "ZPZRecommendSystem_movie", "localhost", "3306", "root", "DZ2175362zhz");
        sysConfigManager.SetMySQLHelper("dbName_director", "ZPZRecommendSystem_director", "localhost", "3306", "root", "DZ2175362zhz");
	    
        // 鐢熸垚绯荤粺鏁版嵁浠ｇ悊瀹炰綋锛�?敤浜庣鐞嗚皟鍏ユ暟鎹強鏁版嵁浜ゆ�?
	    DataAgent systemDataAgent = new DataAgent();
	    System.out.println("[System] start building system data agent...");
	    systemDataAgent.LoadGlobalSeriesMapFromFile("data/ProductEnv/ExpectServe/movies_series.csv");  // 杞藉叆鍏ㄥ眬浣跨敤鐨勭郴鍒楃數褰�?
	    systemDataAgent.LoadTopXMapFromFile("data/ProductEnv/ExpectServe/movies_topx.csv");            // 杞藉叆鍏ㄥ眬浣跨敤鐨勬渶鍙楁杩庣數褰�
	    //systemDataAgent.LoadGlobalKN("data/ProductEnv/HelperSet/k_nearest_comb.csv");
	    systemDataAgent.LoadAllUserTableId(sysConfigManager);                                          // 杞藉叆鎵�鏈夌敤鎴疯〃ID
	    System.out.println("[System] system data agent built. size of two global maps:\n"
	                     + "[System] GlobalSeries: " + systemDataAgent.GetGlobalSeriesMap().size() + "\n"
	                     + "[System] TopX: " + systemDataAgent.GetTopXMap().size() + "\n");
	    
	    // 澶勭悊IMDb瀵煎嚭鏂囦欢锛屾牸寮忓寲鏁版嵁骞惰緭鍏ョ郴缁燂紝杩涜mahout鍩轰簬鐢ㄦ埛鍜岀墿鍝佺殑鍗忓悓杩囨护鐨勬帹鑽�?
	    // Load IMDb exports to MySQL
	    System.out.println("\n[System] Loading IMDb Exports to databses...");
	    IMDbExportsLoader IMDbExports = new IMDbExportsLoader(sysConfigManager.GetMySQLHelper("dbName_user"));
	    ArrayList<Integer> newUsers = IMDbExports.LoadExportsFromFile();
	    StartUser = newUsers.get(0);
	    BatchScale = newUsers.size();
	  
	    // 杩涜鍋堕亣鎬ф帹鑽�
	    if(BatchScale==0) {
	        int batchJobCounter = 0;
	        //Random userGen = new Random();
	        String userIdHolder;
	        
	        Map<String, String> userIdRecommended = new HashMap<String, String>();
	        for(int i=0; i<BatchScale; i++) {
	            
	            System.out.println("\n[System] =========== Recommend Serve =========== >>>>>>>>");
	            System.out.println("[System] start a new recommend serve...");
	            List<String> topRecommendList = null;
	            
	            //userIdHolder = userIds[userGen.nextInt(userIds.length-1)];
	            userIdHolder = newUsers.get(i).toString();
	            if(userIdHolder!=null && !userIdRecommended.containsKey(userIdHolder)) {
	                userIdRecommended.put(userIdHolder, userIdHolder);
	                long starTime=System.currentTimeMillis();
	                
	                System.out.println("[System] start recommend for user " + userIdHolder + "...");
	                RecommendServe newServe = new RecommendServe(String.valueOf(userIdHolder), systemDataAgent, systemLogSession, sysConfigManager);
	                
	                topRecommendList = newServe.RecommendBySerendipity(0.7, RecommendScale);
	                
	                System.out.println("[System] ----------- Recommend Items ----------- ");
	                if(topRecommendList!=null) {
	                    for(int j=0; j<topRecommendList.size(); j++) {
	                        System.out.println("[System] Recommend item " + j + " is " + topRecommendList.get(j) + ".");
	                    }
	                }
	                batchJobCounter ++;
	                System.out.println("[System] --------------------------------------- ");
	                System.out.println("[System] recommend batch " + batchJobCounter + " serve completed. used " + newServe.getLevel() + " levels.");
	                
	                long endTime=System.currentTimeMillis();
	                long Time=endTime-starTime;
	                
	                systemLogSession = newServe.GetLog();
	                systemLogSession.SetRowKeyValue(String.valueOf(Time), "serve_all_time_consuming");
	                systemLogSession.SetARow();
	            }
	            System.out.println("[System] ======================================= \n");
	            
	        }
	        
	    }else {
	        long starTime=System.currentTimeMillis();
	        System.out.println("\n[System] =========== Recommend Serve =========== >>>>>>>>");
            System.out.println("[System] start a new recommend serve...");
            List<String> topRecommendList = null;
            
            System.out.println("[System] start recommend for user " + StartUser + "...");
            RecommendServe newServe = new RecommendServe(String.valueOf(StartUser), systemDataAgent, systemLogSession, sysConfigManager);
            topRecommendList = newServe.RecommendBySerendipity(0.5, RecommendScale);
            
            System.out.println("[System] ----------- Recommend Items ----------- ");
            if(topRecommendList!=null) {
                for(int j=0; j<topRecommendList.size(); j++) {
                    System.out.println("[System] Recommend item " + j + " is " + topRecommendList.get(j) + ".");
                }
            }
            
            System.out.println("\n[System] ----------- Make Up Recommend Items ----------- ");
            ExportRecommendationToCSV(topRecommendList, String.valueOf(StartUser));
            
            System.out.println("\n[System] --------------------------------------- ");
            System.out.println("[System] recommend serve completed. used " + newServe.getLevel() + " levels.");
            
            long endTime=System.currentTimeMillis();
            long Time=endTime-starTime;
            
            systemLogSession = newServe.GetLog();
            systemLogSession.SetRowKeyValue(String.valueOf(Time), "serve_all_time_consuming");
            systemLogSession.SetARow();
            
            System.out.println("[Tracing] Job completed! with time: " + Time);
            
            System.out.println("[System] ======================================= \n");
            
	    }
	    
	    sysConfigManager.CloseAllMySQLHelper();
	    
	}
	
	/*
	 * Export the results of a roll of recommendation as the csv format.
	 */
	private static void ExportRecommendationToCSV(List<String> topRecommendList, String userId) {
	    try {
	        BufferedWriter exportCSVWriter = new BufferedWriter(new FileWriter("data/ProductEnv/RecommendationResults/results_SB_" + userId + ".csv"));
            exportCSVWriter.write("movieId" + "\n");
            
            for(int i=0; i<topRecommendList.size(); i++) {
                exportCSVWriter.write(topRecommendList.get(i) + "\n");
            }
            exportCSVWriter.close();
            
            RecommendationExporter recExporter = new RecommendationExporter();
            recExporter.BatchAFolderInfoMaking("data/ProductEnv/RecommendationResults", "SB");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	/** DevSript **/
	public static void loadDataToDB(String dataFileDir, String dataBaseName, String groupField){
		MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", dataBaseName, "root", "DZ2175362zhz");
		DataLoader dl = new DataLoader(dataFileDir, sqlhp);
		
		try {
			dl.CSVToMySQLGroupByField("", groupField);
			dl.CloseBuffer();
			dl.CloseConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void groupData(){
		DataBuilder dber = new DataBuilder("data/ratings.csv");
		try {
			dber.BucketGroupDataBy("movieId");
			dber.CloseAllBuffer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void RatingsGroupByUserId(String name){
		DataBuilder dber = new DataBuilder("data/ratings_director_matched.csv");
		try {
			dber.BucketGroupDataBy(name);
			dber.CloseAllBuffer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void RatingsGroupByUserIdToMySQL(){
		MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "ZPZRecommendSystem_user", "root", "DZ2175362zhz");
		DataLoader dl = new DataLoader("data/user.csv", sqlhp);
		
		try {
			dl.ESVToMySQL("", "userId", "movieId");
			dl.CloseBuffer();
			dl.CloseConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
