package com.chestnut.runtime.dal.special;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class ExtractLogs {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String[] fileNameList = GetFileList("data/logs/all/");
        try {
            
            BufferedReader br;
            BufferedWriter bw = new BufferedWriter(new FileWriter("data/extract.csv"));
            bw.write("userId,biggestEffectiveSumPearson,biggestPearsonIn2,biggestPearsonIn3,biggestPearsonIn4,biggestPearsonIn5\n");
            
            String lineHolder, directorHolder = "";
            String[] lineSplitHolder;
            List<String> PearsonsHolder;
            Map<String, String> userPearson;
            
            for(int i=0; i<fileNameList.length; i++) {
                System.out.println("[Tracing] File name " + i + " is " + fileNameList[i]);
                
                br = new BufferedReader(new FileReader("data/logs/all/" + fileNameList[i] + "_log.csv"));
                System.out.println("[Tracing] File is opened.");
                br.readLine();
                
                System.out.println("[Tracing] Collecting Pearsons...");
                PearsonsHolder = new ArrayList<String>();
                userPearson = new HashMap<String, String>();
                while((lineHolder = br.readLine())!=null) {
                    lineSplitHolder = lineHolder.split(",");
                    if(lineSplitHolder[4].equals("1")) {
                        PearsonsHolder.add(lineSplitHolder[5]);
                        directorHolder = lineSplitHolder[2];
                        //System.out.println("[Tracing] Put " + lineSplitHolder[3] + " with " + lineSplitHolder[5]);
                        userPearson.put(lineSplitHolder[3], lineSplitHolder[5]);
                    }
                }
                
                br.close();
                System.out.println("[Tracing] Pearsons collected.");
                
                System.out.println("[Tracing] Start build records set.");
                String recordBuild = "";
                for(int j=1; j<PearsonsHolder.size(); j++) {
                    recordBuild = recordBuild  + "," + GetBiggestInFirst(PearsonsHolder, j+1);
                }
                
                if(!PearsonsHolder.isEmpty()) {
                    String mostEffectUserId = GetMostEffectUserId(directorHolder);
                    //System.out.println("[Tracing] Most EffectUserId is: " + mostEffectUserId);
                    String recordset = fileNameList[i] + "," + userPearson.get(mostEffectUserId) + recordBuild;
                    System.out.println("[Tracing] Record set built: " + recordset);
                    bw.write(fileNameList[i] + "," + recordset + "\n");
                    System.out.println("[Tracing] Record set finished.\n");
                }else {
                    System.out.println("[Tracing] Record set empty.\n");
                }
                
            }
            bw.close();
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    private static String GetBiggestInFirst(List<String> pearsons, int amount) {
        Double biggestHolder = Double.valueOf(pearsons.get(0));
        Double pearsonHolder = 0.0;
        for(int i=1; i<amount; i++) {
            pearsonHolder = Double.valueOf(pearsons.get(i));
            if(pearsonHolder>biggestHolder) {
                biggestHolder = pearsonHolder;
            }
        }
        return biggestHolder.toString();
    }
    
    private static String GetMostEffectUserId(String directorTableName) {
        MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "zpzrecommendsystem_director", "root", "DZ2175362zhz");
        DataLoader dl = new DataLoader(sqlhp);
        String userId = "NA", statement;
        
        try {
            statement = "SELECT userId FROM directorid_" + directorTableName + " WHERE rating>=4.0 GROUP BY userId ORDER BY sum(rating) DESC LIMIT 1;";
            System.out.println("[Tracing] GetMostEffectUserId(), statement is: " + statement);
            ResultSet rsTable = sqlhp.ExecuteQuery(statement);
            rsTable.next();
            userId = rsTable.getString("userId");
            dl.CloseConnection();
            sqlhp.CloseConnection();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return userId;
    }
    
    private static String[] GetFileList(String path) {
        File directory = new File(path);
        File[] fileList = directory.listFiles();
        String[] fileNameList = new String[fileList.length];
        String strHolder;
        for(int i=0; i<fileList.length; i++) {
            strHolder = fileList[i].toString();
            fileNameList[i] = strHolder.substring(strHolder.lastIndexOf("\\")+1,strHolder.lastIndexOf("_"));
        }
        return fileNameList;
    }

}
