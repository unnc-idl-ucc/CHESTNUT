package com.chestnut.runtime.booster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class BuildDataBase {

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        String sourceDataDir = "NA", mysqlDB = "NA", mysqlUser = "NA", mysqlPwd = "NA", groupField = "NA";
        
        String operation = "run";
        BufferedReader bufferedInput = new BufferedReader(new InputStreamReader(System.in));
        while(!operation.matches("exit")){
            System.out.println("Please enter the source file directory:");
            sourceDataDir = bufferedInput.readLine();
            System.out.println("Please enter the database to use:");
            mysqlDB = bufferedInput.readLine();
            System.out.println("Please enter the user id to acess the database:");
            mysqlUser = bufferedInput.readLine();
            System.out.println("Please enter the user passwords to acess the database:");
            mysqlPwd = bufferedInput.readLine();
            System.out.println("Please enter the field to group the data:");
            groupField = bufferedInput.readLine();
            System.out.println("All settings correct?(Y/N)");
            System.out.println("sourceDataDir: " + sourceDataDir + "\n" + 
                    "mysqlDB: " + mysqlDB + "\n" + 
                    "mysqlUser: " + mysqlUser + "\n" + 
                    "mysqlPwd: " + mysqlPwd + "\n" + 
                    "groupField: " + groupField);
            operation = bufferedInput.readLine();
            
            if(operation.matches("Y")) operation = "exit";
        }
        
        loadDataToDB(sourceDataDir, mysqlDB, mysqlUser, mysqlPwd, groupField);// dir template "data/movies/ratings_gen_matched.csv"

    }
    
    public static void loadDataToDB(String dataFileDir, String dataBaseName, String mysqlUser, String mysqlPwd, String groupField){
        MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", dataBaseName, mysqlUser, mysqlPwd);
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

}
