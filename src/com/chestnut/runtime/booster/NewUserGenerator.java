package com.chestnut.runtime.booster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class NewUserGenerator {

    public static int STARTPOINT = 138494;
    
    public static void main(String[] args) {
        
        MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "zpzrecommendsystem_user", "root", "DZ2175362zhz");
        InitStartPoint(sqlhp);
        System.out.println("[STARTPOINT]: Initialized to " + STARTPOINT);
        
        int ops = 0;
        String newUserFilePath;
        
        BufferedReader bufferedInput = new BufferedReader(new InputStreamReader(System.in));
        try {
            while(ops>-1) {
                switch(ops) {
                case 0:
                    System.out.println("Please enter the operations index: ");
                    System.out.println("1. import new user into db by file ");
                    System.out.println("-1. exist ");
                    ops = Integer.valueOf(bufferedInput.readLine());
                    break;
                case 1:
                    System.out.println("Please enter the new user generate file path: ");
                    newUserFilePath = String.valueOf(bufferedInput.readLine());
                    GenByFile(newUserFilePath, sqlhp);
                    ops = 0;
                }
                
            }
        } catch (NumberFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
        
    }
    
    public static void InitStartPoint(MySQLHelper sqlhp) {
        try {
            STARTPOINT = sqlhp.QueryAllTables().length;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void GenByFile(String newUserFilePath, MySQLHelper sqlhp) {
        
        DataLoader dl = new DataLoader(newUserFilePath, sqlhp);
        //DataLoader dl = new DataLoader("data/ProductEnv/newUsers/newUser_exp_" + userExpIndex + "_" + userDBId + ".csv", sqlhp);
        
        try {
            dl.CSVToMySQL("", "userId_" + (++STARTPOINT));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }
    
    public static void GenRandomly() {
        Random rm = new Random();
        MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", "zpzrecommendsystem_user", "root", "DZ2175362zhz");
        DataLoader dl = new DataLoader(sqlhp);
        
        String[] genFieldHandler = new String[6];
        genFieldHandler[0] = "movieId";
        genFieldHandler[1] = "rating";
        genFieldHandler[2] = "timestamp";
        genFieldHandler[3] = "directorId";
        genFieldHandler[4] = "year";
        genFieldHandler[5] = "genres";
        
        String[] genRSFieldHandler = new String[7];
        genRSFieldHandler[0] = "NA";
        genRSFieldHandler[1] = "movieId";
        genRSFieldHandler[2] = "rating";
        genRSFieldHandler[3] = "timestamp";
        genRSFieldHandler[4] = "directorId";
        genRSFieldHandler[5] = "year";
        genRSFieldHandler[6] = "genres";
        
        int genSize = 3;
        int userRecordSize = 15;
        int randomIdHolder;
        String[] newUserRowForm;
        String[] randomRowHolder;
        for(int i=0; i<genSize; i++) {
            DataSession newUserFormer = new DataSession("userId_" + String.valueOf(138494+i));
            DataSession randomSelectUser;
            newUserFormer.BuildFields(genFieldHandler, genFieldHandler[0]);
            
            try {
                randomIdHolder = rm.nextInt(138492) + 1;
                genRSFieldHandler[0] = "userId_" + randomIdHolder + "_id";
                randomSelectUser = dl.MySQLToDRS("userId_" + randomIdHolder, genRSFieldHandler, "userId_" + randomIdHolder + "_id");
                
                Map<Integer, Integer> rowFlag = new HashMap<Integer, Integer>();
                int randomRowIndex;
                for(int j=0; j<userRecordSize; j++) {
                    randomRowIndex = rm.nextInt(randomSelectUser.dataRecordSize-1);
                    if(!rowFlag.containsKey(randomRowIndex)) {
                        rowFlag.put(randomRowIndex, randomRowIndex);
                        randomRowHolder = randomSelectUser.GetARow(randomRowIndex);
                        newUserRowForm = new String[6];
                        newUserRowForm[0] = randomRowHolder[1];
                        newUserRowForm[1] = randomRowHolder[2];
                        newUserRowForm[2] = randomRowHolder[3];
                        newUserRowForm[3] = randomRowHolder[4];
                        newUserRowForm[4] = randomRowHolder[5];
                        newUserRowForm[5] = randomRowHolder[6];
                        
                        newUserFormer.SetARow(newUserRowForm);
                    }else {
                        j--;
                    }
                }
                System.out.println(newUserFormer.dataRecordSize);
                
                dl.DRSToMySQL(newUserFormer, "");
                
                
                
            } catch (SQLException e) {
                e.printStackTrace();
            }
            
        }
    }

}
