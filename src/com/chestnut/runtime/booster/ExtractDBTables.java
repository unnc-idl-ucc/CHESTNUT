package com.chestnut.runtime.booster;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class ExtractDBTables {

    
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        String[] tbIdHolder = LoadAllUserTableId("zpzrecommendsystem_movie");
        System.out.println("Size of tables: " + tbIdHolder.length);
        /*
        try {
            BufferedWriter bfw = new BufferedWriter(new FileWriter("data/ProductEnv/allMovies.csv"));
            
            for(int i=0; i<tbIdHolder.length; i++) {
                bfw.write(tbIdHolder[i].substring(tbIdHolder[i].indexOf("_")+1) + "\n");
            }
            bfw.close();
            
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        */
    }
    
    public static String[] LoadAllUserTableId(String dbName) {
        String[] _allTableId = null;
        MySQLHelper sqlhp = new MySQLHelper("localhost", "3306", dbName, "root", "DZ2175362zhz");
        try {
            _allTableId = sqlhp.QueryAllTables();
            sqlhp.CloseStatement();
            sqlhp.CloseConnection();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return _allTableId;
    }

}
