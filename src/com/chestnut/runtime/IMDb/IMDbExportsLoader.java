package com.chestnut.runtime.IMDb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import com.chestnut.runtime.IMDb.exports.TaskBatch;
import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class IMDbExportsLoader {

    private MySQLHelper _sqlhpUser;
    private int _newUserStartPoint = 200000;
    private ArrayList<Integer> _newLoadUserIds;
    
    // IDMb导出文件导入�
    public IMDbExportsLoader(MySQLHelper sqlhpUser) {
        _sqlhpUser = sqlhpUser;
        InitStartPoint();
        _newLoadUserIds = new ArrayList<Integer>();
    }
    
    /***
     * 从文件夹 data/ProductEnv/IMDb 中读取导� IMDb 导出文件
     * @return 成功导入的新用户列表
     */
    public ArrayList<Integer> LoadExportsFromFile() {
        
        TaskBatch tbTest = new TaskBatch("data/ProductEnv/IMDb");
        String[] newUserGenFilesPath = tbTest.RunBatch();
        
        for(int i=0; i<newUserGenFilesPath.length; i++) {
            GenByFile(newUserGenFilesPath[i], _sqlhpUser);
        }
        
        return _newLoadUserIds;
    }
    
    /***
     * 初始化导入新用户在数据库用户表中的起始位�
     */
    private void InitStartPoint() {
        try {
            _newUserStartPoint = _newUserStartPoint + _sqlhpUser.QueryAllTables().length + 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private void GenByFile(String newUserFilePath, MySQLHelper sqlhp) {
        
        DataLoader dl = new DataLoader(newUserFilePath, sqlhp);
        //DataLoader dl = new DataLoader("data/ProductEnv/newUsers/newUser_exp_" + userExpIndex + "_" + userDBId + ".csv", sqlhp);
        
        try {
            dl.CSVToMySQL("", "userId_" + _newUserStartPoint);
            _newLoadUserIds.add(_newUserStartPoint);
            _newUserStartPoint ++;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }
    
}
