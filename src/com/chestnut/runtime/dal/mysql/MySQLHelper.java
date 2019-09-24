package com.chestnut.runtime.dal.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MySQLHelper {

	private String _serverUrl;
	private String _port;
	private String _dbName;
	private Statement _dbStatement;
	private Connection _dbConn;
	private String _dbUserId, _dbUserPwd;
	private HashMap<String, Integer> _dbTableMap;
	
	public MySQLHelper(String serverUrl, String port, String dbName, String dbUserId, String dbUserPwd){
		
		_serverUrl = serverUrl;
		_port = port;
		_dbName = dbName;
		_dbUserId = dbUserId;
		_dbUserPwd = dbUserPwd;
		
		try {
			_dbConn = DriverManager.getConnection("jdbc:mysql://" + _serverUrl + ":" 
																  + _port + "/"
																  + _dbName + "?verifyServerCertificate=false&useSSL=false&useOldAliasMetadataBehavior=true&serverTimezone=GMT",
																  dbUserId, dbUserPwd);
			 
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
	}
	
	public void ChangeDB(String dbName) {
	    try {
	        CloseConnection();
            _dbConn = DriverManager.getConnection("jdbc:mysql://" + _serverUrl + ":" 
                                                                  + _port + "/"
                                                                  + dbName + "?verifyServerCertificate=false&useSSL=false&useOldAliasMetadataBehavior=true",
                                                                  _dbUserId, _dbUserPwd);
             
        } catch (SQLException e) {
            
            e.printStackTrace();
        }
	}
	
	/**
	 * Execute a sql statement and return true if execution successful, false if execution false.
	 * @param statement The sql statement in String type
	 * @return True if execution successful, false if execution false.
	 * @throws SQLException
	 */
	public boolean Execute(String statement) throws SQLException{
		_dbStatement = _dbConn.createStatement();
		return _dbStatement.execute(statement);
	}
	
	/**
	 * Execute a sql statement and return the query result.
	 * @param statement
	 * @return A ResultSet hold the query result.
	 * @throws SQLException
	 */
	public ResultSet ExecuteQuery(String statement) throws SQLException{
		_dbStatement = _dbConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		return _dbStatement.executeQuery(statement);
	}
	
	public void CloseStatement() throws SQLException{
		_dbStatement.close();
	}
	
	public void CloseConnection() throws SQLException{
		_dbConn.close();
	}
	
	public String[] QueryAllTables() throws SQLException {
	    ResultSet rst = ExecuteQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"" + _dbName + "\";");
	    _dbTableMap = new HashMap<String, Integer>();
	    List<String> tableNameHolder = new ArrayList<String>();
	    while(rst.next()) {
	        tableNameHolder.add(rst.getString(1));
	    }
	    String[] tableNames = new String[tableNameHolder.size()];
	    for(int i=0; i<tableNames.length; i++) {
	        _dbTableMap.put(tableNames[i], i);
	        tableNames[i] = tableNameHolder.get(i);
	    }
	    return tableNames;
	}
	
	public boolean ContainsTable(String tableName) {
	    boolean result = false;
	    if(_dbTableMap != null) {
	        result = _dbTableMap.containsKey(tableName);
	    }
	    return result;
	}
	
	public String GetDBName() {
	    return _dbName;
	}
	
}
