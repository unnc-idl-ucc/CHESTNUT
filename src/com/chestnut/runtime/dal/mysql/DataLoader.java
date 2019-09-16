package com.chestnut.runtime.dal.mysql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.ma.DataConstructor;
import com.chestnut.runtime.dal.ma.DataSession;

public class DataLoader {

	private BufferedReader _SourceData;
	private String _fileName;
	private StringBuilder _sqlStatement, _fieldsUnit;
	private MySQLHelper _sqlHelper;
	private Map<String, String> _groups;
	
	
	public DataLoader(String dataFileDir, MySQLHelper mysqlHelper){
		_sqlHelper = mysqlHelper;
		try {
			_SourceData = new BufferedReader(new FileReader(dataFileDir));
			_fileName = dataFileDir.substring(dataFileDir.lastIndexOf('/')+1 ,dataFileDir.lastIndexOf('.'));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public DataLoader(MySQLHelper mysqlHelper){
		_sqlHelper = mysqlHelper;
	}
	
	public String GetInputDataFileName() {
	    return _fileName;
	}
	
	/**
	 * Load data from csv file to MySQL database.
	 * @param tableType Value with "TEMPORARY" indicate that the table is created as temporary table or just value with empty string.
	 * @throws IOException
	 * @throws SQLException
	 */
	public void CSVToMySQL(String tableType, String tableName) throws IOException, SQLException{
	    System.out.println("[DataLoader.CSVToMySQL] Start loading csv with table name " + tableName + " to database.");
		String record = null;
		String[] fields = null;
		
		fields = _SourceData.readLine().split(",");//create table
		_fieldsUnit = BuildFieldUnit(fields);
		
		CreateTable(tableType, fields, tableName);
		
		int jobCounter = 0;
        while ((record = _SourceData.readLine()) != null){//insert data
        	System.out.println("\n======= Job " + (jobCounter+1) + " processing =======");
        	
        	record = record.replaceAll(",\"", ",\"\"");
        	record = record.replaceAll("\",", "\"\",");
        	String[] recordSplitByQuota = record.split(",\"|\",");
        	List<String> ElementsHandler = new ArrayList<String>();
        	for(int i=0; i<recordSplitByQuota.length; i++) {
        	    if(recordSplitByQuota[i].contains("\"")) {
        	        ElementsHandler.add(recordSplitByQuota[i].replaceAll("\"", ""));
        	    }else {
        	        String[] recordSplitByComma = recordSplitByQuota[i].split(",");
        	        for(int j=0; j<recordSplitByComma.length; j++) {
        	            ElementsHandler.add(recordSplitByComma[j]);
        	        }
        	    }
        	}
        	
        	String[] Elements = new String[ElementsHandler.size()];
        	for(int i=0; i<ElementsHandler.size(); i++) {
        	    Elements[i] = ElementsHandler.get(i);
        	}
            
            InsertARow(Elements, tableName);
            
            jobCounter++;
            System.out.println("[Tracing] Job " + jobCounter + " is done!");
        }
	}
	
	public void CSVToMySQLGroupByField(String tableType, String groupField) throws IOException, SQLException{
	    _groups = new HashMap<String, String>();
	    
	    String temp, tableName;
        String[] tempSplited, fields;
        
        int jobCounter = 0, fieldIndex = -1, tableFieldIndex = 0;
        
        
        temp=_SourceData.readLine();
        tempSplited = temp.split(",");
        fields = new String[tempSplited.length-1];
        
        for(int i=0; i<tempSplited.length; i++){
            if(tempSplited[i].equals(groupField)){
                fieldIndex = i;
            }else{
                fields[tableFieldIndex] = tempSplited[i];
                tableFieldIndex++;
            }
        }
        
        _fieldsUnit = BuildFieldUnit(fields);
        
        if(fieldIndex != -1){
            System.out.println("[Tracing] Grouping data by " + groupField + "...");
            while((temp=_SourceData.readLine())!= null){
                jobCounter ++;
                tempSplited = temp.split(",");
                tableName = groupField + "_" + tempSplited[fieldIndex];
                //System.out.println("[Tracing] Processing job: " + jobCounter + " at " + tempSplited[fieldIndex]);
                if(_groups.containsKey(tempSplited[fieldIndex])){
                    InsertARow(ArrayExceptIndex(tempSplited, fieldIndex), tableName);
                }else{
                    //System.out.println("[Tracing]InserDataAtLast is called.");
                    _groups.put(tempSplited[fieldIndex], tempSplited[fieldIndex]);
                    
                    CreateTable("",fields,tableName);
                }
                System.out.println("[Tracing] Job completed: " + jobCounter);
            }
        }else{
            System.out.println("[WARN] Grouping data by " + groupField + " failed, group field not exist!");
        }
        
	}
	
	/**
	 * Load data from an ESV type data file to MySQL database, each row of the data set will be create as a table in MySQL database.
	 * @param tableType Value with "TEMPORARY" indicate that the table is created as temporary table or just value with empty string.
	 * @param mainField The main field to identify the key of each row record.
	 * @param relations Give a map represent the relation ship between each field.
	 * @throws IOException
	 * @throws SQLException
	 */
	public void ESVToMySQL(String tableType, String mainField, String primeField) throws IOException, SQLException{
		String record = null;
		String[] fields = null;
		DataConstructor dcServe;
		DataSession dsServe;
		
		fields = _SourceData.readLine().split(",");//create table
		
		int jobCounter = 0;
        while ((record = _SourceData.readLine()) != null){//insert data
        	System.out.println("\n\n======= Job " + (jobCounter+1) + " processing =======");
            
        	dcServe = new DataConstructor(record, fields, mainField, primeField);
        	dsServe = dcServe.Constructed();
        	DRSToMySQL(dsServe, "");
        	
            jobCounter++;
            System.out.println("[Tracing] Job " + jobCounter + " is done!");
        }
	}
	
	public void DRSToMySQL(DataSession dataRecordSession, String tableType) throws SQLException{
	    _sqlHelper.QueryAllTables();
	    if(!_sqlHelper.ContainsTable(dataRecordSession.sessionName)) {
	        _fieldsUnit = BuildFieldUnit(dataRecordSession.GetFields());
	        CreateTable(tableType, dataRecordSession.GetFields(), dataRecordSession.sessionName);
	    }
		for(int i=0; i<dataRecordSession.dataRecordSize; i++){
			InsertARow(dataRecordSession.GetARow(i),dataRecordSession.sessionName);
		}
	}
	
	public DataSession MySQLToDRS(String tableName, String[] fields, String primeField) throws SQLException{
		_sqlStatement = new StringBuilder("SELECT * FROM " + tableName);
		//String[] fields = GetField(tableName);
		ResultSet rsTable = _sqlHelper.ExecuteQuery(_sqlStatement.toString());
		DataSession tableDataSession = new DataSession(tableName);
		tableDataSession.BuildFields(fields, primeField);
		//System.out.println("[Tracing] DataLoader.MySQLToDRS(), ResultSet size: " + tableSize);
		while(rsTable.next()){
			String[] rowHold = new String[fields.length];
			for(int i=0; i<fields.length; i++){
				//if(i == 0) rowHold[i] = String.valueOf(rsTable.getInt(fields[i]));
				rowHold[i] = rsTable.getString(fields[i]);
			}
			tableDataSession.SetARow(rowHold);
		}
		
		rsTable.close();
		_sqlHelper.CloseStatement();
		return tableDataSession;
	}
	
	/**
	 * Get a record from a table.
	 * @param tableName The name of the table contains query record.
	 * @param recordKey The identity id (Primary key) of the record.
	 * @return A ResultSet handling the record selected from MySQL database.
	 * @throws SQLException
	 */
	public ResultSet GetARecord(String tableName, int recordKey) throws SQLException{
		_sqlStatement = new StringBuilder("SELECT * FROM " + tableName + " where " + tableName + "_id=" + recordKey);
		return _sqlHelper.ExecuteQuery(_sqlStatement.toString());
	}
	
	/**
	 * Get the field of a table.
	 * @param tableName The name of the table.
	 * @return A ResultSet handling the field of the table.
	 * @throws SQLException
	 */
	public String[] GetField(String tableName, String schemaName) throws SQLException{
		_sqlStatement = new StringBuilder("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = '" + tableName + "' AND " + "table_schema = '" + schemaName + "'");
		ResultSet dataHandler = _sqlHelper.ExecuteQuery(_sqlStatement.toString());
		int fieldSize = GetResultSetSize(dataHandler);
		String[] fields = new String[fieldSize];
		for(int i=0; i<fieldSize; i++){
			dataHandler.next();
			//System.out.println("[Tracing] DataLoader.GetField(), Field name: " + dataHandler.getString(1));
			fields[i] = dataHandler.getString(1);
		}
		dataHandler.close();
		_sqlHelper.CloseStatement();
		return fields;
	}
	
	/**
	 * Get the record count number of the table.
	 * @param tableName The name of the table.
	 * @return An integer value of the count result.
	 * @throws NumberFormatException
	 * @throws SQLException
	 */
	public int CountRecords(String tableName) throws NumberFormatException, SQLException{
		_sqlStatement = new StringBuilder("select count(*) from " + tableName);
		ResultSet dataHandler = _sqlHelper.ExecuteQuery(_sqlStatement.toString());
		dataHandler.next();
		int counts = Integer.parseInt(dataHandler.getString(1));
		dataHandler.close();
		_sqlHelper.CloseStatement();
		return counts;
	}
	
	private void CreateTable(String tableType, String[] fields, String tableName) throws SQLException{
		_sqlStatement = new StringBuilder("CREATE " + tableType + "TABLE " + tableName + " (" + tableName + "_id INT UNSIGNED AUTO_INCREMENT,");
		for(int i = 0; i < fields.length; i++){
			_sqlStatement.append(fields[i] + " text,");
		}
		_sqlStatement.append("PRIMARY KEY (" + tableName + "_id))");
		//System.out.println("[Tracing] Create table statement built as: " + _sqlStatement.toString());
		_sqlHelper.Execute(_sqlStatement.toString());
		_sqlHelper.CloseStatement();
	}
	
	private void InsertARow(String[] records, String tableName) throws SQLException{
		_sqlStatement = new StringBuilder("INSERT INTO " + tableName + _fieldsUnit.toString() + " VALUES (\"");
        for(int i = 0; i < records.length; i++){
        	if(i != (records.length - 1)){
        		_sqlStatement.append(records[i] + "\",\"");
			}else{
				_sqlStatement.append(records[i] + "\")");
			}
        }
        //System.out.println("[Tracing] Insert values statement built as: " + _sqlStatement.toString());
        _sqlHelper.Execute(_sqlStatement.toString());
        _sqlHelper.CloseStatement();
	}
	
	private StringBuilder BuildFieldUnit(String[] fields){
		StringBuilder fieldsUnit = new StringBuilder("(");
		for(int i = 0; i < fields.length; i++){
			if(i != (fields.length - 1)){
				fieldsUnit.append(fields[i] + ",");
			}else{
				fieldsUnit.append(fields[i] + ")");
			}
		}
		return fieldsUnit;
	}
	
	private String[] ArrayExceptIndex(String[] inStrs, int indexCut){
	    String outStrs[] = new String[inStrs.length-1];
	    int outPtr = 0;
	    for(int i=0; i<inStrs.length; i++){
	        if(i!=indexCut){
	            outStrs[outPtr] = inStrs[i];
	            outPtr++;
	        }
	    }
	    return outStrs;
	}
	
	private int GetResultSetSize(ResultSet resultSet) throws SQLException{
		resultSet.last();
		int size = resultSet.getRow();
		//System.out.println("[Tracing] DataLoader.GetResultSetSize(), resultSet.getRow(): " + resultSet.getRow());
		resultSet.beforeFirst();
		return size;
	}
	
	/**
	 * Close the buffer steam used for loading files.
	 * @throws IOException
	 * @throws SQLException 
	 */
	public void CloseBuffer() throws IOException{
		_SourceData.close();
	}
	
	public void CloseConnection() throws SQLException{
		_sqlHelper.CloseConnection();
	}
}
