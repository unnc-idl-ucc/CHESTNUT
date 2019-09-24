package com.chestnut.runtime.dal.ma;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DataSession {

	public Map<String, Map<String, String>> _dataRecordExsSession;//Be careful _dataRecordExsSession is not guaranteed to hold a full data set when created from original data set.
	private Map<String, List<String>> _dataRecordMidSession;
	private Map<String, Integer> _dataPrimeKey;
	
	protected Map<String, Integer> _fieldsIndex;
	private String[] _fields;
	protected String _primeField;
	private List<Integer> _sortedListIndex;
	
	public String sessionName;
	public int fieldSize, dataRecordSize;
	
	public DataSession(String sessionName){
		_dataPrimeKey = new HashMap<String, Integer>();
		_dataRecordExsSession = new HashMap<String, Map<String, String>>();
		_dataRecordMidSession = new HashMap<String, List<String>>();
		_fieldsIndex = new HashMap<String, Integer>();
		this.sessionName = sessionName;
		fieldSize = 0;
		dataRecordSize = 0;
	}
	
	public void LoadData(Map<String, Map<String, String>> dataRecordSession, 
						 Map<String, String[]> dataRecordMidSession, 
						 Map<String, Integer> dataPrimeKey,
						 Map<String, Integer> fieldsIndex,
						 String[] fields,
						 int dataRecordSize){
		
		_dataRecordExsSession = dataRecordSession;
		_dataPrimeKey = dataPrimeKey;
		_fieldsIndex = fieldsIndex;
		_fields = fields;
		fieldSize = _dataRecordExsSession.size();
		this.dataRecordSize = dataRecordSize;
		
		for(int i=0; i<_fields.length; i++){
			_dataRecordMidSession.put(_fields[i], ArrayToList(dataRecordMidSession.get(_fields[i])));
		}
	}
	
	public boolean ContainsRecord(String fieldKey, String recordKey){
		return _dataRecordExsSession.get(fieldKey).containsKey(recordKey);
	}
	
	public int GetAPrimeRecordIndex(String primeRecordKey){//not solve the problem of record duplication problem.
		return _dataPrimeKey.get(primeRecordKey);
	}
	
	public int GetARecordWithBiggestValueIndex(String recordKey, String recordField, String valueField){
	    int indexHolder = -1;
	    double valueHolder;
	    double biggestValue = 0.0;
	    for(int i=0; i<this.dataRecordSize; i++){
	        if(_dataRecordMidSession.get(recordField).get(i).equals(recordKey)){
	            valueHolder = Double.valueOf(_dataRecordMidSession.get(valueField).get(i));
	            if(valueHolder>=biggestValue){
	                biggestValue = valueHolder;
	                indexHolder = i;
	            }
	        }
	    }
	    return indexHolder;
	}
	
	public Map<Integer, String> GetARecordAllIndex(String recordKey, String recordField) {
	    Map<Integer, String> indexHolder = new HashMap<Integer, String>();
        for(int i=0; i<this.dataRecordSize; i++){
            if(_dataRecordMidSession.get(recordField).get(i).equals(recordKey)){
                indexHolder.put(i, recordKey);
            }
        }
        return indexHolder;
	}
	
	public String GetARecordByIndex(String fieldKey, int recordIndex){
		return _dataRecordMidSession.get(fieldKey).get(recordIndex);
	}
	
	public String[] GetFields(){
		return _fields;
	}
	
	public String GetPrimeField(){
	    return _primeField;
	}
	
	public int GetFiledsIndex(String FieldName){
		return _fieldsIndex.get(FieldName);
	}
	
	public String[] GetARow(int rowIndex){
		String[] rowRecords = new String[fieldSize];
		for(int i=0; i<_fields.length; i++){
			rowRecords[i] = _dataRecordMidSession.get(_fields[i]).get(rowIndex);
		}
		return rowRecords;
	}
	
	public String[] GetAColum(String fieldName){
	    String[] columRecords = new String[dataRecordSize];
	    for(int i=0; i<dataRecordSize; i++){
	        columRecords[i] = _dataRecordMidSession.get(fieldName).get(i);
	    }
	    return columRecords;
	}
	
	public boolean isIdenticalValuedSession(String valueField){
	    if(_dataRecordExsSession.get(valueField).size()>1){
	        return false;
	    }else{
	        return true;
	    }
	}
	
	/**
	 * Build fields for the data session.
	 * @param fields An array hold all the fields name with the order of the array elements.
	 * @param fieldRecordsSize The scale of the records the data session will contain.
	 */
	public void BuildFields(String[] fields, String primeField){
		_fields = fields;
		_primeField = primeField;
		fieldSize = fields.length;
		
		for(int i=0; i<_fields.length; i++){
			_fieldsIndex.put(_fields[i], i);
			_dataRecordExsSession.put(_fields[i], new HashMap<String, String>());
			_dataRecordMidSession.put(_fields[i], new ArrayList<String>());
		}
	}
	
	/**
	 * Set a row of records to the bottom of data session
	 * @param rowRecords An array hold all record of a row in the data session.
	 */
	public void SetARow(String[] rowRecords){
		_dataPrimeKey.put(rowRecords[_fieldsIndex.get(_primeField)], dataRecordSize + 1);
		for(int i=0; i<rowRecords.length; i++){
			_dataRecordExsSession.get(_fields[i]).put(rowRecords[i], rowRecords[i]);
			_dataRecordMidSession.get(_fields[i]).add(rowRecords[i]);
		}
		dataRecordSize++;
	}
	
	public void SetAColumn(List<String> columnRecords, String columnFieldName) {
	    if(columnRecords.size()>dataRecordSize) {
	        System.out.println("[WARN]: DataSession is trying to set a column with records out of the bound of the DataSession size.");
	    }else {
    	    // Update the fields first
    	    int newFieldIndex = _fields.length;
    	    String[] newFields = new String[newFieldIndex + 1];
    	    for(int i=0; i<_fields.length; i++) {
    	        newFields[i] = _fields[i];
    	    }
    	    newFields[newFieldIndex] = columnFieldName;
    	    _fields = newFields;
    	    _fieldsIndex.put(columnFieldName, newFieldIndex);
    	    
    	    // Update the data map then
    	    List<String> newColumnMidList = new ArrayList<String>();
    	    Map<String, String> newColumnExsMap = new HashMap<String, String>();
    	    for(int i=0; i<columnRecords.size(); i++) {
    	        String recordHolder = columnRecords.get(i);
    	        newColumnMidList.add(recordHolder);
    	        newColumnExsMap.put(recordHolder, recordHolder);
    	    }
    	    _dataRecordExsSession.put(columnFieldName, newColumnExsMap);
    	    _dataRecordMidSession.put(columnFieldName, newColumnMidList);
	    }
	}
	
	public void SetARecords(String primeKey, String fieldName, String value) {
	    if(!_dataPrimeKey.containsKey(primeKey)) {
	        _dataRecordExsSession.get(_primeField).put(primeKey, primeKey);
	        int primeIndex = _dataRecordMidSession.get(_primeField).size();
	        _dataRecordMidSession.get(_primeField).add(primeKey);
	        _dataPrimeKey.put(primeKey, primeIndex);
	        
	    }
	    
        _dataRecordExsSession.get(fieldName).put(value, value);
        _dataRecordMidSession.get(fieldName).add(value); // Not safe if same record added two times;
	    
	}
	
	private void SortByFieldWithAmount(String fieldName, int sortAmount) {
	    _sortedListIndex = new ArrayList<Integer>();
	    List<String> valueList = _dataRecordMidSession.get(fieldName);
	    BinaryNode root = new BinaryNode(Double.valueOf(valueList.get(0)), 0, null, null);
        BinarySearchTree bst = new BinarySearchTree(root, sortAmount, "DESC");
        //System.out.println("[Tracing] DataSession.SortByField(), valueList.size() is " + valueList.size());
	    for(int i=1; i<valueList.size(); i++) {
	        bst.AddNewNode(Double.valueOf(valueList.get(i)), i);
	    }
	    //System.out.println("[Tracing] DataSession.SortByField(), bst size is " + bst.GetTreeSize());
	    _sortedListIndex = bst.GetSortedArr();
	    //System.out.println("[Tracing] DataSession.SortByField(), done!");
	}
	
	public String[] GetColumnFirstsDESCByBST(int Amount, String sortFieldName, String recordFieldName) {
	    SortByFieldWithAmount(sortFieldName, Amount);
	    String selectedColumn[] = new String[Amount];
	    int listPtrBottom = _sortedListIndex.size()-1;
	    for(int i=0; i<Amount; i++) {
	        selectedColumn[i] = _dataRecordMidSession.get(recordFieldName).get(_sortedListIndex.get(listPtrBottom - i));
	        //System.out.println("[Tracing] DataSession.GetColumnFirstsDESC(), selectedColumn[" + i + "] is " + selectedColumn[i]);
	    }
	    //System.out.println("[Tracing] DataSession.GetColumnFirstsDESC(), selectedColumn size is " + selectedColumn.length);
	    return selectedColumn;
	}
	
	public String[] GetColumnFirstDESCByItr(int Amount, String sortFieldName, String recordFieldName) {
	    if(Amount>dataRecordSize) {
	        Amount = dataRecordSize;
	    }
	    
	    String selectedColumn[] = new String[Amount];
	    Double comparedColumn[] = new Double[Amount], valueHolder;
	    
	    for(int i=0; i<Amount; i++) {
	        comparedColumn[i] = 0.0;
	    }
	    
	    int suitIndex = 0;
        for(int i=0; i<dataRecordSize; i++) {
            valueHolder = Double.valueOf(_dataRecordMidSession.get(sortFieldName).get(i));
            suitIndex = FindSmallestReplacable(valueHolder, comparedColumn);
            if(suitIndex!=-1) {
                comparedColumn[suitIndex] = valueHolder;
                selectedColumn[suitIndex] = _dataRecordMidSession.get(recordFieldName).get(i);
            }
            
        }
        //System.out.println("[Tracing] DataSession.GetColumnFirstsDESC(), selectedColumn size is " + selectedColumn.length);
        return selectedColumn;
	}
	
	private int FindSmallestReplacable(Double value, Double[] valuesPool) {
	    int suitIndex = -1;
	    Double suitReplaceLocValue = value;
	    for(int i=0; i<valuesPool.length; i++) {
	        if(value>valuesPool[i] && suitReplaceLocValue>valuesPool[i]) {
	            suitIndex = i;
	            suitReplaceLocValue = valuesPool[i];
	        }else if(value==valuesPool[i]) {
	            Random tfr = new Random();
	            if(tfr.nextBoolean()) {
	                suitIndex = i;
	                suitReplaceLocValue = valuesPool[i];
	            }
	        }
	    }
	    return suitIndex;
	}
	
	public void RemoveARecordByIndex(int recordIndex){//not completed
		for(int i=0; i<_fields.length; i++){
			_dataRecordMidSession.get(_fields[i]).remove(recordIndex);
		}
		dataRecordSize--;
	}
	
	public void ExportToCSV(String filePath, String fileName) {
	    try {
            BufferedWriter csvHandler = new BufferedWriter(new FileWriter(filePath + "/" + fileName + ".csv"));
            csvHandler.write(_fields[0]);
            for(int i = 1; i < _fields.length; i++) {
                csvHandler.write("," + _fields[i]);
            }
            csvHandler.write("\n");
            
            for(int i = 0; i < dataRecordSize; i++) {
                String[] rowHolder = GetARow(i);
                csvHandler.write(rowHolder[0]);
                for(int j = 1; j < rowHolder.length; j++) {
                    csvHandler.write("," + rowHolder[j]);
                }
                csvHandler.write("\n");
            }
            
            csvHandler.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	public void ExportToESV(String filePath, String fileName, String separatePattern) {
        try {
            BufferedWriter esvHandler = new BufferedWriter(new FileWriter(filePath + "/" + fileName + ".csv"));
            esvHandler.write(_fields[0]);
            for(int i = 1; i < _fields.length; i++) {
                esvHandler.write(separatePattern + _fields[i]);
            }
            esvHandler.write("\n");
            
            for(int i = 0; i < dataRecordSize; i++) {
                String[] rowHolder = GetARow(i);
                esvHandler.write(rowHolder[0]);
                for(int j = 1; j < rowHolder.length; j++) {
                    esvHandler.write(separatePattern + rowHolder[j]);
                }
                if(i <= dataRecordSize-1) {
                    esvHandler.write("\n");
                }
                
            }
            
            esvHandler.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	public void ImportFromCSV(String filePath, String[] fields, String primaryKey) {
	    try {
	        BufferedReader csvReader = new BufferedReader(new FileReader(filePath));
            if(fields == null) {
                BuildFields(csvReader.readLine().split(","), primaryKey);
            }else {
                BuildFields(fields, primaryKey);
            }
            
            String csvLine = "";
            while ((csvLine = csvReader.readLine()) != null) {
                
                csvLine = csvLine.replaceAll(",\"", ",\"\"");
                csvLine = csvLine.replaceAll("\",", "\"\",");
                String[] recordSplitByQuota = csvLine.split(",\"|\",", -1);
                List<String> ElementsHandler = new ArrayList<String>();
                for(int i=0; i<recordSplitByQuota.length; i++) {
                    if(recordSplitByQuota[i].contains("\"")) {
                        ElementsHandler.add(recordSplitByQuota[i].replaceAll("\"", ""));
                    }else {
                        String[] recordSplitByComma = recordSplitByQuota[i].split(",", -1);
                        for(int j=0; j<recordSplitByComma.length; j++) {
                            if(recordSplitByComma[j].equals("")) {
                                ElementsHandler.add("NA");
                            }else {
                                ElementsHandler.add(recordSplitByComma[j]);
                            }
                        }
                    }
                }
                
                //System.out.println("[DataSession.ImportFromCSV]: rowSize = " + ElementsHandler.size());
                String[] Elements = new String[ElementsHandler.size()];
                for(int i=0; i<ElementsHandler.size(); i++) {
                    Elements[i] = ElementsHandler.get(i);
                }
                
                SetARow(Elements);
            }
            
            csvReader.close();
            
            
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	private List<String> ArrayToList(String[] inArray){
		List<String> outList = new ArrayList<String>();
		for(int i=0; i<inArray.length; i++){
			outList.add(inArray[i]);
		}
		return outList;
	}
}
