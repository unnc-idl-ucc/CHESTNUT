package com.chestnut.runtime.dal.log;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.ma.DataSession;

public class LogSession extends DataSession{

    private String[] _logRowRecords;
    private String _logFileDir;
    
    public LogSession(String sessionName, String logFileDir) {
        super(sessionName);
        _logFileDir = logFileDir;
    }
    
    public void BuildLogFields(String[] fields, String primeField) {
        addARowToLogFile(_logFileDir, fields, false);
        _logRowRecords = new String[fields.length];
        super.BuildFields(fields, primeField);
    }
    
    public void SetRowKeyValue(String rowKeyValue, String fieldName) {
        _logRowRecords[super._fieldsIndex.get(fieldName)] = rowKeyValue;
    }
    
    public void SetRowKeyValueList(List<String> rowKeyValueList, String fieldName) {
        String compressList = "Empty";
        if(!rowKeyValueList.isEmpty()) {
            compressList = rowKeyValueList.get(0);
            for(int i=1; i<rowKeyValueList.size(); i++) {
                compressList = compressList + "|" + rowKeyValueList.get(i);
            }
        }
        
        _logRowRecords[super._fieldsIndex.get(fieldName)] = compressList;
    }
    
    @SuppressWarnings("rawtypes")
    public void SetRowKeyValueMap(Map rowKeyValueMap, String fieldName) {
        String compressMap = "";
        for(Object rowKeyValueElement : rowKeyValueMap.entrySet()) {
            compressMap = compressMap + "|" + String.valueOf(((Map.Entry)rowKeyValueElement).getKey()) + "_" + String.valueOf(((Map.Entry)rowKeyValueElement).getValue());
        }
        compressMap = compressMap.substring(1);
        _logRowRecords[super._fieldsIndex.get(fieldName)] = compressMap;
    }
    
    public void SetARow() {
        addARowToLogFile(_logFileDir, _logRowRecords, true);
        //super.SetARow(_logRowRecords);
    }
    
    private void addARowToLogFile(String fileDir, String[] rowRecords, boolean ifAdd) {
        String lineHandler = rowRecords[0];
        for(int i=1; i<rowRecords.length; i++) {
            lineHandler = lineHandler + "," + rowRecords[i];
        }
        try {
            FileWriter rowWriter = new FileWriter(fileDir, ifAdd);
            rowWriter.write(lineHandler + "\n");
            rowWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
