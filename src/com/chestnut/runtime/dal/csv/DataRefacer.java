package com.chestnut.runtime.dal.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DataRefacer {
	
	private String _dataFileRootDir, _dataFileName, _dataResultFileDir;
	private BufferedReader _InputData;
	private BufferedWriter _OutData;
	
	public DataRefacer(String dataFileDir){
		_dataFileRootDir = dataFileDir.substring(0, dataFileDir.lastIndexOf('/')+1);
		_dataFileName = dataFileDir.substring(dataFileDir.lastIndexOf('/')+1, dataFileDir.lastIndexOf('.'));
		
		try {
			_InputData = new BufferedReader(new FileReader(dataFileDir));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void AddFields(String[] fields) throws IOException{
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_addField.csv";
		
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		_OutData.write(RebuildRecord(fields, ",") + "\n");
		String temp;
		int jobCounter = 0;
		while((temp = _InputData.readLine())!=null){
			jobCounter ++;
			System.out.println("[Tracing] Processing job: " + jobCounter);
			_OutData.write(temp + "\n");
		}
		_OutData.close();
	}
	
	public void AddID(String fieldName, boolean hasFields) throws IOException{
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_addId.csv";
		
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		String temp;
		
		if(hasFields){
            temp = _InputData.readLine();
            _OutData.write(fieldName + "Id," + temp + "\n");
        }
		
		int jobCounter = 0;
		while((temp = _InputData.readLine())!=null){
			jobCounter ++;
			System.out.println("[Tracing] Processing job: " + jobCounter);
			_OutData.write(jobCounter + "," + temp + "\n");
		}
		_OutData.close();
	}
	
	public void ReplaceAllBy(String oldStr, String newStr, int fieldIndex, boolean hasFields) throws IOException{
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_rp.csv";
		
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		
		String temp;
		String[] tempSplit;
		
		if(hasFields){
			temp = _InputData.readLine();
			_OutData.write(temp + "\n");
		}
		
		int jobCounter = 0;
		while((temp = _InputData.readLine())!=null){
		    jobCounter ++;
            System.out.println("[Tracing] Processing job: " + jobCounter);
            
		    tempSplit = temp.split(",");
			for(int i=0; i<tempSplit.length; i++){
			    if(i!=tempSplit.length-1){
			        if(i!=fieldIndex){
			            _OutData.write(tempSplit[i] + ",");
			        }else{
			            _OutData.write(tempSplit[i].replaceAll(oldStr, newStr) + ",");
			        }
			    }else{
			        if(i!=fieldIndex){
                        _OutData.write(tempSplit[i] + "\n");
                    }else{
                        _OutData.write(tempSplit[i].replaceAll(oldStr, newStr) + "\n");
                    }
			    }
			    
			}
		}
		_OutData.close();
	}
	
	public void RemoveQoute(int fieldIndex, boolean hasFields) throws IOException{
	    _dataResultFileDir = _dataFileRootDir + _dataFileName + "_rmQoute.csv";
        
        _OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
        
        String temp;
        String[] tempSplit;
        
        if(hasFields){
            temp = _InputData.readLine();
            _OutData.write(temp + "\n");
        }
        
        int jobCounter = 0;
        while((temp = _InputData.readLine())!=null){
            jobCounter ++;
            System.out.println("[Tracing] Processing job: " + jobCounter);
            
            tempSplit = temp.split(",");
            for(int i=0; i<tempSplit.length; i++){
                if(i!=tempSplit.length-1){
                    if(i!=fieldIndex){
                        _OutData.write(tempSplit[i] + ",");
                    }else{
                        if(tempSplit[i].contains("\"")){
                            _OutData.write(tempSplit[i].replaceAll("\"", "").replaceAll(",", "#") + ",");
                        }else{
                            _OutData.write(tempSplit[i] + ",");
                        }
                        
                    }
                }else{
                    if(i!=fieldIndex){
                        _OutData.write(tempSplit[i] + "\n");
                    }else{
                        if(tempSplit[i].contains("\"")){
                            _OutData.write(tempSplit[i].replaceAll("\"", "").replaceAll(",", "#") + "\n");
                        }else{
                            _OutData.write(tempSplit[i] + "\n");
                        }
                    }
                }
                
            }
        }
        _OutData.close();
	}
	
	private String RebuildRecord(String[] strs, String separator){
		String record = strs[0];
		for(int i=1; i<strs.length; i++){
			record = record + separator + strs[i];
		}
		return record;
	}
}
