package com.chestnut.runtime.dal.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.ma.DataSession;

public class DataBuilder {

	private String _dataFileRootDir, _dataFileName, _dataTempFildDir, _dataResultFileDir;
	private BufferedReader _InputData;
	private BufferedReader _TempData;
	private BufferedWriter _OutData;
	private String[] _fieldsArray;
	private Map<String, Integer> _fields;
	private Map<String, String> _groups;
	
	
	/**
	 * Use to access csv data file and build new quick access data structure in csv type.
	 * @param dataFileDir the directory of the csv data file.
	 */
	public DataBuilder(String dataFileDir){
		_dataFileRootDir = dataFileDir.substring(0, dataFileDir.lastIndexOf('/')+1);
		_dataFileName = dataFileDir.substring(dataFileDir.lastIndexOf('/')+1, dataFileDir.lastIndexOf('.'));
		
		//System.out.println("[Tracing] data directory: " + dataFileDir);
		try {
			_InputData = new BufferedReader(new FileReader(dataFileDir));
			_fields = new HashMap<String, Integer>();
			_groups = new HashMap<String, String>();
			_fieldsArray = _InputData.readLine().split(",");
			MapFields();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	/**
	 * Group the data by a main field. Use really slow algorithm but cause very low complexity of the file system.
	 * @param fieldName The name of the main field.
	 * @throws Exception
	 */
	public void GroupDataBy(String fieldName) throws Exception{
		
		_dataTempFildDir = _dataFileRootDir + _dataFileName + "_" + fieldName + "_tmp.csv";
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_" + fieldName + "_s.csv";
		//#Check existence of result file and temporary file.
		//#Delect files if exist.
		File fileTemp=new File(_dataTempFildDir);
		File fileResult=new File(_dataResultFileDir);
		if(fileTemp.exists()) fileTemp.delete();
		if(fileResult.exists()) fileResult.delete();
		
		int fieldIndex = _fields.get(fieldName);
		//System.out.println("[Tracing] " + fieldName + " index: " + fieldIndex);
		
		String temp;
		String[] tempSplited;
		
		int jobCounter = 0, groupPtr = 0;
		
		BuildFields();
		
		System.out.println("[Tracing] Grouping data by " + fieldName + "...");
		while((temp=_InputData.readLine())!= null){
			jobCounter ++;
			tempSplited = temp.split(",");
			System.out.println("[Tracing]Processing job: " + jobCounter + " at " + tempSplited[fieldIndex]);
			if(_groups.containsKey(tempSplited[fieldIndex])){
				InsertData(tempSplited, _groups.get(tempSplited[fieldIndex]), fieldIndex);
			}else{
				//System.out.println("[Tracing]InserDataAtLast is called.");
				_groups.put(tempSplited[fieldIndex], String.valueOf(groupPtr));
				groupPtr++;
				InsertDataAtLast(temp);
			}
		}
		System.out.println("[Tracing] Grouping work completed!");
	}
	
	/**
	 * Group the data by main field. Use bucket algorithm to implement the efficiency of grouping process but increase the complexity of file system.
	 * @param fieldName The name of the main field.
	 * @throws Exception
	 */
	public void BucketGroupDataBy(String fieldName) throws Exception{
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_" + fieldName + "_s.csv";
		
		System.out.println("[Tracing] Cleaning temp folder");
		ClearTempFolder();
		
		int fieldIndex = _fields.get(fieldName);
		
		String temp;
		String[] tempSplited;
		
		int jobCounter = 0;
		
		System.out.println("[Tracing] Grouping data by " + fieldName + "...");
		while((temp=_InputData.readLine())!= null){
			jobCounter ++;
			tempSplited = temp.split(",");
			//System.out.println("[Tracing] Processing job: " + jobCounter + " at " + tempSplited[fieldIndex]);
			if(_groups.containsKey(tempSplited[fieldIndex])){
				UpdateBucket(tempSplited, _groups.get(tempSplited[fieldIndex]), fieldIndex);
			}else{
				//System.out.println("[Tracing]InserDataAtLast is called.");
				_groups.put(tempSplited[fieldIndex], tempSplited[fieldIndex]);
				CreateBucket(temp, _groups.get(tempSplited[fieldIndex]));
			}
			System.out.println("[Tracing] Job completed: " + jobCounter);
		}
		MergeBuckets();
		System.out.println("[Tracing] Grouping work completed!");
	}
	
	public static void GenFileFromDataSession(DataSession ds, String dataFileDir) throws IOException{
	    BufferedWriter OutData = new BufferedWriter(new FileWriter(dataFileDir));
	    OutData.write(RebuildRecord(ds.GetFields(), ",") + "\n");
	    for(int i=0; i<ds.dataRecordSize; i++){
	        OutData.write(RebuildRecord(ds.GetARow(i), ",") + "\n");
	    }
	    OutData.close();
	}
	
	public void ReplaceSeparator(String oldSeparator, String newSeparator) throws IOException{
		String temp, tempReplaced;
		_dataResultFileDir = _dataFileRootDir + _dataFileName + "_rp.csv";
		
		int jobCounter = 0;
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		while((temp = _InputData.readLine())!=null){
			jobCounter ++;
			System.out.println("[Tracing] Processing job: " + jobCounter);
			tempReplaced = temp.replaceAll(oldSeparator, newSeparator);
			_OutData.write(tempReplaced + "\n");
		}
		_OutData.close();
	}
	
	private void InsertDataAtLast(String record) throws Exception{
		FileOutputStream out = null;
		out = new FileOutputStream(_dataResultFileDir, true);
		//out.write("\n".getBytes());
		out.write(record.getBytes());
		out.write("\n".getBytes());
		out.close();
		UpdateTempData();
	}
	
	private void InsertData(String[] inValues, String recordIndex, int fieldIndex) throws Exception{
		//System.out.println("[Tracing]InsertData is called.");
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		_TempData = new BufferedReader(new FileReader(_dataTempFildDir));
		String temp, rebuildRecord;
		String[] tempSplited;
		int linePtr = 0;
		while((temp = _TempData.readLine())!=null){
			if(linePtr!=Integer.parseInt(recordIndex)){
				//System.out.println("[Tracing]Not target record. RecordIndex is:" + recordIndex);
				_OutData.write(temp + "\n");
				linePtr++;
			}else{
				//System.out.println("[Tracing]Target record.");
				tempSplited = temp.split(",");
				for(int i=0; i<tempSplited.length; i++){
					if(i!=fieldIndex){
						tempSplited[i] = tempSplited[i] + "#" + inValues[i];
					}
				}
				rebuildRecord = RebuildRecord(tempSplited, ",");
				//System.out.println("[Tracing]Rebuid record: " + rebuildRecord);
				_OutData.write(rebuildRecord + "\n");
				linePtr++;
			}
		}
		_OutData.close();
		_TempData.close();
		UpdateTempData();
	}
	
	private void CreateBucket(String record, String bucketId) throws Exception{
		FileOutputStream out = null;
		out = new FileOutputStream(_dataFileRootDir + "/temp/" + _dataFileName + "_" + bucketId + "_tmp.csv", true);
		//out.write("\n".getBytes());
		out.write(record.getBytes());
		out.close();
	}
	
	private void UpdateBucket(String[] inValues, String bucketId, int fieldIndex) throws Exception{
		_TempData = new BufferedReader(new FileReader(_dataFileRootDir + "/temp/" + _dataFileName + "_" + bucketId + "_tmp.csv"));
		String[] tempSplited = _TempData.readLine().split(",");
		_TempData.close();
		for(int i=0; i<tempSplited.length; i++){
			if(i!=fieldIndex){
				tempSplited[i] = tempSplited[i] + "#" + inValues[i];
			}
		}
		String rebuildRecord = RebuildRecord(tempSplited, ",");
		_OutData = new BufferedWriter(new FileWriter(_dataFileRootDir + "/temp/" + _dataFileName + "_" + bucketId + "_tmp.csv"));
		_OutData.write(rebuildRecord);
		_OutData.close();
	}
	
	private void MergeBuckets() throws Exception{
		_OutData = new BufferedWriter(new FileWriter(_dataResultFileDir));
		_OutData.write(RebuildRecord(_fieldsArray, ",") + "\n");
		for(Map.Entry<String, String> entry : _groups.entrySet()){
			_TempData = new BufferedReader(new FileReader(_dataFileRootDir + "/temp/" + _dataFileName + "_" + entry.getValue() + "_tmp.csv"));
			_OutData.write(_TempData.readLine() + "\n");
			_TempData.close();
		}
		_OutData.close();
	}
	
	private void BuildFields() throws Exception{
		FileOutputStream out = null;
		out = new FileOutputStream(_dataResultFileDir, true);
		//out.write("\n".getBytes());
		out.write(RebuildRecord(_fieldsArray, ",").getBytes());
		out.write("\n".getBytes());
		out.close();
	}
	
	private void MapFields(){
		int index = 0;
		for(String field : _fieldsArray){
			//System.out.println("[Tracing] Map field " + field + " with index: " + index);
			_fields.put(field, index);
			index ++;
		}
	}
	
	private static String RebuildRecord(String[] strs, String separator){
		String record = strs[0];
		for(int i=1; i<strs.length; i++){
			record = record + separator + strs[i];
		}
		return record;
	}
	
	private void UpdateTempData() throws Exception{
		FileInputStream in = null;
		FileOutputStream out = null;
		
		in = new FileInputStream(_dataResultFileDir);
		out = new FileOutputStream(_dataTempFildDir);
		
		FileChannel fcIn = in.getChannel();  
        FileChannel fcOut = out.getChannel();  
        fcIn.transferTo(0, fcIn.size(), fcOut);
        
		in.close();
		out.close();
		fcIn.close();
		fcOut.close();
	}
	
	/**
	 * Remove all stuff in the temp folder.
	 */
	public void ClearTempFolder(){
		FileOperator fo = new FileOperator();
		fo.CreateDirectory("data/temp");
		fo.DeleteAllFiles("data/temp");
	}
	
	/**
	 * Remember to close the buffer after the data access job.
	 * @throws IOException
	 */
	public void CloseAllBuffer() throws IOException{
		_InputData.close();
	}
}
