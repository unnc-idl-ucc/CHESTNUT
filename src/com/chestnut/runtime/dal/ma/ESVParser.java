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

public class ESVParser {

    private BufferedReader _esvBuffer;
    private Map<String, String> _esvHandler;
    
    
    public ESVParser(String esvDir) {
        try {
            _esvBuffer = new BufferedReader(new FileReader(esvDir));
            _esvHandler = new HashMap<String, String>();
            
            String temp;
            
            int lineCounts = 1;
            
            while((temp=_esvBuffer.readLine())!=null) {
                //_esvHandler.put(String.valueOf(lineCounts), temp.substring(temp.indexOf(",")+1));
                _esvHandler.put(temp.substring(0, temp.indexOf(",")), temp.substring(temp.indexOf(",")+1));
                lineCounts++;
            }
            System.out.println("[Tracing] ESVParser: constructed, size of map is " + _esvHandler.size() + ", line counts is " + lineCounts);
            
        } catch (IOException e) {
            System.out.println("[WARN] Some thing wrong when construct ESVParser");
            e.printStackTrace();
        }
    }
    
    public String GetALineByPrimeKey(String primeKey) {
        return _esvHandler.get(primeKey);
    }
    
    public List<List<String>> GetARecordSet(String primeKey) {
        try{
            if(_esvHandler.containsKey(primeKey)) {
                String[] recordsHolder = _esvHandler.get(primeKey).split(",");
                //System.out.println("[Tracing] ESVParser.GetARecordSet: lenth of recordsHolder is " + recordsHolder.length);
                List<List<String>> recordsSet = new ArrayList<List<String>>();
                List<String> recordsColumn;
                for(int i=0; i<recordsHolder.length; i++) {
                    recordsColumn = new ArrayList<String>();
                    String[] tempRecords = recordsHolder[i].split("\\|");
                    for(int j=0; j<tempRecords.length; j++) {
                        recordsColumn.add(tempRecords[j]);
                    }
                    recordsSet.add(recordsColumn);
                }
                return recordsSet;
            }else {
                return null;
            }
        }catch(NullPointerException e) {
            System.out.println("[WARN] ESVParser.GetArecordSet: primeKey " + primeKey + " value is " + _esvHandler.get(primeKey));
            return null;
        }
        
        
    }
    
    public void UniformFormat(String esvDir) {
        
    	Map<String, String[]> esvHandler = new HashMap<String, String[]>();
        try {
            BufferedReader esvBuffer = new BufferedReader(new FileReader(esvDir));
            
        	BufferedWriter bw = new BufferedWriter(new FileWriter("data/ProductEnv/HelperSet/k_nearest_comb.csv"));
        	
            String temp, primeKeyHolder, recordsHolder;
            
            int counter = 1;
            while((temp=esvBuffer.readLine())!=null) {
            	
                
                counter++;
                
                primeKeyHolder = temp.substring(0, temp.indexOf(","));
                recordsHolder = temp.substring(temp.indexOf(",")+1);
                
                String[] recordsVarHolder = recordsHolder.split(",");
                
                if(esvHandler.containsKey(primeKeyHolder)) {
                	String[] varites = new String[2];
                	varites = esvHandler.get(primeKeyHolder);
                	for(int i=0; i<varites.length; i++) {
                		varites[i] = varites[i] + "|" + recordsVarHolder[i];
                	}
                	esvHandler.put(primeKeyHolder, varites);
                }else {
                	esvHandler.put(primeKeyHolder, recordsVarHolder);
                }   
            }
            System.out.println("[Tracing] read line " + counter);
            
            for(Map.Entry<String, String[]> e : esvHandler.entrySet()) {
            	
            	String[] varitesWriteHolder = e.getValue();
            	String varitesWriteRecord = varitesWriteHolder[0];
            	
            	for(int i=1; i<varitesWriteHolder.length; i++) {
            		varitesWriteRecord = varitesWriteRecord + "," + varitesWriteHolder[i];
            	}
            	
            	bw.write(e.getKey() + "," + varitesWriteRecord + "\n");
            }
            
            bw.close();
            esvBuffer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
