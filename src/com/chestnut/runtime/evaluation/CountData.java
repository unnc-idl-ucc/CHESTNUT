package com.chestnut.runtime.evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountData {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            BufferedReader IBUBSBload = new BufferedReader(new FileReader("data/peng/SB_Count_Pre_50.csv"));
            
            BufferedWriter CountFileWriter = new BufferedWriter(new FileWriter("data/peng/result/SB_Count_Pre_50_counts.csv"));
            
            IBUBSBload.readLine();
            
            Map<String, String> CheckDataMapUxp, CheckDataMapUsf;
            
            String userLineHolder;
            String[] userLineSplit;
            
            
            int lineptr = 0;
            int sbptr = 3;
            while((userLineHolder = IBUBSBload.readLine())!=null) { // process a line
                lineptr ++;
                //sbptr ++;
                //System.out.println("Processing line " + lineptr);
                
                userLineSplit = userLineHolder.split(",");
                
                String writerLine = userLineSplit[0];
                
                String allItemsESV, allItemsESVSBUsf;
                if(sbptr==3) {
                    //sbptr = 0;
                    
                    int[] checkMapIndex = new int[1];
                    checkMapIndex[0] = 3;
                    String[] checkMapType = new String[1];
                    checkMapType[0] = "Uxp";
                    writerLine = writerLine + "," + CountsFromRange(userLineSplit, 1, checkMapIndex, checkMapType);
                    
                    /*
                    allItemsESV = userLineSplit[1];
                    
                    if(!allItemsESV.equals("Empty")) {// SB Uxp
                        String[] allItems = allItemsESV.split("\\|");
                        if(!(allItems.length<5)) {
                            
                            CheckDataMapUxp = ArrayToMap(userLineSplit[3].split("\\|"), "Uxp");
                            
                            int countsUxp = 0;
                            String countsUxpHolder = "";
                            for(int i=0; i<allItems.length; i++) {
                                if(CheckDataMapUxp.containsKey(allItems[i])) {
                                    countsUxp++;
                                }
                                
                                
                                if((i+1)%5==0) {
                                    countsUxpHolder = countsUxpHolder + "|" + countsUxp;
                                    countsUxp = 0;
                                }
                            }
                            
                            writerLine = writerLine + "," + countsUxpHolder.substring(1);
                            
                        }else {
                            System.out.println("Serendipity found a NE exception from unexpectedness.");
                            writerLine = writerLine + ",NE,NE";
                        }
                        
                    }else {
                        System.out.println("Serendipity found a NA exception from unexpectedness.");
                        writerLine = writerLine + ",NA,NA";
                    }
                    */
                    
                    checkMapIndex[0] = 4;
                    checkMapType[0] = "Usf";
                    writerLine = writerLine + "," + CountsFromRange(userLineSplit, 2, checkMapIndex, checkMapType);
                    
                    /*
                    allItemsESVSBUsf = userLineSplit[2];
                    
                    if(!allItemsESVSBUsf.equals("Empty")) {// SB Usf
                        String[] allItems = allItemsESVSBUsf.split("\\|");
                        
                        if(!(allItems.length<5)) {
                            
                            CheckDataMapUsf = ArrayToMap(userLineSplit[4].split("\\|"), "Usf");
                            
                            int countsUsf = 0;
                            String countsUsfHolder = "";
                            for(int i=0; i<allItems.length; i++) {
                                if(CheckDataMapUsf.containsKey(allItems[i])) {
                                    countsUsf++;
                                }
                                
                                
                                if((i+1)%5==0) {
                                    countsUsfHolder = countsUsfHolder + "|" + countsUsf;
                                    countsUsf = 0;
                                }
                            }
                            
                            writerLine = writerLine + "," + countsUsfHolder.substring(1);
                            
                        }else {
                            System.out.println("Serendipity found a NE exception from usefulness.");
                            writerLine = writerLine + ",NE";
                            System.out.println("WriterLine: " + writerLine);
                        }
                        
                    }else {
                        System.out.println("Serendipity found a NA exception from usefulness.");
                        writerLine = writerLine + ",NA";
                        System.out.println("WriterLine: " + writerLine);
                    }
                    */
                    
                }else {
                    
                    int[] checkMapIndex = new int[2];
                    checkMapIndex[0] = 2;
                    checkMapIndex[1] = 3;
                    String[] checkMapType = new String[2];
                    checkMapType[0] = "Uxp";
                    checkMapType[1] = "Usf";
                    writerLine = writerLine + "," + CountsFromRange(userLineSplit, 1, checkMapIndex, checkMapType);
                    
                    /*
                    allItemsESV = userLineSplit[1];
                    
                    if(!allItemsESV.equals("Empty")) {
                        String[] allItems = allItemsESV.split("\\|");
                        if(!(allItems.length<5)) {
                            
                            CheckDataMapUxp = ArrayToMap(userLineSplit[2].split("\\|"), "Uxp");
                            CheckDataMapUsf = ArrayToMap(userLineSplit[3].split("\\|"), "Usf");
                            
                            int IBcountsUxp = 0;
                            int IBcountsUsf = 0;
                            String IBcountsUxpHolder = "";
                            String IBcountsUsfHolder = "";
                            for(int i=0; i<allItems.length; i++) {
                                if(CheckDataMapUxp.containsKey(allItems[i])) {
                                    IBcountsUxp++;
                                }
                                
                                if(CheckDataMapUsf.containsKey(allItems[i])) {
                                    IBcountsUsf++;
                                }
                                
                                if((i+1)%5==0) {
                                    IBcountsUxpHolder = IBcountsUxpHolder + "|" + IBcountsUxp;
                                    IBcountsUsfHolder = IBcountsUsfHolder + "|" + IBcountsUsf;
                                    IBcountsUxp = 0;
                                    IBcountsUsf = 0;
                                }
                                
                                
                            }
                            
                            writerLine = writerLine + "," + IBcountsUxpHolder.substring(1) + "," + IBcountsUsfHolder.substring(1);
                            
                        }else {
                            writerLine = writerLine + ",NE,NE";
                        }
                        
                        
                    }else {
                        writerLine = writerLine + ",NA,NA";
                    }
                    */
                }
                
                CountFileWriter.write(writerLine + "\n");
                
                
            }
            
            
            
            IBUBSBload.close();
            CountFileWriter.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    /**
     * Counts data which is checked by several maps to find the same elements in a specific range.
     * @param dataSplit The split of a line of the raw data.
     * @param allDataRefIndex The index of the split data which is going to be compared with the map.
     * @param checkMapDataIndex The index of the split data which is going to build the map. There could be several maps to be used for checking.
     * @param checkMapType This parameter is used to determine the function selected to build the check map.
     * @return The result of counts recorded by ESV line.
     */
    private static String CountsFromRange(String[] dataSplit, int allDataRefIndex, int[] checkMapDataIndex, String[] checkMapType) {
        String resultLine = "";
        
        String allDataRef = dataSplit[allDataRefIndex];
        
        int checkMapAmount = checkMapDataIndex.length;
        if(!allDataRef.equals("Empty")) {
            String[] allItems = allDataRef.split("\\|");
            if(!(allItems.length<5)) {
                Map<Integer, Map<String, String>> checkMaps = new HashMap<Integer, Map<String, String>>();
                int[] counters = new int[checkMapAmount];
                String[] countsHolder = new String[checkMapAmount];
                
                for(int i=0; i<checkMapAmount; i++) {
                    checkMaps.put(i, ArrayToMap(dataSplit[checkMapDataIndex[i]].split("\\|"), checkMapType[i]));
                    counters[i] = 0;
                    countsHolder[i] = "";
                }
                
                for(int i=0; i<allItems.length; i++) {
                    for(int j=0; j<checkMapAmount; j++) {
                        if(checkMaps.get(j).containsKey(allItems[i])) {
                            counters[j]++;
                        }
                        
                        if((i+1)%5==0) {
                            countsHolder[j] = countsHolder[j] + "|" + counters[j];
                            counters[j] = 0;
                        }
                    }
                }
                
                for(int i=0; i<checkMapAmount; i++) {
                    resultLine = resultLine + "," + countsHolder[i].substring(1);
                }
                
                resultLine = resultLine.substring(1);
                
            }else {
                resultLine = "NE,NE";
            }
            
        }else {
            resultLine = "NA,NA";
        }
        
        return resultLine;
    }
    
    private static Map<String, String> ArrayToMap(String[] strArr, String type) {
        Map<String, String> result = new HashMap<String, String>();
        String recordHolder;
        if(type.equals("Uxp")) {
            for(int i=0; i<strArr.length; i++) {
                recordHolder = strArr[i];
                result.put(recordHolder, recordHolder);
            }
        }else {
            if(strArr[0].equals("Empty")) {
                result.put("NE", "NE");
            }else {
                for(int i=0; i<strArr.length; i++) {
                    recordHolder = strArr[i].substring(0, strArr[i].indexOf("_"));
                    result.put(recordHolder, recordHolder);
                }
            }
        }
        
        
        return result;
    }

}
