package com.chestnut.runtime.evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.ma.DataSession;

public class CalcMAE {

    public static void main(String[] args) {
        
        try {
            BufferedReader dataBR = new BufferedReader(new FileReader("data/0.7ps.csv"));
            BufferedWriter resultFile = new BufferedWriter(new FileWriter("data/eval/SBMAE_PS0.7.csv"));
            
            dataBR.readLine();
            String dataLineHolder, accuracyDataHolder;
            String[] dataLineSplit;
            String[] accuracyDataHandlerFields = new String[3];
            accuracyDataHandlerFields[0] = "ItemId";
            accuracyDataHandlerFields[1] = "PredRt";
            accuracyDataHandlerFields[2] = "RealRt";
            
            Map<String, Double> errSumHandler = new HashMap<String, Double>();
            Map<String, Integer> errSumCountHandler = new HashMap<String, Integer>();
            
            while((dataLineHolder = dataBR.readLine()) != null) {
                dataLineSplit = dataLineHolder.split(",");
                
                accuracyDataHolder = dataLineSplit[5];
                
                if(accuracyDataHolder.equals("No useful prediction found")) {
                    
                }else {
                    DataSession accuracyDataHandler = new DataSession(dataLineSplit[0] + "_accuracy_use_data");
                    accuracyDataHandler.BuildFields(accuracyDataHandlerFields, "ItemId");
                    
                    String[] accuracyDataSplit = accuracyDataHolder.split("\\|");
                    String accuracyItemData;
                    for(int i=0; i<accuracyDataSplit.length; i++) {
                        accuracyItemData = accuracyDataSplit[i];
                        String[] accuracyItemSplit = accuracyItemData.split("_");
                        accuracyDataHandler.SetARow(accuracyItemSplit);
                    }
                    
                    String[] sortedItemId = accuracyDataHandler.GetColumnFirstsDESCByBST(accuracyDataHandler.dataRecordSize, "PredRt", "ItemId");
                    
                    double sumSubHandler = 0.0;
                    double realRtHandler, predRtHandler;
                    double maeValueHolder;
                    for(int i=0; i<sortedItemId.length; i++) {
                        realRtHandler = Double.valueOf(accuracyDataHandler.GetARecordByIndex("RealRt", accuracyDataHandler.GetAPrimeRecordIndex(sortedItemId[i])-1));
                        predRtHandler = Double.valueOf(accuracyDataHandler.GetARecordByIndex("PredRt", accuracyDataHandler.GetAPrimeRecordIndex(sortedItemId[i])-1));
                        sumSubHandler = sumSubHandler + Math.abs(predRtHandler - realRtHandler);
                        if((i+1)%5 == 0) {
                            maeValueHolder = sumSubHandler/(i+1);
                            if(errSumHandler.containsKey(String.valueOf(i+1))) {
                                Double maeUpdateHandler = errSumHandler.get(String.valueOf(i+1));
                                int maeCountUpdateHandler = errSumCountHandler.get(String.valueOf(i+1));
                                
                                maeUpdateHandler = maeUpdateHandler + maeValueHolder;
                                errSumHandler.put(String.valueOf(i+1), maeUpdateHandler);
                                
                                maeCountUpdateHandler ++;
                                errSumCountHandler.put(String.valueOf(i+1), maeCountUpdateHandler);
                                
                            }else {
                                errSumHandler.put(String.valueOf(i+1), maeValueHolder);
                                errSumCountHandler.put(String.valueOf(i+1), 1);
                            }
                        }
                    }
                    
                }
                
                
                
                
            }
            
            resultFile.write("sub,counts,MAE" + "\n");
            
            String rangeHandler;
            Double rangeMAE;
            for(int i=0; i<errSumHandler.size(); i++) {
                rangeHandler = String.valueOf((i+1)*5);
                rangeMAE = errSumHandler.get(rangeHandler)/errSumCountHandler.get(rangeHandler);
                resultFile.write(((i+1)*5) + "," + errSumCountHandler.get(rangeHandler) + "," + rangeMAE + "\n");
                
            }
            
            
            
            System.out.println("Finished!");
            dataBR.close();
            resultFile.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
