package com.chestnut.runtime.evaluation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.chestnut.runtime.dal.ma.DataSession;

public class CalcStdMAE {

    public static void main(String[] args) {
        
        System.out.println(CalcStdMAEFromFile("data/peng", "0", false) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "5_2", false) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "8_5", false) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "19_25", false) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "42_5", false) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "50", false) + "\n----------");
        /*
        System.out.println(CalcStdMAEFromFile("data/peng", "0", true) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "5_2", true) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "8_5", true) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "19_25", true) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "42_5", true) + "\n----------");
        System.out.println(CalcStdMAEFromFile("data/peng", "50", true) + "\n----------");
        */
    }
    
    private static Double CalcStdMAEFromFile(String fileDirectory, String fileName, boolean ifAllToOne) {
        try {
            BufferedReader dataBR = new BufferedReader(new FileReader(fileDirectory + "/" + fileName + ".csv"));
            
            MeanAbsoluteErrorEvaluator AllToOneMAEHandler = new MeanAbsoluteErrorEvaluator();
            
            // Handlers for supports
            Double avgSupports = 0.0, sumSupports = 0.0;
            
            List<DataSession> MAERatingsSessionList = GetMAERatingsSessionList(dataBR, ifAllToOne);
            for(int i=0; i<MAERatingsSessionList.size(); i++) {
                DataSession RatingsSessionHandler = MAERatingsSessionList.get(i);
                sumSupports = sumSupports + RatingsSessionHandler.dataRecordSize;
                AllToOneMAEHandler.AppendData(RatingsSessionHandler.sessionName, RatingsSessionHandler);
            }
            
            avgSupports = sumSupports/MAERatingsSessionList.size();
            System.out.println("Number of supports: " + avgSupports);
            
            DataSession MAEResultSession = AllToOneMAEHandler.Evaluate();
            String[] MAEResult = MAEResultSession.GetAColum("MAE");
            
            Double sumMAE = 0.0;
            for(int i=0; i<MAEResultSession.dataRecordSize; i++) {
                sumMAE = sumMAE + Double.valueOf(MAEResult[i]);
            }
            
            Double avgMAEResult = sumMAE/MAEResultSession.dataRecordSize;
            
            MAEResultSession.ExportToCSV(fileDirectory, fileName + "_MAE_logs");
            
            dataBR.close();
            
            return avgMAEResult;
            
        } catch (IOException e) {
            e.printStackTrace();
            
            return -1.0;
        }
    }
    
    /**
     * Load data from file and build format data list.
     * @param dataFile The original data file hold the data compressed in ESV.
     * @param ifAllToOne Set to true if it is not required for user MAE identification.
     * @return A list hold all format data session which could be used to calculate MAE by MeanAbsoluteErrorEvaluator.
     * @throws IOException
     */
    private static List<DataSession> GetMAERatingsSessionList(BufferedReader dataFile, boolean ifAllToOne) throws IOException {
        
        String dataLineHolder = dataFile.readLine();
        String[] dataLineSplit;
        
        List<String> ratingsQueue = new ArrayList<String>();
        List<String> ratingsId = new ArrayList<String>();
        while((dataLineHolder = dataFile.readLine())!=null) {
            dataLineSplit = dataLineHolder.split(",");
            if(!dataLineSplit[5].equals("No useful prediction found")) {
                ratingsQueue.add(dataLineSplit[5]);
                ratingsId.add(dataLineSplit[0]);
            }
        }
        
        List<DataSession> MAERatingsSessionList = new ArrayList<DataSession>();
        String[] ratingsFormatQueue;
        // Convert the ratings queue to a ratings format queue from an array list to an array.
        // And store the format ratings data into the list.
        if(ifAllToOne) {
            ratingsFormatQueue = new String[ratingsQueue.size()];
            for(int i=0; i<ratingsQueue.size(); i++) {
                ratingsFormatQueue[i] = ratingsQueue.get(i);
            }
            MAERatingsSessionList.add(FormatDataSet("MAERatingsSessionAllToOne", ratingsFormatQueue));
        }else {
            for(int i=0; i<ratingsQueue.size(); i++) {
                ratingsFormatQueue = new String[1];
                ratingsFormatQueue[0] = ratingsQueue.get(i);
                MAERatingsSessionList.add(FormatDataSet(ratingsId.get(i), ratingsFormatQueue));
            }
        }
        
        return MAERatingsSessionList;
    }
    
    /**
     * Format data from ESV to a specified DataSession to be used in MAE evaluator.
     * @param formatDataSetName Name or Id for the DataSession.
     * @param ratingsESV The array of all the ESV will be formatted into the DataSession.
     * @return
     */
    private static DataSession FormatDataSet(String formatDataSetName, String[] ratingsESV) {
        // make sure the ESV data set is not null
        if(ratingsESV==null) {
            return null;
        }
        
        // Initialize the result session.
        String[] resultFields = {"ItemId", "PredRt", "RealRt"};
        DataSession resultSession = new DataSession(formatDataSetName);
        resultSession.BuildFields(resultFields, "ItemId");
        
        String[] itemsHandler, ratingsHandler;
        for(int i=0; i<ratingsESV.length; i++) {
            // Travel the ESV list.
            itemsHandler = ratingsESV[i].split("\\|");
            
            for(int j=0; j<itemsHandler.length; j++) {
                // Travel the items list.
                ratingsHandler = itemsHandler[j].split("_");
                // Build a result row handler
                String[] rowHandler = new String[3];
                for(int k=0; k<3; k++) {
                    // Travel the ratings set.
                    rowHandler[k] = ratingsHandler[k];
                }
                // Set a row in the result session.
                resultSession.SetARow(rowHandler);
            }
        }
        
        return resultSession;
    }

}
