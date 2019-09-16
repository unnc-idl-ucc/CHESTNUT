package com.chestnut.runtime.Mahout;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;

public class MahoutRecommender {
    
    private String[] _globalRatingsHolder;
    public MahoutRecommender(String originalRatingsFilePath) {
        _globalRatingsHolder = InitializeGobalRatingsArray(originalRatingsFilePath);
    }
    
    public void Recommend(int recommendationScale, String givenUserId, String userRatingsFilePath) {
        
        CombineDataMotelFile(userRatingsFilePath, givenUserId, true, _globalRatingsHolder);
        
        // Start recommend
        try {
            DataModel globalDM;
            globalDM = new FileDataModel(new File("data/ProductEnv/newCombines/movies.csv"));
            ItemBasedRecommend item_based = new ItemBasedRecommend(globalDM);
            UserBasedRecommend user_based = new UserBasedRecommend(globalDM);
            
            System.out.println("---------- Mahout Data Collecting for User "+ givenUserId +" ----------");
            List<String> ItemBasedHolder, UserBasedHolder;
            ItemBasedHolder = item_based.recommend(Long.valueOf(givenUserId), recommendationScale);
            UserBasedHolder = user_based.recommend(Long.valueOf(givenUserId), recommendationScale);
            
            ExportResultToCSV("IB/results_IB_" + userRatingsFilePath.substring(userRatingsFilePath.lastIndexOf("/") + 1), ItemBasedHolder);
            ExportResultToCSV("UB/results_UB_" + userRatingsFilePath.substring(userRatingsFilePath.lastIndexOf("/") + 1), UserBasedHolder);
            
            System.out.println("---------- Mahout Done Collecting for User "+ givenUserId +" ----------\n");
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    
    private String[] InitializeGobalRatingsArray(String globalRatingsPath) {
        try {
            ArrayList<String> globalRatingsList = new ArrayList<String>();
            BufferedReader globalDefaultRatingBuffer = new BufferedReader(new FileReader(globalRatingsPath));
            globalDefaultRatingBuffer.readLine();
            String globalRatingLine;
            while((globalRatingLine = globalDefaultRatingBuffer.readLine()) != null) {
                globalRatingsList.add(globalRatingLine);
            }
            
            globalDefaultRatingBuffer.close();
            
            String[] globalRatingsArray = new String[globalRatingsList.size()];
            for(int i=0; i<globalRatingsList.size(); i++) {
                globalRatingsArray[i] = globalRatingsList.get(i);
            }
            
            return globalRatingsArray;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private void CombineDataMotelFile(String userDataFilePath, String userId, boolean hasFields, String[] globalRatings) {
        try {
            BufferedWriter combinedDataWriter = new BufferedWriter(new FileWriter("data/ProductEnv/newCombines/movies.csv"));
            
            for(int i=0; i<globalRatings.length; i++) {
                String[] globalRatingSplit = globalRatings[i].split(",");
                combinedDataWriter.write(globalRatingSplit[0] + "," + globalRatingSplit[1] + "," + globalRatingSplit[2] + "\n");
            }
            
            
            BufferedReader userDataReader = new BufferedReader(new FileReader(userDataFilePath));
            if(hasFields) {
                userDataReader.readLine();
            }
            String userDataLine;
            while((userDataLine = userDataReader.readLine()) != null) {
                String[] userDataSplit = userDataLine.split(",");
                if(!userDataSplit[0].equals("NA")) {
                    combinedDataWriter.write(userId + "," + userDataSplit[0] + "," + userDataSplit[1] + "\n");
                }
            }
            
            userDataReader.close();
            combinedDataWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void ExportResultToCSV(String resultFileName, List<String> resultList) {
        try {
            BufferedWriter resultsWriter = new BufferedWriter(new FileWriter("data/ProductEnv/RecommendationResults/" + resultFileName));
            resultsWriter.write("movieId" + "\n");
            for(int i=0; i<resultList.size(); i++) {
                resultsWriter.write(resultList.get(i) + "\n");
            }
            resultsWriter.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
}
