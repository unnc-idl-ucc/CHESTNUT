package com.chestnut.runtime.booster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class SourceFileRefactory {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        try {
            BufferedReader inputFile = new BufferedReader(new FileReader("data/ratings_director_matched.csv"));
            BufferedWriter outputFile = new BufferedWriter(new FileWriter("data/ProductEnv/Refactory/ratings_noTimeStamp.csv"));
            
            String temp;
            String[] tempSplit;
            temp = inputFile.readLine();
            tempSplit = temp.split(",");
            outputFile.write(tempSplit[0] + "," + tempSplit[1] + "," + tempSplit[2] + "," + tempSplit[4] + "\n");
            
            while((temp = inputFile.readLine())!=null) {
                tempSplit = temp.split(",");
                outputFile.write(tempSplit[0] + "," + tempSplit[1] + "," + tempSplit[2] + "," + tempSplit[4] + "\n");
            }
            
            inputFile.close();
            outputFile.close();
            
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
