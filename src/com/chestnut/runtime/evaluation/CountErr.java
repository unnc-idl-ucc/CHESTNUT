package com.chestnut.runtime.evaluation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CountErr {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        
        try {
            BufferedReader countsFileLoader = new BufferedReader(new FileReader("data/IBUBSBcounts.csv"));
            BufferedWriter ErrLogFile = new BufferedWriter(new FileWriter("data/EvalExp.csv"));
            
            String lineHolder;
            String[] lineSplit;
            boolean lineCheck = false;
            int linePtr = 0;
            while((lineHolder = countsFileLoader.readLine())!=null) {
                linePtr ++;
                lineSplit = lineHolder.split(",");
                for(int i=0; i<lineSplit.length; i++) {
                    if(lineSplit[i].equals("NE")||lineSplit[i].equals("NA")) {
                        lineCheck = true;
                    }
                }
                
                if(lineCheck) {
                    ErrLogFile.write(lineHolder + "," + linePtr + "\n");
                    lineCheck = false;
                }
                
                if(linePtr==3) {
                    linePtr = 0;
                }
            }
            
            countsFileLoader.close();
            ErrLogFile.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
