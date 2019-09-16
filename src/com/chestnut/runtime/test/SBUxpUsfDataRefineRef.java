package com.chestnut.runtime.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class SBUxpUsfDataRefineRef {

    public static void main(String[] args) {
        
        try {
            BufferedReader SBload = new BufferedReader(new FileReader("data/peng/50.csv"));
            BufferedWriter SBrefine = new BufferedWriter(new FileWriter("data/peng/SB_Count_Pre_50.csv"));
            
            
            String SBLineHolder;
            String[] SBLineSpilt;
            int rlsAllHolder, rlsUxpHolder, rlsUsfHolder;
            String newAllHolderUxp, newAllHolderUsf, newLine;
            
            SBrefine.write("ServedUID,all_items_uxp_ref,all_items_usf_ref,all_items_unexpected,all_items_useful,serve_time_consuming,final_items_accuracyUse\n");
            SBload.readLine();
            
            while((SBLineHolder = SBload.readLine())!=null) {
                SBLineSpilt = SBLineHolder.split(",");
                rlsAllHolder = SBLineSpilt[1].split("\\|").length;
                rlsUxpHolder = SBLineSpilt[2].split("\\|").length;
                rlsUsfHolder = SBLineSpilt[3].split("\\|").length;
                newAllHolderUxp = SBLineSpilt[2];
                newAllHolderUsf = SBLineSpilt[3];
                
                if(newAllHolderUxp.equals("Empty")) {
                    newAllHolderUxp = "Empty";
                }else {
                    for(int i=0; i<(rlsAllHolder-rlsUxpHolder); i++) {
                        newAllHolderUxp = newAllHolderUxp + "|0";
                    }
                }
                
                
                
                String[] splitAllHolderUsf = newAllHolderUsf.split("\\|");
                if(splitAllHolderUsf[0].equals("Empty")) {
                    newAllHolderUsf = "Empty";
                }else {
                    newAllHolderUsf = splitAllHolderUsf[0].substring(0, splitAllHolderUsf[0].indexOf("_"));
                    
                    for(int i=1; i<splitAllHolderUsf.length; i++) {
                        newAllHolderUsf = newAllHolderUsf + "|" + splitAllHolderUsf[i].substring(0, splitAllHolderUsf[i].indexOf("_"));
                    }
                    
                    for(int i=0; i<(rlsAllHolder-rlsUsfHolder); i++) {
                        newAllHolderUsf = newAllHolderUsf + "|0";
                    }
                }
                
                newLine = SBLineSpilt[0] + "," + newAllHolderUxp + "," + newAllHolderUsf + "," + SBLineSpilt[2] + "," + SBLineSpilt[3] + "," + SBLineSpilt[4] + "," + SBLineSpilt[5];
                
                SBrefine.write(newLine + "\n");
            }
            
            SBload.close();
            SBrefine.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
