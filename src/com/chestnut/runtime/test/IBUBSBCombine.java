package com.chestnut.runtime.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class IBUBSBCombine {

    public static void main(String[] args) {
        try {
            BufferedReader IBUBload = new BufferedReader(new FileReader("data/IBUB.csv"));
            BufferedReader SBload = new BufferedReader(new FileReader("data/SB.csv"));
            
            BufferedWriter IBUBSBcombine = new BufferedWriter(new FileWriter("data/IBUBSB.csv"));
            
            String SBLineHolder;
            
            IBUBload.readLine();
            IBUBSBcombine.write(SBload.readLine() + "\n");
            
            String[] SBLineSpilt;
            int rlsAllHolder, rlsUxpHolder, rlsUsfHolder;
            String newAllHolderUxp, newAllHolderUsf, newLine;
            
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
                
                IBUBSBcombine.write(IBUBload.readLine() + "\n");
                IBUBSBcombine.write(IBUBload.readLine() + "\n");
                IBUBSBcombine.write(newLine + "\n");
            }
            
            
            IBUBload.close();
            SBload.close();
            IBUBSBcombine.close();
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
