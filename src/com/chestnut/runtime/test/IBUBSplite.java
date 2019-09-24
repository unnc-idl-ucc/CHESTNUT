package com.chestnut.runtime.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class IBUBSplite {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            BufferedReader IBUBload = new BufferedReader(new FileReader("data/IBUBMAE.csv"));
            BufferedWriter IBMAE = new BufferedWriter(new FileWriter("data/IBMAE.csv"));
            BufferedWriter UBMAE = new BufferedWriter(new FileWriter("data/UBMAE.csv"));
            
            String lineHolder = IBUBload.readLine();
            IBMAE.write(lineHolder + "\n");
            UBMAE.write(lineHolder + "\n");
            for(int i=0; i<2113; i++) {
                lineHolder = IBUBload.readLine();
                IBMAE.write(lineHolder + "\n");
                lineHolder = IBUBload.readLine();
                UBMAE.write(lineHolder + "\n");
            }
            
            IBUBload.close();
            IBMAE.close();
            UBMAE.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
