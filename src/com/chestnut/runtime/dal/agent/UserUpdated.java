package com.chestnut.runtime.dal.agent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
public class UserUpdated {
    
    public static void main(String[] args) {
	    int counters = 0,counter = 0,counter1 = 0;
	    try {
            //Pre-initial
            BufferedReader SplitData = new BufferedReader(new FileReader("data/ratings.csv"));   
            BufferedWriter OutData = new BufferedWriter(new FileWriter("data/ratings_s.csv"));
            BufferedReader XData = new BufferedReader(new FileReader("data/test1/ratings_sort1.csv"));           
            
            String temp = null; String temps = "1";
            
            SplitData.readLine();
            XData.readLine();
            
            while ((temp = SplitData.readLine()) != null){
               // if ((temp == null) || (temps == null)) break;
                String[] Elments = temp.split(",");  counters++;
                     counter = 0;
                     while ((temps = XData.readLine()) != null){
                       // if ((temp == null) || (temps == null)) break;
                         
                         String[] Elment = temps.split(",");
                         int num = Integer.parseInt(Elment[0]);
                         if (num != counters) {break;}
                         if (Elment[0].equals(Elments[0])){
                            if (Elment[1].equals(Elments[1]) == false){
                             Elments[1] = Elments[1]+"|"+Elment[1];
                             Elments[2] = Elments[2]+"|"+Elment[2];
                             Elments[3] = Elments[3]+"|"+Elment[3];
                             counter++;
                            }
                         }
                                                          }
                     if (Elments[1].indexOf("|") != -1) {OutData.write(Elments[0]+','+Elments[1]+','+Elments[2]+','+Elments[3]+'\n'); counter1++;}                                                  
                     for (int i=1; i <= counter; i++) {temp = SplitData.readLine();} 
                   }
            System.out.println(counters);
            XData.close();
            SplitData.close();
            OutData.close();
            System.out.println("All works finished!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
            System.out.println(counters);
            System.out.println(counter1);
        }
	}
    
}

