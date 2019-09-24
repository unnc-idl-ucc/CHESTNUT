package com.chestnut.runtime.booster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.chestnut.runtime.dal.ma.DataConstructor;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.ma.ESVParser;
import com.chestnut.runtime.dal.math.similarity.PearsonCorrelationSimilarity;

public class Test {

    public static int TestScale = 10000;
    public static double PearsonSimilarityThreshold = 0.2;
    
	public static void main(String[] args) {
	    
		// TODO Auto-generated method stub
	    //SingleTest("102395");
	    //BatchTest();
	    //BatchTestWithoutThread(2,5);
	    
	    
	    //ESVParser esvp = new ESVParser("data/ProductEnv/HelperSet/k_nearest_comb.csv");
	    //System.out.println(esvp.GetARecordSet("138940").get(1).get(0));
		
		RefectoryMahoutUseRatingData();
	}
	
	public static void DataSessionComparatorTest(){
		String[] fields = new String[4];
		fields[0] = "userId";
		fields[1] = "movieId";
		fields[2] = "rating";
		fields[3] = "timestamp";
		//ralations.put("userId", "rating");
		BufferedReader OutData;
		DataConstructor dc;
		PearsonCorrelationSimilarity pcs;
		try {
			DataSession dataV, dataU;
			OutData = new BufferedReader(new FileReader("data/test/ratings_v.csv"));
			dc = new DataConstructor(OutData.readLine(), fields, "movieId", "userId");
			dataV = dc.Constructed();
			OutData.close();
			
			OutData = new BufferedReader(new FileReader("data/test/ratings_u.csv"));
			dc = new DataConstructor(OutData.readLine(), fields, "movieId", "userId");
			dataU = dc.Constructed();
			OutData.close();
			
			pcs = new PearsonCorrelationSimilarity(dataV, dataU);
			System.out.println("[Tracing] Pearson similarity is: " + pcs.ExecuteSimilarity("userId", "rating"));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void RefectoryMahoutUseRatingData() {
		try {
			BufferedReader originalDataReader = new BufferedReader(new FileReader("data/ProductEnv/newCombines/result_inDateNEngelish.csv"));
			BufferedWriter newDataFilesWriter = new BufferedWriter(new FileWriter("data/ProductEnv/newCombines/ratings.csv"));
			
			// Remove fields name column.
			String fieldsNameHold = originalDataReader.readLine();
			String[] fieldsNameSplit = fieldsNameHold.split(",");
			newDataFilesWriter.write(
						fieldsNameSplit[0] + "," +
						fieldsNameSplit[1] + "," +
						fieldsNameSplit[2] + "," +
						fieldsNameSplit[3] + "\n"
					);
			
			
			// Build record body data.
			String recordBody;
			while((recordBody = originalDataReader.readLine()) != null)
			{
				String[] recordBodySplit = recordBody.split(",");
				newDataFilesWriter.write(
							recordBodySplit[0] + "," +
							recordBodySplit[1] + "," +
							recordBodySplit[2] + "," +
							recordBodySplit[3] + "\n"
						);
			}
			
			newDataFilesWriter.close();
			originalDataReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
