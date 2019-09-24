package com.chestnut.runtime.dal.special;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.csv.DataBuilder;
import com.chestnut.runtime.dal.csv.DataRefacer;

public class RecordMatch {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	    try {
	        matchRatingWithDirector();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	    /*
	    DataRefacer drf = new DataRefacer("data/directorResult_rp.csv");
	    try {
            drf.RemoveQoute(2, true);
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        */
	    /*
		DataBuilder dber = new DataBuilder("data/directorResult_rp.csv");
		try {
			dber.BucketGroupDataBy("directorName");
			dber.CloseAllBuffer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    */
	}
	
	private static void MatchWithMovieId(){
		Map<String, String> moviesIdHolder = new HashMap<String, String>();
		try {
			BufferedReader dataA = new BufferedReader(new FileReader("data/prepocess/movies_series_processed.csv"));
			BufferedReader dataB = new BufferedReader(new FileReader("data/prepocess/movies.csv"));
			BufferedWriter dataAPrePocessing = new BufferedWriter(new FileWriter("data/prepocess/movies_series_matched.csv"));
			String temp;
			String[] tempSplit;
			String nameHold, idHold;
			while((temp = dataB.readLine())!=null){
				tempSplit = temp.split(",");
				if(temp.contains("\"")){
					nameHold = temp.substring(temp.indexOf("\"")+1, temp.lastIndexOf("\"")).replace(",", ".");
					idHold = temp.substring(0, temp.indexOf(","));
					System.out.println(nameHold + " " + idHold);
					moviesIdHolder.put(nameHold, idHold);
				}else{
					moviesIdHolder.put(tempSplit[1], tempSplit[0]);
				}
				
			}
			dataB.close();
			
			
			String tempOut;
			while((tempOut = dataA.readLine())!=null){
				dataAPrePocessing.write(moviesIdHolder.get(tempOut) + "," + tempOut + "\n");
				
			}
			dataA.close();
			dataAPrePocessing.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void matchMovieWithDirector() throws IOException{
	    Map<String, String> directorIdHolder = new HashMap<String, String>();
	    BufferedReader dataA = new BufferedReader(new FileReader("data/directorResult_rp_done.csv"));
        BufferedReader dataB = new BufferedReader(new FileReader("data/directorResult_rp_rmQoute.csv"));
        BufferedWriter dataAPrePocessing = new BufferedWriter(new FileWriter("data/prepocess/directorResult_matched.csv"));
        String temp;
        String[] tempSplit;
        
        dataA.readLine();
        while((temp = dataA.readLine())!=null){
            tempSplit = temp.split(",");
            directorIdHolder.put(tempSplit[3], tempSplit[0]);
        }
        
        temp = dataB.readLine();
        dataAPrePocessing.write("movieId,directorId\n");
        while((temp = dataB.readLine())!=null){
            tempSplit = temp.split(",");
            dataAPrePocessing.write(tempSplit[1] + "," + directorIdHolder.get(tempSplit[2]) + "\n");
            
        }
        dataA.close();
        dataB.close();
        dataAPrePocessing.close();
	}
	
	private static void matchRatingWithDirector() throws IOException{
	    Map<String, String> directorIdHolder = new HashMap<String, String>();
        BufferedReader dataA = new BufferedReader(new FileReader("data/movies/moviesYear_extracted.csv"));
        BufferedReader dataB = new BufferedReader(new FileReader("data/movies/ratings_year_matched.csv"));
        BufferedWriter dataAPrePocessing = new BufferedWriter(new FileWriter("data/movies/ratings_gen_matched.csv"));
        String temp;
        String[] tempSplit;
        
        System.out.println("[Tracing] Movie_Director map building...");
        dataA.readLine();
        while((temp = dataA.readLine())!=null){
            tempSplit = temp.split(",");
            directorIdHolder.put(tempSplit[0], tempSplit[1]);
        }
        System.out.println("[Tracing] Movie_Director map building completed!");
        
        System.out.println("[Tracing] Start matching record with director...");
        temp = dataB.readLine();
        dataAPrePocessing.write(temp + ",genres\n");
        while((temp = dataB.readLine())!=null){
            tempSplit = temp.split(",");
            dataAPrePocessing.write(temp + "," + directorIdHolder.get(tempSplit[1]) + "\n");
            
        }
        System.out.println("[Tracing] Matching record with director completed!");
        
        
        dataA.close();
        dataB.close();
        dataAPrePocessing.close();
	}
	
	private static void extractYearFromMovieTitle() throws IOException{
	    BufferedReader dataA = new BufferedReader(new FileReader("data/movies/movies.csv"));
	    BufferedWriter dataAPrePocessing = new BufferedWriter(new FileWriter("data/movies/moviesYear_extracted.csv"));
	    String temp, year;
        String[] tempSplit;
        
        temp = dataA.readLine() + ",year";
        dataAPrePocessing.write(temp + "\n");
        
        while((temp = dataA.readLine())!=null){
            if(temp.lastIndexOf("\"")!=-1){
                
                year = temp.substring(temp.lastIndexOf("\"")-5, temp.lastIndexOf("\"")-1);
            }else{
                tempSplit = temp.split(",");
                if(tempSplit[1].lastIndexOf(")")!=-1){
                    year = tempSplit[1].substring(tempSplit[1].lastIndexOf(")")-4, tempSplit[1].lastIndexOf(")"));
                }else{
                    year = "NA";
                }
            }
            
            
            System.out.println("[Tracing] Extracted year is " + year);
            dataAPrePocessing.write(temp + "," + year + "\n");
        }
        
        dataA.close();
        dataAPrePocessing.close();
	}

}
