package com.chestnut.runtime.HTML;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.chestnut.runtime.IMDb.exports.ContentsCrawler;
import com.chestnut.runtime.IMDb.exports.MovieMatcher;
import com.chestnut.runtime.dal.ma.DataSession;
import com.deeFnFProcessor.fileCounter.FilesCounter;
import com.deeFnFProcessor.fileCounter.FnFStore;

public class RecommendationExporter {
    
    public RecommendationExporter() {
        
    }
    
    public void BatchAFolderInfoMaking(String folderPath, String recType) {
        FilesCounter fc = new FilesCounter(folderPath);
        FnFStore batchDirs = new FnFStore(fc.getFileCount(), fc.getFolderCount(), folderPath);
        
        if(batchDirs != null) {
            String[] dirsList = batchDirs.getFilesAbsPath();
            for(int i=0; i<dirsList.length; i++) {
                System.out.println("[BatchAFolderInfoMaking]: path, " + dirsList[i]);
                BuildAnInfoFile(LoadItemList(dirsList[i]), recType, batchDirs.GetFileName(dirsList[i]));
            }
        }
        
    }
    
    private ArrayList<String> LoadItemList(String itemFilePath) {
        try {
            BufferedReader fileReader = new BufferedReader(new FileReader(itemFilePath));
            fileReader.readLine();
            ArrayList<String> itemList = new ArrayList<String>();
            
            String lineHolder;
            while((lineHolder = fileReader.readLine()) != null) {
                itemList.add(lineHolder);
            }
            fileReader.close();
            
            return itemList;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private void BuildAnInfoFile(ArrayList<String> items, String recType, String fileName) {
        MovieMatcher idMatcher = new MovieMatcher("data/ProductEnv/HelperSet/links.csv");
        idMatcher.SetMoviesInfo("data/ProductEnv/HelperSet/movies.csv");
        
        DataSession infoDataSession = new DataSession("reccomendsInfo");
        String[] fields = {"MovieId", "ItemType", "MovieName", "IMDBLink", "MoviePosterUrl"};
        infoDataSession.BuildFields(fields, "MovieId");
        
        ContentsCrawler posterCrawler = new ContentsCrawler();
        
        for(int i=0; i<items.size(); i++) {
            String movieName = idMatcher.GetMovieTitleByMovieId(items.get(i));
            String IMDbIdHolder = idMatcher.GetIMDBIdByMovieId(items.get(i));
            String url = "https://www.imdb.com/title/tt" + IMDbIdHolder + "/";
            posterCrawler.SetPageDom(url);
            String moviePosterUrl = posterCrawler.GetPosterUrl();
            String[] rowHolder = {items.get(i), "Movie", "\"" + movieName + "\"", url, "\"" + moviePosterUrl + "\""};
            infoDataSession.SetARow(rowHolder);
        }
        
        infoDataSession.ExportToESV("data/ProductEnv/RecommendationResults/" + recType, "export_rec_web_" + fileName, ",");
    }

}

