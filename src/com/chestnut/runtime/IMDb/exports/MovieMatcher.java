package com.chestnut.runtime.IMDb.exports;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.chestnut.runtime.dal.ma.DataSession;

public class MovieMatcher {

    private HashMap<String, String> _movieIdMap, _movieId2IMDbMap;
    private DataSession _moviesInfo;
    private ArrayList<String> _InDateMovie, _EnglishMovie, _InDateEnglishMovie;
    
    public MovieMatcher(String linksFilePath) {
        _movieIdMap = new HashMap<String, String>();
        _movieId2IMDbMap = new HashMap<String, String>();
        _InDateMovie = new ArrayList<String>();
        _EnglishMovie = new ArrayList<String>();
        _InDateEnglishMovie = new ArrayList<String>();
        InitMovieIdMap(linksFilePath);
    }
    
    public String GetMovieIdByIMDbId(String IMDbId) {
        if(_movieIdMap.containsKey(IMDbId)) {
            return _movieIdMap.get(IMDbId);
        }else {
            return "not exist";
        }
    }
    
    public void SetMoviesInfo(String moviesInfoFilePath) {
        _moviesInfo = new DataSession("moviesInfo");
        _moviesInfo.ImportFromCSV(moviesInfoFilePath, null, "movieId");
        System.out.println("[MovieMatcher.SetMoviesInfo]: Movie matcher set, size of movies = " + _moviesInfo.dataRecordSize);
        //CheckAllMoviesSuitable();
    }
    
    public String GetMovieTitleByMovieId(String movieId) {
        if(_moviesInfo != null) {
            
            String movieTitleHolder = _moviesInfo.GetARecordByIndex("title", _moviesInfo.GetAPrimeRecordIndex(movieId)-1);
            System.out.println("[MovieMatcher.GetMovieTitleByMovieId]: The movie[" + movieId + "] original title is " + movieTitleHolder);
            // Fix movie title with separate 'The' problem.
            movieTitleHolder = movieTitleHolder.substring(0, movieTitleHolder.length()-7);
            System.out.println("[MovieMatcher.GetMovieTitleByMovieId]: The movie simple title is " + movieTitleHolder);
            System.out.println("[MovieMatcher.GetMovieTitleByMovieId]: Last index of ',' is " + movieTitleHolder.lastIndexOf(","));
            if(movieTitleHolder.lastIndexOf(",") != -1) {
                if(movieTitleHolder.substring(movieTitleHolder.lastIndexOf(",")).equals(", The")) {
                    movieTitleHolder = "The " + movieTitleHolder.substring(0, movieTitleHolder.length()-5);
                }
            }
            
            System.out.println("[MovieMatcher.GetMovieTitleByMovieId] get movie title for movie id " + movieId);
            System.out.println("[MovieMatcher.GetMovieTitleByMovieId] movie title => " + movieTitleHolder);
            
            return movieTitleHolder;
        }else {
            return "movies info not set";
        }
    }
    
    public String GetIMDBIdByMovieId(String movieId) {
        return _movieId2IMDbMap.get(movieId);
    }
    
    private void InitMovieIdMap(String linksFilePath) {
        try {
            BufferedReader linksFileReader = new BufferedReader(new FileReader(linksFilePath));
            linksFileReader.readLine(); // skip first row
            String linksRow;
            while((linksRow = linksFileReader.readLine())!= null) {
                String[] linksRowSplit = linksRow.split(",");
                _movieIdMap.put(linksRowSplit[1], linksRowSplit[0]);
                _movieId2IMDbMap.put(linksRowSplit[0], linksRowSplit[1]);
            }
            linksFileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @SuppressWarnings("unused")
    private void CheckAllMoviesSuitable() {
        String[] movieId = _moviesInfo.GetAColum("movieId");
        String[] movieTitle = _moviesInfo.GetAColum("title");
        
        for(int i=0; i<movieTitle.length; i++) {
            CheckMovieSuitable(movieTitle[i], movieId[i]);
        }
        
         System.out.println("[MovieMatcher.CheckAllMoviesSuitable]: _InDateMovie size = " + _InDateMovie.size());
         System.out.println("[MovieMatcher.CheckAllMoviesSuitable]: _EnglishMovie size = " + _EnglishMovie.size());
         System.out.println("[MovieMatcher.CheckAllMoviesSuitable]: _InDateEnglishMovie size = " + _InDateEnglishMovie.size());
    } 
    
    private void CheckMovieSuitable(String movieTitle, String movieId) {
        boolean movieOutOfTime = IsMovieOutOfTime(movieTitle, 30, 2016);
        boolean movieNoneEnglish = IsMovieContainNoneEnglishChar(movieTitle);
        if(!movieOutOfTime) {
            _InDateMovie.add(movieId);
        }
        
        if(!movieNoneEnglish) {
            _EnglishMovie.add(movieId);
        }
        
        if((!movieOutOfTime)&&(!movieNoneEnglish)) {
            _InDateEnglishMovie.add(movieId);
        }
    }
    
    private boolean IsMovieOutOfTime(String movieTitle, int yearRange, int yearNow) {
        int indexOfLastQuote = movieTitle.lastIndexOf(")");
            if(indexOfLastQuote>4) {
            System.out.println("[MovieMatcher.IsMovieOutOfTime] Movie " + movieTitle);
            String yearStr = movieTitle.substring(indexOfLastQuote-4, indexOfLastQuote);
            System.out.println("[MovieMatcher.IsMovieOutOfTime] Movie year " + yearStr);
            if(Integer.valueOf(yearStr)<(yearNow - yearRange)) {
                return true;
            }else {
                return false;
            }
        }
        return false;
    }
    
    private boolean IsMovieContainNoneEnglishChar(String movieTitle) {
        byte[] titleBytes = movieTitle.getBytes();
        for(int i=0; i<titleBytes.length; i++) {
            if(!isIncludeANSIEnglish(titleBytes[i])) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isIncludeANSIEnglish(byte checkByte) {
        if((31<checkByte && checkByte<127)) {
            return true;
        }else {
            return false;
        }
    }
}
