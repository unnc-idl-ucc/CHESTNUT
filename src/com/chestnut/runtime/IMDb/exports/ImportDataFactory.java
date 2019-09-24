package com.chestnut.runtime.IMDb.exports;

import com.chestnut.runtime.dal.ma.DataSession;

public class ImportDataFactory {

    private DataSession _exportFileDataSession;
    
    public ImportDataFactory() {
        
    }
    
    public void SetExportFileDataSession(DataSession incommingDataSession) {
        _exportFileDataSession = incommingDataSession;
    }
    
    public DataSession BuildWebUsageDataSession() {
        DataSession webUsageDS = new DataSession("webUseImportData");
        String[] fields = {"Const", "ItemType", "MovieName", "MovieYear", "MovieDirector", "MovieRating", "MoviePosterUrl"};
        webUsageDS.BuildFields(fields, "Const");
        
        // Initialize poster crawler.
        ContentsCrawler posterCrawler = new ContentsCrawler();
        
        // Initialize movie matcher for movie title fixing.
        MovieMatcher idMatcher = new MovieMatcher("data/ProductEnv/HelperSet/links.csv");
        idMatcher.SetMoviesInfo("data/ProductEnv/HelperSet/movies.csv");
        
        // Initialize movie filter for director name fixing.
        MovieFilter directorFilter = new MovieFilter("data/ProductEnv/HelperSet/directorResult_rp_done.csv", 3, 0, 2);
        
        for(int i=0; i<_exportFileDataSession.dataRecordSize; i++) {
            String[] exportsRowHolder = _exportFileDataSession.GetARow(i);
            
            // Fix movie title
            String movieIdHolder = GetMatchedMovieId(exportsRowHolder[0], idMatcher);
            String movieName;
            if(movieIdHolder.equals("NA")) {
                movieName = "\"" + exportsRowHolder[3] + "\"";
            }else {
                movieName = "\"" + idMatcher.GetMovieTitleByMovieId(movieIdHolder) + "\"";
            }
            
            // Fix director name
            String directorIdHolder, directorNameHolder, directorStr = exportsRowHolder[12];
            directorIdHolder = GetDirectorId(directorStr, directorFilter);
            if(directorIdHolder.equals("not exist")) {
                directorNameHolder = "\"" + exportsRowHolder[12] + "\"";
            }else {
                directorNameHolder = directorFilter.GetDirectorName(directorIdHolder);
                if(directorNameHolder.contains("#")) {
                    directorNameHolder = directorNameHolder.substring(0, directorNameHolder.indexOf("#"));
                }
            }
            
            // Get poster url
            posterCrawler.SetPageDom(exportsRowHolder[4]);
            
            String[] newRow = {exportsRowHolder[0], "Movie", movieName, exportsRowHolder[8], directorNameHolder, exportsRowHolder[1], "\"" + posterCrawler.GetPosterUrl() + "\""};
            webUsageDS.SetARow(newRow);
        }
        
        return webUsageDS;
    }
    
    public DataSession BuildSysUsageDataSession() {
        DataSession sysUsageDS = new DataSession("sysUseImportData");
        String[] fields = {"movieId", "rating", "timestamp", "directorId", "year", "genres"};
        sysUsageDS.BuildFields(fields, "movieId");
        
        MovieMatcher idMatcher = new MovieMatcher("data/ProductEnv/HelperSet/links.csv");
        MovieFilter directorFilter = new MovieFilter("data/ProductEnv/HelperSet/directorResult_rp_done.csv", 3, 0, 2);
        
        //System.out.println("[ImportDataFactory.BuildSysUsageDataSession]: _exportFileDataSession.dataRecordSize => " + _exportFileDataSession.dataRecordSize);
        for(int i=0; i<_exportFileDataSession.dataRecordSize; i++) {
            String[] exportsRowHolder = _exportFileDataSession.GetARow(i);
            
            String directorIdHolder, directorStr = exportsRowHolder[12];
            
            directorIdHolder = GetDirectorId(directorStr, directorFilter);
            
            //System.out.println("[ImportDataFactory.BuildSysUsageDataSession]: director name from export => " + directorStr);
            //System.out.println("[ImportDataFactory.BuildSysUsageDataSession]: cleaned director name from export => " + directorFilter.cleanDirectorStr(directorStr));
            
            if(!directorIdHolder.equals("not exist")) {
                String[] newRow = {GetMatchedMovieId(exportsRowHolder[0], idMatcher), String.valueOf(Double.valueOf(exportsRowHolder[1])/2), "NA", directorIdHolder, exportsRowHolder[8], exportsRowHolder[9].replaceAll(",", "|")};
                sysUsageDS.SetARow(newRow);
            }else {
                //System.out.println("[ImportDataFactory.BuildSysUsageDataSession]: director " + directorStr + " not exist!");
            }
            
            //System.out.println("[ImportDataFactory.BuildSysUsageDataSession]: --------------------------------------------");
        }
        return sysUsageDS;
    }
    
    private String GetMatchedMovieId(String Const, MovieMatcher mm) {
        Const = Const.replaceAll("t", "");
        String matchResult = mm.GetMovieIdByIMDbId(Const);
        if(!matchResult.equals("not exist")) {
            return matchResult;
        }else {
            return "NA";
        }
    }
    
    private String GetDirectorId(String directorStr, MovieFilter directorFilter) {
        String directorIdHolder;
        directorIdHolder = directorFilter.GetDirectorId(directorStr);
        if(directorIdHolder.equals("not exist")) {
            if(directorStr.contains(",")) { // If mix one is not matched, choose the first director to check.
                directorStr = directorStr.substring(0, (directorStr.indexOf(",")));
                directorIdHolder = directorFilter.GetDirectorId(directorStr);
            }
        }
        return directorIdHolder;
    }
    
    
}
