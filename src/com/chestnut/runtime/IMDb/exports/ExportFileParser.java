package com.chestnut.runtime.IMDb.exports;

import com.chestnut.runtime.dal.ma.DataSession;

public class ExportFileParser {
    
    public static DataSession GetExportsDataSession(String filePath, String[] buildFields, String primaryKey, String nameDataSession) {
        DataSession newExportsDataSession = new DataSession(nameDataSession);
        newExportsDataSession.ImportFromCSV(filePath, buildFields, primaryKey);
        return newExportsDataSession;
    }
    
}
