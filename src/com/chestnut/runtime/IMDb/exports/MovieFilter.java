package com.chestnut.runtime.IMDb.exports;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class MovieFilter {

    private HashMap<String, String> _directorIdMap;
    private HashMap<String, String> _idDirectorMap;
    
    public MovieFilter(String directorFileDir, int directorColumnIndex, int directorIdColumnIndex, int startRow) {
        InitDirectorMap(directorFileDir, directorColumnIndex, directorIdColumnIndex, startRow);
    }
    
    public String GetDirectorId(String directorKey) {
        String cleanDirectorKey = cleanDirectorStr(directorKey);
        if(_directorIdMap.containsKey(cleanDirectorKey)) {
            return _directorIdMap.get(cleanDirectorKey);
        }else {
            return "not exist";
        }
    }
    
    public String GetDirectorName(String directorId) {
        if(_idDirectorMap.containsKey(directorId)) {
            return _idDirectorMap.get(directorId);
        }else {
            return "not exist";
        }
    }
    
    private void InitDirectorMap(String directorFileDir, int directorColumnIndex, int directorIdColumnIndex, int startRow) {
        _directorIdMap = new HashMap<String, String>();
        _idDirectorMap = new HashMap<String, String>();
        try {
            BufferedReader directorFileReader = new BufferedReader(new FileReader(directorFileDir));
            SkipRows(startRow - 1, directorFileReader);
            String rowHolder = "";
            while((rowHolder = directorFileReader.readLine())!=null) {
                String[] rowSplit = rowHolder.split(",");
                String directorStrHolder = rowSplit[directorColumnIndex];
                /*
                if(directorStrHolder.contains("#")) {
                    String[] directors = directorStrHolder.split("#");
                    for(int i=0; i<directors.length; i++) {
                        _directorIdMap.put(cleanDirectorStr(directors[i]), rowSplit[directorIdColumnIndex]);
                    }
                }else {
                    _directorIdMap.put(cleanDirectorStr(directorStrHolder), rowSplit[directorIdColumnIndex]);
                }
                */
                _directorIdMap.put(cleanDirectorStr(directorStrHolder), rowSplit[directorIdColumnIndex]);
                _idDirectorMap.put(rowSplit[directorIdColumnIndex], directorStrHolder);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void SkipRows(int skipAmount, BufferedReader br) throws IOException {
        for(int i=0; i<skipAmount; i++) {
            br.readLine();
        }
    }
    
    public String cleanDirectorStr(String directorName) {
        byte[] originalBytes = directorName.getBytes();
        ArrayList<Byte> newBytes = new ArrayList<Byte>();
        for(int i=0; i<originalBytes.length; i++) {
            if(isIncludeANSIEnglish(originalBytes[i])) {
                newBytes.add(originalBytes[i]);
            }
        }
        
        byte[] resultBytes = new byte[newBytes.size()];
        for(int i=0; i<resultBytes.length; i++) {
            resultBytes[i] = newBytes.get(i);
        }
        return new String(resultBytes);
    }
    
    @SuppressWarnings("unused")
    private boolean isExcludes(byte checkByte) {
        byte[] byteExcludesSet = {-17, -65, -67, 63};
        for(int i=0; i<byteExcludesSet.length; i++) {
            if(checkByte == byteExcludesSet[i]) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isIncludeANSIEnglish(byte checkByte) {
        if((64<checkByte && checkByte<91)||(96<checkByte && checkByte<123)) {
            return true;
        }else {
            return false;
        }
    }
}
