package com.chestnut.runtime.core;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.mysql.DataLoader;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public abstract class ZPZAtom {

    protected String _atomId;
    protected DataSession _atomData;
    protected DataSession _processedData;
    private Map<String, Double[]> _filterGroups;  
    protected ConfigManager _cfgManager;
    
    protected ZPZAtom(String id, ConfigManager cfgManager){
        _atomId = id;
        _cfgManager = cfgManager;
    }
    
    protected String GetID(){
        return _atomId;
    }
    
    protected void loadDataFromMySQL(MySQLHelper sqlhp, String tableName, String primeField){
        DataLoader dl = new DataLoader(sqlhp);
        
        try {
            _atomData = dl.MySQLToDRS(tableName, dl.GetField(tableName, sqlhp.GetDBName()), primeField);
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    protected boolean isIdenticalValuedAtom(String valuedField){
        return _atomData.isIdenticalValuedSession(valuedField);
    }
    
    /***
     * Get an effective set according to the given threshold, only the records with value of request field greater than the threshold selected.
     * @param valuedField The column used to record the values to be filtered by the threshold.
     * @param groupField The primary key used to identify a row.
     * @param threshold A double threshold use to constraint the filtering.
     * @return A DataSession hold all the data of each primary key with two information "effectiveValueSum" and "effectiveValueNum".
     *         "effectiveValueSum" means the sum of all effective values for a primary key.
     *         "effectiveValueNum" means the number of counts of effective values for a primary key.
     */
    protected DataSession GetEffectiveSet(String valuedField, String groupField, double threshold){
        _processedData = new DataSession(_atomData.sessionName + "_FilteredBy_" + valuedField);
        _filterGroups = new HashMap<String, Double[]>();//index 0 is effectiveValueSum, index 1 is effectiveValueNum
        
        String[] fields = new String[3];
        fields[0] = groupField;
        fields[1] = "effectiveValueSum";
        fields[2] = "effectiveValueNum";
        
        _processedData.BuildFields(fields, groupField);
        
        String[] rowHolder;
        String groupFieldValueHolder;
        Double valueHolder;
        
        for(int i=0; i<_atomData.dataRecordSize; i++){
            rowHolder = _atomData.GetARow(i);
            valueHolder = Double.parseDouble(rowHolder[_atomData.GetFiledsIndex(valuedField)]);
            if(valueHolder>=threshold){
                groupFieldValueHolder = rowHolder[_atomData.GetFiledsIndex(groupField)];
                if(_filterGroups.containsKey(groupFieldValueHolder)){
                    _filterGroups.get(groupFieldValueHolder)[0] = _filterGroups.get(groupFieldValueHolder)[0] + valueHolder;
                    _filterGroups.get(groupFieldValueHolder)[1] ++;
                }else{
                    Double[] effectiveValueHolder = new Double[2];
                    effectiveValueHolder[0] = valueHolder;
                    effectiveValueHolder[1] = 1.0;
                    _filterGroups.put(groupFieldValueHolder, effectiveValueHolder);
                }
            }
        }
        
        rowHolder = new String[3];
        for(Map.Entry<String, Double[]> entry : _filterGroups.entrySet()){
            rowHolder[0] = entry.getKey();
            rowHolder[1] = String.valueOf(entry.getValue()[0]);
            rowHolder[2] = String.valueOf(entry.getValue()[1]);
            _processedData.SetARow(rowHolder);
        }
        
        return _processedData;
    }
    
    protected DataSession FindEffectiveRecordByGroup(String valuedField, String groupField, double threadHold, Map<String, String> existList, boolean ifExist){
        DataSession effectSet = GetEffectiveSet(valuedField, groupField, threadHold);
        if(ifExist) effectSet = FilterByExistedRecord(effectSet, existList);
        effectSet = FilterByEffectiveFeature(effectSet, "effectiveValueNum", groupField);
        if(effectSet.dataRecordSize>1){
            effectSet = FilterByEffectiveFeature(effectSet, "effectiveValueSum", groupField);
        }
        return effectSet;
    }
    
    protected DataSession FindEffectedList(String valuedField, String groupField, double threadHold, Map<String, String> existList, boolean ifExist) {
        DataSession effectSet = GetEffectiveSet(valuedField, groupField, threadHold);
        if(ifExist) effectSet = FilterByExistedRecord(effectSet, existList);
        return effectSet;
    }
    
    private DataSession FilterByExistedRecord(DataSession effectSet, Map<String, String> existList){
        String[] rowHolder;
        DataSession filteredEffectSet = new DataSession("filteredEffectSetByExistList");
        filteredEffectSet.BuildFields(effectSet.GetFields(), effectSet.GetPrimeField());
        
        
        for(int i=0; i<effectSet.dataRecordSize; i++){
            rowHolder = effectSet.GetARow(i);
            if(!existList.containsKey(rowHolder[0])){
                filteredEffectSet.SetARow(rowHolder);
            }
        }
        return filteredEffectSet;
    }
    
    private DataSession FilterByEffectiveFeature(DataSession effectSet, String effectFeature, String groupField){
        Double valueHolder, valueBiggestHolder = 0.0;
        DataSession filteredEffectSet = new DataSession("filteredEffectSetBy_" + effectFeature);
        filteredEffectSet.BuildFields(effectSet.GetFields(), effectSet.GetPrimeField());
        
        Map<String, Integer> recommendSet = new HashMap<String, Integer>();
        
        for(int i=0; i<effectSet.dataRecordSize; i++){
            valueHolder = Double.parseDouble(effectSet.GetARecordByIndex(effectFeature, i));
            if(valueHolder > valueBiggestHolder){
                CleanMap(recommendSet);
                valueBiggestHolder = valueHolder;
                recommendSet.put(effectSet.GetARecordByIndex(groupField, i), i);
            }else if(valueHolder == valueBiggestHolder){
                recommendSet.put(effectSet.GetARecordByIndex(groupField, i), i);
            }
        }
        
        for(Map.Entry<String, Integer> entry : recommendSet.entrySet()){
            filteredEffectSet.SetARow(effectSet.GetARow(entry.getValue()));
        }
        
        return filteredEffectSet;
    }
    
    private void CleanMap(Map<String, Integer> recommendSet){
        for(Map.Entry<String, Integer> entry : recommendSet.entrySet()){
            recommendSet.remove(entry.getKey());
        }
    }
}
