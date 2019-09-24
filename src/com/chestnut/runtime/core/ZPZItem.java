package com.chestnut.runtime.core;

import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public class ZPZItem extends ZPZAtom{
    
    public ZPZItem(String id, ConfigManager cfgManager){
        super(id, cfgManager);
    }
    
    public String GetID(){
        return super._atomId;
    }
    
    public DataSession GetData() {
        return super._atomData;
    }
    
    public void loadDataFromMySQL(MySQLHelper sqlhp, String tableName, String primeField){
        super.loadDataFromMySQL(sqlhp, tableName, primeField);
    }
    
    protected String[] GetMostEffectedRecord(String valuedField, String groupField, double threadHold, Map<String, String> existList, boolean ifExist){
        DataSession mostEffectiveRecordDS = super.FindEffectiveRecordByGroup(valuedField, groupField, threadHold, existList, ifExist);
        String[] mostEffectiveRecordArr = mostEffectiveRecordDS.GetAColum(groupField);
        return mostEffectiveRecordArr;
    }
    
    protected String[] GetEffectedList(String valuedField, String groupField, int effAmount, double threadHold, Map<String, String> existList, boolean ifExist) {
        DataSession effectiveList = super.FindEffectedList(valuedField, groupField, threadHold, existList, ifExist);
        if(effAmount>effectiveList.dataRecordSize) {
            effAmount = effectiveList.dataRecordSize;
        }
        String[] effectiveListArr = effectiveList.GetColumnFirstDESCByItr(effAmount, "effectiveValueSum", groupField);
        return effectiveListArr;
    }
}
