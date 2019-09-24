package com.chestnut.runtime.dal;

import java.util.Map;

import com.chestnut.runtime.core.ZPZItem;
import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;

public class Director extends ZPZItem{

	public Director(String id, ConfigManager cfgManager, String tableName, String primeField){
		super(id, cfgManager);
        super.loadDataFromMySQL(_cfgManager.GetMySQLHelper("dbName_director"), tableName, primeField);
	}
	
	public DataSession GetData(){
        return super._atomData;
    }
	
	public String[] GetMostEffectiveUser(String valuedField, String groupField, double threadHold, Map<String, String> existList){
        String[] effectedUser;
        effectedUser = super.GetMostEffectedRecord(valuedField, groupField, threadHold, existList, true);
        return effectedUser;
    }
	
	public String[] GetEffectiveList(String valuedField, String groupField, int effAmount, double threadHold, Map<String, String> existList) {
	    String[] effectiveUserList;
	    effectiveUserList = super.GetEffectedList(valuedField, groupField, effAmount, threadHold, existList, true);
	    /*
	    for(int i=0; i<effectiveUserList.length; i++) {
	        System.out.println("[Tracing] Director.GetEffectiveList(), selectedColumn[" + i + "] is " + effectiveUserList[i]);
	    }
	    */
        return effectiveUserList;
	}
}
