package com.chestnut.runtime.core;

import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.mysql.MySQLHelper;

public abstract class ZPZUser extends ZPZAtom{
    
	protected ZPZUser(String id, ConfigManager cfgManager){
	    super(id, cfgManager);
	}
	
	protected String GetID(){
		return super._atomId;
	}
	
	protected void loadDataFromMySQL(MySQLHelper sqlhp, String tableName, String primeField){
	    super.loadDataFromMySQL(sqlhp, tableName, primeField);
	}
	
	protected boolean isIdenticalValuedAtom(String valuedField){
        return super.isIdenticalValuedAtom(valuedField);
    }
	
	protected String[] GetMostEffectedRecord(String valuedField, String groupField, double threadHold, Map<String, String> existList, boolean ifExist){
	    DataSession mostEffectedRecordDS = super.FindEffectiveRecordByGroup(valuedField, groupField, threadHold, existList, ifExist);
	    String[] mostEffectedRecordArr = mostEffectedRecordDS.GetAColum(groupField);
	    return mostEffectedRecordArr;
	}
	
}
