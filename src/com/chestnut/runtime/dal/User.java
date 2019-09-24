package com.chestnut.runtime.dal;

import java.util.Map;

import com.chestnut.runtime.core.ZPZUser;
import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;

public class User extends ZPZUser {
    
	public User(String id, ConfigManager cfgManager, String tableName, String primeField){
		super(id, cfgManager);
		super.loadDataFromMySQL(_cfgManager.GetMySQLHelper("dbName_user"), tableName, primeField);
	}
	
	public DataSession GetData(){
	    return super._atomData;
	}
	
	protected boolean isIdenticalValuedUser(String valuedField){
        return super.isIdenticalValuedAtom(valuedField);
    }
	
	public String[] GetMostEffectiveDirector(String valuedField, String groupField, double threadHold, Map<String, String> existList){
	    String[] effectedDirector;
	    effectedDirector = super.GetMostEffectedRecord(valuedField, groupField, threadHold, existList, false);
	    return effectedDirector;
	}
	
}
