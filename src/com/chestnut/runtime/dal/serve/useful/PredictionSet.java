package com.chestnut.runtime.dal.serve.useful;

import com.chestnut.runtime.dal.ma.DataSession;

public class PredictionSet extends DataSession{

    public PredictionSet(String sessionName) {
        super(sessionName+"_predict");
        String[] fields = {"userId","r_ui","r_uavg","w_au"};
        super.BuildFields(fields,"userId");
    }
    
    public void SetARow(String[] rowRecords) {
        super.SetARow(rowRecords);
    }
    
    public String[] GetARow(int rowIndex) {
        return super.GetARow(rowIndex);
    }
}
