package com.chestnut.runtime.evaluation;

import java.util.HashMap;
import java.util.Map;

import com.chestnut.runtime.dal.ma.DataSession;

public class MeanAbsoluteErrorEvaluator implements Evaluator{

    private Map<String, DataSession> _dataSets;
    private String _resultSetName;
    
    public MeanAbsoluteErrorEvaluator() {
        _dataSets = new HashMap<String, DataSession>();
        _resultSetName = "MAE";
    }
    
    public void AppendData(String newDataSetId, DataSession newDataSet) {
        _dataSets.put(newDataSetId, newDataSet);
        _resultSetName = _resultSetName + "_" + newDataSetId; // Update result set name.
    }
    
    public void RemoveData(String dataSetId) {
        if(_dataSets.containsKey(dataSetId)) {
            _dataSets.remove(dataSetId);
            
            // Update result set name.
            String[] resultSetNameElements = _resultSetName.split("_");
            _resultSetName = resultSetNameElements[0];
            for(int i=1; i<resultSetNameElements.length; i++) {
                if(!resultSetNameElements[i].equals(dataSetId)) {
                    _resultSetName = _resultSetName + "_" + resultSetNameElements[i];
                }
            }
        }
    }
    
    /**
     * This method implement the MAE method and calculate all the MAE for every data set appended.
     * The result is a DataSession with two fields, "DataSetId" and "MAE". 
     * "DataSetId" represent the specified id when appended the data set.
     * "MAE" represent the mean absolute error result of that data set. Its a double in string.
     */
    public DataSession Evaluate() {
        // Initialize the result set DataSession.
        DataSession standardResults = new DataSession(_resultSetName);
        String[] resultsFields = {"DataSetId", "MAE"};
        standardResults.BuildFields(resultsFields, "DataSetId");
        
        // Travel all data sets.
        String[] rowHandler = new String[2];
        for(Map.Entry<String, DataSession> dataSet : _dataSets.entrySet()) {
            rowHandler[0] = dataSet.getKey();
            rowHandler[1] = String.valueOf(CalculateMAE(dataSet.getValue()));
            standardResults.SetARow(rowHandler);
        }
        
        return standardResults;
    }
    
    /**
     * Calculate MAE for a data set.
     * @param dataSet The DataSession contains all the real and predict ratings. The field to access real and predict rating should be "RealRt" and "PredRt".
     * @return The MAE of the data set as a double.
     */
    private Double CalculateMAE(DataSession dataSet) {
        
        double absoluteErrSumHandler = 0.0;
        String[] realRtList = dataSet.GetAColum("RealRt");
        String[] PredRtList = dataSet.GetAColum("PredRt");
        
        // The real rating list are not mapping to predict rating list.
        if(realRtList.length != PredRtList.length) {
            return -1.0;
        }
        
        for(int i=0; i<realRtList.length; i++) {
            absoluteErrSumHandler = absoluteErrSumHandler + Math.abs(Double.valueOf(PredRtList[i]) - Double.valueOf(realRtList[i]));
            //absoluteErrSumHandler = absoluteErrSumHandler + Math.pow((Double.valueOf(PredRtList[i]) - Double.valueOf(realRtList[i])),2);
        }
        
        return absoluteErrSumHandler/realRtList.length;
        //return Math.sqrt(absoluteErrSumHandler/realRtList.length);
    }
    
}
