package com.chestnut.runtime.evaluation;

import com.chestnut.runtime.dal.ma.DataSession;

public interface Evaluator {
    
    /**
     * This interface method provides idea to add a new set of data to be evaluated by this evaluator with specified evaluation method.
     * @param newDataSetId Assign an id to the new appended data set.
     * @param newDataSet A DataSession hold a set of data to be evaluated.
     */
    void AppendData(String newDataSetId, DataSession newDataSet);
    
    /**
     * This interface method provides idea to remove a set of data added but wont be evaluated any more.
     * @param dataSetIndex Get the data set with the given id to be removed.
     */
    void RemoveData(String dataSetId);
    
    /**
     * This interface method gives idea to implement the specified evaluation function.
     * @return A DataSession holds all the evaluation results of all the appended data set.
     */
    DataSession Evaluate();
}
