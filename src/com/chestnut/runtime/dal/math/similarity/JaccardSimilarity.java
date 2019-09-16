package com.chestnut.runtime.dal.math.similarity;

import com.chestnut.runtime.dal.ma.DataSession;

public class JaccardSimilarity extends Similarity{

    private CompareResultSet _dataIntersection;
    private double _intersectionSize, _unionSize;
    
    public JaccardSimilarity(DataSession dataV, DataSession dataU){
        super(dataV, dataU);
    }
    
    public double ExecuteSimilarity(String CorrFieldName, String ValueFieldName){
        InitExecution(CorrFieldName, ValueFieldName);
        //System.out.println("[Tracing] JaccardSimilarity.ExecuteSimilarity(), _IntersectionSize = " + _intersectionSize + ", _unionSize = " + _unionSize);
        return _intersectionSize/_unionSize;
        
    }
    
    private void InitExecution(String CorrFieldName, String ValueFieldName){
        DataSessionComparator dsc = new DataSessionComparator(_dataV, _dataU, CorrFieldName, ValueFieldName);
        _dataIntersection = dsc.GetUnion();
        _intersectionSize = _dataIntersection.IntersectionSize;
        _unionSize = _dataIntersection.UnionSize;
    }
}
