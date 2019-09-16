package com.chestnut.runtime.dal.math.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.ma.DataSession;

public class DataSessionComparator {
	private DataSession _dataV, _dataU;
	private String _comparedField, _valueField;
	private List<Integer> _sameRecordIndexCT, _sameRecordIndexSD;
	private Map<String, String> _union;
	//private List<List<Integer>> _compareResult;
	private CompareResultSet _compareResult;
	
	public DataSessionComparator(DataSession dataV, DataSession dataU, String comparedField, String valueField){
		_dataV = dataV;
		_dataU = dataU;
		_comparedField = comparedField;
		_valueField = valueField;
		_sameRecordIndexCT = new ArrayList<Integer>();
		_sameRecordIndexSD = new ArrayList<Integer>();
		_union = new HashMap<String, String>();
	}
	
	public CompareResultSet GetIntersection(){
		if(_dataV.dataRecordSize>=_dataU.dataRecordSize){
			IntersectBy(_dataV, _dataU);
			_compareResult = new CompareResultSet(_sameRecordIndexCT, _sameRecordIndexSD);
		}else{
			IntersectBy(_dataU, _dataV);
			_compareResult = new CompareResultSet(_sameRecordIndexSD, _sameRecordIndexCT);
		}
		return _compareResult;
	}
	
	private void IntersectBy(DataSession dataCT, DataSession dataSD){
		String recordHold;
		for(int i=0; i<dataCT.dataRecordSize; i++){
			//System.out.println("[Tracing] DataSessionComparator.IntersectBy, CT loop to " + i);
			recordHold = dataCT.GetARecordByIndex(_comparedField, i);
			if(dataSD.ContainsRecord(_comparedField, recordHold)){
				//System.out.println("[Tracing] DataSessionComparator.IntersectBy(), Found same element! recordHold is: " + recordHold);
				_sameRecordIndexCT.add(i);
				_sameRecordIndexSD.add(dataSD.GetARecordWithBiggestValueIndex(recordHold, _comparedField, _valueField));
			}
		}
	}
	
	public CompareResultSet GetUnion(){
        if(_dataV.dataRecordSize>=_dataU.dataRecordSize){
            UnionBy(_dataV, _dataU);
            _compareResult = new CompareResultSet(_sameRecordIndexCT, _sameRecordIndexSD, _union);
        }else{
            UnionBy(_dataU, _dataV);
            _compareResult = new CompareResultSet(_sameRecordIndexSD, _sameRecordIndexCT, _union);
        }
        return _compareResult;
    }
	
	private void UnionBy(DataSession dataCT, DataSession dataSD){
	    String recordHold;
        for(int i=0; i<dataCT.dataRecordSize; i++){
            //System.out.println("[Tracing] DataSessionComparator.IntersectBy, CT loop to " + i);
            recordHold = dataCT.GetARecordByIndex(_comparedField, i);
            _union.put(recordHold, recordHold);
            if(dataSD.ContainsRecord(_comparedField, recordHold)){
                //System.out.println("[Tracing] DataSessionComparator.IntersectBy(), Found same element! recordHold is: " + recordHold);
                _sameRecordIndexCT.add(i);
                _sameRecordIndexSD.add(dataSD.GetARecordWithBiggestValueIndex(recordHold, _comparedField, _valueField));
            }
        }
        
        for(int i=0; i<dataSD.dataRecordSize; i++){
            //System.out.println("[Tracing] DataSessionComparator.IntersectBy, CT loop to " + i);
            recordHold = dataSD.GetARecordByIndex(_comparedField, i);
            _union.put(recordHold, recordHold);
        }
	}
	
	public DataSession GetExIntersection(){// not completed
		String recordHold;
		for(int i=0; i<_dataV.dataRecordSize; i++){
			//System.out.println("[Tracing]DataSessionComparator.IntersectBy, CT loop to " + i);
			recordHold = _dataV.GetARecordByIndex(_comparedField, i);
			if(_dataU.ContainsRecord(_comparedField, recordHold)){
				//System.out.println("[Tracing]DataSessionComparator.IntersectBy, Found same element");
				_dataV.RemoveARecordByIndex(i);
			}
		}
		return _dataV;
	}

}
