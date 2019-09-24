package com.chestnut.runtime.dal.math.similarity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompareResultSet {

	public List<Integer> _sameRecordIndexV, _sameRecordIndexU;
	public Map<String, String> _union;
	
	public int IntersectionSize;
	public int UnionSize;
	
	public CompareResultSet(List<Integer> sameRecordIndexV, List<Integer> sameRecordIndexU){
		_sameRecordIndexV = sameRecordIndexV;
		_sameRecordIndexU = sameRecordIndexU;
		if(_sameRecordIndexV.size() == _sameRecordIndexU.size()){
		    IntersectionSize = _sameRecordIndexV.size();
		}else{
		    IntersectionSize = 0;
			System.out.println("[WARN]CompareResultSet V and U have different size!");
		}
		
	}
	
	public CompareResultSet(List<Integer> sameRecordIndexV, List<Integer> sameRecordIndexU, Map<String, String> union){
	    _sameRecordIndexV = sameRecordIndexV;
        _sameRecordIndexU = sameRecordIndexU;
        _union = new HashMap<String, String>();
        _union = union;
        UnionSize = _union.size();
        if(_sameRecordIndexV.size() == _sameRecordIndexU.size()){
            IntersectionSize = _sameRecordIndexV.size();
        }else{
            IntersectionSize = 0;
            System.out.println("[WARN]CompareResultSet V and U have different size!");
        }
	}
}
