package com.chestnut.runtime.dal.math.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListFilter {

    private List<String> _originalList;
    
    public ListFilter(List<String> originalList) {
        _originalList = originalList;
    }
    
    public List<String> FilterByMap(Map<String, String> filterMap) {
        List<String> resultList = new ArrayList<String>();
        String elementHold;
        for(int i=0; i<_originalList.size();i++) {
            elementHold = _originalList.get(i);
            if(!filterMap.containsKey(elementHold)) {
                resultList.add(elementHold);
            }
        }
        return resultList;
    }
    
    
}
