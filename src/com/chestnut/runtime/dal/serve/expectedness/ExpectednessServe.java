package com.chestnut.runtime.dal.serve.expectedness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.log.LogSession;

public class ExpectednessServe {

    private Map<String, String> _globalSeriesMap, _topXMap, _expandedItemMap;
    private String[] _servedUserItems;
    private LogSession _sysLogSession;
    private List<String> _intersectItemList;
    
    public ExpectednessServe(Map<String, String> globalSeriesMap, Map<String, String> topXMap, String[] servedUserItems, LogSession sysLogSession) {
        _globalSeriesMap = globalSeriesMap;
        _topXMap = topXMap;
        _servedUserItems = servedUserItems;
        _sysLogSession = sysLogSession;
        _expandedItemMap = new HashMap<String, String>();
    }
    
    public List<String> BuildUnexpectedList(List<String> recommendItems, boolean includeKnownItems){
        BuildIntersectList(recommendItems);
        
        System.out.println("[ExpectednessServe] before filter, recommend list size is " + recommendItems.size());
        
        
        //_sysLogSession.SetRowKeyValue(String.valueOf(recommendItems.size()), "N_all_target_user_items"); // serenUsed
        //_sysLogSession.SetRowKeyValue(String.valueOf(_servedUserItems.length), "N_all_active_user_items");
        _sysLogSession.SetRowKeyValueList(recommendItems, "all_items");
        List<String> resultsHolder = new ArrayList<String>();
        
        BuildSeriesExpandedList();
        
        for(int i=0; i<recommendItems.size(); i++) {
            //System.out.println("[ExpectednessServe] recommend item " + i + ": " + recommendItems.get(i));
            if(!_topXMap.containsKey(recommendItems.get(i))) {
                if(!_expandedItemMap.containsKey(recommendItems.get(i))) {
                    //System.out.println("[ExpectednessServe] remove item: " + recommendItems.get(i));
                    resultsHolder.add(recommendItems.get(i));
                }
                
            }
            
        }
        
        System.out.println("[ExpectednessServe] after filter, recommend list size is " + resultsHolder.size());
        
        //_sysLogSession.SetRowKeyValue(String.valueOf(resultsHolder.size()), "N_unexpected_target_user_items");// serenUsed
        _sysLogSession.SetRowKeyValueList(resultsHolder, "all_items_unexpected");
        
        
       return resultsHolder;
        
        
    }
    
    private void BuildIntersectList(List<String> recommendItems) {
        _intersectItemList = new ArrayList<String>();
        for(int i=0; i<_servedUserItems.length; i++) {
            if(recommendItems.contains(_servedUserItems[i])) {
                _intersectItemList.add(_servedUserItems[i]);
            }
        }
    }
    
    public List<String> getIntersectRecommendList() {
        return _intersectItemList;
    }
    
    public LogSession GetLog() {
        return _sysLogSession;
    }
    
    /**** Private ****/
    /*---------------*/
    
    private void BuildSeriesExpandedList() {
        
        for(int i=0; i<_servedUserItems.length; i++) {
            //System.out.println("[ExpectednessServe] user item: " + _servedUserItems[i]);
            if(_globalSeriesMap.containsKey(_servedUserItems[i])) {
                //System.out.println("[ExpectednessServe] contain item: " + _servedUserItems[i]);
                ExpandListBySeries(_globalSeriesMap.get(_servedUserItems[i]));
            }else {
                _expandedItemMap.put(_servedUserItems[i], _servedUserItems[i]);
            }
        }
    }
    
    private void ExpandListBySeries(String seriesId) {
        for(Map.Entry<String, String> e : _globalSeriesMap.entrySet()) {
            if(e.getValue().equals(seriesId)) {
                if(!_expandedItemMap.containsKey(e.getKey())) {
                    //if(seriesId.equals("56")) System.out.println("[ExpectednessServe] contain item in series 56: " + e.getKey());
                    _expandedItemMap.put(e.getKey(), e.getKey());
                }
            }
        }
    }
}
