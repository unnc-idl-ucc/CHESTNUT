package com.chestnut.runtime.dal.math.similarity;

import java.util.List;
import java.util.Map;

import com.chestnut.runtime.dal.agent.ConfigManager;

public class VarianceWeightCalc {
    
    private double[] _varianceWeightSet;
    
    @SuppressWarnings("unused")
	private ConfigManager _cfgManager;
    
    private int _varianceMaxIndex, _varianceMinIndex;
    private Map<String, List<String>> _itemRatingsOfNeighbors;
    
    public VarianceWeightCalc(ConfigManager cfgManager, Map<String, List<String>> itemRatingsOfNeighbors) {
        _cfgManager = cfgManager;
        _itemRatingsOfNeighbors = itemRatingsOfNeighbors;
    }
    
    public double[] GetVarianceSet(String[] itemIdSet) {
        int coRatedSize = itemIdSet.length;
        double[] avgVarianceSet = new double[coRatedSize];
        String itemIdHolder;
        
        for(int i=0; i<coRatedSize; i++) {
            itemIdHolder = itemIdSet[i];
            avgVarianceSet[i] = CalcOneItemRatingsVariance(itemIdHolder);
        }
        _varianceWeightSet = new double[coRatedSize];
        CalcAllItemsRatingsVarianceWeight(avgVarianceSet);
        return _varianceWeightSet;
    }
    
    private void CalcAllItemsRatingsVarianceWeight(double[] avgVarianceSet) {
        // Find the maximum and minimum variance rating of an item.
        FindBoarderVarianceItemIndex(avgVarianceSet);
        // Calculate the variance weight for each item.
        for(int i=0; i<avgVarianceSet.length; i++) {
            _varianceWeightSet[i] = (avgVarianceSet[i]-avgVarianceSet[_varianceMinIndex])/avgVarianceSet[_varianceMaxIndex];
        }
    }
    
    private void FindBoarderVarianceItemIndex(double[] avgVarianceSet) {
        _varianceMaxIndex = 0;
        _varianceMinIndex = 0;
        for(int i=1; i<avgVarianceSet.length; i++) {
            if(avgVarianceSet[i]>avgVarianceSet[_varianceMaxIndex]) {
                _varianceMaxIndex = i;
            }
            if(avgVarianceSet[i]<avgVarianceSet[_varianceMinIndex]) {
                _varianceMinIndex = i;
            }
        }
    }
    
    private double CalcOneItemRatingsVariance(String itemId) {
        
        if(_itemRatingsOfNeighbors.containsKey(itemId)) {
        	List<String> itemCoRatedUserRatingHolder = _itemRatingsOfNeighbors.get(itemId);
        	Double[] itemRatings = new Double[itemCoRatedUserRatingHolder.size()];
        	double sumRatings = 0.0, avgRatings = 0.0;
        	
        	for(int i=0; i<itemRatings.length; i++) {
        		String ratingStr = itemCoRatedUserRatingHolder.get(i);
        		itemRatings[i] = Double.valueOf(ratingStr.substring(ratingStr.indexOf("_")+1));
        		
        		sumRatings = sumRatings + itemRatings[i];
        	}
        	avgRatings = sumRatings/itemRatings.length;
        	
        	double sumVariance = 0.0;
            for(int i=0; i<itemRatings.length; i++) {
                sumVariance = sumVariance + CalcVariance(itemRatings[i], avgRatings);
            }
            double avgVariance = sumVariance/Double.valueOf(itemRatings.length-1);
        	
            return avgVariance;
        }else {
        	System.out.println("[WARN] VarianceWeightCalc.CalcOneItemRatingsVariance: The item-" + itemId + " is not the co-rated item between active user and its neighbors.");
        	
        	return -1.0;
        }
        
    }
    
    private double CalcVariance(double rating, double avgR) {
        double variance = (rating-avgR)*(rating-avgR);
        return variance;
    }
}
