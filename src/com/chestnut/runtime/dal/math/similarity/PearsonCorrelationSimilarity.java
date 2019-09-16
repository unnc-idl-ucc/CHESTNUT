package com.chestnut.runtime.dal.math.similarity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.chestnut.runtime.dal.agent.ConfigManager;
import com.chestnut.runtime.dal.ma.DataSession;
import com.chestnut.runtime.dal.ma.ESVParser;

public class PearsonCorrelationSimilarity extends Similarity{
	
	private List<Double> _dataVItems, _dataUItems;
	private CompareResultSet _dataIntersection;
	private double _avgVItems, _avgUItems;
	private double[] _varianceWeights;
	private ConfigManager _cfgManager;
	private ESVParser _globalPearsonHandler;
	
	@SuppressWarnings("unused")
    private double _significanceWeight = 1.0;
	
	/**
	 * This class is driven by index one to one relationship
	 * @param dataV
	 * @param dataU
	 */
	public PearsonCorrelationSimilarity(DataSession dataV, DataSession dataU){
		super(dataV, dataU);
		_dataVItems = new ArrayList<Double>();
		_dataUItems = new ArrayList<Double>();
		
	}
	
	public PearsonCorrelationSimilarity(DataSession dataV, DataSession dataU, ConfigManager cfgManager){
        super(dataV, dataU);
        _dataVItems = new ArrayList<Double>();
        _dataUItems = new ArrayList<Double>();
        _cfgManager = cfgManager;
    }
	
	public void SetGlobalPearsonHandler(ESVParser globalPearsonHandler) {
	    _globalPearsonHandler = globalPearsonHandler;
	}
	
	public double ExecuteSimilarity(String CorrFieldName, String ValueFieldName){
		InitExecution(CorrFieldName, ValueFieldName);
		
		if(_globalPearsonHandler!=null) {
		    List<List<String>> userAKNList;
	        if((userAKNList = _globalPearsonHandler.GetARecordSet(_dataV.sessionName))!=null) {
	            List<String> userListKN = userAKNList.get(0);
	            List<String> psListKN = userAKNList.get(1);
	            for(int i=0; i<userListKN.size(); i++) {
	                if(_dataV.sessionName.equals(userListKN.get(i))) {
	                    return Double.valueOf(psListKN.get(i));
	                }
	            }
	        }
		}
		
		double weightVU;
		double avgSubstractV, avgSubstractU;
		double itemHolderV, itemHolderU;
		double sumA = 0, sumB = 0, sumC = 0;
		for(int i=0; i<_dataIntersection.IntersectionSize; i++){
			itemHolderV = _dataVItems.get(i);
			itemHolderU = _dataUItems.get(i);
			avgSubstractV = itemHolderV - _avgVItems;
			//System.out.println("[Tracing]PearsonCorrelationSimilarity.ExecuteSimilarity: avgSubstractV = " + avgSubstractV);
			avgSubstractU = itemHolderU - _avgUItems;
			sumA = sumA + (avgSubstractV * avgSubstractU);
			sumB = sumB + (avgSubstractV * avgSubstractV);
			sumC = sumC + (avgSubstractU * avgSubstractU);
			//System.out.println("[Tracing] PearsonCorrelationSimilarity.ExecuteSimilarity: sumB = " + sumB + ", itemHolderV = " + itemHolderV + ", _avgVItems = " + _avgVItems);
		}
		//System.out.println("[Tracing] PearsonCorrelationSimilarity.ExecuteSimilarity: _dataIntersection.size = " + _dataIntersection.IntersectionSize);
		/*
		System.out.println("[Tracing] PearsonCorrelationSimilarity.ExecuteSimilarity: " + 
		                   "dataV is " + super._dataV.sessionName + ", dataU is " + super._dataU.sessionName + 
		                   ", sumA = " + sumA + ", sumB = " + sumB + ", sumC = " + sumC);
		*/
		weightVU = sumA/(Math.sqrt(sumB)*Math.sqrt(sumC));
		/*
		if(Math.abs(weightVU)>0.5) {
		    System.out.println("[Tracing] PearsonCorrelationSimilarity.ExecuteSimilarity: weightVU is " + Math.abs(weightVU));
		}
		*/
		//pause();
		
		//return Math.abs(weightVU);
		return weightVU;
		
	}
	
	public double ExecuteVarianceWeightedSimilarity(String CorrFieldName, String ValueFieldName, String[] itemsToCalcVarianceWeight, Map<String, List<String>> itemRatingsOfNeighbors) {
		
		InitExecution(CorrFieldName, ValueFieldName);
		
		if(_cfgManager!=null&&_cfgManager.GetConfigVal("weightToggle")!=null) {
		    SetVarianceWeightSet(_cfgManager, itemsToCalcVarianceWeight, itemRatingsOfNeighbors);
		    InitVarianceWeightSetToOne(_dataIntersection.IntersectionSize);
		    
		    double swBoarder = Double.valueOf(_cfgManager.GetConfigVal("pearsonSignificanceWeightBoarder"));
		    double intersectSize = _dataIntersection.IntersectionSize;
		    if(intersectSize<swBoarder) {
		        //SetSignificanceWeight(intersectSize/swBoarder);
		    }
		}else {
		    InitVarianceWeightSetToOne(_dataIntersection.IntersectionSize);
		}
		
		double weightUV = 0.0;
		double avgSubstractU, avgSubstractV;
		double itemHolderU, itemHolderV;
		double sumA = 0, sumB = 0, sumC = 0;
		for(int i=0; i<_dataIntersection.IntersectionSize; i++){
			itemHolderU = _dataUItems.get(i);
			itemHolderV = _dataVItems.get(i);
			//System.out.println("[Tracing]PearsonCorrelationSimilarity.ExecuteSimilarity: avgSubstractV = " + avgSubstractV);
			avgSubstractU = itemHolderU - _avgUItems;
			avgSubstractV = itemHolderV - _avgVItems;
			sumA = sumA + (avgSubstractV * avgSubstractU) * _varianceWeights[i];
			sumB = sumB + (avgSubstractV * avgSubstractV) * _varianceWeights[i];
			sumC = sumC + (avgSubstractU * avgSubstractU);
			//System.out.println("[Tracing] PearsonCorrelationSimilarity.ExecuteSimilarity: sumB = " + sumB + ", itemHolderV = " + itemHolderV + ", _avgVItems = " + _avgVItems);
		}
		
		return weightUV;
	}
	
	private void SetVarianceWeightSet(ConfigManager cfManager, String[] itemsToCalcVarianceWeight, Map<String, List<String>> itemRatingsOfNeighbors) {
	    VarianceWeightCalc varianceWeightHandler = new VarianceWeightCalc(cfManager, itemRatingsOfNeighbors);
        _varianceWeights = varianceWeightHandler.GetVarianceSet(itemsToCalcVarianceWeight);
        
        /*
        for(int i=0; i<_varianceWeights.length; i++) {
            System.out.println("[Tracing] PearsonCorrelationSimilarity.SetVarianceWeightSet: index " + i + " is " + _varianceWeights[i]);
        }
        */
        
	}
	
	@SuppressWarnings("unused")
    private void SetSignificanceWeight(double significanceWeight) {
	    _significanceWeight = significanceWeight;
	}
	
	private void InitExecution(String CorrFieldName, String ValueFieldName) {
		DataSessionComparator dsc = new DataSessionComparator(_dataV, _dataU, CorrFieldName, ValueFieldName);
		_dataIntersection = dsc.GetIntersection();
		double itemHolderV, itemHolderU;
		double itemSumV = 0, itemSumU = 0;
		double itemsSize = _dataIntersection.IntersectionSize;
		for(int i=0; i<itemsSize; i++){
			itemHolderV = Double.parseDouble(_dataV.GetARecordByIndex(ValueFieldName, _dataIntersection._sameRecordIndexV.get(i)));
			itemSumV = itemSumV + itemHolderV;
			_dataVItems.add(itemHolderV);
			itemHolderU = Double.parseDouble(_dataU.GetARecordByIndex(ValueFieldName, _dataIntersection._sameRecordIndexU.get(i)));
			itemSumU = itemSumU + itemHolderU;
			_dataUItems.add(itemHolderU);
		}
		_avgVItems = itemSumV/itemsSize;
		_avgUItems = itemSumU/itemsSize;
	}
	
	private void InitVarianceWeightSetToOne(int size) {
	    _varianceWeights = new double[size];
	    for(int i=0; i<size; i++) {
	        _varianceWeights[i] = 1.0;
	    }
	}
	
	@SuppressWarnings("unused")
    private void pause() {
	    Scanner s=new Scanner(System.in);
	    s.nextLine().trim();
	    s.close();
	}

}
