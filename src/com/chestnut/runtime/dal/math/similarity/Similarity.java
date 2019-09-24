package com.chestnut.runtime.dal.math.similarity;

import com.chestnut.runtime.dal.ma.DataSession;

public abstract class Similarity {
	protected DataSession _dataV, _dataU;
	
	public Similarity(DataSession dataV, DataSession dataU){
		_dataV = dataV;
		_dataU = dataU;
	}

}
