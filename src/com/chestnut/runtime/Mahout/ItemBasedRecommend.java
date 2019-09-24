package com.chestnut.runtime.Mahout;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

public class ItemBasedRecommend {

    private DataModel _globalDM;
    private ItemSimilarity _similarity;
    
    public ItemBasedRecommend(DataModel globalDM) {
        _globalDM = globalDM;
        // Implement similarity method.
        try {
            _similarity = new PearsonCorrelationSimilarity(_globalDM);
        } catch (TasteException e) {
            e.printStackTrace();
        }
    }
    
    public List<String> recommend(long userId, int recommendsScale) {
        // Create recommender.
        GenericItemBasedRecommender recommender =  new GenericItemBasedRecommender(_globalDM, _similarity);
        // Generate and collect recommends.
        List<RecommendedItem> recommendations;
        List<String> recommendationsOnly;
        recommendationsOnly = new ArrayList<String>();
        try {
            recommendations = recommender.recommend(userId, recommendsScale, true);
            for(RecommendedItem recommendation : recommendations){
                //System.out.println("User " + userId + ", Recommend Item " + recommendation.getItemID() + ", Estimate Rating " + recommendation.getValue());
                recommendationsOnly.add(String.valueOf(recommendation.getItemID()));
            }
        } catch (TasteException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return recommendationsOnly;
    }
    
}
