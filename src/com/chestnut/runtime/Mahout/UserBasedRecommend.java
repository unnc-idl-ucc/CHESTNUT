package com.chestnut.runtime.Mahout;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class UserBasedRecommend {

    private DataModel _globalDM;
    private UserSimilarity _similarity;
    
    public UserBasedRecommend(DataModel globalDM) {
        _globalDM = globalDM;
        // Implement similarity method.
        try {
            _similarity = new PearsonCorrelationSimilarity(_globalDM);
        } catch (TasteException e) {
            e.printStackTrace();
        }
    }
    
    public List<String> recommend(long userId, int recommendsScale) {
        // Build user neighborhood.
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.9, _similarity, _globalDM);
        
        
        // Create recommender.
        UserBasedRecommender recommender = new GenericUserBasedRecommender(_globalDM, neighborhood, _similarity);
        // Generate and collect recommends.
        List<RecommendedItem> recommendations;
        
        List<String> recommendationsOnly;
        recommendationsOnly = new ArrayList<String>();
        
        try {
            long[] allNeighborhood = neighborhood.getUserNeighborhood(userId);
            recommendations = recommender.recommend(userId, recommendsScale, false);
            for(RecommendedItem recommendation : recommendations){
                //System.out.println("User " + userId + ", Recommend Item " + recommendation.getItemID() + ", Estimate Rating " + recommendation.getValue());
                recommendationsOnly.add(String.valueOf(recommendation.getItemID()));
            }
            
            System.out.println("Neighbor size " + allNeighborhood.length);
        } catch (TasteException e) {
            e.printStackTrace();
        }
        
        return recommendationsOnly;
    }
    
}
