package com.chestnut.runtime.dal.serve.useful;

public class PredictionCalc {
    
    public PredictionCalc() {
        
    }
    
    public Double CalcWSOR(PredictionSet calcSet, Double currentUIRatingAvg) {
        Double sumA = 0.0, sumB = 0.0, result;
        Double ratingUI = 0.0, ratingUAvg = 0.0, weightAU = 0.0;
        String[] rowHold;
        
        for(int i=0; i<calcSet.dataRecordSize; i++) {
            rowHold = calcSet.GetARow(i);
            ratingUI = Double.parseDouble(rowHold[1]);
            ratingUAvg = Double.parseDouble(rowHold[2]);
            weightAU = Double.parseDouble(rowHold[3]);
            
            sumA = sumA + (ratingUI - ratingUAvg)*weightAU;
            sumB = sumB + Math.abs(weightAU);
        }
        
        if(sumB!=0.0) {
            result = currentUIRatingAvg + sumA/sumB;
            if(result>5.0) {
                /**
                System.out.println("---------------------------------------------------");
                System.out.println("[Tracing] PredictionCalc.CalcWSOR: sumA is " + sumA);
                System.out.println("[Tracing] PredictionCalc.CalcWSOR: sumB is " + sumB);
                **/
            }
            return result;
        }else {
            System.out.println("[WARN] PredictionCalc.CalcWSOR, denominator is zero!");
            return 0.0;
        }
    }
}
