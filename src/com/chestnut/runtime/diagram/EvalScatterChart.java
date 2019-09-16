package com.chestnut.runtime.diagram;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.ScatterChart;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

public class EvalScatterChart extends Application {

    private static int dataFieldIndex = 1;
    private static String title = "Unexpectedness";
    
    @SuppressWarnings("unchecked")
    public void start(Stage stage) {
        stage.setTitle("IBUBSB Scatter");
        final NumberAxis xAxis = new NumberAxis(0, 200, 5);
        final NumberAxis yAxis = new NumberAxis(0, 1, 0.1);        
        final LineChart<Number,Number> sc = new LineChart<Number,Number>(xAxis,yAxis);
        xAxis.setLabel("RLS");                
        yAxis.setLabel(title);
        sc.setTitle(title + " IBUBSB");
        
        
        XYChart.Series<Number, Number> IBseries = new XYChart.Series<Number, Number>();
        IBseries.setName("IB");
        XYChart.Series<Number, Number> UBseries = new XYChart.Series<Number, Number>();
        UBseries.setName("UB");
        XYChart.Series<Number, Number> SBseries = new XYChart.Series<Number, Number>();
        SBseries.setName("SB");
        
        try {
            BufferedReader IBUBSBreader = new BufferedReader(new FileReader("data/peng/result/SB_Count_Pre_42_5_counts.csv"));
            
            String LineHolder;
            String[] lineRecordsHolder, IBHolder, UBHolder, SBHolder;
            
            Map<Integer, Double> sumIB = new HashMap<Integer, Double>();
            Map<Integer, Double> sumUB = new HashMap<Integer, Double>();
            Map<Integer, Double> sumSB = new HashMap<Integer, Double>();
            Map<Integer, Integer> countsIB = new HashMap<Integer, Integer>();
            Map<Integer, Integer> countsUB = new HashMap<Integer, Integer>();
            Map<Integer, Integer> countsSB = new HashMap<Integer, Integer>();
            
            int linePtr = 3, linecouter = 0;
            while((LineHolder = IBUBSBreader.readLine())!=null) {
                linecouter ++;
                lineRecordsHolder = LineHolder.split(",");
                //linePtr++;
                System.out.println("Line " + linePtr + " starts to draw. To the " + linecouter + "th line of all.");
                if(!(lineRecordsHolder[dataFieldIndex].equals("NE")||lineRecordsHolder[dataFieldIndex].equals("NA"))) {
                    if(linePtr == 1) { //IB Line
                        System.out.println("IB -----");
                        IBHolder = lineRecordsHolder[dataFieldIndex].split("\\|");
                        Double sumHolder = 0.0, Unxpectedness = 0.0;
                        for(int i=0; i<IBHolder.length; i++) {
                            
                            sumHolder = sumHolder + Double.valueOf(IBHolder[i]);
                            Unxpectedness = sumHolder/((i+1)*5);
                            
                            if(sumIB.containsKey(i)) {
                                sumIB.put(i, sumIB.get(i) + Unxpectedness);
                                countsIB.put(i, countsIB.get(i) + 1);
                            }else {
                                sumIB.put(i, Unxpectedness);
                                countsIB.put(i, 1);
                            }
                            
                            
                            //IBseries.getData().add(new XYChart.Data<Number, Number>((i+1)*5.0, Unxpectedness));
                        }
                        
                        
                    }else if(linePtr == 2){ //UB Line
                        System.out.println("UB -----");
                        UBHolder = lineRecordsHolder[dataFieldIndex].split("\\|");
                        Double sumHolder = 0.0, Unxpectedness = 0.0;
                        for(int i=0; i<UBHolder.length; i++) {
                            
                            sumHolder = sumHolder + Double.valueOf(UBHolder[i]);
                            Unxpectedness = sumHolder/((i+1)*5);
                            
                            if(sumUB.containsKey(i)) {
                                sumUB.put(i, sumUB.get(i) + Unxpectedness);
                                countsUB.put(i, countsUB.get(i) + 1);
                            }else {
                                sumUB.put(i, Unxpectedness);
                                countsUB.put(i, 1);
                            }
                            
                            
                            //UBseries.getData().add(new XYChart.Data<Number, Number>((i+1)*5.0, Unxpectedness));
                            //System.out.println("Range " + ((i+1)*5.0) + ": Unxpectedness = " + Unxpectedness);
                        }
                        
                    }else {
                        //linePtr = 2;
                        
                        System.out.println("SB -----");
                        SBHolder = lineRecordsHolder[dataFieldIndex].split("\\|");
                        Double sumHolder = 0.0, Unxpectedness = 0.0;
                        System.out.println(SBHolder.length);
                        for(int i=0; i<SBHolder.length; i++) {
                            
                            sumHolder = sumHolder + Double.valueOf(SBHolder[i]);
                            Unxpectedness = sumHolder/((i+1)*5);
                            
                            if(sumSB.containsKey(i)) {
                                sumSB.put(i, sumSB.get(i) + Unxpectedness);
                                countsSB.put(i, countsSB.get(i) + 1);
                            }else {
                                sumSB.put(i, Unxpectedness);
                                countsSB.put(i, 1);
                            }
                            
                            
                            //UBseries.getData().add(new XYChart.Data<Number, Number>((i+1)*5.0, Unxpectedness));
                            //System.out.println("Range " + ((i+1)*5.0) + ": Unxpectedness = " + Unxpectedness);
                        }
                    }
                }else {
                    if(linePtr == 3) {
                        linePtr = 0;
                    }
                }
            }
            
            System.out.println("Size of sum IB: " + sumIB.size());
            System.out.println("Size of sum UB: " + sumUB.size());
            System.out.println("Size of sum SB: " + sumSB.size());
            System.out.println("Size of count IB: " + countsIB.size());
            System.out.println("Size of count UB: " + countsUB.size());
            System.out.println("Size of count SB: " + countsSB.size());
            
            double avgUnexpectedness;
            BufferedWriter evalResultHandler = new BufferedWriter(new FileWriter("data/peng/result/EvalResultsSBUnexpectedness_42_5.csv"));
            evalResultHandler.write("sub,counts,avg\n");
            
            for(int i=0; i<sumIB.size(); i++) {
                avgUnexpectedness = 0.0;
                avgUnexpectedness = sumIB.get(i)/countsIB.get(i);
                //evalResultHandler.write((i+1)*5 + "," + countsIB.get(i) + "," + avgUnexpectedness + "\n");
                IBseries.getData().add(new XYChart.Data<Number, Number>((i+1)*5.0, avgUnexpectedness));
            }
            
            for(int i=0; i<sumUB.size(); i++) {
                avgUnexpectedness = 0.0;
                avgUnexpectedness = sumUB.get(i)/countsUB.get(i);
                //evalResultHandler.write((i+1)*5 + "," + countsUB.get(i) + "," + avgUnexpectedness + "\n");
                UBseries.getData().add(new XYChart.Data<Number, Number>((i+1)*5.0, avgUnexpectedness));
            }
            
            for(int i=0; i<sumSB.size(); i++) {
                avgUnexpectedness = 0.0;
                avgUnexpectedness = sumSB.get(i)/countsSB.get(i);
                evalResultHandler.write((i+1)*5 + "," + countsSB.get(i) + "," + avgUnexpectedness + "\n");
                SBseries.getData().add(new XYChart.Data<Number, Number>((i+1)*5.0, avgUnexpectedness));
            }
            
            sc.getData().addAll(IBseries, UBseries, SBseries);
            Scene scene  = new Scene(sc, 1260, 720);
            stage.setScene(scene);
            stage.show();
            
            evalResultHandler.close();
            IBUBSBreader.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    
    public static void main(String[] args) {
        launch(args);
    }
}

