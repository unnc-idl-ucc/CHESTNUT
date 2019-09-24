package com.chestnut.runtime.diagram;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

public class EvalChartFromFile extends Application {

    private static String title = "MAE - SB";
    
    public void start(Stage stage) {
        stage.setTitle("IBUBSB Scatter");
        final NumberAxis xAxis = new NumberAxis(0, 200, 5);
        final NumberAxis yAxis = new NumberAxis(0, 1, 0.1);        
        final LineChart<Number,Number> sc = new LineChart<Number,Number>(xAxis,yAxis);
        xAxis.setLabel("RLS");                
        yAxis.setLabel(title);
        sc.setTitle(title);
        
        
        XYChart.Series<Number, Number> SBseries = new XYChart.Series<Number, Number>();
        SBseries.setName("SB");
        
        try {
            BufferedReader MAEreader = new BufferedReader(new FileReader("data/eval/SBMAE.csv"));
            
            String maeLineHolder;
            String[] maeLineSplit;
            MAEreader.readLine();
            while((maeLineHolder = MAEreader.readLine())!=null) {
                maeLineSplit = maeLineHolder.split(",");
                
                SBseries.getData().add(new XYChart.Data<Number, Number>(Integer.valueOf(maeLineSplit[0]), Double.valueOf(maeLineSplit[2])));
            }
            
            sc.getData().addAll(SBseries);
            Scene scene  = new Scene(sc, 1260, 720);
            stage.setScene(scene);
            stage.show();
            
            
            
            MAEreader.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        launch(args);
    }
}
