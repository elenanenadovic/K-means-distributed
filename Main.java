package org.example;

import com.gluonhq.maps.MapView;
import com.gluonhq.maps.MapPoint;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.stage.Stage;
import javafx.scene.Node;
import com.google.gson.Gson;
import java.io.FileReader;

public class Main extends Application {

    static PoiLayer poiLayer = new PoiLayer();

    @Override
    public void start(Stage stage) throws Exception {
        MapView mapView = new MapView();

        Gson gson = new Gson();
        Location[] centroids = gson.fromJson(new FileReader("centroids.json"), Location[].class);
        Location[] locations = gson.fromJson(new FileReader("locations.json"), Location[].class);

        drawing(centroids, locations, mapView);

        StackPane root = new StackPane(mapView);
        Scene scene = new Scene(root, 800, 600);
        stage.setTitle("Distributed");
        stage.setScene(scene);
        stage.show();
    }

    public static void drawing(Location[] centroids, Location[] locations, MapView mapView) {
        mapView.removeLayer(poiLayer);
        poiLayer = new PoiLayer();

        for (Location centroid : centroids) {
            MapPoint cityLocation = new MapPoint(centroid.la, centroid.lo);
            Color color = Color.web(centroid.color);
            Node cityIcon = new Circle(5, color);
            poiLayer.addPoint(cityLocation, cityIcon);
        }

        for (Location loc : locations) {
            MapPoint cityLocation = new MapPoint(loc.la, loc.lo);
            Color color = Color.web(loc.color);
            Node cityIcon = new Circle(1.5, color);
            poiLayer.addPoint(cityLocation, cityIcon);
        }

        mapView.addLayer(poiLayer);
    }

    public static void main(String[] args) {
        launch(args);
    }
}
