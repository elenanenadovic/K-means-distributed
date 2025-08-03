package org.example;

import com.google.gson.Gson;

import java.io.*;
import java.util.Random;

public class Utils {

    public static Location2[] gson() throws IOException {
        InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream("cities.json");
        if (inputStream == null) {
            throw new FileNotFoundException("cities.json not found");
        }
        InputStreamReader reader = new InputStreamReader(inputStream);
        Gson gson = new Gson();
        Location2[] data = gson.fromJson(reader, Location2[].class);
        reader.close();
        return data;
    }

    public static String generateRandomColor() {
        Random rand = new Random();
        int r = rand.nextInt(256);
        int g = rand.nextInt(256);
        int b = rand.nextInt(256);
        return String.format("#%02X%02X%02X", r, g, b);
    }

    public static Location2[] additionalLocations(Location2[] original, int newSize) {
        Random rnd = new Random();
        Location2[] additionalLocations = new Location2[newSize];

        for (int i = 0; i < newSize; i++) {
            if (i < original.length) {
                additionalLocations[i] = original[i];
            } else {
                double la = 47.0 + rnd.nextDouble() * (55.0 - 47.0);

                double lo = 5.0 + rnd.nextDouble() * (15.0 - 5.0);
                additionalLocations[i] = new Location2("New " + i, lo, la, rnd.nextInt(100));
            }
        }
        return additionalLocations;
    }

    public static double distance(Location location1, Location location2) {
        int earthRadius = 6371;
        double lat1 = Math.toRadians(location1.la);
        double lat2 = Math.toRadians(location2.la);
        double lon1 = Math.toRadians(location1.lo);
        double lon2 = Math.toRadians(location2.lo);
        double deltaLat = lat2 - lat1;
        double deltaLon = lon2 - lon1;
        double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) + Math.cos(lat1) * Math.cos(lat2)  * Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return earthRadius * c;
    }

    public static Location[] convertToLocation(Location2[] locations2) {
        Location[] locations = new Location[locations2.length];
        for (int i = 0; i < locations2.length; i++) {
            locations[i] = new Location(
                    locations2[i].name,
                    locations2[i].capacity,
                    locations2[i].la,
                    locations2[i].lo,
                    generateRandomColor()
            );
        }
        return locations;
    }
}
