package org.example.distributed;

import mpi.*;
import org.example.Location;
import org.example.Location2;
import org.example.Utils;
import java.io.*;
import java.util.*;
import java.io.FileWriter;
import com.google.gson.Gson;

public class distributed {

    public static void main(String[] args) throws Exception {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        //0 and 1 x
        int numClusters = Integer.parseInt(args[args.length - 2]);
        int maxCycles = Integer.parseInt(args[args.length - 1]);

        Location2[] allLocations = null;
        Location[] locations = null;

        if (rank == 0) {
            allLocations = Utils.gson();
            //once again because of colors
            locations = Utils.convertToLocation(allLocations);
        }

        int totalPoints;
        if (rank == 0) {
            totalPoints = locations.length;
        } else {
            totalPoints = 0;
        }

        int[] items = new int[size]; //items per process
        int[] start = new int[size];

        if (rank == 0) {
            int numItems = totalPoints / size;
            int remainder = totalPoints % size; //leftover stuff
            int offset = 0;

            for (int i = 0; i < size; i++) {
                if (i < remainder) {
                    items[i] = numItems + 1;
                } else {
                    items[i] = numItems;
                }

                start[i] = offset;
                offset += items[i];
            }
        }

        int[] localItems = new int[1];
        MPI.COMM_WORLD.Scatter(items, 0, 1, MPI.INT, localItems, 0, 1, MPI.INT, 0);
        Location[] localPoints = new Location[localItems[0]];

        Object[] send = null;
        if (rank == 0) {
            send = new Object[locations.length];
            for (int i = 0; i < locations.length; i++) {
                send[i] = locations[i];
            }
        }
        Object[] receive = new Object[localItems[0]];

        MPI.COMM_WORLD.Scatterv(send, 0, items, start, MPI.OBJECT, receive, 0, localItems[0], MPI.OBJECT, 0);
        for (int i = 0; i < localItems[0]; i++) {
            localPoints[i] = (Location) receive[i];
        }

        //random centroids
        Location[] centroids = new Location[numClusters];
        Random rnd = new Random();

        if (rank == 0) {
            for (int i = 0; i < numClusters; i++) {
                double la = 47.0 + (55.0 - 47.0) * rnd.nextDouble();
                double lo = 5.0 + (15.0 - 5.0) * rnd.nextDouble();
                centroids[i] = new Location("Centroid " + (i + 1), 0, la, lo, Utils.generateRandomColor());
            }
        }

        MPI.COMM_WORLD.Bcast(centroids, 0, numClusters, MPI.OBJECT, 0);



        int cycles = 0;
        boolean changed = true;

        int[] labels = new int[localPoints.length];
        double[] sumLat = new double[numClusters];
        double[] sumLon = new double[numClusters];
        int[] count = new int[numClusters];

        long startTime = 0;
        if (rank == 0) {
            startTime = System.nanoTime();
        }


        while (changed && cycles < maxCycles) {
            boolean localChanged = false;
            Arrays.fill(sumLat, 0);
            Arrays.fill(sumLon, 0);
            Arrays.fill(count, 0);

            for (int i = 0; i < localPoints.length; i++) {
                Location l = localPoints[i];
                int best = -1;
                double distance = Double.MAX_VALUE;

                for (int c = 0; c < numClusters; c++) {
                    double d = Utils.distance(l, centroids[c]);
                    if (d < distance) {
                        distance = d;
                        best = c;
                    }
                }

                if (labels[i] != best) {
                    labels[i] = best;
                    localChanged = true;
                }

                sumLat[best] += l.la;
                sumLon[best] += l.lo;
                count[best]++;
            }

            double[] globalSumLat = new double[numClusters];
            double[] globalSumLon = new double[numClusters];
            int[] globalCount = new int[numClusters];

            MPI.COMM_WORLD.Reduce(sumLat, 0, globalSumLat, 0, numClusters, MPI.DOUBLE, MPI.SUM, 0);
            MPI.COMM_WORLD.Reduce(sumLon, 0, globalSumLon, 0, numClusters, MPI.DOUBLE, MPI.SUM, 0);
            MPI.COMM_WORLD.Reduce(count, 0, globalCount, 0, numClusters, MPI.INT, MPI.SUM, 0);

            boolean[] globalChangedArr = new boolean[1];

            if (rank == 0) {
                changed = false;
                for (int c = 0; c < numClusters; c++) {
                    if (globalCount[c] == 0) {
                        double la = 47.0 + (55.0 - 47.0) * rnd.nextDouble();
                        double lo = 5.0 + (15.0 - 5.0) * rnd.nextDouble();
                        centroids[c] = new Location("Centroid " + (c + 1), 0, la, lo, Utils.generateRandomColor());
                        changed = true;
                    } else {
                        double newLa = globalSumLat[c] / globalCount[c];
                        double newLo = globalSumLon[c] / globalCount[c];
                        if (Math.abs(newLa - centroids[c].la) > 0.0001 || Math.abs(newLo - centroids[c].lo) > 0.0001) {
                            centroids[c].la = newLa;
                            centroids[c].lo = newLo;
                            changed = true;
                        }
                    }
                }
                globalChangedArr[0] = changed;
            }

            MPI.COMM_WORLD.Bcast(globalChangedArr, 0, 1, MPI.BOOLEAN, 0);
            MPI.COMM_WORLD.Bcast(centroids, 0, numClusters, MPI.OBJECT, 0);
            changed = globalChangedArr[0];

            cycles++;
        }

        if (rank == 0) {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double seconds = duration / 1_000_000_000.0;
            System.out.println("Total computation time: " + seconds + " seconds");
        }


        if (rank == 0) {
            System.out.println("Distributed cycles: " + cycles);
            for (int c = 0; c < numClusters; c++) {
                System.out.println("Centroid " + c + ": (" + centroids[c].la + ", " + centroids[c].lo + ")");
            }

            try (FileWriter writer = new FileWriter("centroids.json")) {
                Gson gson = new Gson();
                gson.toJson(centroids, writer);
                System.out.println("saved to centroids.json");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        MPI.Finalize();
    }
}
