package org.example.distributed;

import mpi.*;
import org.example.Location;
import org.example.Location2;
import org.example.Utils;
import com.google.gson.Gson;
import java.io.*;
import java.util.*;

public class distributed {

    public static void main(String[] args) throws Exception {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();

        boolean testing = false;
        int whichTest = 0;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--test")) {
                testing = true;
            }
            if (args[i].equals("1") || args[i].equals("2")) {
                whichTest = Integer.parseInt(args[i]);
            }
        }

        if (testing) {
            while (true) {
                boolean[] testingStop = new boolean[1];
                int[] parameters = new int[3];
                if (rank == 0) {
                    testingStop[0] = Test.shouldRunTest(whichTest);
                    if (testingStop[0]) {
                        parameters[0] = Test.getNumClusters(whichTest);
                        parameters[1] = 1000;
                        parameters[2] = Test.getAccumulationSites(whichTest);
                    }
                }

                MPI.COMM_WORLD.Bcast(testingStop, 0, 1, MPI.BOOLEAN, 0);
                if (!testingStop[0]) {
                    break;
                }

                MPI.COMM_WORLD.Bcast(parameters, 0, 3, MPI.INT, 0);
                distributedkmeans(parameters[0], parameters[1], parameters[2], testing);

                if (rank == 0) {
                    Test.results(whichTest);
                }
            }

            MPI.Finalize();
            return;
        }

        //ne radi 0 i 1
        int numClusters = Integer.parseInt(args[args.length - 2]);
        int accumulationSites = Integer.parseInt(args[args.length - 1]);

        //i limited max cycles to 200, project description said it
        distributedkmeans(numClusters, 200, accumulationSites, false);

        MPI.Finalize();
    }

    public static void distributedkmeans(int numClusters, int maxCycles, int accumulationSites, boolean testing) throws Exception {
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();

        if (rank == 0 && testing) {
            DistributedTimer.startTime = System.nanoTime();
        }
        Location2[] allLocations = null;
        Location[] locations = null;

        if (rank == 0) {
            allLocations = Utils.gson();
            if (accumulationSites > allLocations.length) {
                allLocations = Utils.additionalLocations(allLocations, accumulationSites);
            } else {
                allLocations = Arrays.copyOf(allLocations, accumulationSites);
            }
            locations = Utils.convertToLocation(allLocations);
        }

        int totalPoints;
        if (rank == 0) {
            totalPoints = locations.length;
        } else {
            totalPoints = 0;
        }

        int[] items = new int[size];
        int[] start = new int[size];

        if (rank == 0) {
            int numItems = totalPoints / size;
            int remainder = totalPoints % size;
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
            send = Arrays.copyOf(locations, locations.length, Object[].class);
        }
        Object[] receive = new Object[localItems[0]];

        MPI.COMM_WORLD.Scatterv(send, 0, items, start, MPI.OBJECT, receive, 0, localItems[0], MPI.OBJECT, 0);
        for (int i = 0; i < localItems[0]; i++) {
            localPoints[i] = (Location) receive[i];
        }

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

        int[] labels = new int[localPoints.length];
        double[] sumLat = new double[numClusters];
        double[] sumLon = new double[numClusters];
        int[] count = new int[numClusters];

        long startTime = 0;
        if (rank == 0) {
            startTime = System.nanoTime();
        }

        int cycles = 0;
        boolean changed = true;
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

            if (!testing) {
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

                try (FileWriter writer = new FileWriter("locations.json")) {
                    Gson gson = new Gson();
                    for (Location loc : locations) {
                        int best = -1;
                        double minDist = Double.MAX_VALUE;
                        for (int c = 0; c < numClusters; c++) {
                            double d = Utils.distance(loc, centroids[c]);
                            if (d < minDist) {
                                minDist = d;
                                best = c;
                            }
                        }
                        loc.color = centroids[best].color;
                    }
                    gson.toJson(locations, writer);
                    System.out.println("saved to locations.json");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}



class Test {
    private static int currentSites = 500;
    private static int currentClusters = 5;
    private static final int addSites = 500;
    private static final int addClusters = 5;
    private static final int maxSites = 300000;
    private static final int sitesTest2 = 30000;
    private static final int runs = 3;
    private static int currentRun = 0;
    private static long totalTime = 0;

    public static boolean shouldRunTest(int testModeType) {
        if (testModeType == 1 && currentSites > maxSites) {
            return false;
        }
        if (testModeType == 2 && currentClusters > sitesTest2 / 3) {
            return false;
        }
        return true;
    }

    public static int getNumClusters(int testMode) {
        if (testMode == 1) {
            return 20;
        } else {
            return currentClusters;
        }
    }

    public static int getAccumulationSites(int testMode) {
        if (testMode == 1) {
            return currentSites;
        } else {
            return sitesTest2;
        }
    }

    public static void results(int testModeType) {
        currentRun++;
        long duration = System.nanoTime() - DistributedTimer.startTime;
        totalTime += duration / 1_000_000;

        if (currentRun == runs) {
            long avgTime = totalTime / runs;
            if (testModeType == 1) {
                System.out.println(currentSites + "|" + 20 + "|" + avgTime);
                if (avgTime > 120000) {
                    currentSites = maxSites + 1;
                } else {
                    currentSites = currentSites + addSites;
                }
            } else {
                System.out.println(sitesTest2 + "|" + currentClusters + "|" + avgTime);
                if (avgTime > 120000) {
                    currentClusters = (sitesTest2 / 3) + 1;
                } else {
                    currentClusters = currentClusters + addClusters;
                }
            }
            currentRun = 0;
            totalTime = 0;
        }
    }
}

class DistributedTimer {
    public static long startTime = 0;
}
