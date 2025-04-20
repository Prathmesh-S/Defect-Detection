package main.java.solution;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.*;

public class DBScanFunction implements MapFunction<Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>, JSONObject> {

    @Override
    public JSONObject map(Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>> in) throws Exception {
        JSONObject batch = in.f0;
        List<OutlierDetectionFunction.OutlierPoint> outliers = in.f1;
        batch.put("outlier_count", outliers.size());

        // Adjusted parameters to match Python's DBSCAN.
        double eps = 20.0;
        int minPts = 5;

        // Perform DBSCAN clustering in 2D (using row and col).
        List<DBScanCluster> clusters = dbScan(outliers, eps, minPts);

        // Use a minheap to keep track of the top 10 clusters by size.
        PriorityQueue<DBScanCluster> topClusters = new PriorityQueue<>(Comparator.comparingInt(c -> c.points.size()));
        for (DBScanCluster cluster : clusters) {
            topClusters.offer(cluster);
            if (topClusters.size() > 10) {
                topClusters.poll();
            }
        }

        // Extract top clusters into a list sorted descending by cluster size.
        List<DBScanCluster> topClusterList = new ArrayList<>(topClusters);
        topClusterList.sort((a, b) -> Integer.compare(b.points.size(), a.points.size()));

        // Compute centroids for each top cluster.
        JSONArray centroidsArray = new JSONArray();
        for (DBScanCluster cluster : topClusterList) {
            double sumRow = 0;
            double sumCol = 0;
            for (OutlierDetectionFunction.OutlierPoint p : cluster.points) {
                sumRow += p.row;
                sumCol += p.col;
            }
            double centroidRow = sumRow / cluster.points.size();
            double centroidCol = sumCol / cluster.points.size();
            JSONObject centroidObj = new JSONObject();
            centroidObj.put("clusterId", cluster.clusterId);
            centroidObj.put("x", centroidRow);
            centroidObj.put("y", centroidCol);
            centroidObj.put("count", cluster.points.size());
            centroidsArray.put(centroidObj);
        }

        batch.put("centroids", centroidsArray);
        return batch;
    }

    // DBScan helper methods.
    private List<DBScanCluster> dbScan(List<OutlierDetectionFunction.OutlierPoint> points, double eps, int minPts) {
        List<DBScanCluster> clusters = new ArrayList<>();
        int clusterId = 0;
        Set<OutlierDetectionFunction.OutlierPoint> visited = new HashSet<>();
        for (OutlierDetectionFunction.OutlierPoint p : points) {
            if (visited.contains(p)) continue;
            visited.add(p);
            List<OutlierDetectionFunction.OutlierPoint> neighbors = regionQuery(points, p, eps);
            if (neighbors.size() < minPts) {
                continue; // Mark as noise.
            }
            clusterId++;
            DBScanCluster cluster = new DBScanCluster(clusterId);
            expandCluster(p, neighbors, cluster, points, eps, minPts, visited);
            clusters.add(cluster);
        }
        return clusters;
    }

    private void expandCluster(OutlierDetectionFunction.OutlierPoint p, List<OutlierDetectionFunction.OutlierPoint> neighbors,
                               DBScanCluster cluster, List<OutlierDetectionFunction.OutlierPoint> points, double eps, int minPts,
                               Set<OutlierDetectionFunction.OutlierPoint> visited) {
        cluster.points.add(p);
        Queue<OutlierDetectionFunction.OutlierPoint> seeds = new LinkedList<>(neighbors);
        while (!seeds.isEmpty()) {
            OutlierDetectionFunction.OutlierPoint current = seeds.poll();
            // Optimize by combining visited check and insertion.
            if (visited.add(current)) {
                List<OutlierDetectionFunction.OutlierPoint> currentNeighbors = regionQuery(points, current, eps);
                if (currentNeighbors.size() >= minPts) {
                    seeds.addAll(currentNeighbors);
                }
            }
            cluster.points.add(current);
        }
    }

    private List<OutlierDetectionFunction.OutlierPoint> regionQuery(List<OutlierDetectionFunction.OutlierPoint> points, OutlierDetectionFunction.OutlierPoint center, double eps) {
        List<OutlierDetectionFunction.OutlierPoint> neighbors = new ArrayList<>();
        double epsSquared = eps * eps; // Pre-calculate eps squared to avoid repeated sqrt computations.
        for (OutlierDetectionFunction.OutlierPoint p : points) {
            double dx = p.row - center.row;
            double dy = p.col - center.col;
            // Use squared Euclidean distance for efficiency.
            if (dx * dx + dy * dy <= epsSquared) {
                neighbors.add(p);
            }
        }
        return neighbors;
    }

    // DBScan cluster helper class.
    private static class DBScanCluster {
        int clusterId;
        // Changed to a HashSet for faster membership checks.
        Set<OutlierDetectionFunction.OutlierPoint> points = new HashSet<>();

        DBScanCluster(int id) {
            this.clusterId = id;
        }
    }
}
