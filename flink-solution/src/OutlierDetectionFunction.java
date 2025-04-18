package src;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.List;

public class OutlierDetectionFunction implements MapFunction<Tuple2<JSONObject, List<JSONObject>>, Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>> {

    // Outlier Detection Parameters
    private static final int EMPTY_THRESHOLD = 5000;
    private static final int SATURATION_THRESHOLD = 65000;
    private static final int DISTANCE_THRESHOLD = 2;
    private static final double OUTLIER_THRESHOLD = 6000.0;

    @Override
    public Tuple2<JSONObject, List<OutlierPoint>> map(Tuple2<JSONObject, List<JSONObject>> windowedTuple) throws Exception {
        JSONObject batch = windowedTuple.f0;
        List<JSONObject> window = windowedTuple.f1;

        // If we don't have a full window, return an empty outlier list.
        if (window.size() < 3) {
            System.out.println("Window size less than 3, skipping outlier detection for batch " + batch.get("batch_id"));
            return Tuple2.of(batch, new ArrayList<>());
        }

        // Build a 3D array (depth x rows x cols) from the window data.
        // The current layer (the last in the window) defines the dimensions.
        JSONObject currentLayer = window.get(window.size() - 1);
        JSONArray imageArray = currentLayer.getJSONArray("image");
        final int rows = imageArray.length();
        final int cols = imageArray.getJSONArray(0).length();
        final int depth = window.size();
        int[][][] images = new int[depth][rows][cols];

        // Convert each JSONObject image into a primitive int array.
        for (int l = 0; l < depth; l++) {
            JSONArray img = window.get(l).getJSONArray("image");
            for (int i = 0; i < rows; i++) {
                JSONArray rowArray = img.getJSONArray(i);
                for (int j = 0; j < cols; j++) {
                    images[l][i][j] = rowArray.getInt(j);
                }
            }
        }

        // Precompute neighbor offsets for the entire window.
        // Each entry in neighborOffsets is an int array: [d, di, dj, manhattan]
        // where 'd' is the depth index, 'di' and 'dj' are the row and column offsets,
        // and 'manhattan' is the Manhattan distance from the current pixel.
        List<int[]> neighborOffsets = new ArrayList<>();
        for (int d = 0; d < depth; d++) {
            int depthDiff = (depth - 1) - d;  // how far a given layer is from the current layer.
            // Offsets extend from -2*DISTANCE_THRESHOLD to 2*DISTANCE_THRESHOLD.
            for (int di = -2 * DISTANCE_THRESHOLD; di <= 2 * DISTANCE_THRESHOLD; di++) {
                for (int dj = -2 * DISTANCE_THRESHOLD; dj <= 2 * DISTANCE_THRESHOLD; dj++) {
                    int manhattan = Math.abs(di) + Math.abs(dj) + depthDiff;
                    if (manhattan <= 2 * DISTANCE_THRESHOLD) {
                        neighborOffsets.add(new int[]{d, di, dj, manhattan});
                    }
                }
            }
        }

        // Outlier detection: examine each pixel (i, j) in the current layer.
        List<OutlierPoint> outliers = new ArrayList<>();
        final int currentLayerIndex = depth - 1;
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                int currentPixel = images[currentLayerIndex][i][j];
                // Skip pixels outside the valid intensity range.
                if (currentPixel <= EMPTY_THRESHOLD || currentPixel >= SATURATION_THRESHOLD) {
                    continue;
                }

                double closeNeighborSum = 0.0;
                int closeNeighborCount = 0;
                double outerNeighborSum = 0.0;
                int outerNeighborCount = 0;

                // Iterate over precomputed neighbor offsets.
                for (int[] offset : neighborOffsets) {
                    int d = offset[0];
                    int di = offset[1];
                    int dj = offset[2];
                    int manhattan = offset[3];

                    // Compute the neighbor's absolute coordinates.
                    int ii = i + di;
                    int jj = j + dj;
                    // Inline boundary check for rows and columns.
                    if (ii < 0 || ii >= rows || jj < 0 || jj >= cols) {
                        continue; // Skip out-of-bound neighbors.
                    }

                    double neighborValue = images[d][ii][jj];
                    // Based on Manhattan distance, classify as a close or outer neighbor.
                    if (manhattan <= DISTANCE_THRESHOLD) {
                        closeNeighborSum += neighborValue;
                        closeNeighborCount++;
                    } else {  // Automatically in range: (DISTANCE_THRESHOLD, 2*DISTANCE_THRESHOLD]
                        outerNeighborSum += neighborValue;
                        outerNeighborCount++;
                    }
                }

                // Calculate means (with protection against division by zero).
                double meanClose = (closeNeighborCount > 0) ? closeNeighborSum / closeNeighborCount : 0.0;
                double meanOuter = (outerNeighborCount > 0) ? outerNeighborSum / outerNeighborCount : 0.0;
                double deviation = Math.abs(meanClose - meanOuter);

                // If deviation exceeds the defined threshold, register as an outlier.
                if (deviation > OUTLIER_THRESHOLD) {
                    outliers.add(new OutlierPoint(i, j, deviation));
                }
            }
        }
        System.out.println("Total outliers detected in batch " + batch.get("batch_id") + ": " + outliers.size());
        return Tuple2.of(batch, outliers);
    }

    // Helper function: returns the value at images[l][i][j] or 0 if out-of-bounds.
    private double getPadded(int[][][] images, int l, int i, int j) {
        int depth = images.length;
        int rows = images[0].length;
        int cols = images[0][0].length;
        if (l < 0 || l >= depth || i < 0 || i >= rows || j < 0 || j >= cols) {
            return 0.0;
        }
        return images[l][i][j];
    }

    // Define outlier point class.
    public static class OutlierPoint {
        public int row;
        public int col;
        public double d; // deviation value

        public OutlierPoint(int row, int col, double d) {
            this.row = row;
            this.col = col;
            this.d = d;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OutlierPoint that = (OutlierPoint) o;
            return row == that.row && col == that.col;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(row, col);
        }
    }
}
