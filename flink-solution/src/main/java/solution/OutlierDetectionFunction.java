package solution;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.List;

public class OutlierDetectionFunction implements MapFunction<Tuple2<JSONObject, List<JSONObject>>, Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>> {

    // Outlier Detection Parameters (taken from python naive solution).
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

        // Build a 3D array from the window.
        JSONObject currentLayer = window.get(window.size() - 1);
        JSONArray imageArray = currentLayer.getJSONArray("image");
        int rows = imageArray.length();
        int cols = imageArray.getJSONArray(0).length();
        int depth = window.size();
        int[][][] images = new int[depth][rows][cols];

        for (int l = 0; l < depth; l++) {
            JSONArray img = window.get(l).getJSONArray("image");
            for (int i = 0; i < rows; i++) {
                JSONArray rowArray = img.getJSONArray(i);
                for (int j = 0; j < cols; j++) {
                    images[l][i][j] = rowArray.getInt(j);
                }
            }
        }

        // Outlier detection logic:
        List<OutlierPoint> outliers = new ArrayList<>();
        // Loop over each pixel (i, j) in the current (last) layer.
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                int currentPixel = images[depth - 1][i][j];
                // Skip pixels that are outside the valid range.
                if (currentPixel <= EMPTY_THRESHOLD || currentPixel >= SATURATION_THRESHOLD) {
                    continue;
                }

                // Compute close neighbours (within DISTANCE_THRESHOLD).
                double cnSum = 0.0;
                int cnCount = 0;
                for (int j_offset = -DISTANCE_THRESHOLD; j_offset <= DISTANCE_THRESHOLD; j_offset++) {
                    for (int i_offset = -DISTANCE_THRESHOLD; i_offset <= DISTANCE_THRESHOLD; i_offset++) {
                        for (int d = 0; d < depth; d++) {
                            int manhattan = Math.abs(i_offset) + Math.abs(j_offset) + Math.abs((depth - 1) - d);
                            if (manhattan <= DISTANCE_THRESHOLD) {
                                cnSum += getPadded(images, d, i + i_offset, j + j_offset);
                                cnCount++;
                            }
                        }
                    }
                }

                // Compute outer neighbours (between DISTANCE_THRESHOLD and 2*DISTANCE_THRESHOLD).
                double onSum = 0.0;
                int onCount = 0;
                for (int j_offset = -2 * DISTANCE_THRESHOLD; j_offset <= 2 * DISTANCE_THRESHOLD; j_offset++) {
                    for (int i_offset = -2 * DISTANCE_THRESHOLD; i_offset <= 2 * DISTANCE_THRESHOLD; i_offset++) {
                        for (int d = 0; d < depth; d++) {
                            int manhattan = Math.abs(i_offset) + Math.abs(j_offset) + Math.abs((depth - 1) - d);
                            if (manhattan > DISTANCE_THRESHOLD && manhattan <= 2 * DISTANCE_THRESHOLD) {
                                onSum += getPadded(images, d, i + i_offset, j + j_offset);
                                onCount++;
                            }
                        }
                    }
                }

                double meanClose = (cnCount > 0) ? cnSum / cnCount : 0.0;
                double meanOuter = (onCount > 0) ? onSum / onCount : 0.0;
                double deviation = Math.abs(meanClose - meanOuter);

                // If deviation exceeds threshold, mark as an outlier.
                if (currentPixel > EMPTY_THRESHOLD && currentPixel < SATURATION_THRESHOLD && deviation > OUTLIER_THRESHOLD) {
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
