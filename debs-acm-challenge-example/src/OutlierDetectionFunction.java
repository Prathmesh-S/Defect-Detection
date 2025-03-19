import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class OutlierDetectionFunction implements MapFunction<Tuple2<JSONObject, List<JSONObject>>, Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>> {

    @Override
    public Tuple2<JSONObject, List<OutlierPoint>> map(Tuple2<JSONObject, List<JSONObject>> windowedTuple) throws Exception {
        JSONObject batch = windowedTuple.f0;
        List<JSONObject> window = windowedTuple.f1;

        // If we don't have a full window, return an empty outlier list.
        if (window.size() < 3) {
            return Tuple2.of(batch, new ArrayList<>());
        }

        // Build a 3D array from the window.
        JSONObject currentLayer = window.get(window.size() - 1);
        JSONArray imageArray = currentLayer.getJSONArray("image");
        int rows = imageArray.length();
        int cols = imageArray.getJSONArray(0).length();
        int[][][] images = new int[window.size()][rows][cols];

        for (int l = 0; l < window.size(); l++) {
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
        // Process each pixel of the current layer.
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double sumClose = 0.0;
                int countClose = 0;
                double sumOuter = 0.0;
                int countOuter = 0;
                for (int l = 0; l < window.size(); l++) {
                    for (int di = -4; di <= 4; di++) {
                        for (int dj = -4; dj <= 4; dj++) {
                            int ni = i + di;
                            int nj = j + dj;
                            if (ni < 0 || ni >= rows || nj < 0 || nj >= cols) {
                                continue;
                            }
                            int manhattan = Math.abs(di) + Math.abs(dj);
                            if (manhattan <= 2) {
                                sumClose += images[l][ni][nj];
                                countClose++;
                            } else if (manhattan > 2 && manhattan <= 4) {
                                sumOuter += images[l][ni][nj];
                                countOuter++;
                            }
                        }
                    }
                }
                double meanClose = (countClose > 0) ? sumClose / countClose : 0.0;
                double meanOuter = (countOuter > 0) ? sumOuter / countOuter : 0.0;
                double d = Math.abs(meanClose - meanOuter);
                // Add all points above the saturation (deviation) threshold.
                if (d > 5000) {
                    OutlierPoint op = new OutlierPoint(i, j, d);
                    outliers.add(op);
                    //System.out.println("Outlier detected at (" + i + ", " + j + ") with deviation: " + d);
                }
            }
        }
        return Tuple2.of(batch, outliers);
    }

    // Define your outlier point class as a public static inner class.
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
