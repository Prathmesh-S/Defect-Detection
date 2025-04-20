package main.java.solution;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.io.*;
import java.util.*;
import javax.imageio.ImageIO;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;

public class ClientRef {


    public static void main(String[] args) throws Exception {

        // Create our data source in Flink.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Define WaterMark strategy
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy
                .<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getInt("layer"));

        // Extract endpoint from args
        String endpoint = args[0];

        // Create bench ID (requires ApiSource.createBench to be public static)

        String benchId = ApiSource.createBench(endpoint);
        ApiSource apiSource = new ApiSource(args, benchId);

        // Register the job listener to end the benchmark when the job completes
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(JobClient jobClient, Throwable throwable) {
                // Job submitted, nothing to do yet
            }

            @Override
            public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
                // Job has completed, now we can end the benchmark
                try{
                    ApiSource.endBench(endpoint, benchId);
                    System.out.println("Job completed, ended benchmark: " + benchId);
                } catch (Exception e) {
                    System.err.println("Failed to end benchmark: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        });

        //Get Our RAW API Data
        DataStream<JSONObject> apiData = env.addSource(apiSource)
                .name("Faucet");

        //Process our raw API Data to get our response Objects
        DataStream<JSONObject> cleanedAPIData = apiData
                .map(batch -> {
                    JSONObject result = new JSONObject();
                    result.put("batch_id", batch.get("batch_id").toString());
                    result.put("print_id", batch.get("print_id").toString());
                    result.put("tile_id", batch.get("tile_id").toString());
                    result.put("layer", batch.getInt("layer"));
                    result.put("bench_id", batch.get("bench_id").toString());

                    // Use ImageIO to load 16-bit TIFF data.
                    byte[] imageBytes = (byte[]) batch.get("tifBytes");
                    BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageBytes));
                    if (image == null) {
                        throw new RuntimeException("Failed to load image from raw bytes.");
                    }
                    int width = image.getWidth();
                    int height = image.getHeight();

                    // Access the 16-bit raw data.
                    Raster raster = image.getData();
                    if (!(raster.getDataBuffer() instanceof DataBufferUShort)) {
                        throw new RuntimeException("Image is not 16-bit; unexpected data buffer type: " + raster.getDataBuffer().getClass());
                    }
                    DataBufferUShort dataBuffer = (DataBufferUShort) raster.getDataBuffer();
                    short[] rawData = dataBuffer.getData();
                    int[][] pixels = new int[height][width];
                    int idx = 0;
                    for (int y = 0; y < height; y++) {
                        for (int x = 0; x < width; x++) {
                            // Convert the signed short to an unsigned 16-bit value.
                            int value = rawData[idx++] & 0xFFFF;
                            pixels[y][x] = value;
                        }
                    }
                    result.put("image", new JSONArray(pixels));
                    result.put("centroids", new JSONArray());
                    return result;
                }).setParallelism(4);

        //Add watermarks + Stream to find saturated points per tile
        DataStream<JSONObject> apiDataWithSatPoints = cleanedAPIData
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .map(batch -> {
                    // Count Saturated points for each batch
                    JSONArray imageArray = batch.getJSONArray("image");
                    int saturatedCount = 0;

                    // Scan entire 2D array
                    for (int i = 0; i < imageArray.length(); i++) {
                        JSONArray row = imageArray.getJSONArray(i);
                        for (int j = 0; j < row.length(); j++) {
                            if (row.getInt(j) > 65000) {
                                saturatedCount++;
                            }
                        }
                    }

                    batch.put("saturated", saturatedCount);
                    return batch;
                }).setParallelism(4);
        ;

        // Create a windowed stream (last 3 layers per tile).
        DataStream<Tuple2<JSONObject, List<JSONObject>>> windowedStream = apiDataWithSatPoints
                .keyBy(batch -> batch.getString("tile_id"))
                .window(SlidingEventTimeWindows.of(Time.milliseconds(3), Time.milliseconds(1)))
                .allowedLateness(Time.milliseconds(2))
                .process(new ProcessWindowFunction<JSONObject, Tuple2<JSONObject, List<JSONObject>>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<JSONObject> elements, Collector<Tuple2<JSONObject, List<JSONObject>>> out) {
                        List<JSONObject> window = new ArrayList<>();
                        elements.forEach(window::add);

                        //Only output for valid window
                        if ((window.size() == 3) || (Integer.parseInt(window.get(window.size() - 1).get("batch_id").toString()) <= 31)) {
                            JSONObject currentLayer = window.get(window.size() - 1);
                            out.collect(Tuple2.of(currentLayer, window));
                        }
                    }
                }).setParallelism(4);
        ;

        // Use Outlier detection on window stream
        DataStream<Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>> outlierDetectionStream = windowedStream
                .map(new OutlierDetectionFunction())
                .returns(new TypeHint<Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>>() {
                }.getTypeInfo());

        // DBScan Clustering using outlier values and post results
        DataStream<JSONObject> enrichedData = outlierDetectionStream.map(new DBScanFunction());

        enrichedData.addSink(new ResultSubmitterSink(endpoint, benchId));





        // DEBUG OPERATOR (REMOVE FOR SUBMISSION): Print summary per batch, including cluster details.
        DataStream<JSONObject> finalPrintStream = enrichedData
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject batch) throws Exception {
                        // Retrieve the saturated count, outlier count, and number of clusters (centroids).
                        int saturated = batch.getInt("saturated");
                        int outlierCount = batch.getInt("outlier_count"); // Set in DBScanFunction.
                        JSONArray centroidsArray = batch.getJSONArray("centroids");
                        int clusters = centroidsArray.length();

                        // Print the summary for this batch.
                        System.out.println("Batch " + batch.get("batch_id") +
                                ": Saturated points = " + saturated +
                                ", Outliers = " + outlierCount +
                                ", Clusters = " + clusters);

                        // Print details for each cluster.
                        for (int i = 0; i < centroidsArray.length(); i++) {
                            JSONObject cluster = centroidsArray.getJSONObject(i);
                            int clusterId = cluster.getInt("clusterId");
                            double x = cluster.getDouble("x");
                            double y = cluster.getDouble("y");
                            int count = cluster.getInt("count");
                            System.out.println("  Cluster " + clusterId + ": Centroid (" + x + ", " + y + "), Size " + count);
                        }
                        return batch;
                    }
                })
                .returns(JSONObject.class).setParallelism(4);
        ;

        env.execute("Benchmark");
    }
}
