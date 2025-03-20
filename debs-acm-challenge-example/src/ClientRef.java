import java.awt.image.BufferedImage;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.imageio.ImageIO;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;

public class ClientRef {

    private static final String API_TOKEN = "polimi-deib";

    public static void main(String[] args) throws Exception {

        //Create our data source in Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Process our raw API Data to get our response Objects
        DataStream<JSONObject> apiData = env.addSource(new ApiSource(args))
            .name("Faucet")
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
                // Initialize dummy centroids array.
                result.put("centroids", new JSONArray());
                return result;
            });

        // Stream to find saturated points per tile
        DataStream<JSONObject> apiDataWithSatPoints = apiData
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

            });

        DataStream<Tuple2<JSONObject, List<JSONObject>>> windowedStream = apiDataWithSatPoints
                .keyBy(batch -> batch.getString("tile_id"))
                .process(new KeyedProcessFunction<String, JSONObject, Tuple2<JSONObject, List<JSONObject>>>() {

                // Store last three layers/tileID
                private transient ListState<JSONObject> windowState;

                @Override
                public void open(Configuration parameters) {
                    windowState = getRuntimeContext().getListState(
                            new ListStateDescriptor<>("layerWindow", JSONObject.class));
                }

                @Override
                public void processElement(
                        JSONObject batch,
                        Context ctx,
                        Collector<Tuple2<JSONObject, List<JSONObject>>> out) throws Exception {

                    // Get current window state
                    List<JSONObject> window = new ArrayList<>();
                    windowState.get().forEach(window::add);

                    // Add new batch to window
                    window.add(batch);

                    // Trim to last 3 layers
                    if(window.size() > 3) {
                        window = new ArrayList<>(window.subList(window.size() - 3, window.size()));
                    }

                    // Update state and emit
                    windowState.update(window);
                    out.collect(Tuple2.of(batch, window));
                }
            });

        //Responsible for finding outliers for each tile we receive using the past three layers.
        SingleOutputStreamOperator<Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>> outlierDetectionStream = windowedStream
            .map(new OutlierDetectionFunction())
            .returns(new TypeHint<Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>>() {}.getTypeInfo())
            .map(tuple -> {
                //System.out.println("Detected " + tuple.f1.size() + " outliers.");
                return tuple;
            })
            .returns(new TypeHint<Tuple2<JSONObject, List<OutlierDetectionFunction.OutlierPoint>>>() {}.getTypeInfo());

        // DBScan Clustering using outlier values and post results
        DataStream<JSONObject> enrichedData = outlierDetectionStream.map(new DBScanFunction());

        env.execute("Benchmark");
    }

    // Flink source to get batch data.
    private static class ApiSource extends RichSourceFunction<JSONObject> {
        private volatile boolean isRunning = true;
        private final String[] args;

        private ApiSource(String[] args) {
            this.args = args;
        }

        @Override
        public void run(SourceContext<JSONObject> ctx) throws Exception {
            String endpoint = args[0];
            String benchId = createBench(endpoint);
            startBench(endpoint, benchId);

            // Collect batches and break if we finish.
            while (isRunning) {
                JSONObject batch;
                try {
                    batch = getNextBatch(endpoint, benchId);
                    batch.put("bench_id", benchId);
                } catch (JSONException e) {
                    System.out.println("Stopping - Invalid JSON response");
                    break;
                }
                ctx.collect(batch);
            }
            endBench(endpoint, benchId);
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // Creates a benchmark.
    private static String createBench(String endpoint) throws Exception {
        JSONObject payload = new JSONObject();
        payload.put("apitoken", API_TOKEN);
        payload.put("name", "unoptimized");
        payload.put("test", true);
        payload.put("max_batches", JSONObject.NULL);

        //Get our response + clean it up.
        String response = sendPostRequest(endpoint + "/api/create", payload.toString());
        response = response.trim();
        response = response.substring(1, response.length() - 1);

        return response;
    }

    // Starts the benchmark.
    private static void startBench(String endpoint, String benchId) throws Exception {
        sendPostRequest(endpoint + "/api/start/" + benchId, "");
    }

    // Unpacks the MessagePack response to extract batch fields and the raw TIFF bytes.
    private static JSONObject getNextBatch(String endpoint, String benchId) throws Exception {
        byte[] responseBytes = sendGetRequestBytes(endpoint + "/api/next_batch/" + benchId);
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(responseBytes);
        int mapSize = unpacker.unpackMapHeader();
        JSONObject result = new JSONObject();
        byte[] tifBytes = null;
        for (int i = 0; i < mapSize; i++) {
            String key = unpacker.unpackString();
            if ("tif".equals(key)) {
                int len = unpacker.unpackBinaryHeader();
                tifBytes = unpacker.readPayload(len);
            } else {
                Value value = unpacker.unpackValue();
                result.put(key, value.toJson());
            }
        }
        unpacker.close();
        if (tifBytes != null) {
            result.put("tifBytes", tifBytes);
        }
        return result;
    }

    // Helper method to send the GetRequest for our next batch in the benchmark.
    private static byte[] sendGetRequestBytes(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();
        if (is == null) {
            throw new IOException("No response received from server; response code: " + responseCode);
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data = new byte[16384];
        int nRead;
        while ((nRead = is.read(data)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    // Request to end the benchmark.
    private static void endBench(String endpoint, String benchId) throws Exception {
        sendPostRequest(endpoint + "/api/end/" + benchId, "");
    }

    // Helper method to sent Post Request to end the benchmark.
    private static String sendPostRequest(String url, String jsonPayload) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
            os.write(input);
            os.flush();
        }
        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();
        if (is == null) {
            throw new IOException("No response received; response code: " + responseCode);
        }
        StringBuilder response = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
        }
        return response.toString();
    }
}
