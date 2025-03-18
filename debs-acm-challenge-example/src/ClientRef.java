import java.awt.*;
import java.awt.image.Raster;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.json.*;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

//ClientRef is in charge of collecting our data in a sink, running our pipeline, and outputting our results
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

                    //TODO: Decode Image Data Correctly
                    //Object tifObject = batch.get("tif");

                    //Use Fake data for now. This creates an image where the top-left quadrant is all above the threshold, with all other values being 1.
                    int rows = 10;
                    int cols = 10;
                    int[][] image = new int[rows][cols];

                    // Initialize ALL elements to 1
                    for (int[] row : image) {
                        Arrays.fill(row, 1);
                    }

                    // Top-left quadrant dimensions
                    int qRows = rows / 2;    // 125,000
                    int qCols = cols / 2;    // 250

                    for (int i = 0; i < qRows; i++) {
                        for (int j = 0; j < qCols; j++) {
                            image[i][j] = 65001;
                        }
                    }
                    result.put("image", new JSONArray(image));

                    //Dummy output results.
                    result.put("centroids", new JSONArray());

                    //System.out.println(result.getInt("layer") + ", " + result.getString("tile_id") + ", " + result.getString("batch_id"));
                    return result;
                });

        //Stream to find saturated points per tile
        DataStream<JSONObject> apiDataWithSatPoints = apiData
                .map(batch -> {
                    //Count Saturated points for each batch
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

        //Create Window logic to store past three inclusive tiles for each tile that arrives.
        //Output is Tuple of : JSON Tile Object itself for layer x, List of JSONObjects for the tiles for layer x, x-1,and x-2.
        DataStream<Tuple2<JSONObject, List<JSONObject>>> apiDataWithOutlierDetection = apiDataWithSatPoints
                .keyBy(batch ->
                        batch.getString("tile_id")
                ).process(new KeyedProcessFunction<String, JSONObject, Tuple2<JSONObject, List<JSONObject>>>() {

                    //Store last three layers/tileID
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


        //Run the program.
        env.execute("Benchmark");
    }

    //Private class to create our data flink Source
    private static class ApiSource extends RichSourceFunction<JSONObject> {
        private volatile boolean isRunning = true;
        private final String[] args;

        private ApiSource(String[] args) {
            this.args = args;
        }

        //The Run method will loop to get all of our Batch data
        @Override
        public void run(SourceContext<JSONObject> ctx) throws Exception {
            String endpoint = args[0];
            String benchId = createBench(endpoint);
            startBench(endpoint, benchId);

            //Collect batches and break if we finish.
            while (isRunning) {
                JSONObject batch;
                try {
                    batch = getNextBatch(endpoint, benchId);
                } catch (JSONException e) {
                    System.out.println("Stopping - Invalid JSON response");
                    break;
                }
                ctx.collect(batch);
            }

            //When we exit the loop, we are done with the entire benchmark.
            endBench(endpoint, benchId);
        }

        //Called by Flink when needed to end the benchmark
        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    //Creates a benchmark.
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

    //Actually start the benchmark.
    private static void startBench(String endpoint, String benchId) throws Exception {
        sendPostRequest(endpoint + "/api/start/" + benchId, "");
    }

    //Get the next batch in the benchmark.
    private static JSONObject getNextBatch(String endpoint, String benchId) throws Exception {
        byte[] responseBytes = sendGetRequestBytes(endpoint + "/api/next_batch/" + benchId);

        // Unpack MessagePack bytes
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(responseBytes);
        Value value = unpacker.unpackValue();
        unpacker.close();

        // Convert MessagePack value to JSON string
        String jsonString = value.toJson();
        return new JSONObject(jsonString);
    }

    //Helper method to send the GetRequest for our next batch in the benchmark.
    private static byte[] sendGetRequestBytes(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");

        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300)
                ? conn.getInputStream()
                : conn.getErrorStream();
        if (is == null) {
            throw new IOException("No response received from server; response code: " + responseCode);
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16384];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    //Request to end the benchmark.
    private static void endBench(String endpoint, String benchId) throws Exception {
        sendPostRequest(endpoint + "/api/end/" + benchId, "");
    }

    //Helper method to sent Post Request to end the benchmark.
    private static String sendPostRequest(String url, String jsonPayload) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        conn.setDoOutput(true);

        // Write the JSON payload to the request body.
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonPayload.getBytes("UTF-8");
            os.write(input, 0, input.length);
            os.flush();
        }

        // Get response code and select the proper stream.
        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300)
                ? conn.getInputStream()
                : conn.getErrorStream();

        if (is == null) {
            throw new IOException("No response received from server; response code: " + responseCode);
        }

        // Read the full response.
        StringBuilder response = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
        }

        return response.toString();
    }

}
