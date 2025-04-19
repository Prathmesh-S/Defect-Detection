package src;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.json.JSONException;
import org.json.JSONObject;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

// Flink source to get batch data.
public class ApiSource extends RichSourceFunction<JSONObject> {
    private volatile boolean isRunning = true;
    private final String[] args;
    private static final int MAX_BATCHES = 100; // Set the desired number of batches
    private static final String API_TOKEN = "cxjbvgcftxxjmkhhtgkfivknvxsccgkr";

    public ApiSource(String[] args) {
        this.args = args;
    }

    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {
        String endpoint = args[0];
        String benchId = createBench(endpoint);
        startBench(endpoint, benchId);

        // Collect batches and break if we finish.
        int count = 0;
        while (isRunning && count < MAX_BATCHES) {
            JSONObject batch;
            try {
                batch = getNextBatch(endpoint, benchId);
                batch.put("bench_id", benchId);
            } catch (JSONException e) {
                System.out.println("Stopping - Invalid JSON response");
                break;
            }
            ctx.collect(batch);
            count++;
        }
        endBench(endpoint, benchId);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    // Creates a benchmark.
    private static String createBench(String endpoint) throws Exception {
        JSONObject payload = new JSONObject();
        payload.put("apitoken", API_TOKEN);
        payload.put("name", "unoptimized");
        payload.put("test", true);

        if (API_TOKEN == "polimi-deib"){
            payload.put("max_batches", JSONObject.NULL);
        }

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