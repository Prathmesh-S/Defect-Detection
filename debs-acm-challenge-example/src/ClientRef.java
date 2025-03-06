import java.awt.image.Raster;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.json.*;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;


public class ClientRef {
    private static final String API_TOKEN = "polimi-deib";

    public static void main(String[] args) throws Exception {
        String endpoint = args[0]; // Passed as a command-line argument
        int limit = args.length > 1 ? Integer.parseInt(args[1]) : -1;

        HttpURLConnection conn;
        String benchId = createBench(endpoint, limit);
        startBench(endpoint, benchId);

        int i = 0;
        while (limit == -1 || i < limit) {
            JSONObject batch = getNextBatch(endpoint, benchId);
            if (batch == null) break;

            JSONObject result = process(batch);
            sendResult(endpoint, benchId, i, result);
            i++;
        }

        endBench(endpoint, benchId);
    }

    private static String createBench(String endpoint, int limit) throws Exception {
        JSONObject payload = new JSONObject();
        payload.put("apitoken", API_TOKEN);
        payload.put("name", "unoptimized");
        payload.put("test", true);
        payload.put("max_batches", JSONObject.NULL);

        String response = sendPostRequest(endpoint + "/api/create", payload.toString());
        response = response.trim();
        // If response starts with '{', assume it's JSON; otherwise, it's plain text.
        if (response.startsWith("{")) {
            return new JSONObject(response).getString("bench_id");
        } else {
            // If response is plain text, remove any wrapping quotes.
            if (response.startsWith("\"") && response.endsWith("\"")) {
                response = response.substring(1, response.length() - 1);
            }
            return response;
        }
    }

    private static void startBench(String endpoint, String benchId) throws Exception {
        sendPostRequest(endpoint + "/api/start/" + benchId, "");
    }

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

    // New method to get raw bytes from GET request
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

    private static void sendResult(String endpoint, String benchId, int i, JSONObject result) throws Exception {
        sendPostRequest(endpoint + "/api/result/0/" + benchId + "/" + i, result.toString());
    }

    private static void endBench(String endpoint, String benchId) throws Exception {
        sendPostRequest(endpoint + "/api/end/" + benchId, "");
    }

    private static String sendPostRequest(String url, String jsonPayload) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        conn.setDoOutput(true);

        System.out.println("Processing this URL now: " + url);

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

    private static JSONObject process(JSONObject batch) throws IOException {
        JSONObject result = new JSONObject();
        result.put("batch_id", batch.get("batch_id").toString());
        result.put("print_id", batch.get("print_id").toString());
        result.put("tile_id", batch.get("tile_id").toString());
        result.put("layer", batch.getInt("layer"));

        //TODO: Decode Image Data
        Object tifObject = batch.get("tif");
        byte[] imageBytes;

        if (tifObject instanceof String) {
            // If it's mistakenly stored as a string, convert it back
            imageBytes = ((String) tifObject).getBytes(StandardCharsets.ISO_8859_1);
        } else if (tifObject instanceof byte[]) {
            imageBytes = (byte[]) tifObject;
        } else {
            throw new RuntimeException("Unknown type for 'tif'");
        }

// Print raw bytes
        System.out.println(Arrays.toString(imageBytes));
        System.exit(0);

        result.put("saturated", 0);

        result.put("centroids", new JSONArray());

        return result;
    }

}
