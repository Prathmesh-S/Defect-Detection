package solution;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.json.JSONObject;


//  a sink function that submits each enriched data point to the API
class ResultSubmitterSink implements SinkFunction<JSONObject> {

    // Track total number of batches with a counter
    private final String endpoint;
    private final String benchId;



    public ResultSubmitterSink(String endpoint, String benchId) {
        this.endpoint = endpoint;
        this.benchId = benchId;
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // Extract batch ID from your enriched data, or track it with a counter
        int batchId = value.getInt("batch_id"); // Assuming the batch ID is in your JSON

        // Submit the results to the API
        String response = ApiSource.submitResults(endpoint, benchId, batchId, value);
        System.out.println("Submit response: "+response);
    }
}
