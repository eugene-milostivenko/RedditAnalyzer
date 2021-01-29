import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONObject;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PravegaReader {

    /**
     * Pravega namespace
     */
    public final String scope;

    /**
     * Pravega stream
     */
    public final String streamName;

    /**
     * Pravega controller URI
     */
    public final URI controllerURI;

    private StreamManager streamManager;

    private StreamConfiguration streamConfig;

    private EventStreamClientFactory clientFactory;

    EventStreamReader<String> reader;

    private static final int READER_TIMEOUT_MS = 2000;

    public PravegaReader(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;

        streamManager = StreamManager.create(controllerURI);

        streamManager.createScope(scope);

        streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        streamManager.createStream(scope, streamName, streamConfig);


        final String readerGroup = UUID.randomUUID().toString().replace("-", "");

        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());

        reader = clientFactory.createReader("reader",
                readerGroup,
                new JavaSerializer<String>(),
                ReaderConfig.builder().build());
    }

    public void startReading() throws Exception {
        EventRead<String> event = null;


        OkHttpClient client = new OkHttpClient.Builder()
                .callTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .connectTimeout(30, TimeUnit.SECONDS)
                .build();

        BidirectionalLstmSentimentClassifier classifier = new BidirectionalLstmSentimentClassifier();
        classifier.load_model(ResourceUtils.getInputStream("bidirectional_lstm_softmax.pb"));
        classifier.load_vocab(ResourceUtils.getInputStream("bidirectional_lstm_softmax.csv"));

        while (true) {
            event = reader.readNextEvent(READER_TIMEOUT_MS);
            if (event.getEvent() != null) {
                String data = event.getEvent();
                JSONObject dataJson = new JSONObject(data);
                String body = dataJson.getString("body");
                String subreddit = dataJson.getString("subreddit");
                float[] predicted = classifier.predict(body);
                //String predicted_label = classifier.predict_label(body);

                System.out.format("Read event '%s'%n", dataJson);
                System.out.format("Result: pos %s neg %s\n", predicted[0], predicted[1]);

                var metricString = String.format("reddit_metrics,subreddit=%s value=%s", subreddit, predicted[0]);

                var requestBody = RequestBody
                        .create(metricString.getBytes(StandardCharsets.UTF_8));


                Request request = new Request.Builder()
                        .post(requestBody)
                        .url(Constants.INFLUX_CREATE_URI)
                        .build();

                Response response = client.newCall(request).execute();

            } else {
                System.out.println("No event");
            }

            //Thread.sleep(500);
        }
    }
}