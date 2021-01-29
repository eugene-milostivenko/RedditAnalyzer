import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class PravegaWriter {

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
    private EventStreamWriter<String> writer;


    public PravegaWriter(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;

        streamManager = StreamManager.create(controllerURI);

        streamManager.createScope(scope);

        streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        streamManager.createStream(scope, streamName, streamConfig);

        clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());

        writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
    }

    public void sendMessage(String routingKey, String message) {

        System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message, routingKey, scope, streamName);
        final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
    }

}