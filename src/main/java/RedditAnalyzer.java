import java.net.URI;

public class RedditAnalyzer {
    public static void main(String[] args) throws Exception {
        var pravegaReader = new PravegaReader(Constants.DEFAULT_SCOPE, Constants.DEFAULT_STREAM_NAME, URI.create(Constants.DEFAULT_CONTROLLER_URI));

        pravegaReader.startReading();
    }
}
