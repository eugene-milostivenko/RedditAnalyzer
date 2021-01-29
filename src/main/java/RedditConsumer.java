import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class RedditConsumer {

    public static void main(String[] args) throws Exception {

        var pravegaWriter = new PravegaWriter(Constants.DEFAULT_SCOPE, Constants.DEFAULT_STREAM_NAME, URI.create(Constants.DEFAULT_CONTROLLER_URI));

        OkHttpClient client = new OkHttpClient.Builder()
                .callTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .connectTimeout(30, TimeUnit.SECONDS)
                .build();

        String lastPostId = null;

        while (true) {
            var url = "https://api.pushshift.io/reddit/search/submission/?sort=desc&sort_type=created_utc&size=200&over_18=false&is_video=false";

            if (lastPostId != null) {
                url += String.format("&after_id=%s", lastPostId);
            }

            System.out.println(url);
            System.out.println("");

            Request request = new Request.Builder()
                    .url(url)
                    .build();

            try (Response response = client.newCall(request).execute()) {
                System.out.println(response.code());

                var responseText = response.body().string();
                JSONObject json = new JSONObject(responseText);
                var postsArray = (JSONArray) json.get("data");

                for (int i = 0; i < postsArray.length(); i++) {
                    var postDataJson = postsArray.getJSONObject(i);

                    // Presasve the latest post id for the next request
                    if (i == 0)
                        lastPostId = postDataJson.getString("id");

                    var isMedia = postDataJson.getBoolean("media_only");

                    // Filter MediaOnly
                    if (isMedia || !postDataJson.has("selftext"))
                        continue;

                    var postDataParsed = new PostData();
                    postDataParsed.body = (String) postDataJson.get("selftext");
                    postDataParsed.title = (String) postDataJson.get("title");
                    postDataParsed.fullLink = (String) postDataJson.get("full_link");
                    postDataParsed.subreddit = (String) postDataJson.get("subreddit");

                    var len = postDataParsed.body.length();

                    if (len < 100 || len > 1500)
                        continue;

                    var stringRepresentation = (new JSONObject(postDataParsed, new String[]{ "fullLink", "subreddit", "title", "body" })).toString();

                    pravegaWriter.sendMessage(Constants.DEFAULT_ROUTING_KEY, stringRepresentation);

                    System.out.println(stringRepresentation);
                }

            }

            Thread.sleep(1000);
        }



    }
}
