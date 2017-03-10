package com.trw.elastic;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.trw.Utils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by tweissin on 3/9/17.
 */
public class Simple51Client {
    public static void main(String[] args) throws Exception {

        createIndexWithTransportClient();
        createIndexWithHttpClient();

        testOrdering();
    }

    private static void testOrdering() throws Exception {
        try (
                TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        ) {
            client.prepareDelete("twitter", "tweet", "1").get();

            IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("docs", Collections.EMPTY_LIST)
                            .endObject()
                    )
                    .execute()
                    .actionGet();


            Utils.testOrdering(1, (currentDocNum) -> {
                UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1")
                        .script(new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, "ctx._source.docs.add(params.currentDoc)", new HashMap<String,Object>(){{
                            put("currentDoc", currentDocNum);
                        }}));
                try {
                    client.update(updateRequest).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return true;
            });
        }
    }

    private static void createIndexWithHttpClient() throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet("http://localhost:9200/twitter");
        CloseableHttpResponse response1 = httpclient.execute(httpGet);
        try {
            System.out.println(response1.getStatusLine());
            HttpEntity entity1 = response1.getEntity();
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JsonParser jp = new JsonParser();
            String json = EntityUtils.toString(entity1);
            JsonElement je = jp.parse(json);
            System.out.println(gson.toJson(je));
        } finally {
            response1.close();
        }
    }

    private static void createIndexWithTransportClient() throws IOException {
        try (
                TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        ) {
            IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
                    .setSource(jsonBuilder()
                            .startObject()
                            .field("user", "kimchy")
                            .field("postDate", new Date())
                            .field("message", "trying out Elasticsearch")
                            .endObject()
                    )
                    .execute()
                    .actionGet();
        }
    }
}
