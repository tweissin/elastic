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
import org.elasticsearch.client.Client;
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
import java.util.Map;

import static com.trw.Utils.initElasticSearchClient;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by tweissin on 3/9/17.
 */
public class Simple51Client {

    public static final String INDEX = "twitter";
    public static final String TYPE = "tweet";
    public static void main(String[] args) throws Exception {

        createIndexWithTransportClient2();
//        createIndexWithHttpClient();

//        testOrdering();

//        printJson();
    }

    private static void printJson() throws IOException {
        String foo = jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject()
                .prettyPrint()
                .string();
        System.out.println(foo);
    }

    private static void testOrdering() throws Exception {
        try (
                TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        ) {
            cleanupIndices(client, 100);

            tryInLoop(client, 100);
            tryWithThreadPool(client, 2, 10);
        }
    }

    private static void cleanupIndices(TransportClient client, int docCount) {
        for (int i=0; i<docCount; i++) {
            deleteIndex(client, INDEX, TYPE, String.valueOf(i));
        }
    }

    private static void deleteIndex(TransportClient client, String index, String type, String id) {
        client.prepareDelete(index,type,id).get();

    }

    private static void createIndex(TransportClient client, String index, String type, String id) throws IOException {
        IndexResponse response = client.prepareIndex(INDEX, TYPE, id)
                .setSource(jsonBuilder()
                        .startObject()
                        .field("docs", Collections.EMPTY_LIST)
                        .endObject()
                )
                .execute()
                .actionGet();
    }

    private static void tryWithThreadPool(TransportClient client, int threadPoolSize, int docCount) throws InterruptedException {
        Utils.testOrdering(threadPoolSize, docCount, (currentDocNum) -> {
            UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, "1")
//            UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, String.valueOf(currentDocNum))
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

    private static void tryInLoop(TransportClient client, int docCount) throws IOException {
        createIndex(client, INDEX, TYPE, "1");
        for (int i=0; i<docCount; i++) {
            Map<String,Object> params = new HashMap<>();
            params.put("currentDoc",i);
            UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, "1")
//            UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, String.valueOf(i))
                    .script(new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, "ctx._source.docs.add(params.currentDoc)", params));
            try {
                client.update(updateRequest).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        Settings.Builder builder = Settings.builder().put("cluster.name", "foo");
        try (
                Client client = new PreBuiltTransportClient(Settings.EMPTY)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        ) {
            IndexResponse response = client.prepareIndex(INDEX, TYPE, "1")
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

    private static void createIndexWithTransportClient2() throws IOException {
        try (
                Client client = (Client)initElasticSearchClient(new String[]{"localhost"}, 9300, "foo", false, true);
        ) {
            IndexResponse response = client.prepareIndex(INDEX, TYPE, "1")
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
