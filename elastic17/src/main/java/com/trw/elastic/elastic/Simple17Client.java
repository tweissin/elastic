package com.trw.elastic.elastic;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by tweissin on 3/9/17.
 */
public class Simple17Client {
    public static void main(String[] args) throws IOException {
        try (
                Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300))
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
