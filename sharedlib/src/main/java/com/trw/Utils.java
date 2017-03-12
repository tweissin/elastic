package com.trw;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by tweissin on 3/10/17.
 */
public class Utils {
    /**
     * This tests that sending in requests
     */
    public static void testOrdering(int threadPoolSize, int docCount, Function<Integer,Boolean> updateDocFunction) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        for (int currentDoc=0; currentDoc<docCount; currentDoc++) {
            executorService.submit(new UpdateDoc(updateDocFunction, currentDoc));
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.DAYS);
    }

    public static Object initElasticSearchClient(String[] addresses, int port, String clusterName, boolean disableTransportSniff, boolean is51) {
        Object c = null;
        try {
            Class transportClientClass = Class.forName("org.elasticsearch.client.transport.TransportClient");
            Class inetSocketTransportAddress = Class.forName("org.elasticsearch.common.transport.InetSocketTransportAddress");
            Class settingsClass = Class.forName("org.elasticsearch.common.settings.Settings");
            Class transportAddressClass = Class.forName("org.elasticsearch.common.transport.TransportAddress");
            Method addTransportAddressMethod = transportClientClass.getMethod("addTransportAddress", transportAddressClass);
            Constructor constructor = inetSocketTransportAddress.getConstructor(InetAddress.class, int.class);
            if (is51) {
                Class settingsBuilderClass = Class.forName("org.elasticsearch.common.settings.Settings$Builder");
                Class preBuiltTransportClientClass = Class.forName("org.elasticsearch.transport.client.PreBuiltTransportClient");
                Method builderMethod = settingsClass.getMethod("builder");
                Method putMethod = settingsBuilderClass.getMethod("put", String.class, String.class);
                Method putBooleanMethod = settingsBuilderClass.getMethod("put", String.class, boolean.class);
                Method buildMethod = settingsBuilderClass.getMethod("build");

                Object settingsBuilder = builderMethod.invoke(null);
                if(!disableTransportSniff) {
                    putBooleanMethod.invoke(settingsBuilder, "client.transport.sniff", true);
                }
                putMethod.invoke(settingsBuilder, "transport.type", "netty3");
                putMethod.invoke(settingsBuilder, "http.type", "netty3");
                Object settings = buildMethod.invoke(settingsBuilder);

                c = preBuiltTransportClientClass.getConstructor(settingsClass, Class[].class).newInstance(settings, new Class[]{});
                for (String address : addresses) {
                    Object ta = constructor.newInstance(InetAddress.getByName(address), port);
                    addTransportAddressMethod.invoke(c, ta);
                }
            } else { // 1.7
                Class immutableSettingsClass = Class.forName("org.elasticsearch.common.settings.ImmutableSettings");
                Class immutableSettingsBuilderClass = Class.forName("org.elasticsearch.common.settings.ImmutableSettings$Builder");
                Method settingsBuilderMethod = immutableSettingsClass.getMethod("settingsBuilder");
                Method putMethod = immutableSettingsBuilderClass.getMethod("put", String.class, String.class);
                Method putBooleanMethod = immutableSettingsBuilderClass.getMethod("put", String.class, boolean.class);
                Method buildMethod = immutableSettingsBuilderClass.getMethod("build");

                Object settingsBuilder = settingsBuilderMethod.invoke(null);
//                putMethod.invoke(settingsBuilder, "cluster.name", clusterName);
                putBooleanMethod.invoke(settingsBuilder, "client.transport.sniff", true);
                Object settings = buildMethod.invoke(settingsBuilder);
                c = transportClientClass.getConstructor(settingsClass).newInstance(settings);
                for (String address : addresses) {
                    Object ta = constructor.newInstance(InetAddress.getByName(address), port);
                    addTransportAddressMethod.invoke(c, ta);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("problem initializing elastic search client", e);
        }
        return c;
    }

    private static class UpdateDoc implements Callable<Boolean> {
        private Function<Integer, Boolean> updateDocFunction;
        private int currentDoc;

        UpdateDoc(Function<Integer,Boolean> updateDocFunction, int currentDoc) {
            this.updateDocFunction = updateDocFunction;
            this.currentDoc = currentDoc;
        }

        @Override
        public Boolean call() throws Exception {
            System.out.println("Updating doc " + currentDoc);
            updateDocFunction.apply(currentDoc);
            return true;
        }
    }
}
