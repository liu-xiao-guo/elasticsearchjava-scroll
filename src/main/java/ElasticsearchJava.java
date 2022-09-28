import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import co.elastic.clients.elasticsearch._types.Time;

public class ElasticsearchJava {

    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;

    private static synchronized void makeConnection() {
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "FW5S2hBXhCNZDZ7BX9O-"));

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient restClient = builder.build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        try {
            // And create the API client
            client = new ElasticsearchClient(transport);
            asyncClient = new ElasticsearchAsyncClient(transport);
        } catch (Exception e) {
            System.out.println("Error in connecting Elasticsearch");
            e.printStackTrace();
        }
    }

    private static synchronized void makeConnection_https() throws CertificateException, IOException, NoSuchAlgorithmException,
            KeyStoreException, KeyManagementException {
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "FW5S2hBXhCNZDZ7BX9O-"));

        Path caCertificatePath = Paths.get("/Users/liuxg/test/elasticsearch-8.1.2/config/certs/http_ca.crt");
        CertificateFactory factory =
                CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext)
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient restClient = builder.build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);
    }

    private static synchronized void makeConnection_truststore() throws CertificateException, IOException, NoSuchAlgorithmException,
            KeyStoreException, KeyManagementException {
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "Xs5YVobQtp2Nul71YdBI"));

        String storePath = "/Users/liuxg/elastic/elasticsearch-8.4.1/config/certs/truststore.p12";
//        String keyStorePass = "dI8wbt8VTkigRjxvOrvP9w";
        String keyStorePass = "password";
        Path trustStorePath = Paths.get(storePath);
        KeyStore truststore = KeyStore.getInstance("pkcs12");
        try (InputStream is = Files.newInputStream(trustStorePath)) {
            truststore.load(is, keyStorePass.toCharArray());
        }
        SSLContextBuilder sslBuilder = SSLContexts.custom()
                .loadTrustMaterial(truststore, null);
        final SSLContext sslContext = sslBuilder.build();
        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext)
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestClient restClient = builder.build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);
    }

    private static synchronized void makeConnection_token() throws CertificateException, IOException, NoSuchAlgorithmException,
            KeyStoreException, KeyManagementException {
        Path caCertificatePath = Paths.get("/Users/liuxg/test/elasticsearch-8.1.2/config/certs/http_ca.crt");
        CertificateFactory factory =
                CertificateFactory.getInstance("X.509");
        Certificate trustedCa;
        try (InputStream is = Files.newInputStream(caCertificatePath)) {
            trustedCa = factory.generateCertificate(is);
        }
        KeyStore trustStore = KeyStore.getInstance("pkcs12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", trustedCa);
        SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                .loadTrustMaterial(trustStore, null);
        final SSLContext sslContext = sslContextBuilder.build();

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setSSLContext(sslContext);
                    }
                });

//        String apiKeyId = "SY6uOoABwRrDJxOdlx78";
//        String apiKeySecret = "E8Ae8-FgScqT-nXCSBN0ew";
//        String apiKeyAuth =
//                Base64.getEncoder().encodeToString(
//                        (apiKeyId + ":" + apiKeySecret)
//                                .getBytes(StandardCharsets.UTF_8));
//        Header[] defaultHeaders =
//                new Header[]{new BasicHeader("Authorization",
//                        "ApiKey " + apiKeyAuth)};
//        builder.setDefaultHeaders(defaultHeaders);

        Header[] defaultHeaders =
                new Header[]{new BasicHeader("Authorization",
                        "ApiKey U1k2dU9vQUJ3UnJESnhPZGx4Nzg6RThBZTgtRmdTY3FULW5YQ1NCTjBldw==")};
        builder.setDefaultHeaders(defaultHeaders);

        RestClient restClient = builder.build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);
    }

    public static void main(String[] args) throws IOException {
//        try {
//            makeConnection_https();
//        } catch (CertificateException e) {
//            e.printStackTrace();
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        } catch (KeyStoreException e) {
//            e.printStackTrace();
//        } catch (KeyManagementException e) {
//            e.printStackTrace();
//        }

//        try {
//            makeConnection_token();
//        } catch (CertificateException e) {
//            e.printStackTrace();
//        } catch (NoSuchAlgorithmException e) {
//            e.printStackTrace();
//        } catch (KeyStoreException e) {
//            e.printStackTrace();
//        } catch (KeyManagementException e) {
//            e.printStackTrace();
//        }

        try {
            makeConnection_truststore();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }

        final String INDEX_NAME = "twitter";
        SearchRequest searchRequest = new SearchRequest.
                Builder().index(INDEX_NAME)
                .query( q -> q.bool(b -> b
                                .must(must->must.match(m ->m.field("city").query("北京")))
                                .filter(f -> f.range(r -> r.field("age").gte(JsonData.of(0)).lte(JsonData.of(100))))
                              )
                      )
                .size(2)
                .scroll(Time.of(t -> t.time("2m")))
                .build();

        SearchResponse<Twitter> response = client.
                search(searchRequest, Twitter.class);

        do {
            System.out.println("size: " + response.hits().hits().size());

            for (Hit<Twitter> hit : response.hits().hits()) {
                System.out.println("hit: " + hit.index() + ": " + hit.id());
            }

            final SearchResponse<Twitter> old_response = response;
            System.out.println("scrollId: " + old_response.scrollId());

            response = client.scroll(s -> s.scrollId(old_response.scrollId()).scroll(Time.of(t -> t.time("2m"))),
                    Twitter.class);

            System.out.println("=================================");

        } while (response.hits().hits().size() != 0); // 0 hits mark the end of the scroll and the while loop.
    }
}