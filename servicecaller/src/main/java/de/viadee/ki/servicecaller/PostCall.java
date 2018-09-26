package de.viadee.ki.servicecaller;

import java.io.IOException;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.http.HttpHeaders;

/**
 * Post
 */
public class PostCall {

    private final static Logger LOGGER = Logger.getLogger("LOAN-REQUESTS");

    public String postCall(String payload, String address) {

        String result = null;
        HttpClientBuilder builder = HttpClientBuilder.create();
        HttpClient client = builder.build();
        HttpPost post = new HttpPost(address);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        HttpEntity body = new ByteArrayEntity(payload.getBytes());
        post.setEntity(body);

        HttpResponse response = null;

        try {
            response = client.execute(post);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            result = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }


}
