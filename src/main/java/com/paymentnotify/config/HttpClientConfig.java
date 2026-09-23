package com.paymentnotify.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * One shared, connection-pooled {@link HttpClient} for all merchant calls
 * (C1 and C2 alike). Java's HttpClient multiplexes connections internally
 * and performs I/O on a small bounded selector-driven pool — we still give
 * it an explicit bounded executor for response-handling callbacks so it
 * never falls back to creating threads without limit.
 */
@Configuration
public class HttpClientConfig {

    @Bean(destroyMethod = "close")
    public HttpClient merchantHttpClient(HttpClientProperties props, ExecutorService httpClientExecutor) {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(props.connectTimeoutMs()))
                .executor(httpClientExecutor)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService httpClientExecutor() {
        return Executors.newFixedThreadPool(64, runnable -> {
            Thread t = new Thread(runnable, "merchant-http-io");
            t.setDaemon(true);
            return t;
        });
    }
}
