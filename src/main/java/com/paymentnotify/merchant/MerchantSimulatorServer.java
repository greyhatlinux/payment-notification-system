package com.paymentnotify.merchant;

import com.paymentnotify.config.MerchantSimulatorProperties;
import com.paymentnotify.domain.MerchantId;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A tiny, dependency-free "merchant": a real embedded HTTP server (JDK's
 * built-in {@link HttpServer}, no servlet container) so {@link
 * com.paymentnotify.delivery.HttpMerchantClient} exercises genuine sockets,
 * timeouts and status codes rather than an in-process fake.
 *
 * One process serves every demo merchant, routed by path
 * ({@code /merchants/{merchantId}/notify}) — that's an implementation
 * shortcut for the demo, not a claim that a real merchant works this way.
 *
 * Behavior per request is driven entirely by {@link MerchantSimulatorConfigService},
 * which the failure-injection API mutates at runtime.
 */
@Component
public class MerchantSimulatorServer {

    private static final Logger log = LoggerFactory.getLogger(MerchantSimulatorServer.class);

    /** How long a TIMEOUT-mode request hangs before the simulator gives up and closes it. */
    private static final long TIMEOUT_MODE_HANG_MS = 30_000;

    private final MerchantSimulatorConfigService configService;
    private final int port;
    private final int workerThreads;

    private HttpServer server;
    private ExecutorService executor;

    public MerchantSimulatorServer(MerchantSimulatorConfigService configService,
                                    MerchantSimulatorProperties props) {
        this.configService = configService;
        this.port = props.port();
        this.workerThreads = props.workerThreads();
    }

    @PostConstruct
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            executor = Executors.newFixedThreadPool(workerThreads, runnable -> {
                Thread t = new Thread(runnable, "merchant-sim-worker");
                t.setDaemon(true);
                return t;
            });
            server.setExecutor(executor);
            server.createContext("/merchants/", this::handle);
            server.start();
            log.info("Merchant simulator listening on port {} ({} worker threads)", port, workerThreads);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start merchant simulator on port " + port, e);
        }
    }

    /** Actual bound port — useful in tests where {@code port} is configured as 0 (ephemeral). */
    public int boundPort() {
        return server.getAddress().getPort();
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("Merchant simulator stopped");
    }

    private void handle(HttpExchange exchange) {
        try {
            handleInternal(exchange);
        } catch (Exception e) {
            log.error("Merchant simulator error handling {}", exchange.getRequestURI(), e);
            try {
                exchange.sendResponseHeaders(500, -1);
            } catch (IOException ignored) {
                // client already gone
            }
        } finally {
            exchange.close();
        }
    }

    private void handleInternal(HttpExchange exchange) throws IOException {
        // Drain the request body regardless of outcome.
        exchange.getRequestBody().readAllBytes();

        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        MerchantId merchantId = parseMerchantId(exchange.getRequestURI().getPath());
        if (merchantId == null) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        FailureInjectionConfig config = configService.configFor(merchantId);
        sleep(config.latencyMs());

        boolean shouldFail = config.failurePercentage() > 0
                && ThreadLocalRandom.current().nextInt(100) < config.failurePercentage();

        if (!shouldFail || config.mode() == FailureMode.SUCCESS) {
            respond(exchange, 200, "{\"status\":\"accepted\"}");
            return;
        }

        switch (config.mode()) {
            case HTTP_4XX -> respond(exchange, config.specific4xxStatus(), "{\"error\":\"rejected\"}");
            case HTTP_429 -> respond(exchange, 429, "{\"error\":\"rate_limited\"}");
            case HTTP_500 -> respond(exchange, 500, "{\"error\":\"internal_error\"}");
            case HTTP_503 -> respond(exchange, 503, "{\"error\":\"unavailable\"}");
            case TIMEOUT -> {
                // Never respond within any sane client timeout; just hold the
                // (bounded) worker thread and then give up.
                sleep(TIMEOUT_MODE_HANG_MS);
            }
            default -> respond(exchange, 200, "{\"status\":\"accepted\"}");
        }
    }

    private MerchantId parseMerchantId(String path) {
        // Expected: /merchants/{merchantId}/notify
        String[] parts = path.split("/");
        if (parts.length != 4 || !"merchants".equals(parts[1]) || !"notify".equals(parts[3])) {
            return null;
        }
        return MerchantId.of(parts[2]);
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
