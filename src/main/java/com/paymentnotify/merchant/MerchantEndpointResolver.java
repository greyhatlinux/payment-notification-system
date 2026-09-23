package com.paymentnotify.merchant;

import com.paymentnotify.config.MerchantSimulatorProperties;
import com.paymentnotify.domain.MerchantId;
import org.springframework.stereotype.Component;

/**
 * Maps a merchant to the (simulated) URL notifications are POSTed to. The
 * merchant simulator (Phase 4) listens on one embedded HTTP server and
 * routes by merchant id in the path, so every merchant gets an isolated
 * "endpoint" without spinning up a real server per merchant.
 */
@Component
public class MerchantEndpointResolver {

    private final int simulatorPort;

    public MerchantEndpointResolver(MerchantSimulatorProperties props) {
        this.simulatorPort = props.port();
    }

    public String resolve(MerchantId merchantId) {
        return "http://localhost:" + simulatorPort + "/merchants/" + merchantId.value() + "/notify";
    }
}
