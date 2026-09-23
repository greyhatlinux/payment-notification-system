package com.paymentnotify.merchant;

import com.paymentnotify.config.MerchantsProperties;
import com.paymentnotify.domain.MerchantId;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The fixed set of demo merchants known to the system. Real merchant
 * onboarding is out of scope; this just gives the traffic generator and
 * failure-injection UI a stable list to work with.
 */
@Component
public class MerchantRegistry {

    private final List<MerchantId> merchants;

    public MerchantRegistry(MerchantsProperties props) {
        this.merchants = props.ids().stream().map(MerchantId::of).collect(Collectors.toUnmodifiableList());
    }

    public List<MerchantId> all() {
        return merchants;
    }

    public boolean isKnown(MerchantId id) {
        return merchants.contains(id);
    }
}
