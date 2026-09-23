package com.paymentnotify.api;

import com.paymentnotify.dlq.DeadLetterStore;
import com.paymentnotify.domain.DeadLetterEntry;
import com.paymentnotify.domain.MerchantId;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/dlq")
public class DlqController {

    private final DeadLetterStore deadLetterStore;

    public DlqController(DeadLetterStore deadLetterStore) {
        this.deadLetterStore = deadLetterStore;
    }

    @GetMapping
    public List<DeadLetterEntry> all() {
        return deadLetterStore.all();
    }

    @GetMapping("/merchants/{merchantId}")
    public List<DeadLetterEntry> forMerchant(@PathVariable String merchantId) {
        return deadLetterStore.forMerchant(MerchantId.of(merchantId));
    }

    @GetMapping("/count")
    public long count() {
        return deadLetterStore.totalCount();
    }
}
