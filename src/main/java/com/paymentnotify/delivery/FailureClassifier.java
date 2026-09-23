package com.paymentnotify.delivery;

import com.paymentnotify.config.FailureClassificationProperties;
import com.paymentnotify.domain.FailureCategory;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Decides whether a failed HTTP status should be retried. Exceptions
 * (timeout, connection failure) are always retryable — they're classified
 * directly by {@link HttpMerchantClient}, not through this class.
 */
@Component
public class FailureClassifier {

    private final Set<Integer> retryable;
    private final Set<Integer> permanent;

    public FailureClassifier(FailureClassificationProperties props) {
        this.retryable = Set.copyOf(props.retryableStatusCodes());
        this.permanent = Set.copyOf(props.permanentStatusCodes());
    }

    public FailureCategory classify(int httpStatus) {
        if (retryable.contains(httpStatus)) {
            return FailureCategory.RETRYABLE;
        }
        if (permanent.contains(httpStatus)) {
            return FailureCategory.PERMANENT;
        }
        if (httpStatus >= 500 && httpStatus < 600) {
            return FailureCategory.RETRYABLE;
        }
        if (httpStatus >= 400 && httpStatus < 500) {
            return FailureCategory.PERMANENT;
        }
        // Anything else unexpected: fail safe by retrying.
        return FailureCategory.RETRYABLE;
    }
}
