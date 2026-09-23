package com.paymentnotify.delivery;

import com.paymentnotify.config.FailureClassificationProperties;
import com.paymentnotify.domain.FailureCategory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FailureClassifierTest {

    private final FailureClassifier classifier = new FailureClassifier(
            new FailureClassificationProperties(List.of(408, 429), List.of(400, 401, 403, 404)));

    @Test
    void classifiesConfiguredTransientCodesAsRetryable() {
        assertThat(classifier.classify(408)).isEqualTo(FailureCategory.RETRYABLE);
        assertThat(classifier.classify(429)).isEqualTo(FailureCategory.RETRYABLE);
    }

    @Test
    void classifiesAny5xxAsRetryable() {
        assertThat(classifier.classify(500)).isEqualTo(FailureCategory.RETRYABLE);
        assertThat(classifier.classify(503)).isEqualTo(FailureCategory.RETRYABLE);
        assertThat(classifier.classify(599)).isEqualTo(FailureCategory.RETRYABLE);
    }

    @Test
    void classifiesConfiguredPermanentCodesAsPermanent() {
        assertThat(classifier.classify(400)).isEqualTo(FailureCategory.PERMANENT);
        assertThat(classifier.classify(404)).isEqualTo(FailureCategory.PERMANENT);
    }

    @Test
    void classifiesOtherFourXxAsPermanentByDefault() {
        assertThat(classifier.classify(422)).isEqualTo(FailureCategory.PERMANENT);
    }
}
