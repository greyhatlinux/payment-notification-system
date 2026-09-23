package com.paymentnotify.merchant;

/** The kind of failure the merchant simulator injects when its failure roll hits. */
public enum FailureMode {
    SUCCESS,
    HTTP_4XX,
    HTTP_429,
    HTTP_500,
    HTTP_503,
    TIMEOUT
}
