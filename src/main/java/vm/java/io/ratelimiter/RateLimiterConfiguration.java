package vm.java.io.ratelimiter;

/**
 * type: FIXED_WINDOW
 * max_allowed_request_count: 1000
 * window_size: 10000
 * enabled: true
 */
public class RateLimiterConfiguration {
    private final RateLimiterType rateLimiterType;
    private final long maxAllowedRequests;
    private final long windowSize;
    private final boolean isEnabled;

    public RateLimiterConfiguration(RateLimiterType rateLimiterType, long maxAllowedRequests, long windowSize, boolean isEnabled) {
        this.rateLimiterType = rateLimiterType;
        this.maxAllowedRequests = maxAllowedRequests;
        this.windowSize = windowSize;
        this.isEnabled = isEnabled;
    }

    public RateLimiterType getType() {
        return rateLimiterType;
    }

    public long getMaxAllowedRequests() {
        return maxAllowedRequests;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public boolean isEnabled() {
        return isEnabled;
    }
}
