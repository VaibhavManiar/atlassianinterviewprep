
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vm.java.io.ratelimiter.FixedWindowRateLimiter;
import vm.java.io.ratelimiter.Request;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class FixedWindowRateLimiterTest {

    private FixedWindowRateLimiter rateLimiter;

    @BeforeEach
    public void setUp() {
        rateLimiter = new FixedWindowRateLimiter(1000, 5); // 1 second window, 5 tokens
    }

    @Test
    public void testAcquireTokenWithinLimit() {
        Request request = new Request(1L, 1L);
        Optional<String> token = rateLimiter.tryAcquire(request);
        assertTrue(token.isPresent());
    }

    @Test
    public void testAcquireMultipleTokensWithinLimit() {
        Request request = new Request(1L, 1L);
        for (int i = 0; i < 5; i++) {
            Optional<String> token = rateLimiter.tryAcquire(request);
            assertTrue(token.isPresent());
        }
    }

    @Test
    public void testAcquireTokenAtLimitBoundary() {
        Request request = new Request(1L, 1L);
        for (int i = 0; i < 5; i++) {
            rateLimiter.tryAcquire(request);
        }
        Optional<String> token = rateLimiter.tryAcquire(request);
        assertFalse(token.isPresent());
    }

    @Test
    public void testAcquireTokenAfterWindowRefresh() throws InterruptedException {
        Request request = new Request(1L, 1L);
        for (int i = 0; i < 5; i++) {
            rateLimiter.tryAcquire(request);
        }
        Thread.sleep(1000); // Wait for window to refresh
        Optional<String> token = rateLimiter.tryAcquire(request);
        assertTrue(token.isPresent());
    }

    @Test
    public void testAcquireTokenWhenLimitExceeded() {
        Request request = new Request(1L, 1L);
        for (int i = 0; i < 5; i++) {
            rateLimiter.tryAcquire(request);
        }
        Optional<String> token = rateLimiter.tryAcquire(request);
        assertFalse(token.isPresent());
    }

    @Test
    public void testAcquireMultipleTokensWhenLimitExceeded() {
        Request request = new Request(1L, 1L);
        for (int i = 0; i < 5; i++) {
            rateLimiter.tryAcquire(request);
        }
        for (int i = 0; i < 5; i++) {
            Optional<String> token = rateLimiter.tryAcquire(request);
            assertFalse(token.isPresent());
        }
    }

    @Test
    public void testAcquireTokenWithInvalidClientId() {
        assertThrows(IllegalArgumentException.class, ()-> new Request(-1L, 1L));
    }

    @Test
    public void testAcquireTokenWithZeroBucketSize() {
        FixedWindowRateLimiter zeroBucketRateLimiter = new FixedWindowRateLimiter(1000, 0);
        Request request = new Request(1L, 1L);
        Optional<String> token = zeroBucketRateLimiter.tryAcquire(request);
        assertFalse(token.isPresent());
    }
}