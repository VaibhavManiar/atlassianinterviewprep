import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vm.java.io.ratelimiter.Request;
import vm.java.io.ratelimiter.SlidingWindowRateLimiter;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SlidingWindowRateLimiterTest {

    SlidingWindowRateLimiter rateLimiter;

    @BeforeEach
    public void setup() {
        rateLimiter = new SlidingWindowRateLimiter(10000, 1000);
    }

    @Test
    public void testGetToken_Positive1() {
        Optional<String> token = rateLimiter.tryAcquire(new Request(1, 1));
        assertTrue(token.isPresent());
    }

    @Test
    public void testGetToken_Positive2() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            Optional<String> token = rateLimiter.tryAcquire(new Request(1, 1));
            Thread.sleep(10);
            assertTrue(token.isPresent());
        }
    }

    @Test
    public void testGetToken_Negative() {
        for (int i = 0; i < 1005; i++) {
            rateLimiter.tryAcquire(new Request(1, 1));
        }
        Optional<String> token = rateLimiter.tryAcquire(new Request(1, 1));
        assertFalse(token.isPresent());
    }
}
