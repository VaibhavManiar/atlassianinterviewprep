package vm.java.io.router;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DefaultPathFinderTest {

    private DefaultPathFinder pathFinder;

    @BeforeEach
    void setUp() {
        pathFinder = new DefaultPathFinder();
        // Setup some paths for testing
        PathNode root = pathFinder.getRoot();
        root.addNextPath("home");
        root.getNextPath("home").ifPresent(home -> {
            home.setFunctionName("homeFunction");
            home.addNextPath("about");
            home.getNextPath("about").ifPresent(about -> {
                about.setFunctionName("aboutFunction");
                about.addNextPath("me");
                about.getNextPath("me").ifPresent(me -> me.setFunctionName("meFunction"));
            });

            home.addNextPath("that");
            home.getNextPath("that").ifPresent(about -> {
                about.setFunctionName("thatFunction");
                about.addNextPath("object");
                about.getNextPath("object").ifPresent(me -> me.setFunctionName("objectFunction"));
            });
        });
    }

    @Test
    void testIsPathFound_Positive() {
        assertTrue(pathFinder.isPathFound("home"));
        assertTrue(pathFinder.isPathFound("home/about"));
    }

    @Test
    void testIsPathFoundWithStarCharacter_Positive() {
        assertTrue(pathFinder.isPathFound("home"));
        assertTrue(pathFinder.isPathFound("home/*/me"));
    }

    @Test
    void testIsPathFoundWithStarCharacterInTheEnd_Positive() {
        assertTrue(pathFinder.isPathFound("home"));
        assertTrue(pathFinder.isPathFound("home/about/*"));
    }

    @Test
    void testIsPathFoundWithStarCharacterInTheStart_Positive() {
        assertTrue(pathFinder.isPathFound("home"));
        assertTrue(pathFinder.isPathFound("*/about/me"));
    }

    @Test
    void testIsPathFound_Negative() {
        assertFalse(pathFinder.isPathFound("home/contact"));
        assertFalse(pathFinder.isPathFound("home/about/team"));
    }

    @Test
    void testGetFunction_Positive() {
        assertEquals("homeFunction", pathFinder.getFunction("home"));
        assertEquals("aboutFunction", pathFinder.getFunction("home/about"));
    }

    @Test
    void testGetFunction_Negative() {
        Exception exception = assertThrows(RuntimeException.class, () -> {
            pathFinder.getFunction("home/contact");
        });
        assertEquals("Path not found : home/contact", exception.getMessage());

        exception = assertThrows(RuntimeException.class, () -> {
            pathFinder.getFunction("home/about/team");
        });
        assertEquals("Path not found : home/about/team", exception.getMessage());
    }

    @Test
    void testWildcardPath() {
        PathNode root = pathFinder.getRoot();
        root.addNextPath("*");
        root.getNextPath("*").ifPresent(wildcard -> wildcard.setFunctionName("wildcardFunction"));

        assertTrue(pathFinder.isPathFound("*"));
        assertEquals("wildcardFunction", pathFinder.getFunction("*"));
    }

    @Test
    void testEmptyPath() {
        Exception exception = assertThrows(RuntimeException.class, () -> {
            pathFinder.isPathFound("");
        });
        assertEquals("Path not found : ", exception.getMessage());

        exception = assertThrows(RuntimeException.class, () -> {
            pathFinder.getFunction("");
        });
        assertEquals("Path not found : ", exception.getMessage());
    }

    @Test
    void testNullPath() {
        Exception exception = assertThrows(RuntimeException.class, () -> {
            pathFinder.isPathFound(null);
        });
        assertEquals("Path not found : null", exception.getMessage());

        exception = assertThrows(RuntimeException.class, () -> {
            pathFinder.getFunction(null);
        });
        assertEquals("Path not found : null", exception.getMessage());
    }
}