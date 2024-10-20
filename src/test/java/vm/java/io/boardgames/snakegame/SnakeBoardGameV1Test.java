package vm.java.io.boardgames.snakegame;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SnakeBoardGameV1Test {

    private SnakeBoardGame game;

    @BeforeEach
    public void setup() {
        int size = 5;
        int[][] foodPos = new int[size][size];
        int[][] wallPos = new int[size][size];
        game = new SnakeBoardGameV1(size, foodPos, wallPos);
    }

    @Test
    public void testInitialScore() {
        assertEquals(0, game.score());
    }

    @Test
    public void testMoveUpFromZeroPos() {
        assertThrows(RuntimeException.class, () -> game.move(Direction.UP));
    }

    @Test
    public void testMoveDownFromZeroPos() {
        int score = game.move(Direction.DOWN);
        assertEquals(0, score);
        assertEquals(0, game.score());
    }

    @Test
    public void testMoveLeftFromZeroPos() {
        assertThrows(RuntimeException.class, () -> game.move(Direction.LEFT));
    }

    @Test
    public void testMoveRightFromZeroPos() {
        int score = game.move(Direction.RIGHT);
        assertEquals(0, score);
        assertEquals(0, game.score());
    }

    @Test
    public void testGameScore() {

        int size = 5;
        int[][] foodPos = new int[size][size];
        foodPos[0][1] = 1;
        int[][] wallPos = new int[size][size];
        game = new SnakeBoardGameV1(size, foodPos, wallPos);

        int score = game.move(Direction.RIGHT);
        assertEquals(1, score);
        assertEquals(1, game.score());
        assertEquals(2, game.currentPositions().size());
    }

    @Test
    public void testGameWithWall() {
        int size = 5;
        int[][] foodPos = new int[size][size];
        foodPos[0][1] = 1;
        int[][] wallPos = new int[size][size];
        wallPos[1][0] = 1;
        game = new SnakeBoardGameV1(size, foodPos, wallPos);

        assertThrows(RuntimeException.class, () -> game.move(Direction.DOWN));
        assertEquals(0, game.score());
        assertEquals(0, game.currentPositions().size());
    }

    @Test
    public void testGameWithSelfHit() {
        int size = 5;
        int[][] foodPos = new int[size][size];
        foodPos[0][1] = 1;
        foodPos[1][0] = 1;
        int[][] wallPos = new int[size][size];
        game = new SnakeBoardGameV1(size, foodPos, wallPos);

        game.move(Direction.DOWN);
        assertThrows(RuntimeException.class, () -> game.move(Direction.UP));
        assertEquals(1, game.score());
        assertEquals(0, game.currentPositions().size());
    }

    @Test
    public void testZeroBoardSize() {
        int size = 0;
        assertThrows(IllegalArgumentException.class, () -> new SnakeBoardGameV1(size, null, null));
    }
}