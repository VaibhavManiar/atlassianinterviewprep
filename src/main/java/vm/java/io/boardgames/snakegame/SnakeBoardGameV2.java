package vm.java.io.boardgames.snakegame;

import vm.java.io.boardgames.Board;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

public class SnakeBoardGameV2 implements SnakeBoardGame {

    private final Board board;
    private final Deque<Integer> snakeBody;
    private final Set<Integer> currSnakePositions;
    private final int[][] foodPos;
    private final int[][] wallPos;
    private int score;
    private Direction currDirection;

    public SnakeBoardGameV2(int boardSize, int[][] foodPos, int[][] wallPos, Direction currDirection) {
        this.board = new Board(boardSize);

        this.snakeBody = new ArrayDeque<>();
        this.snakeBody.add(0);

        this.currSnakePositions = new HashSet<>();
        this.currSnakePositions.add(0);


        this.foodPos = foodPos;
        this.wallPos = wallPos;

        this.score = 0;
        this.currDirection = currDirection;

        while (true) {
            try {
                moveToDirection();
            } catch (Exception ex) {
                System.err.println(ex.getMessage());
                break;
            }
        }
    }

    public int move(Direction direction) {
        this.currDirection = direction;
        return this.score;
    }

    @Override
    public Deque<Integer> currentPositions() {
        return this.snakeBody;
    }

    @Override
    public Board getBoard() {
        return this.board;
    }

    public int score() {
        return this.score;
    }

    private int moveToDirection() {

        int flattenPos = snakeBody.peekFirst();
        int row = row(flattenPos);
        int col = col(flattenPos);
        int nextRow = row;
        int nextCol = col;

        // Calculate next position
        if (Direction.UP.equals(currDirection)) {
            nextRow--;
        } else if (Direction.DOWN.equals(currDirection)) {
            nextRow++;
        } else if (Direction.LEFT.equals(currDirection)) {
            nextCol--;
        } else if (Direction.RIGHT.equals(currDirection)) {
            nextCol++;
        }

        // Validate next positions
        if (nextRow < 0 || nextRow >= board.size() || nextCol < 0 || nextCol >= board.size()) {
            while(!snakeBody.isEmpty()) {
                currSnakePositions.remove(snakeBody.pollFirst());
            }
            throw new RuntimeException("Game Ends. Snake hit at pos[: " + nextRow + "," + nextCol + "]. Score : " + score);
        }

        int nextFlattenPos = flattenPosition(nextRow, nextCol);
        // Validate Wall Positions
        if (wallPos[nextRow][nextCol] == 1) {
            while(!snakeBody.isEmpty()) {
                currSnakePositions.remove(snakeBody.pollFirst());
            }
            throw new RuntimeException("Game Ends. Snake hit at pos[: " + nextRow + "," + nextCol + "]. Score : " + score);
        }

        // Validate Snake is not hitting at its own
        else if (currSnakePositions.contains(nextFlattenPos)) {
            while(!snakeBody.isEmpty()) {
                currSnakePositions.remove(snakeBody.pollFirst());
            }
            throw new RuntimeException("Game Ends. Snake hit at pos[: " + nextRow + "," + nextCol + "]. Score : " + score);
        }

        // Validate Food Position
        else if (foodPos[nextRow][nextCol] == 1) {
            snakeBody.offerFirst(nextFlattenPos);
            score++;
        }

        // Neither a wall nor food nor any other hit
        else {
            int lastFlattenPos = snakeBody.pollLast();
            snakeBody.offerFirst(nextFlattenPos);
            currSnakePositions.remove(lastFlattenPos);
        }
        return score;
    }

    // Converts 2D grid coordinates to a single integer.
    private int flattenPosition(int row, int col) {
        return row * board.size() + col;
    }

    private int row(int flattenValue) {
        return flattenValue / board.size();
    }

    private int col(int flattenValue) {
        return flattenValue % board.size();
    }
}
