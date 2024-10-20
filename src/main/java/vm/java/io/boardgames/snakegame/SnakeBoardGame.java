package vm.java.io.boardgames.snakegame;

import vm.java.io.boardgames.Board;

import java.util.Deque;

public interface SnakeBoardGame {
    int score();
    int move(Direction direction);
    Deque<Integer> currentPositions();
    Board getBoard();
}
