package vm.java.io.boardgames;

public class Board {
    private int[][] board;

    public Board(int size) {
        this.board = new int[size][size];
    }

    public int valueAt(int row, int col) {
        if(row < 0 || row >= board.length || col < 0 || col >= board.length ) {
            return -1;
        }
        return board[row][col];
    }

    public int size() {
        return this.board.length;
    }
}