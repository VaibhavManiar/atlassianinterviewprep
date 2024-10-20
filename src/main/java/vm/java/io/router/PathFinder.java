package vm.java.io.router;

public interface PathFinder {
    boolean isPathFound(String path);
    String getFunction(String path);
}
