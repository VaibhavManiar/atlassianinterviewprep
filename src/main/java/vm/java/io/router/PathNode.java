package vm.java.io.router;

import java.util.*;

public class PathNode {
    private boolean endOfPath;
    private final String name;
    private String functionName;
    private final Map<String, PathNode> nextPaths;

    public PathNode(String name) {
        this.name = name;
        this.nextPaths = new HashMap<>();
    }

    public boolean isEndOfPath() {
        return endOfPath;
    }

    public String getName() {
        return name;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Optional<PathNode> getNextPath(String name) {
        if(name == null) return Optional.empty();
        return Optional.ofNullable(this.nextPaths.get(name));
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
        this.endOfPath = true;
    }

    public void addNextPath(String nextPath) {
        this.nextPaths.put(nextPath, new PathNode(nextPath));
    }

    public Set<PathNode> getNextPaths() {
        return new HashSet<>(nextPaths.values());
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PathNode pathNode)) return false;

        return Objects.equals(name, pathNode.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
