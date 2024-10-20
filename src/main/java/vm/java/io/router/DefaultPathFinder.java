package vm.java.io.router;

import java.util.Optional;

public class DefaultPathFinder implements PathFinder {

    private final PathNode root;

    public DefaultPathFinder() {
        this.root = new PathNode("root");
    }

    @Override
    public boolean isPathFound(String path) {
        if (path == null || path.isEmpty()) throw new IllegalArgumentException("Path not found : " + path);
        return findPathNode(root, path.split("/"), 0)
                .map(PathNode::isEndOfPath)
                .orElse(false);
    }

    @Override
    public String getFunction(String path) {
        if (path == null || path.isEmpty()) throw new IllegalArgumentException("Path not found : " + path);
        return findPathNode(root, path.split("/"), 0)
                .map(PathNode::getFunctionName)
                .orElseThrow(() -> new RuntimeException("Path not found : " + path));
    }

    private Optional<PathNode> findPathNode(PathNode node, String[] paths, int index) {

        //
        if (index == paths.length) {
            if(node.isEndOfPath()) {
                return Optional.of(node);
            } else {
                return Optional.empty();
            }
        }

        String path = paths[index];
        if ("*".equals(path)) {
            for (PathNode nextPath : node.getNextPaths()) {
                if(findPathNode(nextPath, paths, index + 1).isPresent()) {
                    return Optional.of(nextPath);
                }
            }
            return Optional.empty();
        } else if (node.getNextPath(path).isPresent()) {
            PathNode nextPath = node.getNextPath(path).get();
            return findPathNode(nextPath, paths, index + 1);
        } else {
            return Optional.empty();
        }
    }

    public PathNode getRoot() {
        return root;
    }
}
