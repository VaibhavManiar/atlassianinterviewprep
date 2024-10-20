package vm.java.io.router;

public class DefaultRequestRouter implements RequestRouter {

    private final PathFinder pathFinder;

    public DefaultRequestRouter() {
        this.pathFinder = new DefaultPathFinder();
    }


    @Override
    public String getFunctionName(String path) {
        return pathFinder.getFunction(path);
    }
}
