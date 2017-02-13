package dmlab.main;

public class Output {
    private DoublePoint point;
    private double lastError;
    private int numberOfIterations;

    public double getLastError() {
        return lastError;
    }

    public void setLastError(double lastError) {
        this.lastError = lastError;
    }

    public DoublePoint getPoint() {
        return point;
    }

    public void setPoint(DoublePoint point) {
        this.point = point;
    }

    public int getNumberOfIterations() {
        return numberOfIterations;
    }

    public void setNumberOfIterations(int numberOfIterations) {
        this.numberOfIterations = numberOfIterations;
    }
}
