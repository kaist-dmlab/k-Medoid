package dmlab.main;

public class Output {
    private FloatPoint point;
    private double lastError;
    private int numberOfIterations;

    public double getLastError() {
        return lastError;
    }

    public void setLastError(double lastError) {
        this.lastError = lastError;
    }

    public FloatPoint getPoint() {
        return point;
    }

    public void setPoint(FloatPoint point) {
        this.point = point;
    }

    public int getNumberOfIterations() {
        return numberOfIterations;
    }

    public void setNumberOfIterations(int numberOfIterations) {
        this.numberOfIterations = numberOfIterations;
    }
}
