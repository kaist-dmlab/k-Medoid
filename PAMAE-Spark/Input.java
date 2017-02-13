package dmlab.main;

import java.util.List;

public class Input {
    private int dimension; // dimension of the points
    private List<FloatPoint> points;
    private Double permissibleError; // maximum permissible permissibleError
    private Integer maxIterations;

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public List<FloatPoint> getPoints() {
        return points;
    }

    public void setPoints(List<FloatPoint> points) {
        this.points = points;
    }

    public Double getPermissibleError() {
        return permissibleError;
    }

    public void setPermissibleError(Double permissibleError) {
        this.permissibleError = permissibleError;
    }

    public Integer getMaxIterations() {
        return maxIterations;
    }

    public void setMaxIterations(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }
}
