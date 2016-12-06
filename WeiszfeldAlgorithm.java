package dmlab.main;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by josue on 12/08/15.
 * To get the geometric median for a set of n-dimensional points.
 * Based on the Modified Weiszfeld Method from
 * @link{http://ie.technion.ac.il/Home/Users/becka/Weiszfeld_review-v3.pdf}
 * described up to the section 6.3
 */
public class WeiszfeldAlgorithm implements Algorithm<Input, Output> {
    
    @Override
    public Output process(Input input, FloatPoint start) {

        // filtering repeated points
        Map<FloatPoint, Double> map = new HashMap<>();
        for (FloatPoint wPoint : input.getPoints()) {
            FloatPoint point = wPoint;
            if (map.containsKey(point)) {
                map.put(point, map.get(point) + wPoint.getWeight());
            } else {
                map.put(point, wPoint.getWeight());
            }
        }
        
        // anchor points
        List<FloatPoint> aPoints = new ArrayList<>(map.size());
        for (Map.Entry<FloatPoint, Double> entry : map.entrySet()) {
            FloatPoint wPoint = entry.getKey();
            wPoint.setWeight(entry.getValue());
            aPoints.add(wPoint);
        }

        int n = input.getDimension();
        int maxIterations = Integer.MAX_VALUE;
        if (input.getMaxIterations() != null) {
            maxIterations = input.getMaxIterations();
        }
        Double permissibleError = input.getPermissibleError();
        if (permissibleError == null) {
            permissibleError = Double.MIN_VALUE;
        }
        
        FloatPoint x = start;
        
        FloatPoint lastX;
        double error;
        int iterationCounter = 0;
        do {
            lastX = x;
            if (map.containsKey(x)) {
                FloatPoint rj = R(x, aPoints, n);
                double wj = map.get(x);
                if (rj.getNorm() > wj) {
                    x = operatorS(x, wj, rj, aPoints, n);
                }
            } else {
                x = operatorT(x, aPoints, n);
            }
            error = FloatPoint.substraction(x, lastX).getNorm();
            iterationCounter++;
        } while (error > permissibleError && iterationCounter < maxIterations);
        
        /* 
         * Stops whenever the error is less than or equal the permissibleError
         *  or reaches the maximum number of iterations.
         */

        Output output = new Output();
        output.setPoint(x);
        output.setLastError(error);
        output.setNumberOfIterations(iterationCounter);
        
        return output;
    }
    private FloatPoint operatorT(FloatPoint x, List<FloatPoint> aPoints, int dimension) {
        FloatPoint result = new FloatPoint(dimension,-1);

        double weightsSum = 0;
        for (FloatPoint a: aPoints) {
            double w = a.getWeight();
            double curWeight = w/FloatPoint.substraction(x,a).getNorm();
            FloatPoint cur = FloatPoint.multiply(a, curWeight);

            weightsSum += curWeight;
            result.add(cur);
        }

        return result.multiply(1d/weightsSum);
    }
    
    private FloatPoint operatorS(FloatPoint aj, double wj, FloatPoint rj
            , List<FloatPoint> aPoints, int dimension) {
        double rjNorm = rj.getNorm();        
        FloatPoint dj = new FloatPoint(dimension,-1);
        dj.add(rj);
        dj.multiply(-1.0/rjNorm);
        
        // calculating tj (stepsize) taken from Vardi and Zhang
        double lj = operatorL(aj, aPoints);
        double tj = (rjNorm - wj)/lj;
        
        dj.multiply(tj);
        dj.add(aj);
        
        return dj;
    }
    
    private FloatPoint R(FloatPoint aj, List<FloatPoint> aPoints, int dimension) {
        FloatPoint result = new FloatPoint(dimension,-1);
        
        for (FloatPoint ai: aPoints) {
            if (ai.compareTo(aj) != 0) {
                double w = ai.getWeight();
                FloatPoint dif = FloatPoint.substraction(ai, aj);
                double factor = w/dif.getNorm();
                dif.multiply(factor);

                result.add(dif);
            }
        }
        
        return result;
    }

    private double operatorL(FloatPoint aj, List<FloatPoint> aPoints) {
        double res = 0;
        for (FloatPoint ai: aPoints) {
            if (aj.compareTo(ai) != 0) {
                FloatPoint dif = FloatPoint.substraction(aj, ai);
                res += ai.getWeight()/dif.getNorm();
            }
        }
        return res;
    }
    
    /**
     * Evaluating the objective function in a given point x
     * @param x Point to evaluate the function.
     * @param aPoints List of weighted points.
     * @return 
     */
    private double evaluateF(FloatPoint x, List<FloatPoint> aPoints) {
        double res = 0;
        for (FloatPoint ai: aPoints) {
            res += ai.getWeight() * FloatPoint.substraction(ai, x).getNorm();
        }
        return res;
    }
    

}
