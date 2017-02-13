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
    public Output process(Input input, DoublePoint start) {

        // filtering repeated points
        Map<DoublePoint, Double> map = new HashMap<>();
        for (DoublePoint wPoint : input.getPoints()) {
            DoublePoint point = wPoint;
            if (map.containsKey(point)) {
                map.put(point, map.get(point) + wPoint.getWeight());
            } else {
                map.put(point, wPoint.getWeight());
            }
        }
        
        // anchor points
        List<DoublePoint> aPoints = new ArrayList<>(map.size());
        for (Map.Entry<DoublePoint, Double> entry : map.entrySet()) {
            DoublePoint wPoint = entry.getKey();
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
     
       /* long starttime = System.currentTimeMillis();
        // choosing starting point
        Point startPoint = null;
        double mini = Double.POSITIVE_INFINITY;
        for (Point wPoint : aPoints) {
            double eval = evaluateF(wPoint, aPoints);
            if (eval < mini) {
                mini = eval;
                startPoint = wPoint;
            }
        }
        long endtime = System.currentTimeMillis();
        System.out.println("subtime: " + (endtime-starttime));
*/
        //random select
       // Point startPoint = aPoints.get((int)(Math.random()*aPoints.size()));
        
        
        DoublePoint x = start;
        
        DoublePoint lastX;
        double error;
        int iterationCounter = 0;
        do {
            lastX = x;
            if (map.containsKey(x)) {
                DoublePoint rj = R(x, aPoints, n);
                double wj = map.get(x);
                if (rj.getNorm() > wj) {
                    x = operatorS(x, wj, rj, aPoints, n);
                }
            } else {
                x = operatorT(x, aPoints, n);
            }
            error = DoublePoint.substraction(x, lastX).getNorm();
            iterationCounter++;
        } while (error > permissibleError && iterationCounter < maxIterations);
        /* Stops whenever the error is less than or equal the permissibleError
        *  or reaches the maximum number of iterations.
        */
        System.out.println("iteration counter : " + iterationCounter);

        Output output = new Output();
        output.setPoint(x);
        output.setLastError(error);
        output.setNumberOfIterations(iterationCounter);
        
        return output;
    }
    private DoublePoint operatorT(DoublePoint x, List<DoublePoint> aPoints, int dimension) {
        DoublePoint result = new DoublePoint(dimension,-1);

        double weightsSum = 0;
        for (DoublePoint a: aPoints) {
            double w = a.getWeight();
            double curWeight = w/DoublePoint.substraction(x,a).getNorm();
            DoublePoint cur = DoublePoint.multiply(a, curWeight);

            weightsSum += curWeight;
            result.add(cur);
        }

        return result.multiply(1d/weightsSum);
    }
    
    private DoublePoint operatorS(DoublePoint aj, double wj, DoublePoint rj
            , List<DoublePoint> aPoints, int dimension) {
        double rjNorm = rj.getNorm();        
        DoublePoint dj = new DoublePoint(dimension,-1);
        dj.add(rj);
        dj.multiply(-1.0/rjNorm);
        
        // calculating tj (stepsize) taken from Vardi and Zhang
        double lj = operatorL(aj, aPoints);
        double tj = (rjNorm - wj)/lj;
        
        dj.multiply(tj);
        dj.add(aj);
        
        return dj;
    }
    
    private DoublePoint R(DoublePoint aj, List<DoublePoint> aPoints, int dimension) {
        DoublePoint result = new DoublePoint(dimension,-1);
        
        for (DoublePoint ai: aPoints) {
            if (ai.compareTo(aj) != 0) {
                double w = ai.getWeight();
                DoublePoint dif = DoublePoint.substraction(ai, aj);
                double factor = w/dif.getNorm();
                dif.multiply(factor);

                result.add(dif);
            }
        }
        
        return result;
    }

    private double operatorL(DoublePoint aj, List<DoublePoint> aPoints) {
        double res = 0;
        for (DoublePoint ai: aPoints) {
            if (aj.compareTo(ai) != 0) {
                DoublePoint dif = DoublePoint.substraction(aj, ai);
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
    private double evaluateF(DoublePoint x, List<DoublePoint> aPoints) {
        double res = 0;
        for (DoublePoint ai: aPoints) {
            res += ai.getWeight() * DoublePoint.substraction(ai, x).getNorm();
        }
        return res;
    }
    

}
