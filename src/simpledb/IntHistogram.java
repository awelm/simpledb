package simpledb;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int min;
    private int max;
    private int buckets;
    private int bucketWidth;
    private HashMap<Integer, Integer> bucketCount;
    private int valueCount;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.valueCount = 0;
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.bucketWidth = (int) Math.ceil((max-min+1.0)/buckets);
        bucketCount = new HashMap<Integer, Integer>();
        for(int x=0; x<buckets; x++)
            bucketCount.put(x, 0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int bucket = (int) Math.floor((v-min)/bucketWidth);
        bucketCount.put(bucket, bucketCount.get(bucket)+1);
        valueCount++;
    }

    private double _estimateEqualitySelectivity(int v) {
        int bucket = (int) Math.floor((v-min)/bucketWidth);
        if(bucket >= 0 && bucket < this.buckets)
            return (bucketCount.get(bucket) / (1.0 * bucketWidth)) / valueCount;
        else
            return 0;
    }

    private double _estimateStrictComparisonSelectivity(boolean greaterThan, int v) {
        int bucket = (int) Math.floor((v-min)/bucketWidth);

        if(bucket >= 0 && bucket < this.buckets) {
            int bucketStartValue = min + bucket * bucketWidth;
            int bucketEndValue = min + (bucket+1) * bucketWidth - 1;
            int valuesInBucket = bucketCount.get(bucket);
            double valuesMatching;

            if(greaterThan)
                valuesMatching = valuesInBucket * ((bucketEndValue - v*1.0) / bucketWidth);
            else
                valuesMatching = valuesInBucket * ((v*1.0 - bucketStartValue) / bucketWidth);

            int nextBucket = bucket + (greaterThan ? 1 : -1);
            while(nextBucket >= 0 && nextBucket < this.buckets) {
                valuesMatching += bucketCount.get(nextBucket);
                nextBucket = nextBucket + (greaterThan ? 1 : -1);
            }

            return valuesMatching / valueCount;
        }
        else if(bucket < 0)
            return greaterThan ? 1 : 0;
        else
            return greaterThan ? 0 : 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch(op) {
            case EQUALS:
            case LIKE:
                return _estimateEqualitySelectivity(v);
            case NOT_EQUALS:
                return 1 - _estimateEqualitySelectivity(v);
            case GREATER_THAN:
                return _estimateStrictComparisonSelectivity(true, v);
            case LESS_THAN:
                return _estimateStrictComparisonSelectivity(false, v);
            case LESS_THAN_OR_EQ:
                return _estimateEqualitySelectivity(v) + _estimateStrictComparisonSelectivity(false, v);
            case GREATER_THAN_OR_EQ:
                return _estimateEqualitySelectivity(v) + _estimateStrictComparisonSelectivity(true, v);
        }
        return -1;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return String.format("Histogram with a min value %d, max value %d, %d buckets, and %d tuples", min, max, this.buckets, valueCount);
    }
}
