package com.webir.popcornsaver;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

/**
 * Created by rparra on 16/10/15.
 */
public class AveragedRecommenderEvaluator {

    private RecommenderEvaluator evaluator;


    public AveragedRecommenderEvaluator() {
        this.evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
    }

    public Double evaluate(Recommender recommender, RecommenderType type, int numIterations) {
        RecommenderBuilder builder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                return new RecommenderFactory(dataModel).getRecommender(type);
            }
        };

        double resultSum = 0;
        for (int i = 0; i < numIterations; i++) {
            try {
                resultSum += evaluator.evaluate(builder, null, recommender.getDataModel(), 0.8, 1.0);
            } catch (TasteException e) {
                System.out.println("Error evaluating recommender");
            }
        }

        return resultSum / numIterations;
    }
}
