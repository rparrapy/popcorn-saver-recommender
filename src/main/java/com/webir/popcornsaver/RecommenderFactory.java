package com.webir.popcornsaver;

import com.webir.popcornsaver.cluster.FarthestNeighborClusterSimilarity;
import com.webir.popcornsaver.cluster.TreeClusteringRecommender;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.RatingSGDFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.net.UnknownHostException;

/**
 * Created by rparra on 15/10/15.
 * Returns a recommender based on its type
 */
public class RecommenderFactory {

    private DataModel model;
    private UserSimilarity similarity;
    private UserNeighborhood neighborhood;
    private Factorizer factorizer;

    public RecommenderFactory() {

        try {
            this.model = new FixedMongoDBDataModel("localhost", 27017, "popcorn-saver", "ratings", false, false, null);
            this.similarity = new PearsonCorrelationSimilarity(model);
            this.neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
            this.factorizer = new RatingSGDFactorizer(model, 50, 25);
        } catch (UnknownHostException e) {
            System.out.println("Error connecting to MONGODB, is the database server running?");
        } catch (TasteException e) {
            e.printStackTrace();
            System.out.println("Error with the data model, can not compute similarity measure");
        }
    }


    public RecommenderFactory(DataModel model) {

        try {
            this.model = model;
            this.similarity = new PearsonCorrelationSimilarity(model);
            this.neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
            this.factorizer = new RatingSGDFactorizer(model, 50, 25);
        } catch (TasteException e) {
            e.printStackTrace();
            System.out.println("Error with the data model, can not compute similarity measure");
        }
    }

    public DataModel getModel() {
        return this.model;
    }

    public Recommender getRecommender(RecommenderType type) {
        Recommender recommender;
        switch (type) {
            case USER_USER:
                recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
                break;
            case ITEM_ITEM:
                recommender = new GenericItemBasedRecommender(model, (ItemSimilarity)similarity);
                break;
            case SVD:
                try {
                    recommender = new SVDRecommender(model, factorizer);
                } catch (TasteException e) {
                    System.out.println("Error creating SVDRecommender");
                    return null;
                }
                break;
            case CLUSTER:
                try {
                    recommender = new TreeClusteringRecommender(model, new FarthestNeighborClusterSimilarity(similarity), 20);
                } catch (TasteException e) {
                    System.out.println("Error creating ClusteringRecommender");
                    return null;
                }
                break;
            default:
                throw new IllegalArgumentException("Not a valid recommender type");
        }
        return recommender;
    }
}

