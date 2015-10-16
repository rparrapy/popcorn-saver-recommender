package com.webir.popcornsaver;

import com.google.gson.Gson;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.util.List;

import static spark.Spark.*;

/**
 * Created by rparra on 15/10/15.
 */
public class RestRecommender {
    private static Recommender getRecommenderByType(String typeLabel) {
        RecommenderFactory factory = new RecommenderFactory();
        RecommenderType type = getRecommenderTypeByQueryParam(typeLabel);
        return type == null ? null : factory.getRecommender(type);
    }

    private static RecommenderType getRecommenderTypeByQueryParam(String typeLabel) {
        RecommenderType type = null;
        switch (typeLabel) {
            case "user":
                type = RecommenderType.USER_USER;
                break;
            case "item":
                type = RecommenderType.ITEM_ITEM;
                break;
            case "svd":
                type = RecommenderType.SVD;
                break;
            case "cluster":
                type = RecommenderType.CLUSTER;
                break;
            default:
                break;
        }
        return type;
    }

    public static void main(String[] args) {
        port(7000);
        get("/recommendations", (req, res) -> {
            RecommenderFactory factory = new RecommenderFactory();
            String result = "";
            if (req.queryParams("type") != null) {
                Recommender recommender = getRecommenderByType(req.queryParams("type"));
                if(recommender == null) {
                    result = "Not a valid recommendation type";
                } else {
                    result = new Gson().toJson(recommender.recommend(1, 10));
                }
            } else {
                res.status(400);
                result = "Need a recommender type";
            }
            return result;
        });

        get("/evaluations", (req, res) -> {
            RecommenderFactory factory = new RecommenderFactory();
            String result = "";
            if (req.queryParams("type") != null) {
                Recommender recommender = getRecommenderByType(req.queryParams("type"));
                RecommenderType type = getRecommenderTypeByQueryParam(req.queryParams("type"));
                if(recommender == null) {
                    result = "Not a valid recommendation type";
                } else {
                    AveragedRecommenderEvaluator evaluator = new AveragedRecommenderEvaluator();
                    result = evaluator.evaluate(recommender, type, 10).toString();
                }
            } else {
                res.status(400);
                result = "Need a recommender type";
            }
            return result;
        });
    }
}
