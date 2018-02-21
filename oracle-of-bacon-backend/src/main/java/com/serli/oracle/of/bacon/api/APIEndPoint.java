package com.serli.oracle.of.bacon.api;

import com.google.gson.Gson;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import net.codestory.http.annotations.Get;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {
        redisRepository.addActor(actorName);
        return new Gson().toJson((ArrayList) neo4JRepository.getConnectionsToKevinBacon(actorName));
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) throws IOException {
        return elasticSearchRepository.getActorsSuggests(searchQuery);
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return null;//redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
        /*Optional<Document> optionalDocument = mongoDbRepository.getActorByName(actorName);
        if(optionalDocument.isPresent()){
            return new Gson().toJson(optionalDocument.get());
        }*/
        return null;
    }
}
