package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.utils.AuthorSuggestNameSplitter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/*
    Récupéré depuis le GitHub de
     * Steeve SINAGAGLIA
     * Damien RENAUD
    Car nos données ont été insérées à l'aide du script insert_actors fourni dans le elasticsearch-102 :
        nous n'avons pas réussi à obtenir un environnement Java stable pendant le TP...
        Notre script (js) est disponible dans ce package
*/
public class CompletionLoader {
    private static AtomicLong count = new AtomicLong(0);
    private static final long MB_SIZE = 1048576;
    private static final Logger logger = LoggerFactory.getLogger(CompletionLoader.class);

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }


        String inputFilePath = args[0];


        LinkedList<BulkRequest> requests = new LinkedList<>();
        requests.add(new BulkRequest());

        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader
                    .lines()
                    .forEach(line -> {

                        // If the BulkRequest size is greater or equals than 9MB.
                        // Creates a new BulkRequest.
                        if (requests.peekLast().estimatedSizeInBytes() > 9 * MB_SIZE) {
                            requests.add(new BulkRequest());
                            logger.info("The {} bulkRequest was created", requests.size());

                        }

                        line = line.substring(1, line.length() - 1); //remove double quote

                        Map<String, Object> jsonMap = new HashMap<>();

                        jsonMap.put("name", line);
                        String[] strings = line.trim().split("\\s+");
                        jsonMap.put("suggest", AuthorSuggestNameSplitter.getAllCombinations(line));

                        BulkRequest currentRequest = requests.peekLast(); //takes the last request manipulated.
                        currentRequest.add(new IndexRequest("imdb", "actors")
                                .source(jsonMap)
                        );
                    });
        }

        long totalRequestSize = requests.stream().mapToLong(BulkRequest::estimatedSizeInBytes).sum();
        totalRequestSize /= MB_SIZE;
        logger.info("total of {} requests for a size of {}MB", requests.size(), totalRequestSize);

        final long totalItemsToInsert = requests.stream().mapToInt(r -> r.requests().size()).sum();
        logger.info("total of actors : {}", totalItemsToInsert);

        makeAllRequestsAsynch(client, requests);

    }

    /**
     * Makes all BulkRequest asynchronously. A BulkRequest is sent after the callback of the previous one (recursive function).
     *
     * @param client   client of elasticsearch db.
     * @param requests list of all BulkRequest (A BulkRequest data lenght is lte to 10MB).
     *                 This is a LinkedList in order to poll it from BulkRequest.
     * @throws IOException
     */
    public static void makeAllRequestsAsynch(RestHighLevelClient client, LinkedList<BulkRequest> requests) throws IOException {

        if (requests.size() == 0) {
            client.close();
            logger.info("Inserted total of {} actors", count);
            return;
        }

        final BulkRequest currentRequest = requests.pollFirst();

        client.bulkAsync(currentRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                count.addAndGet(bulkItemResponses.getItems().length); //increments the count number
                logger.info("Inserted {} new actors", count);
                try {
                    makeAllRequestsAsynch(client, requests); //call the same function with the tail of the list
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Reponse fail ", e);
                try {
                    client.close();
                    return;
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        });
    }

}