package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;

import java.util.*;

public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
    }

    public List<?> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        Transaction t = session.beginTransaction();

        // MATCH p=shortestPath((bacon:Person {name:"Bacon, Kevin (I)"})-[*]-(act:Person {name: "Hewetson, Joe"})) RETURN p
        StatementResult sr = t.run("MATCH p=shortestPath((bacon:Person {name:\"Bacon, Kevin (I)\"})-[*]-(act:Person {name: \"" + actorName + "\"})) RETURN p");

        ArrayList<Map<String, GraphItem>> result = new ArrayList<Map<String, GraphItem>>();

        while (sr.hasNext()) {
            Record rec = sr.next();
            Path path = (Path) rec.asMap().get("p");

            // On parcourt chaque noeud, et on cree un GraphNode pour chaque
            path.nodes().forEach(node -> {
                long id;
                String type, value;
                GraphNode graphNode;

                id = node.id();
                if(node.containsKey("name")) {
                    graphNode = new GraphNode(
                                        node.id(),
                                        node.get("name").asString(),
                                        "Person");
                    HashMap<String, GraphItem> map = new HashMap();
                    map.put("data", graphNode);
                    result.add(map);
                }

                if(node.containsKey("title")) {
                    type = "Movie";
                    value = node.get("title").asString();
                    graphNode = new GraphNode(
                                        node.id(),
                                        node.get("title").asString(),
                                        "Movie");
                    HashMap<String, GraphItem> map = new HashMap();
                    map.put("data", graphNode);
                    result.add(map);
                }
            });

            // On parcourt chaque arete, et on cree un GraphEdge pour chaque
            path.relationships().forEach(r -> {
                GraphEdge graphEdge = new GraphEdge(
                                        r.id(),
                                        r.startNodeId(),
                                        r.endNodeId(),
                                        "PLAYED_IN");
                HashMap<String, GraphItem> map = new HashMap();
                map.put("data", graphEdge);
                result.add(map);
            });
        }

        t.success();
        return result;
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
