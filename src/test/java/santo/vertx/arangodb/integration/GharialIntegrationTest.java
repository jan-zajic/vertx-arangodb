/*
 * Copyright 2014 sANTo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package santo.vertx.arangodb.integration;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.rest.GharialAPI;
/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.GharialAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GharialIntegrationTest extends BaseIntegrationTest {
    
    public static String idVertex01;
    public static String idVertex02;
    
    private String endVertices = "endVertices";
    
    /*
    @Test
    public void test00PrepareGraphDocuments(TestContext context) {
        System.out.println("*** test00PrepareGraphDocuments ***");
        JsonObject documentObject = new JsonObject().put("name", "vertex01");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
        requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
        requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(DocumentAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Document creation resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                    idVertex01 = arangoResult.getString("_id");
                    
                    // Create another document
                    JsonObject documentObject = new JsonObject().put("name", "vertex02");
                    JsonObject requestObject = new JsonObject(); final Async async = context.async();
                    requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_DOCUMENT);
                    requestObject.put(DocumentAPI.MSG_PROPERTY_ACTION, DocumentAPI.MSG_ACTION_CREATE);
                    requestObject.put(DocumentAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                    requestObject.put(DocumentAPI.MSG_PROPERTY_COLLECTION, endVertices);
                    vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> reply) {
                            try {
                                JsonObject response = reply.result().body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getJsonObject("result");
                                context.assertTrue(!arangoResult.getBoolean("error"), "Document creation resulted in an error: " + arangoResult.getString("errorMessage"));
                                if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                                idVertex02 = arangoResult.getString("_id");
                            }
                            catch (Exception e) {
                                context.fail("test00PrepareGraphDocuments");
                            }
                            async.complete();
                        }
                    });
                }
                catch (Exception e) {
                    context.fail("test00PrepareGraphDocuments");
                }
            }
        });
    }
    */

    
    @Test
    public void test01CreateGraph(TestContext context) {
        System.out.println("*** test01CreateGraph ***");
        JsonObject documentObject = new JsonObject().put(GharialAPI.DOC_ATTRIBUTE_NAME, "testgraph");
        JsonArray edgeDefinitions = new JsonArray();
        JsonObject edgeObject = new JsonObject();
        edgeObject.put("collection", edgeColName);
        edgeObject.put("from", new JsonArray().add(vertexColName));
        edgeObject.put("to", new JsonArray().add(endVertices));
        edgeDefinitions.add(edgeObject);
        documentObject.put(GharialAPI.DOC_ATTRIBUTE_EDGE_DEFINITIONS, edgeDefinitions);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_CREATE);
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Graph operation resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertNotNull(arangoResult.getJsonObject("graph"), "No graph object received");
                    context.assertNotNull(arangoResult.getJsonObject("graph").getString("_id"), "No id received");
                }
                catch (Exception e) {
                    context.fail("test01CreateGraph");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test02GetGraph(TestContext context) {
        System.out.println("*** test02GetGraph ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_READ);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("graph details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test02GetGraph");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test03CreateVertices(TestContext context) {
        System.out.println("*** test03CreateVertices ***");    
        // Create test vertex
        JsonObject documentObject = new JsonObject();
        documentObject.put(GharialAPI.DOC_ATTRIBUTE_KEY, "testvertex");
        documentObject.put("testfield", "testvalue");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_CREATE_VERTEX);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, endVertices);
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Graph operation resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertNotNull(arangoResult.getJsonObject("vertex"), "No vertex object received");
                    context.assertNotNull(arangoResult.getJsonObject("vertex").getString("_id"), "No id received");
                    
                    // Create vertex01
                    JsonObject documentObject = new JsonObject();
                    documentObject.put(GharialAPI.DOC_ATTRIBUTE_KEY, "vertex01");
                    JsonObject requestObject = new JsonObject(); final Async async = context.async();
                    requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
                    requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_CREATE_VERTEX);
                    requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
                    requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, vertexColName);
                    requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                    vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> reply) {
                            try {
                                JsonObject response = reply.result().body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getJsonObject("result");
                                context.assertTrue(!arangoResult.getBoolean("error"), "Graph operation resulted in an error: " + arangoResult.getString("errorMessage"));
                                context.assertNotNull(arangoResult.getJsonObject("vertex"), "No vertex object received");
                                context.assertNotNull(arangoResult.getJsonObject("vertex").getString("_id"), "No id received");
                                idVertex01 = arangoResult.getJsonObject("vertex").getString("_id");
                                
                                // Create vertex02
                                JsonObject documentObject = new JsonObject();
                                documentObject.put(GharialAPI.DOC_ATTRIBUTE_KEY, "vertex02");
                                JsonObject requestObject = new JsonObject(); final Async async = context.async();
                                requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
                                requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_CREATE_VERTEX);
                                requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
                                requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, endVertices);
                                requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                                vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                                    @Override
                                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                                        try {
                                            JsonObject response = reply.result().body();
                                            System.out.println("response: " + response);
                                            JsonObject arangoResult = response.getJsonObject("result");
                                            context.assertTrue(!arangoResult.getBoolean("error"), "Graph operation resulted in an error: " + arangoResult.getString("errorMessage"));
                                            context.assertNotNull(arangoResult.getJsonObject("vertex"), "No vertex object received");
                                            context.assertNotNull(arangoResult.getJsonObject("vertex").getString("_id"), "No id received");
                                            idVertex02 = arangoResult.getJsonObject("vertex").getString("_id");
                                        }
                                        catch (Exception e) {
                                            context.fail("test03CreateVertex");
                                        }
                                        async.complete();
                                    }
                                });

                            }
                            catch (Exception e) {
                                context.fail("test03CreateVertex");
                            }
                        }
                    });
                }
                catch (Exception e) {
                    context.fail("test03CreateVertices");
                }
                //async.complete();
            }
        });
    }

    @Test
    public void test04GetVertex(TestContext context) {
        System.out.println("*** test04GetVertex ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_READ_VERTEX);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, endVertices);
        requestObject.put(GharialAPI.MSG_PROPERTY_VERTEX_KEY, "testvertex");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("vertex details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test04GetVertex");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test05UpdateVertex(TestContext context) {
        System.out.println("*** test05UpdateVertex ***");
        JsonObject documentObject = new JsonObject();
        documentObject.put("testfield", "modified testvalue");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_UPDATE_VERTEX);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, endVertices);
        requestObject.put(GharialAPI.MSG_PROPERTY_VERTEX_KEY, "testvertex");
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("vertex details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test05UpdateVertex");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test06ReplaceVertex(TestContext context) {
        System.out.println("*** test06ReplaceVertex ***");
        JsonObject documentObject = new JsonObject();
        documentObject.put("replaced testfield", "replaced testvalue");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_REPLACE_VERTEX);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, endVertices);
        requestObject.put(GharialAPI.MSG_PROPERTY_VERTEX_KEY, "testvertex");
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("vertex details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test06ReplaceVertex");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test07DeleteVertex(TestContext context) {
        System.out.println("*** test07DeleteVertex ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_DELETE_VERTEX);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, endVertices);
        requestObject.put(GharialAPI.MSG_PROPERTY_VERTEX_KEY, "testvertex");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test07DeleteVertex");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test08CreateEdge(TestContext context) {
        System.out.println("*** test08CreateEdge ***");        
        JsonObject documentObject = new JsonObject();
        documentObject.put(GharialAPI.DOC_ATTRIBUTE_KEY, "testedgegraph");
        documentObject.put(GharialAPI.DOC_ATTRIBUTE_FROM, idVertex01);
        documentObject.put(GharialAPI.DOC_ATTRIBUTE_TO, idVertex02);
        documentObject.put("testfield", "testvalue");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_CREATE_EDGE);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, edgeColName);
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Graph operation resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertNotNull(arangoResult.getJsonObject("edge"), "No edge object received");
                    context.assertNotNull(arangoResult.getJsonObject("edge").getString("_id"), "No id received");
                }
                catch (Exception e) {
                    context.fail("test08CreateEdge");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test09GetEdge(TestContext context) {
        System.out.println("*** test09GetEdge ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_READ_EDGE);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, edgeColName);
        requestObject.put(GharialAPI.MSG_PROPERTY_EDGE_KEY, "testedgegraph");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test09GetEdge");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test10UpdateEdge(TestContext context) {
        System.out.println("*** test10UpdateEdge ***");
        JsonObject documentObject = new JsonObject();
        documentObject.put("testfield", "modified testvalue");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_UPDATE_EDGE);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, edgeColName);
        requestObject.put(GharialAPI.MSG_PROPERTY_EDGE_KEY, "testedgegraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test10UpdateEdge");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test11ReplaceEdge(TestContext context) {
        System.out.println("*** test11ReplaceEdge ***");
        JsonObject documentObject = new JsonObject();
        documentObject.put("replaced testfield", "replaced testvalue");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_REPLACE_EDGE);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, edgeColName);
        requestObject.put(GharialAPI.MSG_PROPERTY_EDGE_KEY, "testedgegraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test11ReplaceEdge");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test12GetEdgeDefinitions(TestContext context) {
        System.out.println("*** test12GetEdgeDefinitions ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_LIST_EDGE_COLLECTIONS);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, new JsonObject());
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("result details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test12GetEdgeDefinitions");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test13GetVertexCollections(TestContext context) {
        System.out.println("*** test13GetVertexCollections ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_LIST_VERTEX_COLLECTIONS);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_DOCUMENT, new JsonObject());
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("result details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test13GetVertexCollections");
                }
                async.complete();
            }
        });
    }

    /*
    @Test
    public void test14Traverse(TestContext context) {
        System.out.println("*** test14Traverse ***");
        JsonObject documentObject = new JsonObject().put(TraversalAPI.DOC_ATTRIBUTE_START_VERTEX, idVertex01);
        documentObject.put(TraversalAPI.DOC_ATTRIBUTE_EDGE_COLLECTION, edgeColName);
        documentObject.put(TraversalAPI.DOC_ATTRIBUTE_DIRECTION, "any");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_TRAVERSAL);
        requestObject.put(TraversalAPI.MSG_PROPERTY_ACTION, TraversalAPI.MSG_ACTION_TRAVERSE);
        requestObject.put(TraversalAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Traversal resulted in an error: " + arangoResult.getString("errorMessage"));
                    //if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No document key received");
                }
                catch (Exception e) {
                    context.fail("test14Traverse");
                }
                async.complete();
            }
        });
    }
    */

    @Test
    public void test15DeleteEdge(TestContext context) {
        System.out.println("*** test15DeleteEdge ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_DELETE_EDGE);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        requestObject.put(GharialAPI.MSG_PROPERTY_COLLECTION_NAME, edgeColName);
        requestObject.put(GharialAPI.MSG_PROPERTY_EDGE_KEY, "testedgegraph");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test15DeleteEdge");
                }
                async.complete();
            }
        });
    }
    
    @Test
    public void test16DeleteGraph(TestContext context) {
        System.out.println("*** test16DeleteGraph ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_GHARIAL);
        requestObject.put(GharialAPI.MSG_PROPERTY_ACTION, GharialAPI.MSG_ACTION_DELETE_GRAPH);
        requestObject.put(GharialAPI.MSG_PROPERTY_DROP_COLLECTIONS, false);
        requestObject.put(GharialAPI.MSG_PROPERTY_GRAPH_NAME, "testgraph");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The graph operation resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test16DeleteGraph");
                }
                async.complete();
            }
        });
    }
}
