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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.rest.DocumentAPI;
import santo.vertx.arangodb.rest.EdgeAPI;
/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.EdgeAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EdgeIntegrationTest extends BaseIntegrationTest {
    
    public static String edgeRevision = null;
    public static String edgeId = null;
    public static String fromId = null;
    public static String toId = null;

    @Test
    public void test01CreateEdge(TestContext context) {
        System.out.println("*** test01CreateEdge ***");
        
        // Create from-document in testcol
        JsonObject documentObject = new JsonObject().put("description", "from-doc");
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
                    
                    fromId = arangoResult.getString("_id");

                    // Create to-document in testcol
                    JsonObject documentObject = new JsonObject().put("description", "to-doc");
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

                                toId = arangoResult.getString("_id");
                                
                                // Now create an edge between from-doc and to-doc
                                JsonObject documentObject = new JsonObject().put("description", "testedge");
                                JsonObject requestObject = new JsonObject(); final Async async = context.async();
                                requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
                                requestObject.put(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_CREATE);
                                requestObject.put(EdgeAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                                requestObject.put(EdgeAPI.MSG_PROPERTY_COLLECTION, edgeColName);
                                requestObject.put(EdgeAPI.MSG_PROPERTY_FROM, fromId);
                                requestObject.put(EdgeAPI.MSG_PROPERTY_TO, toId);
                                vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                                    @Override
                                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                                        try {
                                            JsonObject response = reply.result().body();
                                            System.out.println("response: " + response);
                                            JsonObject arangoResult = response.getJsonObject("result");
                                            context.assertTrue(!arangoResult.getBoolean("error"), "Edge creation resulted in an error: " + arangoResult.getString("errorMessage"));
                                            if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No edge key received");

                                            edgeId = arangoResult.getString("_id");
                                            edgeRevision = arangoResult.getString("_rev");
                                        }
                                        catch (Exception e) {
                                            context.fail("test01CreateEdge");
                                        }
                                        async.complete();
                                    }
                                });

                            }
                            catch (Exception e) {
                                context.fail("test01CreateEdge");
                            }
                        }
                    });

                }
                catch (Exception e) {
                    context.fail("test01CreateEdge");
                }
            }
        });
    }

    @Test
    public void test02GetEdge(TestContext context) {
        System.out.println("*** test02GetEdge ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_READ);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The retrieval of the specified edge resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    //context.assertTrue(arangoResult.getInteger("code") == 200, "The request for the specified document was invalid: (returncode: " + arangoResult.getInteger("code") + ")");
                    System.out.println("edge details: " + arangoResult);                    
                }
                catch (Exception e) {
                    context.fail("test02GetEdge");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test03GetEdgeHeader(TestContext context) {
        System.out.println("*** test03GetEdgeHeader ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_HEAD);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The retrieval of the specified edge header resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("edge details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test03GetEdgeHeader");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test04UpdateEdge(TestContext context) {
        System.out.println("*** test04UpdateEdge ***");
        JsonObject documentObject = new JsonObject().put("description", "updated testedge");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_UPDATE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Edge update resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No edge key received");
                    context.assertNotEquals(edgeRevision,arangoResult.getString("_rev"),"Edge not correctly updated");
                    System.out.println("edge details: " + arangoResult);
                    
                    edgeRevision = arangoResult.getString("_rev");
                }
                catch (Exception e) {
                    context.fail("test04UpdateEdge");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test05ReplaceEdge(TestContext context) {
        System.out.println("*** test05ReplaceEdge ***");
        JsonObject documentObject = new JsonObject().put("description", "replaced testedge");
        documentObject.put("name", "replacement edge");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_REPLACE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Edge replacement resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("_id"), "No edge key received");
                    context.assertNotEquals(edgeRevision,arangoResult.getString("_rev"),"Edge not correctly replaced");
                    System.out.println("edge details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test05ReplaceEdge");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test06GetEdgeRelations(TestContext context) {
        System.out.println("*** test06GetEdgeRelations ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_RELATIONS);
        requestObject.put(EdgeAPI.MSG_PROPERTY_COLLECTION, edgeColName);
        requestObject.put(EdgeAPI.MSG_PROPERTY_VERTEX, fromId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The listing of all the edges for the specified collection resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("edges for collection: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test06GetEdgeRelations");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test07DeleteEdge(TestContext context) {
        System.out.println("*** test07DeleteEdge ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_EDGE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ACTION, EdgeAPI.MSG_ACTION_DELETE);
        requestObject.put(EdgeAPI.MSG_PROPERTY_ID, edgeId);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The deletion of the specified edge resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test07DeleteEdge");
                }
                async.complete();
            }
        });
    }

}
