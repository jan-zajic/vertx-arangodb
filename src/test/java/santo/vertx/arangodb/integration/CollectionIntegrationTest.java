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
import santo.vertx.arangodb.rest.CollectionAPI;
import santo.vertx.arangodb.rest.DocumentAPI;

/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.CollectionAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CollectionIntegrationTest extends BaseIntegrationTest {

    @Test
    public void test01CreateTestCollections(TestContext context) {
        System.out.println("*** test01CreateTestCollections ***");
        // Create a test collection that we can use throughout the whole test cycle
        JsonObject documentObject = new JsonObject().put("name", vertexColName);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CREATE);
        requestObject.put(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertTrue(!arangoResult.getBoolean("error"), "Collection creation resulted in an error: " + arangoResult.getString("errorMessage"));
                    if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("id"), "No collection id received");

                    // Create an extra collection for edges
                    JsonObject documentObject = new JsonObject().put("name", edgeColName);
                    documentObject.put("type", 3);
                    JsonObject requestObject = new JsonObject(); final Async async = context.async();
                    requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
                    requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CREATE);
                    requestObject.put(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                    vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> reply) {
                            try {
                                JsonObject response = reply.result().body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getJsonObject("result");
                                context.assertTrue(!arangoResult.getBoolean("error"), "Collection creation resulted in an error: " + arangoResult.getString("errorMessage"));
                                if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("id"), "No collection id received");
                                
                                // Create an extra temp collection that we can remove in the next test
                                JsonObject documentObject = new JsonObject().put("name", "tempcol");
                                JsonObject requestObject = new JsonObject(); final Async async = context.async();
                                requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
                                requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CREATE);
                                requestObject.put(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
                                vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                                    @Override
                                    public void handle(AsyncResult<Message<JsonObject>> reply) {
                                        try {
                                            JsonObject response = reply.result().body();
                                            System.out.println("response: " + response);
                                            JsonObject arangoResult = response.getJsonObject("result");
                                            context.assertTrue(!arangoResult.getBoolean("error"), "Collection creation resulted in an error: " + arangoResult.getString("errorMessage"));
                                            if (!arangoResult.getBoolean("error")) context.assertNotNull(arangoResult.getString("id"), "No collection id received");
                                        }
                                        catch (Exception e) {
                                            context.fail("test01CreateTestCollections");
                                        }
                                        async.complete();
                                    }
                                });
                            }
                            catch (Exception e) {
                                context.fail("test01CreateTestCollections");
                            }
                        }
                    });
                }
                catch (Exception e) {
                    context.fail("test01CreateTestCollections");
                }
            }
        });
    }

    @Test
    public void test02Rename(TestContext context) {
        System.out.println("*** test02Rename ***");
        JsonObject documentObject = new JsonObject().put("name", "tempcol-renamed");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_RENAME);
        requestObject.put(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, "tempcol");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Loading of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test02Rename");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test03DeleteCollection(TestContext context) {
        System.out.println("*** test03DeleteCollection ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_DELETE);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, "tempcol-renamed");
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("The removal of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test03DeleteCollection");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test04Rotate(TestContext context) {
        System.out.println("*** test04Rotate ***");
        
        // A collection can only be truncated if it already has a journal, meaning we should have inserted at least 1 document already.
        JsonObject documentObject = new JsonObject().put("description", "test");
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

                    JsonObject requestObject = new JsonObject(); final Async async = context.async();
                    requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
                    requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_ROTATE);
                    requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
                    vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> reply) {
                            try {
                                JsonObject response = reply.result().body();
                                System.out.println("response: " + response);
                                JsonObject arangoResult = response.getJsonObject("result");
                                // Treat 400 as ok, because it just means there is currently no journal (which is not an error with the API call)
                                context.assertTrue(response.getInteger("statuscode") == 200 || response.getInteger("statuscode") == 400, "Rotation of the specified collection resulted in an error: " + response.getString("message"));
                                System.out.println("response details: " + arangoResult);
                            }
                            catch (Exception e) {
                                context.fail("test04Rotate");
                            }
                            async.complete();
                        }
                    });
                }
                catch (Exception e) {
                    context.fail("test04Rotate");
                }
                //async.complete();
            }
        });
    }

    @Test
    public void test05TruncateCollection(TestContext context) {
        System.out.println("*** test05TruncateCollection ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_TRUNCATE);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Truncation of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test05TruncateCollection");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test06Unload(TestContext context) {
        System.out.println("*** test06Unload ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_UNLOAD);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Unloading of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test06Unload");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test07Load(TestContext context) {
        System.out.println("*** test07Load ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_LOAD);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Loading of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test07Load");
                }
                async.complete();
            }
        });
    }
    
    @Test
    public void test08ChangeProperties(TestContext context) {
        System.out.println("*** test08ChangeProperties ***");
        JsonObject documentObject = new JsonObject().put("waitForSync", false);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CHANGE_PROPERTIES);
        requestObject.put(CollectionAPI.MSG_PROPERTY_DOCUMENT, documentObject);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Changing properties of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test08ChangeProperties");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test09ReadCollection(TestContext context) {
        System.out.println("*** test09ReadCollection ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_READ);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Reading the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test09ReadCollection");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test10ListCollections(TestContext context) {
        System.out.println("*** test10ListCollections ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_LIST);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Listing all collections for the specified database resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test10ListCollections");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test11GetCollectionProperties(TestContext context) {
        System.out.println("*** test11GetCollectionProperties ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_GET_PROPERTIES);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Getting properties of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test11GetCollectionProperties");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test12GetCollectionCount(TestContext context) {
        System.out.println("*** test12GetCollectionCount ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_COUNT);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Getting count info of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test12GetCollectionCount");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test13GetCollectionFigures(TestContext context) {
        System.out.println("*** test13GetCollectionFigures ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_FIGURES);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Getting figures of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test13GetCollectionFigures");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test14GetCollectionRevision(TestContext context) {
        System.out.println("*** test14GetCollectionRevision ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_REVISION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Getting revision info of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test14GetCollectionRevision");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test15GetCollectionChecksum(TestContext context) {
        System.out.println("*** test15GetCollectionChecksum ***");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_COLLECTION);
        requestObject.put(CollectionAPI.MSG_PROPERTY_ACTION, CollectionAPI.MSG_ACTION_CHECKSUM);
        requestObject.put(CollectionAPI.MSG_PROPERTY_COLLECTION, vertexColName);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("Getting checksum info of the specified collection resulted in an error: " + response.getString("message"), "ok", response.getString("status"));
                    System.out.println("response details: " + arangoResult);
                }
                catch (Exception e) {
                    context.fail("test15GetCollectionChecksum");
                }
                async.complete();
            }
        });
    }
}
