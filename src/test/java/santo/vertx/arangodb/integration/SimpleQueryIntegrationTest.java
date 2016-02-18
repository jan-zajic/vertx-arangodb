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
import santo.vertx.arangodb.rest.SimpleQueryAPI;
/**
 * Integration tests for the {@link santo.vertx.arangodb.rest.SimpleQueryAPI} against an external <a href="http://www.arangodb.com">ArangoDB</a> instance
 * 
 * @author sANTo
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleQueryIntegrationTest extends BaseIntegrationTest {
        
    @Test
    public void test01GetAll(TestContext context) {
        System.out.println("*** test01GetAll ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_ALL);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetAll() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetAll() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test01GetAll");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test02GetByExample(TestContext context) {
        System.out.println("*** test02GetByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("description", "from-doc");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetByExample() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test02GetByExample");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test03GetFirstExample(TestContext context) {
        System.out.println("*** test03GetFirstExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("description", "to-doc");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_FIRST_EXAMPLE);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetFirstExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetFirstExample() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test03GetFirstExample");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test04GetByExampleHash(TestContext context) {
        System.out.println("*** test04GetByExampleHash ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, indexHashId);
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("description", "from-doc");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE_HASH);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetByExampleHash() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetByExampleHash() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test04GetByExampleHash");
                }
                async.complete();
            }
        });
    }
    
    @Test
    public void test05GetByExampleSkiplist(TestContext context) {
        System.out.println("*** test05GetByExampleSkiplist ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, indexSkiplistId);
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("age", 30);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE_SKIPLIST);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetByExampleSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetByExampleSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test05GetByExampleSkiplist");
                }
                async.complete();
            }
        });
    }
    
    // TODO
    /*
    @Test
    public void test06GetByExampleBitarray(TestContext context) {
        System.out.println("*** test06GetByExampleBitarray ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, "bitarray-id");
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("description", "from-doc");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_EXAMPLE_BITARRAY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetByExampleBitarray() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetByExampleBitarray() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test06GetByExampleBitarray");
                }
                async.complete();
            }
        });
    }
    */
    
    @Test
    public void test07GetByConditionSkiplist(TestContext context) {
        System.out.println("*** test07GetByConditionSkiplist ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, indexSkiplistId);
        JsonObject conditionObject = new JsonObject();
        JsonArray conditionArray = new JsonArray();
        conditionArray.add(new JsonArray().add("<").add(40));
        conditionObject.put("age", conditionArray);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_CONDITION, conditionObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_CONDITION_SKIPLIST);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetByConditionSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetByConditionSkiplist() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test07GetByConditionSkiplist");
                }
                async.complete();
            }
        });
    }
    
    // TODO
    /*
    @Test
    public void test08GetByConditionBitarray(TestContext context) {
        System.out.println("*** test08GetByConditionBitarray ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_INDEX, "bitarray-id");
        JsonObject conditionObject = new JsonObject();
        conditionObject.put("description", "from-doc");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_CONDITION, conditionObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_BY_CONDITION_BITARRAY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetByConditionBitarray() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetByConditionBitarray() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test08GetByConditionBitarray");
                }
                async.complete();
            }
        });
    }
    */

    @Test
    public void test09GetAny(TestContext context) {
        System.out.println("*** test09GetAny ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_ANY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetAny() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetAny() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test09GetAny");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test10GetRange(TestContext context) {
        System.out.println("*** test10GetRange ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_ATTRIBUTE, "age");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_LEFT, 2);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_RIGHT, 100);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_RANGE);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetRange() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetRange() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test10GetRange");
                }
                async.complete();
            }
        });
    }
    
    @Test
    public void test11GetNear(TestContext context) {
        System.out.println("*** test11GetNear ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_LATITUDE, 1);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_LONGITUDE, 1);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_NEAR);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetNear() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetNear() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test11GetNear");
                }
                async.complete();
            }
        });
    }
    
    @Test
    public void test12GetWithin(TestContext context) {
        System.out.println("*** test12GetWithin ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_LATITUDE, 1);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_LONGITUDE, 1);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_RADIUS, 5000);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_WITHIN);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetWithin() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetWithin() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test12GetWithin");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test13GetFulltext(TestContext context) {
        System.out.println("*** test13GetFulltext ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_ATTRIBUTE, "description");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_QUERY, "doc");
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_FULLTEXT);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetFulltext() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetFulltext() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 201, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test13GetFulltext");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test14UpdateByExample(TestContext context) {
        System.out.println("*** test14UpdateByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("description", "test2");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject newObject = new JsonObject();
        newObject.put("description", "test2-updated");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_NEW_VALUE, newObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_UPDATE_BY_EXAMPLE);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("UpdateByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "UpdateByExample() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test14UpdateByExample");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test15ReplaceByExample(TestContext context) {
        System.out.println("*** test15ReplaceByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("description", "test2-updated");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject newObject = new JsonObject();
        newObject.put("description", "removeme");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_NEW_VALUE, newObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_REPLACE_BY_EXAMPLE);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("ReplaceByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "ReplaceByExample() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test15ReplaceByExample");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test16RemoveByExample(TestContext context) {
        System.out.println("*** test16RemoveByExample ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject exampleObject = new JsonObject();
        exampleObject.put("description", "removeme");
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_EXAMPLE, exampleObject);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_REMOVE_BY_EXAMPLE);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("RemoveByExample() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "RemoveByExample() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test16RemoveByExample");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test17GetFirst(TestContext context) {
        System.out.println("*** test17GetFirst ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_FIRST);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetFirst() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetFirst() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test17GetFirst");
                }
                async.complete();
            }
        });
    }

    @Test
    public void test18GetLast(TestContext context) {
        System.out.println("*** test18GetLast ***");
        JsonObject queryObject = new JsonObject();
        queryObject.put(SimpleQueryAPI.DOC_ATTRIBUTE_COLLECTION, vertexColName);
        JsonObject requestObject = new JsonObject(); final Async async = context.async();
        requestObject.put(ArangoPersistor.MSG_PROPERTY_TYPE, ArangoPersistor.MSG_TYPE_SIMPLE_QUERY);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_ACTION, SimpleQueryAPI.MSG_ACTION_GET_LAST);
        requestObject.put(SimpleQueryAPI.MSG_PROPERTY_QUERY, queryObject);
        vertx.eventBus().send(address, requestObject, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                try {
                    JsonObject response = reply.result().body();
                    System.out.println("response: " + response);
                    JsonObject arangoResult = response.getJsonObject("result");
                    context.assertEquals("GetLast() resulted in an error: " + arangoResult.getString("errorMessage"), "ok", response.getString("status"));
                    context.assertTrue(!arangoResult.getBoolean("error"), "GetLast() resulted in an error: " + arangoResult.getString("errorMessage"));
                    context.assertTrue(arangoResult.getInteger("code") == 200, "the query could not be executed successfully (returncode: " + arangoResult.getInteger("code") + ")");
                }
                catch (Exception e) {
                    context.fail("test18GetLast");
                }
                async.complete();
            }
        });
    }
}
