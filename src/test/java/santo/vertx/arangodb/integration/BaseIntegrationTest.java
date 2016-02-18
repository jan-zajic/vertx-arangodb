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

import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.DeploymentOptionsConverter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import santo.vertx.arangodb.ArangoPersistor;
import santo.vertx.arangodb.Helper;

/**
 *
 * @author sANTo
 */
@RunWith(VertxUnitRunner.class)
public abstract class BaseIntegrationTest {

	protected static final Logger logger = Logger.getLogger(BaseIntegrationTest.class.getName());
	
	public static String indexHashId = null;
	public static String indexSkiplistId = null;
	public static String indexFulltextId = null;
	public static String indexCapId = null;
	public static String indexGeoId = null;

	public static final String DEFAULT_ADDRESS = "santo.vertx.arangodb";
	public static final String DEFAULT_TEST_DB = "testdb";
	
	public final String logPrefix = "";
	public JsonObject config;
	public String address;
	public String dbName;
	public String dbUser;
	public String dbPwd;

	public String vertexColName = "vertexcol";
	public String edgeColName = "edgecol";

	protected Vertx vertx;
	
	@Before
	public void before(TestContext context) {
		 // Use the underlying vertx instance
		vertx = Vertx.vertx();
		config = loadConfig();
		address = Helper.getHelper().getOptionalString(config, "address", DEFAULT_ADDRESS);
		dbName = Helper.getHelper().getOptionalString(config, "dbname", DEFAULT_TEST_DB);
		dbUser = Helper.getHelper().getOptionalString(config, "username", null);
		dbPwd = Helper.getHelper().getOptionalString(config, "password", null);
		DeploymentOptions options = new DeploymentOptions();
		options.setConfig(config);
		// Deploy our persistor before starting the tests
		deployVerticle(ArangoPersistor.class.getName(), options, context);
	}

	private void deployVerticle(final String vertName, DeploymentOptions vertConfig, TestContext context) {
		logger.log(Level.INFO, logPrefix + "(deployVerticle) vertName: " + vertName);
		if (vertName == null || vertConfig == null) {
			logger.log(Level.SEVERE,
					logPrefix + "Unable to deploy the requested verticle because one of the parameters is invalid: "
							+ "Name=" + vertName + ",Config=" + vertConfig);
			return;
		}
		vertx.deployVerticle(vertName, vertConfig, context.asyncAssertSuccess());
	}

	private JsonObject loadConfig() {
		logger.info(logPrefix + "(re)loading Config");
		URL url = getClass().getResource("/config.json");
		url.getFile();
		Buffer configBuffer = vertx.fileSystem().readFileBlocking(url.getFile());
		if (configBuffer != null) {
			return new JsonObject(configBuffer.toString());
		}

		return new JsonObject();
	}
	
	@After
	public void tearDown(TestContext context) {
	  vertx.close(context.asyncAssertSuccess());
	}
	
}
