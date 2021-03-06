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

package santo.vertx.arangodb;

import java.util.Base64;
import java.util.HashMap;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.JksOptions;
import santo.vertx.arangodb.rest.AbstractRestAPI;
import santo.vertx.arangodb.rest.AqlAPI;
import santo.vertx.arangodb.rest.CollectionAPI;
import santo.vertx.arangodb.rest.DocumentAPI;
import santo.vertx.arangodb.rest.EdgeAPI;
import santo.vertx.arangodb.rest.GenericAPI;
import santo.vertx.arangodb.rest.GharialAPI;
import santo.vertx.arangodb.rest.GraphAPI;
import santo.vertx.arangodb.rest.SimpleQueryAPI;
import santo.vertx.arangodb.rest.TransactionAPI;
import santo.vertx.arangodb.rest.TraversalAPI;

/**
 *
 * @author sANTo
 */
public class ArangoPersistor extends AbstractVerticle implements Handler<Message<JsonObject>> {

    // DEFAULT SETTINGS
    public final String SYSTEM_DATABASE = "_system";
    public final String DEFAULT_DATABASE = SYSTEM_DATABASE;
    
    // CONFIGURATION PROPERTIES
    private final String CFG_PROPERTY_ADDRESS = "address";
    
    public final String CFG_PROPERTY_HOSTNAME = "host";
    public final String CFG_PROPERTY_PORT = "port";
    public final String CFG_PROPERTY_DBNAME = "dbname";
    public final String CFG_PROPERTY_USERNAME = "username";
    public final String CFG_PROPERTY_PASSWORD = "password";
    public final String CFG_PROPERTY_SSL = "ssl";
    public final String CFG_PROPERTY_SSL_TRUSTALL = "ssl_trustall";
    public final String CFG_PROPERTY_SSL_VERIFYHOST = "ssl_verifyhost";
    public final String CFG_PROPERTY_SSL_TRUSTSTORE = "truststore";
    public final String CFG_PROPERTY_SSL_TRUSTSTORE_PASSWORD = "truststore_password";
    public final String CFG_PROPERTY_SSL_KEYSTORE = "keystore";
    public final String CFG_PROPERTY_SSL_KEYSTORE_PASSWORD = "keystore_password";
    public final String CFG_PROPERTY_GENERIC = "generic";
    public final String CFG_PROPERTY_MAXPOOLSIZE = "maxpoolsize";
    public final String CFG_PROPERTY_KEEPALIVE = "keepalive";
    public final String CFG_PROPERTY_COMPRESSION = "compression";
    public final String CFG_PROPERTY_CONNECT_TIMEOUT = "connect_timeout";
    public final String CFG_PROPERTY_REUSE_ADDRESS = "reuse_address";
    public final String CFG_PROPERTY_TCP_KEEPALIVE = "tcp_keepalive";
    public final String CFG_PROPERTY_TCP_NODELAY = "tcp_nodelay";

    // MESSAGE PROPERTIES
    public static final String MSG_PROPERTY_TYPE = "type";

    // MODULE SETTINGS
    private String SETTING_ADDRESS = "santo.vertx.arangodb";
    
    public int DEFAULT_SETTING_PORT_HTTP = 8529;
    public int DEFAULT_SETTING_PORT_HTTPS = 8529;
    public String SETTING_HOSTNAME = "localhost";
    public int SETTING_PORT = 8529;
    public String SETTING_DBNAME = DEFAULT_DATABASE;
    public String SETTING_USERNAME = null;
    public String SETTING_PASSWORD = null;
    public boolean SETTING_SSL = false;
    public boolean SETTING_SSL_TRUSTALL = false;
    public boolean SETTING_SSL_VERIFYHOST = true;
    public String SETTING_SSL_TRUSTSTORE = null;
    public String SETTING_SSL_TRUSTSTORE_PASSWORD = null;
    public String SETTING_SSL_KEYSTORE = null;
    public String SETTING_SSL_KEYSTORE_PASSWORD = null;
    public boolean SETTING_GENERIC = true;
    public int SETTING_MAXPOOLSIZE = 10;
    public boolean SETTING_KEEPALIVE = false;
    public boolean SETTING_COMPRESSION = false;
    public int SETTING_CONNECT_TIMEOUT = 15000;
    public boolean SETTING_REUSE_ADDRESS = false;
    public boolean SETTING_TCP_KEEPALIVE = false;
    public boolean SETTING_TCP_NODELAY = true;

    // Request Types
    public static final String MSG_TYPE_GENERIC = "generic";
    public static final String MSG_TYPE_DOCUMENT = "document";
    public static final String MSG_TYPE_EDGE = "edge";
    public static final String MSG_TYPE_AQL = "aql";
    public static final String MSG_TYPE_SIMPLE_QUERY = "query";
    public static final String MSG_TYPE_COLLECTION = "collection";
    public static final String MSG_TYPE_INDEX = "index";
    public static final String MSG_TYPE_TRANSACTION = "transaction";
    public static final String MSG_TYPE_GRAPH = "graph";
    public static final String MSG_TYPE_GHARIAL = "gharial";
    public static final String MSG_TYPE_TRAVERSAL = "traversal";

    private Logger logger = LoggerFactory.getLogger(ArangoPersistor.class.getName());
    
    private final String logPrefix = "";

    private JsonObject config = null;

    private volatile HashMap<String, HttpClient> clients = new HashMap<>();
    private volatile HashMap<String, String> credentials = new HashMap<>();

    @Override
    public void init(Vertx vertx, Context context) {
    	super.init(vertx, context);
    	logger.info(logPrefix + "Initializing " + this.getClass().getCanonicalName());
    	setConfig(context.config());
    }
    
    @Override
    public void start() {
        configure();
        listen();
    }
        
    private void configure() {
        logger.trace(logPrefix + "Initializing " + this.getClass().getCanonicalName());

        logger.trace(logPrefix + "parsing configuration");
        if (getConfig() == null) {
            logger.fatal(logPrefix + "configuration missing, aborting");
            throw new RuntimeException("Configuration is missing");
        }

        SETTING_ADDRESS = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_ADDRESS, SETTING_ADDRESS);
        
        SETTING_HOSTNAME = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_HOSTNAME, SETTING_HOSTNAME);
        SETTING_DBNAME = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_DBNAME, SETTING_DBNAME);
        SETTING_USERNAME = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_USERNAME, SETTING_USERNAME);
        SETTING_PASSWORD = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_PASSWORD, SETTING_PASSWORD);
        SETTING_SSL = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_SSL, SETTING_SSL);
        if (SETTING_SSL) SETTING_PORT = Helper.getHelper().getOptionalInt(getConfig(), CFG_PROPERTY_PORT, DEFAULT_SETTING_PORT_HTTPS);
        else SETTING_PORT = Helper.getHelper().getOptionalInt(getConfig(), CFG_PROPERTY_PORT, DEFAULT_SETTING_PORT_HTTP);
        SETTING_SSL_TRUSTALL = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_SSL_TRUSTALL, SETTING_SSL_TRUSTALL);
        SETTING_SSL_VERIFYHOST = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_SSL_VERIFYHOST, SETTING_SSL_VERIFYHOST);
        SETTING_SSL_KEYSTORE = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_SSL_KEYSTORE, SETTING_SSL_KEYSTORE);
        SETTING_SSL_KEYSTORE_PASSWORD = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_SSL_KEYSTORE_PASSWORD, SETTING_SSL_KEYSTORE_PASSWORD);
        SETTING_SSL_TRUSTSTORE = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_SSL_TRUSTSTORE, SETTING_SSL_TRUSTSTORE);
        SETTING_SSL_TRUSTSTORE_PASSWORD = Helper.getHelper().getOptionalString(getConfig(), CFG_PROPERTY_SSL_TRUSTSTORE_PASSWORD, SETTING_SSL_TRUSTSTORE_PASSWORD);
        SETTING_GENERIC = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_GENERIC, SETTING_GENERIC);
        SETTING_MAXPOOLSIZE = Helper.getHelper().getOptionalInt(getConfig(), CFG_PROPERTY_MAXPOOLSIZE, SETTING_MAXPOOLSIZE);
        SETTING_KEEPALIVE = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_KEEPALIVE, SETTING_KEEPALIVE);
        SETTING_COMPRESSION = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_COMPRESSION, SETTING_COMPRESSION);
        SETTING_CONNECT_TIMEOUT = Helper.getHelper().getOptionalInt(getConfig(), CFG_PROPERTY_CONNECT_TIMEOUT, SETTING_CONNECT_TIMEOUT);
        SETTING_REUSE_ADDRESS = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_REUSE_ADDRESS, SETTING_REUSE_ADDRESS);
        SETTING_TCP_KEEPALIVE = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_TCP_KEEPALIVE, SETTING_TCP_KEEPALIVE);
        SETTING_TCP_NODELAY = Helper.getHelper().getOptionalBoolean(getConfig(), CFG_PROPERTY_TCP_NODELAY, SETTING_TCP_NODELAY);

        logger.trace(logPrefix + "configuration parsed successfully");
        //System.out.println("Address: " + SETTING_ADDRESS);        
    }
    
    public synchronized HttpClient getClient() {
        return getClient(SETTING_HOSTNAME, SETTING_PORT);
    }
    
    public synchronized HttpClient getClient(String hostname, int port) {
        return getClient(hostname, port, SETTING_SSL);
    }
    
    public synchronized HttpClient getClient(String hostname, int port, boolean ssl) {
        if (hostname == null) hostname = SETTING_HOSTNAME;
        if (port == 0) port = SETTING_PORT;
        
        HttpClient client = clients.get(hostname + ":" + port);
        if (client == null) {
        	HttpClientOptions options = new HttpClientOptions()
        			.setSsl(SETTING_SSL)
        			.setTrustAll(SETTING_SSL_TRUSTALL);
        	
            if (options.isSsl()) options.setVerifyHost(SETTING_SSL_VERIFYHOST);
            options.setDefaultHost(hostname);
            options.setDefaultPort(port);
            options.setKeepAlive(SETTING_KEEPALIVE);
            if (SETTING_SSL_TRUSTSTORE != null)
            	options.setTrustStoreOptions(new JksOptions()
            			.setPath(SETTING_SSL_TRUSTSTORE)
            			.setPassword(SETTING_SSL_TRUSTSTORE_PASSWORD)
			);
             
            if (SETTING_SSL_KEYSTORE != null) {
            	options.setKeyStoreOptions(new JksOptions()
            			.setPath(SETTING_SSL_KEYSTORE)
            			.setPassword(SETTING_SSL_KEYSTORE_PASSWORD));
            }
            options.setMaxPoolSize(SETTING_MAXPOOLSIZE);
            options.setConnectTimeout(SETTING_CONNECT_TIMEOUT);
            options.setReuseAddress(SETTING_REUSE_ADDRESS);
            options.setKeepAlive(SETTING_TCP_KEEPALIVE);
            options.setTcpNoDelay(SETTING_TCP_NODELAY);
            options.setTryUseCompression(SETTING_COMPRESSION);
            client = vertx.createHttpClient(options);
            // Keep reference
            clients.put(hostname + ":" + port, client);
        }
        
        return client;
    }

    public synchronized String getCredentials() {
        return getCredentials(SETTING_HOSTNAME, SETTING_PORT, SETTING_USERNAME, SETTING_PASSWORD);
    }
    
    public synchronized String getCredentials(String hostname, int port) {
        return getCredentials(hostname, port, SETTING_USERNAME, SETTING_PASSWORD);
    }
    
    public synchronized String getCredentials(String hostname, int port, String username, String password) {
        String credentialsHeader = credentials.get(hostname + ":" + port);
        if (credentialsHeader == null) {
            if (username != null && password != null) {
                credentialsHeader = Base64.getEncoder().encodeToString(new StringBuilder(SETTING_USERNAME + ":").append(SETTING_PASSWORD).toString().getBytes());
                
                // Keep reference
                credentials.put(hostname + ":" + port, credentialsHeader);
            }
            else {
                // Trigger warning because authentication is not enabled!
                logger.warn(logPrefix + "Authentication disabled because " + CFG_PROPERTY_USERNAME + " and/or " + CFG_PROPERTY_PASSWORD + " is not specified!");
            }
        }
        
        return credentialsHeader;
    }
    
    private void listen() {
        vertx.eventBus().consumer(SETTING_ADDRESS, this);
    }
    
    @Override
    public void handle(Message<JsonObject> msg) {
        logger.trace(logPrefix + "new request received");
        
        // MANDATORY: type of REST request to perform
        String type = Helper.getHelper().getMandatoryString(msg.body(), MSG_PROPERTY_TYPE, msg);
        if (type == null) return;        
        logger.trace(logPrefix + "type of request: " + type);
        
        AbstractRestAPI api = null;

        switch (type) {
            case MSG_TYPE_GENERIC:
                if (!SETTING_GENERIC) {
                    logger.warn(logPrefix + "The generic API is currently disabled");
                    Helper.getHelper().sendError(msg, "The generic API is currently disabled");
                    return;
                }
                api = new GenericAPI(logger, this);
                break;
            case MSG_TYPE_DOCUMENT:
                api = new DocumentAPI(logger, this);
                break;                
            case MSG_TYPE_EDGE:
                api = new EdgeAPI(logger, this);
                break;
            case MSG_TYPE_AQL:
                api = new AqlAPI(logger, this);
                break;
            case MSG_TYPE_SIMPLE_QUERY:
                api = new SimpleQueryAPI(logger, this);
                break;                
            case MSG_TYPE_COLLECTION:
                api = new CollectionAPI(logger, this);
                break;
            case MSG_TYPE_TRANSACTION:
                api = new TransactionAPI(logger, this);
                break;                
            case MSG_TYPE_GRAPH:
                api = new GraphAPI(logger, this);
                break;
            case MSG_TYPE_GHARIAL:
                api = new GharialAPI(logger, this);
                break;
            case MSG_TYPE_TRAVERSAL:
                api = new TraversalAPI(logger, this);
                break;
                
            default:
                logger.warn(logPrefix + "invalid request type, ignoring (" + type + ")");
                Helper.getHelper().sendError(msg, "invalid type specified (" + type + ")");
                return;
        }
        
        // If the request was valid, then handle it
        if (api != null) {
            api.processRequest(msg);
        }

    }
    
    public JsonObject getConfig() {
        if (config == null) config = new JsonObject();
        return config;
    }

    public void setConfig(JsonObject config) {
        this.config = config;
    }

}
