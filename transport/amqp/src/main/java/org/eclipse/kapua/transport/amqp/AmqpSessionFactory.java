/*******************************************************************************
 * Copyright (c) 2019 Eurotech and/or its affiliates and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Eurotech - initial API and implementation
 *******************************************************************************/
package org.eclipse.kapua.transport.amqp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.kapua.KapuaErrorCodes;
import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.broker.client.amqp.AmqpConnection;
import org.eclipse.kapua.broker.client.amqp.AmqpReceiverSender;
import org.eclipse.kapua.broker.client.amqp.AmqpSender;
import org.eclipse.kapua.broker.client.amqp.ClientOptions;
import org.eclipse.kapua.broker.client.amqp.ClientOptions.AmqpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Vertx Proton session factory.
 * Creates the connection for the specified endpoint, if doesn't exist, and bound a new session on this connection.
 * To be used for the device managment operations.
 *
 */
public class AmqpSessionFactory {

    private static final Logger logger = LoggerFactory.getLogger(AmqpSender.class);

    private static Vertx vertx = Vertx.vertx();
    private static Map<String, AmqpConnection> connectionMap = new HashMap<>();

    public static AmqpReceiverSender getInstance(String nodeUri, ClientOptions clientOptions) throws InterruptedException, KapuaException {
        AmqpConnection connection = getConnection(nodeUri, clientOptions);
        return new AmqpReceiverSender(connection.createSession(), clientOptions);
    }

    public static void cleanClient(AmqpReceiverSender client) {
        client.clean();
    }

    private static AmqpConnection getConnection(String nodeUri, ClientOptions clientOptions) throws InterruptedException, KapuaException {
        AmqpConnection connection = connectionMap.get(nodeUri);
        if (connection != null) {
            return connection;
        }
        else {
            synchronized (connectionMap) {
                connection = connectionMap.get(nodeUri);
                if (connection == null) {
                    CountDownLatch countDown = new CountDownLatch(1);
                    connection = new AmqpConnection(vertx, clientOptions);
                    Future<Void> startFuture = Future.future();
                    startFuture.setHandler(han -> {
                        if (han.failed()) {
                            logger.error("Return connection to {}... ERROR", clientOptions.get(AmqpClientOptions.BROKER_HOST), han.cause());
                        }
                        else {
                            logger.info("Return connection to {}... DONE", clientOptions.get(AmqpClientOptions.BROKER_HOST));
                            countDown.countDown();
                        }
                    });
                    connection.connect(startFuture);
                    if (!countDown.await(30000, TimeUnit.MILLISECONDS)) {
                        logger.info("Return connection to {}... ERROR", clientOptions.get(AmqpClientOptions.BROKER_HOST));
                        throw new KapuaException(KapuaErrorCodes.INTERNAL_ERROR);
                    }
                    connectionMap.put(nodeUri, connection);
                }
            }
        }
        return connection;
    }

    //TODO to be conected to the application lifecycle
    public static void cleanUp() {
        //TODO
    }
}
