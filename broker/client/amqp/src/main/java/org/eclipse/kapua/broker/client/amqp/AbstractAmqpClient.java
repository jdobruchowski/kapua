/*******************************************************************************
 * Copyright (c) 2018 Eurotech and/or its affiliates and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Eurotech - initial API and implementation
 *******************************************************************************/
package org.eclipse.kapua.broker.client.amqp;

import java.util.concurrent.CountDownLatch;

import org.eclipse.kapua.broker.client.amqp.ClientOptions.AmqpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonMessageHandler;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonSession;

public abstract class AbstractAmqpClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractAmqpClient.class);

    public static int maxWait = 3000;

    private String clientId;

    protected ProtonMessageHandler messageHandler;
    protected ProtonReceiver receiver;
    protected ProtonSender sender;

    protected ProtonSession session;
    protected AmqpConnection connection;

    protected AbstractAmqpClient(Vertx vertx, ClientOptions clientOptions) {
        connection = new AmqpConnection(vertx, clientOptions);
        clientId = clientOptions.getString(AmqpClientOptions.CLIENT_ID);
        logger.info("##################Created client {} for connection {}", clientId, connection);
    }

    protected AbstractAmqpClient(ProtonSession session, ClientOptions clientOptions, AfterConnect afterConnect) {
        this.session = session;
        clientId = clientOptions.getString(AmqpClientOptions.CLIENT_ID);
        logger.info("##################Created client {} for session {}", clientId, session);
    }

    public String getClientId() {
        return clientId;
    }

    public void messageHandler(ProtonMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    public void disconnect(Future<Void> stopFuture) {
        logger.info("Closing session {} for session {}", session, session);
        clean(null);
        if (session != null) {
            session.close();
            logger.info("Closing session {} for session {} DONE", session, session);
            session = null;
        }
        //in any case complete with a positive result the future
        stopFuture.complete();
    }

    protected void clean(CountDownLatch cleanCountDown) {
        if (sender!=null) {
            logger.info("Closing sender {} for session {}", sender, session);
            sender.closeHandler(ar -> {
                if(ar.succeeded()) {
                    logger.info("Sender close (session: {})", session, ar.cause());
                }
                else {
                    logger.warn("Sender close FAILED (session: {})", session, ar.cause());
                }
                if (cleanCountDown!=null) {
                    cleanCountDown.countDown();
                }
            });
            sender.close();
            sender = null;
            logger.info("Closing sender for session {} DONE", session);
        }
        if (receiver!=null) {
            logger.info("Closing receiver {} for session {}", receiver, session);
            receiver.closeHandler(ar -> {
                if(ar.succeeded()) {
                    logger.info("Receiver close (session: {})", session, ar.cause());
                }
                else {
                    logger.warn("Receiver close FAILED (session: {})", session, ar.cause());
                }
                if (cleanCountDown!=null) {
                    cleanCountDown.countDown();
                }
            });
            receiver.close();
            receiver = null;
            logger.info("Closing receiver for session {} DONE", session);
        }
    }

}
