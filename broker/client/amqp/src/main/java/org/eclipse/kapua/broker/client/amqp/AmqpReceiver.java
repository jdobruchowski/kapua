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

import org.eclipse.kapua.broker.client.amqp.ClientOptions.AmqpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSession;

public class AmqpReceiver extends AbstractAmqpClient {

    private static final Logger logger = LoggerFactory.getLogger(AmqpSender.class);

    private final static Integer PREFETCH = new Integer(10);
    private final static boolean AUTO_ACCEPT = false;
    private final static ProtonQoS QOS = ProtonQoS.AT_MOST_ONCE;

    private ProtonSession session;
    private String destination;

    public AmqpReceiver(Vertx vertx, ClientOptions clientOptions) {
        super(vertx, clientOptions);
        connection.setAfterConnect(() -> {
            session = connection.createSession();
            session.openHandler(ar -> {
                if (ar.succeeded()) {
                    receiver = connection.createReceiver(session, destination, PREFETCH, AUTO_ACCEPT, QOS, null, messageHandler);
                    receiver.openHandler(snd -> {
                        logger.info("================Created receiver {}", receiver);
                    });
                    receiver.open();
                }
            });
            session.closeHandler(ar -> {
                logger.info("================Closed receiver {}", receiver);
            });
            session.open();
        });
        destination = clientOptions.getString(AmqpClientOptions.DESTINATION);
    }

    public void connect(Future<Void> connectFuture) {
        connection.connect(connectFuture);
    }

}
