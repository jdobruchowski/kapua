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

import org.apache.qpid.proton.message.Message;
import org.eclipse.kapua.broker.client.amqp.ClientOptions.AmqpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonQoS;

public class AmqpSender extends AbstractAmqpClient {

    private static final Logger logger = LoggerFactory.getLogger(AmqpSender.class);

    private final static boolean AUTO_SETTLE = true;
    private final static ProtonQoS QOS = ProtonQoS.AT_LEAST_ONCE;

    private String destination;

    public AmqpSender(Vertx vertx, ClientOptions clientOptions) {
        super(vertx, clientOptions);
        destination = clientOptions.getString(AmqpClientOptions.DESTINATION);
        connection.setAfterConnect(() -> {
            session = connection.createSession();
            session.openHandler(ar -> {
                if (ar.succeeded()) {
                    sender = connection.createSender(session, destination, AUTO_SETTLE, QOS, null);
                    sender.openHandler(snd -> {
                        logger.info("================Created sender {}", sender);
                    });
                    sender.open();
                }
            });
            session.closeHandler(ar -> {
                logger.info("================Closed sender {}", sender);
            });
        });
    }

    public void connect(Future<Void> connectFuture) {
        connection.connect(connectFuture);
    }

    public void send(Message message) {
        connection.send(sender, message, destination, ar -> {
            logger.debug("Message sent to destination: {} - Message: {}", destination, message);
        });
    }

}
