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
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton.message.Message;
import org.eclipse.kapua.broker.client.amqp.ClientOptions.AmqpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Handler;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonLinkOptions;
import io.vertx.proton.ProtonMessageHandler;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonSession;

public class AmqpReceiverSender extends AbstractAmqpClient {

    private static final Logger logger = LoggerFactory.getLogger(AmqpReceiverSender.class);

    private final static Integer PREFETCH = new Integer(10);
    private final static boolean AUTO_ACCEPT = true;
    private final static ProtonQoS QOS = ProtonQoS.AT_LEAST_ONCE;

    private DestinationTranslator destinationTranslator;

    public AmqpReceiverSender(ProtonSession session, ClientOptions clientOptions) {
        super(session, clientOptions, () -> {});
        destinationTranslator = (DestinationTranslator)clientOptions.get(AmqpClientOptions.DESTINATION_TRANSLATOR);
    }

    public void send(Message message, String destination) {
        String senderDestination = destination;
        if (destinationTranslator!=null) {
            senderDestination = destinationTranslator.translate(destination);
        }
        long startTime = System.currentTimeMillis();
        CountDownLatch senderCountDown = new CountDownLatch(1);
        boolean operationSucceed = true;
        sender = createSender(session, senderDestination, true, QOS, senderCountDown);
        try {
            operationSucceed = senderCountDown.await(AbstractAmqpClient.maxWait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.debug("Wait interrupted");
        }
        if (operationSucceed && sender.isOpen()) {
            logger.info("================Acquired open sender after: {}ms DONE", (System.currentTimeMillis() - startTime));
        }
        else {
            logger.info("================Acquired open sender after: {}ms FAILED", (System.currentTimeMillis() - startTime));
//            notifySenderError();
        }
        logger.info("Sender ready after: {}ms", System.currentTimeMillis() - startTime);
        connection.send(sender, message, senderDestination, delivery -> {
            logger.info("MESSAGE SENT!!!! {} - {}", message);
        });
    }

    public void subscribe(String destination, ProtonMessageHandler messageHandler) {
        String receiverDestination = destination;
        if (destinationTranslator!=null) {
            receiverDestination = destinationTranslator.translate(destination);
        }
        messageHandler(messageHandler);
        long startTime = System.currentTimeMillis();
        CountDownLatch receiverCountDown = new CountDownLatch(1);
        boolean operationSucceed = true;
        receiver = createReceiver(session, receiverDestination, PREFETCH, AUTO_ACCEPT, QOS, receiverCountDown, messageHandler);
        try {
            operationSucceed = receiverCountDown.await(AbstractAmqpClient.maxWait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.debug("Wait interrupted");
        }
        if (operationSucceed && receiver.isOpen()) {
            logger.info("================Acquired open receiver after: {}ms DONE", (System.currentTimeMillis() - startTime));
        }
        else {
            logger.info("================Acquired open receiver after: {}ms FAILED", (System.currentTimeMillis() - startTime));
//            notifyReceiverError();
        }
        logger.info("Receiver ready after {}ms", System.currentTimeMillis() - startTime);
    }

    protected ProtonReceiver createReceiver(ProtonSession session, String destination, int prefetch, boolean autoAccept, ProtonQoS qos, CountDownLatch receiverCountDown, ProtonMessageHandler messageHandler) {
        ProtonReceiver receiver = null;
        try {
            logger.info("Register consumer for destination {}... (session: {})", destination, session);
            // The client ID is set implicitly into the destination subscribed
            receiver = session.createReceiver(destination);

            //default values not changeable by config
            receiver.setAutoAccept(autoAccept);
            receiver.setQoS(qos);
            receiver.setPrefetch(prefetch);
            logger.info("Setting auto accept: {} - QoS: {} - prefetch: {}", autoAccept, qos, prefetch);

            receiver.handler(messageHandler);
            receiver.openHandler(ar -> {
                if(ar.succeeded()) {
                    logger.info("Succeeded establishing consumer link! (session: {})", session);
                    if (receiverCountDown != null) {
                        receiverCountDown.countDown();
                    }
                }
                else {
                    logger.warn("Cannot establish link! (session: {})", session, ar.cause());
                    notifyReceiverError();
                }
            });
            receiver.closeHandler(recv -> {
                logger.warn("Receiver is closed! (session: {})", session, recv.cause());
                notifyReceiverError();
            });
            receiver.open();
            logger.info("Register consumer for destination {}... DONE (session: {})", destination, session);
        }
        catch(Exception e) {
            notifyReceiverError();
        }
        return receiver;
    }

    public void notifyReceiverError() {
        logger.info("!!!!!!!!!!!!!Receiver ERROR");
    }

    public void notifySenderError() {
        logger.info("!!!!!!!!!!!!!Sender ERROR");
    }

    protected ProtonSender createSender(ProtonSession session, String destination, boolean autoSettle, ProtonQoS qos, CountDownLatch senderCountDown) {
        ProtonSender sender = null;
        try {
            logger.info("Register sender for destination {}... (session: {})", destination, session);
            ProtonLinkOptions senderOptions = new ProtonLinkOptions();
            // The client ID is set implicitly into the destination subscribed
            sender = session.createSender(destination, senderOptions);

            //default values not changeable by config
            sender.setQoS(qos);
            sender.setAutoSettle(autoSettle);
            logger.info("Setting auto accept: {} - QoS: {}", autoSettle, qos);

            sender.openHandler(ar -> {
               if (ar.succeeded()) {
                   logger.info("Register sender for destination {}... DONE (session: {})", destination, session);
                   if (senderCountDown != null) {
                       senderCountDown.countDown();
                   }
               }
               else {
                   logger.info("Register sender for destination {}... FAILED... (session: {})", destination, session, ar.cause());
                   notifySenderError();
               }
            });
            sender.closeHandler(snd -> {
                logger.warn("Sender is closed! (session: {})", session, snd.cause());
                notifySenderError();
            });
            sender.open();
            logger.info("Register sender for destination {}... DONE (session: {})", destination, session);
        }
        catch(Exception e) {
            notifySenderError();
        }
        return sender;
    }

    protected void send(ProtonSender sender, Message message, String destination, Handler<ProtonDelivery> deliveryHandler) {
        message.setAddress(destination);
        sender.send(message, deliveryHandler);
        //TODO check if its better to create a new message like
//        import org.apache.qpid.proton.Proton;
//        Message msg = Proton.message();
//        msg.setBody(message.getBody());
//        msg.setAddress(destination);
//        protonSender.send(msg, deliveryHandler);
    }

    public void clean() {
        long startTime = System.currentTimeMillis();
        CountDownLatch cleanCountDown;
        if (sender != null && receiver != null) {
            cleanCountDown = new CountDownLatch(2);
        }
        else if (sender == null && receiver == null) {
            cleanCountDown = new CountDownLatch(0);
        }
        else {
            cleanCountDown = new CountDownLatch(1);
        }
        boolean operationSucceed = true;
        super.clean(cleanCountDown);
        try {
            operationSucceed = cleanCountDown.await(AbstractAmqpClient.maxWait, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.debug("Wait interrupted");
        }
        if (sender!=null && sender.isOpen()) {
            logger.warn("================Cleaned sender {} - {} after: {}ms FAILED (sender not null or still connected)", sender, this, (System.currentTimeMillis() - startTime));
            notifySenderError();
        }
        else if (receiver!=null && receiver.isOpen()) {
            logger.warn("================Cleaned receiver {} - {} after: {}ms FAILED (receiver not null or still connected)", receiver, this, (System.currentTimeMillis() - startTime));
            notifyReceiverError();
        }
        else if (operationSucceed) {
            logger.info("!!!!!!!!!!!!!!!!!Cleaned client {} after: {}ms {} DONE", this, (System.currentTimeMillis() - startTime), Thread.currentThread().getId(), Thread.currentThread().getName());
        }
        else {
            logger.info("!!!!!!!!!!!!!!!!!Cleaned client {} after: {}ms {} FAILED", this, (System.currentTimeMillis() - startTime), Thread.currentThread().getId(), Thread.currentThread().getName());
        }
    }

}
