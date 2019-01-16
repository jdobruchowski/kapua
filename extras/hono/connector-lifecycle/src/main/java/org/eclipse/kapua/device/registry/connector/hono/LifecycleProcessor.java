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
package org.eclipse.kapua.device.registry.connector.hono;

import java.util.List;

import org.eclipse.kapua.KapuaErrorCodes;
import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.connector.MessageContent;
import org.eclipse.kapua.connector.MessageContext;
import org.eclipse.kapua.connector.MessageTarget;
import org.eclipse.kapua.connector.Properties;
import org.eclipse.kapua.connector.hono.SystemMessageUtil;
import org.eclipse.kapua.message.transport.TransportMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class LifecycleProcessor implements MessageTarget<TransportMessage> {

    private static final Logger logger = LoggerFactory.getLogger(LifecycleProcessor.class);

    private static final String INVALID_TOPIC = "Cannot detect destination!";

    private LifecycleListener lifecycleListener;

    public enum LifecycleTypes {
        APPS,
        BIRTH,
        DC,
        MISSING
    }

    public static LifecycleProcessor create() {
        return new LifecycleProcessor();
    }

    protected LifecycleProcessor() {
        lifecycleListener = new LifecycleListener();
    }

    @Override
    public void start(Future<Void> startFuture) {
        // nothing to do
        startFuture.complete();
    }

    @Override
    public void stop(Future<Void> stopFuture) {
        // nothing to do
        stopFuture.complete();
    }

    @Override
    public void process(MessageContext<TransportMessage> message, Handler<AsyncResult<Void>> result) {
        MessageContent messageContent = (MessageContent)message.getProperties().get(Properties.MESSAGE_CONTENT);
        if (MessageContent.SYSTEM.equals(messageContent)) {
            result.handle(Future.failedFuture(new KapuaException(KapuaErrorCodes.INTERNAL_ERROR, "Cannot handle SYSTEM messages!")));
        }
        else {
            List<String> destination = message.getMessage().getChannel().getSemanticParts();
            if (destination!=null && destination.size()>1) {
                try {
                    String messageType = destination.get(1);
                    LifecycleTypes token = null;
                    try {
                        token = LifecycleTypes.valueOf(messageType);
                    }
                    catch (IllegalArgumentException | NullPointerException e) {
                        logger.debug("Invalid message type ({})", messageType);
                        result.handle(Future.failedFuture(INVALID_TOPIC));
                        return;
                    }
                    switch (token) {
                        case APPS:
                            SystemMessageUtil.checkDeviceConnectionId(message);
                            lifecycleListener.processAppsMessage(message);
                            break;
                        case BIRTH:
                            SystemMessageUtil.createOrUpdateDeviceConnection(message);
                            lifecycleListener.processBirthMessage(message);
                            break;
                        case DC:
                            SystemMessageUtil.disconnectDeviceConnection(message);
                            lifecycleListener.processDisconnectMessage(message);
                            break;
                        case MISSING:
                            SystemMessageUtil.checkDeviceConnectionId(message);
                            lifecycleListener.processMissingMessage(message);
                            break;
                        default:
                            result.handle(Future.succeededFuture());
                            break;
                    }
                }
                catch (KapuaException e) {
                    result.handle(Future.failedFuture(e));
                }
            }
            else {
                result.handle(Future.failedFuture(INVALID_TOPIC));
            }
        }
    }

}
