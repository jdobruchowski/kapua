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
package org.eclipse.kapua.app.console.module.job.client;

import org.eclipse.kapua.app.console.module.api.client.resources.icons.IconSet;
import org.eclipse.kapua.app.console.module.api.client.resources.icons.KapuaIcon;
import org.eclipse.kapua.app.console.module.api.client.ui.dialog.SimpleDialog;
import org.eclipse.kapua.app.console.module.api.client.util.ConsoleInfo;
import org.eclipse.kapua.app.console.module.api.client.util.DialogUtils;
import org.eclipse.kapua.app.console.module.job.client.messages.ConsoleJobMessages;
import org.eclipse.kapua.app.console.module.job.shared.model.GwtJob;
import org.eclipse.kapua.app.console.module.job.shared.service.GwtJobEngineService;
import org.eclipse.kapua.app.console.module.job.shared.service.GwtJobEngineServiceAsync;

import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;

public class JobRestartDialog extends SimpleDialog {

    private static final ConsoleJobMessages JOB_MSGS = GWT.create(ConsoleJobMessages.class);

    private static final GwtJobEngineServiceAsync JOB_ENGINE_SERVICE = GWT.create(GwtJobEngineService.class);

    private final GwtJob gwtJob;

    public JobRestartDialog(GwtJob gwtJob) {
        this.gwtJob = gwtJob;

        DialogUtils.resizeDialog(this, 300, 185);
    }

    @Override
    public void createBody() {
        formPanel.disableEvents(true);
    }

    @Override
    protected void addListeners() {
        // No action needed
    }

    @Override
    public void submit() {
        JOB_ENGINE_SERVICE.restart(gwtJob.getScopeId(), gwtJob.getId(), new AsyncCallback<Void>() {

            @Override
            public void onFailure(Throwable caught) {
                ConsoleInfo.display(MSGS.popupError(), JOB_MSGS.jobRestartErrorMessage(caught.getLocalizedMessage()));
                unmask();
                hide();
            }

            @Override
            public void onSuccess(Void result) {
                ConsoleInfo.display(MSGS.popupInfo(), JOB_MSGS.jobRestartRestartedMessage());
                unmask();
                hide();
            }
        });
    }

    @Override
    public String getHeaderMessage() {
        return JOB_MSGS.jobRestartDialogHeader(gwtJob.getJobName());
    }

    @Override
    public KapuaIcon getInfoIcon() {
        return new KapuaIcon(IconSet.WARNING);
    }

    @Override
    public String getInfoMessage() {
        return JOB_MSGS.jobRestartDialogInfo();
    }
}
