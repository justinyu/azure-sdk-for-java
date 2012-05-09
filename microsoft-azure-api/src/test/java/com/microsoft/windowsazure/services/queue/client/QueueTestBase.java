/**
 * Copyright 2011 Microsoft Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.windowsazure.services.queue.client;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;

/**
 * Queue Test Base
 * Queue test refactoring will be done in future.
 */
public class QueueTestBase {
    public static boolean USE_DEV_FABRIC = false;
    public static final String CLOUD_ACCOUNT_HTTP = "BlobEndpoint=http://65.55.25.123:80/xscltest;QueueEndpoint=http://65.55.25.124:80/xscltest;TableEndpoint=http://65.55.25.125:80/xscltest;AccountName=xscltest;AccountKey=FjUfNl1KiJttbXlsdkMzBTC7WagvrRM9/g6UPBuy0ypCpAbYTL6/KA+dI/7gyoWvLFYmah3IviUP1jykOHHOlA==";
    public static final String CLOUD_ACCOUNT_HTTPS = "DefaultEndpointsProtocol=https;AccountName=[ACCOUNT NAME];AccountKey=[ACCOUNT KEY]";

    protected static CloudStorageAccount httpAcc;
    protected static CloudQueueClient qClient;
    protected static String testSuiteQueueName = generateRandomQueueName();

    @BeforeClass
    public static void setup() throws URISyntaxException, StorageException, InvalidKeyException {

        // UNCOMMENT TO USE FIDDLER
        System.setProperty("http.proxyHost", "localhost");
        System.setProperty("http.proxyPort", "8888");
        System.setProperty("https.proxyHost", "localhost");
        System.setProperty("https.proxyPort", "8888");

        if (USE_DEV_FABRIC) {
            httpAcc = CloudStorageAccount.getDevelopmentStorageAccount();
        }
        else {
            httpAcc = CloudStorageAccount.parse(CLOUD_ACCOUNT_HTTP);
        }

        qClient = httpAcc.createCloudQueueClient();
        testSuiteQueueName = generateRandomQueueName();
        CloudQueue queue = qClient.getQueueReference(testSuiteQueueName);
        queue.create();
    }

    @AfterClass
    public static void teardown() throws StorageException, URISyntaxException {
        CloudQueue queue = qClient.getQueueReference(testSuiteQueueName);
        queue.delete();
    }

    protected static String generateRandomQueueName() {
        String queueName = "queue" + UUID.randomUUID().toString();
        return queueName.replace("-", "");
    }
}
