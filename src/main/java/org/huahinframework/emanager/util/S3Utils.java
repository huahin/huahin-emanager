/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.huahinframework.emanager.util;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import com.amazonaws.services.s3.AmazonS3;

/**
 *
 */
public class S3Utils {
    /**
     * @param s3
     * @param path
     * @param file
     * @throws URISyntaxException
     */
    public static void upload(AmazonS3 s3, String path, File file)
            throws URISyntaxException {
        URI uri = new URI(path);
        String key = uri.getPath().substring(1);
        s3.putObject(uri.getHost(), key, file);
    }

    /**
     * @param s3
     * @param path
     * @throws URISyntaxException
     */
    public static void delete(AmazonS3 s3, String path)
            throws URISyntaxException {
        URI uri = new URI(path);
        String key = uri.getPath().substring(1);
        s3.deleteObject(uri.getHost(), key);
    }
}
