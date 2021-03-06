/**
 *
 * Copyright (C) 2010 Cloud Conscious, LLC. <info@cloudconscious.com>
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.jclouds.scaleup.storage;

import org.testng.annotations.Test;

/**
 * @author Adrian Cole
 */
// NOTE:without testName, this will not call @Before* and fail w/NPE during surefire
@Test(enabled = false, groups = "unit", testName = "ScaleUpStorageAsyncClientTest")
public class ScaleUpStorageAsyncClientTestDisabled extends org.jclouds.s3.S3AsyncClientTest {

   public ScaleUpStorageAsyncClientTestDisabled() {
      this.provider = "scaleup-storage";
      this.url = "commondatastorage.googleapis.com";
   }

   // TODO parameterize this test so that it can pass
}
