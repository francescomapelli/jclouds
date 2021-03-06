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

package org.jclouds.date.joda.config;

import org.jclouds.date.DateService;
import org.jclouds.date.joda.JodaDateService;

import com.google.inject.AbstractModule;

/**
 * Configures DateService of type {@link JodaDateService}
 * 
 * @author Adrian Cole
 * 
 */
public class JodaDateServiceModule extends AbstractModule {

   @Override
   protected void configure() {
      bind(DateService.class).to(JodaDateService.class);
   }

}
