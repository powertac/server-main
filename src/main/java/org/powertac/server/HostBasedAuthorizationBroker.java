/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS,  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.powertac.server;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.log4j.Logger;

public class HostBasedAuthorizationBroker extends BrokerFilter
{
  static private Logger log = Logger.getLogger(HostBasedAuthorizationBroker.class);

  /**
   * @param next
   */
  public HostBasedAuthorizationBroker (Broker next)
  {
    super(next);
  }
  
  /* (non-Javadoc)
   * @see org.apache.activemq.broker.BrokerFilter#addDestination(org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.ActiveMQDestination, boolean)
   */
  @Override
  public Destination addDestination (ConnectionContext context,
                                     ActiveMQDestination destination,
                                     boolean createIfTemporary)
          throws Exception
  {
    log.info("addDestination - remoteAddress:" + context.getConnection().getRemoteAddress());
//    if (notAllowed) {
//      throw new SecurityException("Hey!!!");
//    }
    return super.addDestination(context, destination, createIfTemporary);
  }

  /* (non-Javadoc)
   * @see org.apache.activemq.broker.BrokerFilter#addConnection(org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.ConnectionInfo)
   */
  @Override
  public void addConnection (ConnectionContext context, ConnectionInfo info)
          throws Exception
  {
    log.info("addConnection - remoteAddress:" + context.getConnection().getRemoteAddress());
    super.addConnection(context, info);
  }

  /* (non-Javadoc)
   * @see org.apache.activemq.broker.BrokerFilter#removeConnection(org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.ConnectionInfo, java.lang.Throwable)
   */
  @Override
  public void removeConnection (ConnectionContext context, ConnectionInfo info,
                                Throwable error) throws Exception
  {
    log.info("removeConnection - remoteAddress:" + context.getConnection().getRemoteAddress());
    super.removeConnection(context, info, error);
  }

  /* (non-Javadoc)
   * @see org.apache.activemq.broker.BrokerFilter#removeDestination(org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.ActiveMQDestination, long)
   */
  @Override
  public void removeDestination (ConnectionContext context,
                                 ActiveMQDestination destination, long timeout)
          throws Exception
  {
    log.info("removeDestination - remoteAddress:" + context.getConnection().getRemoteAddress());
    super.removeDestination(context, destination, timeout);
  }
}
