/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.dto;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@XmlRootElement(name = "connector")
@XmlAccessorType(XmlAccessType.FIELD)
public class AcceptingConnectorDTO extends ConnectorTypeDTO {

  /**
   * The transport uri which it will accept connections on.
   */
  @XmlAttribute
  public String bind;

  /**
   * The protocol that the transport will use.
   */
  @XmlAttribute
  public String protocol;

  /**
   * A broker accepts connections via it's configured connectors.
   */
  @XmlElementRef
  public List<ProtocolDTO> protocols = new ArrayList<ProtocolDTO>();

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AcceptingConnectorDTO)) return false;
    if (!super.equals(o)) return false;

    AcceptingConnectorDTO that = (AcceptingConnectorDTO) o;

    if (bind != null ? !bind.equals(that.bind) : that.bind != null)
      return false;
    if (protocol != null ? !protocol.equals(that.protocol) : that.protocol != null)
      return false;
    if (protocols != null ? !protocols.equals(that.protocols) : that.protocols != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (bind != null ? bind.hashCode() : 0);
    result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
    result = 31 * result + (protocols != null ? protocols.hashCode() : 0);
    return result;
  }
}