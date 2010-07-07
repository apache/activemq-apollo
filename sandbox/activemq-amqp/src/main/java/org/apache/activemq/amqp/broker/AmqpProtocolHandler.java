package org.apache.activemq.amqp.broker;

import java.io.IOException;

import org.apache.activemq.amqp.protocol.AmqpCommand;
import org.apache.activemq.amqp.protocol.AmqpCommandHandler;
import org.apache.activemq.amqp.protocol.types.AmqpAttach;
import org.apache.activemq.amqp.protocol.types.AmqpBar;
import org.apache.activemq.amqp.protocol.types.AmqpClose;
import org.apache.activemq.amqp.protocol.types.AmqpDetach;
import org.apache.activemq.amqp.protocol.types.AmqpDisposition;
import org.apache.activemq.amqp.protocol.types.AmqpDrain;
import org.apache.activemq.amqp.protocol.types.AmqpEnlist;
import org.apache.activemq.amqp.protocol.types.AmqpFlow;
import org.apache.activemq.amqp.protocol.types.AmqpLink;
import org.apache.activemq.amqp.protocol.types.AmqpNoop;
import org.apache.activemq.amqp.protocol.types.AmqpOpen;
import org.apache.activemq.amqp.protocol.types.AmqpRelink;
import org.apache.activemq.amqp.protocol.types.AmqpTransfer;
import org.apache.activemq.amqp.protocol.types.AmqpTxn;
import org.apache.activemq.amqp.protocol.types.AmqpUnlink;
import org.apache.activemq.amqp.wireformat.AmqpWireFormat;
import org.apache.activemq.apollo.broker.BrokerConnection;
import org.apache.activemq.apollo.broker.BrokerMessageDelivery;
import org.apache.activemq.apollo.broker.ProtocolHandler;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.wireformat.WireFormat;

public class AmqpProtocolHandler implements ProtocolHandler {

    protected AmqpWireFormat wireFormat;
    protected AmqpWireFormat storeWireFormat;

    protected BrokerConnection connection;
    private final AmqpCommandHandler handler = new AmqpCommandHandler(){

        public void handleAttach(AmqpAttach attach) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleBar(AmqpBar bar) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleClose(AmqpClose close) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleDetach(AmqpDetach detach) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleDisposition(AmqpDisposition disposition) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleDrain(AmqpDrain drain) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleEnlist(AmqpEnlist enlist) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleFlow(AmqpFlow flow) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleLink(AmqpLink link) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleNoop(AmqpNoop noop) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleOpen(AmqpOpen open) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleRelink(AmqpRelink relink) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleTransfer(AmqpTransfer transfer) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleTxn(AmqpTxn txn) throws Exception {
            // TODO Auto-generated method stub
            
        }

        public void handleUnlink(AmqpUnlink unlink) throws Exception {
            // TODO Auto-generated method stub
            
        }

    };
    
    AmqpProtocolHandler()
    {
        
    }
    
    public BrokerMessageDelivery createMessageDelivery(MessageRecord arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    public BrokerConnection getConnection() {
        return connection;
    }

    public void onCommand(Object command) {
        try {
            ((AmqpCommand) command).handle(handler);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void onException(Exception arg0) {
        // TODO Auto-generated method stub
        
    }

    public void setConnection(BrokerConnection connection) {
        this.connection = connection;
    }

    private void setStoreWireFormat(AmqpWireFormat wireFormat) {
        this.storeWireFormat = wireFormat;
        storeWireFormat.setVersion(AmqpWireFormat.DEFAULT_VERSION);
    }
    
    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = (AmqpWireFormat) wireFormat;
    }

    public void start() throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void stop() throws Exception {
        // TODO Auto-generated method stub
        
    }

}
