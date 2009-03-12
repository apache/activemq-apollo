package org.apache.activemq.transport;

public interface AsyncTransport extends Transport{

    public interface CommandSource
    {
        public Object pollNextCommand();
        public boolean delayable();
    }
    
    public void setCommandSource(CommandSource source);
    public void onCommandReady();
}
