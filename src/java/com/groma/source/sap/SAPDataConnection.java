package com.groma.source.sap;

import com.groma.core.*;
import com.groma.db.*;
import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.*;
import java.util.*;

final class SAPDataConnection implements IDataConnection {

    private String _connectionString;

    private JCoDestination _destination;

    private String _connectionId;

    public SAPDataConnection(String connectionString) {
        this._connectionString = connectionString;
        this._connectionId = UUID.randomUUID().toString();
    }

    @Override
    public void open() throws Exception {
        if (this._connectionString == null) {
            throw new Exception("The connection string couldn't be null.");
        }

        SAPConnectionDataManager.INSTANCE.add(this._connectionId, this._connectionString);
        this._destination = JCoDestinationManager.getDestination(this._connectionId);
        if (this._destination == null) {
            throw new Exception("Unable to get the destination..");
        }

        try {
            this._destination.ping();
        } catch (Exception ex) {
            throw new Exception("Fails to open connection.", ex);
        }
    }

    @Override
    public void close() throws Exception {
        if (this._destination != null) {
            try {
                JCoContext.end(this._destination);
            } catch (Exception ex) {
                throw new Exception("Fails to close connection.", ex);
            }
        }
    }

    public JCoDestination getDestination() {
        return this._destination;
    }
}

class SAPConnectionDataManager implements DestinationDataProvider {

    public static SAPConnectionDataManager INSTANCE = new SAPConnectionDataManager();

    private Hashtable<String, Properties> _connections = new Hashtable<String, Properties>();

    public void add(String connectionId, String connectionString) throws Exception {
        Properties properties = new Properties();
        properties.put(DestinationDataProvider.JCO_ASHOST, "127.0.0.1");
        properties.put(DestinationDataProvider.JCO_SYSNR, "00");
        ConnectionStringReader reader = new ConnectionStringReader(connectionString);
        String name;
        while(reader.read()) {
            String key = reader.getPropertyKey();
            if (key.equals(ConnectionPropertyNames.SERVER)) {
                name = DestinationDataProvider.JCO_ASHOST;
            } else if (key.equals(ConnectionPropertyNames.USER)) {
                name = DestinationDataProvider.JCO_USER;
            } else if (key.equals(ConnectionPropertyNames.PASSWORD)) {
                name = DestinationDataProvider.JCO_PASSWD;
            } else if (key.equals(ConnectionPropertyNames.CLIENT)) {
                name = DestinationDataProvider.JCO_CLIENT;
            } else if (key.equals(ConnectionPropertyNames.LANGUAGE)) {
                name = DestinationDataProvider.JCO_LANG;
            } else {
              throw new Exception("Unknown connection property '" + reader.getPropertyName() + "'.");
            }

            properties.put(name, reader.getPropertyValue());
        }


        synchronized (this._connections) {
            this._connections.put(connectionId, properties);
        }
    }

    public Properties getDestinationProperties(String connectionId) {
        if (this._connections.containsKey(connectionId)) {
            return this._connections.get(connectionId);
        }

        return null;
    }

    public boolean supportsEvents() {
        return false;
    }

    public void setDestinationDataEventListener(DestinationDataEventListener var1) {
        System.out.println("Unsupported Method: GSAPDestinationDataProvider.setDestinationDataEventListener()");
    }
}