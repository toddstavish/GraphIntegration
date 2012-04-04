package com.infinitegraph.rest;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.infinitegraph.Vertex;
import com.infinitegraph.BaseVertex;
import com.infinitegraph.AccessMode;
import com.infinitegraph.Transaction;
import com.infinitegraph.GraphFactory;
import com.infinitegraph.GraphDatabase;
import com.infinitegraph.ConfigurationException;


/**
 * Resource which has only one representation.
 */
public class GephiJSONResource extends ServerResource {

    @Get
    public String represent() {
        
        // JSON stream
    	String json = new String();
    	
        // Create null transaction, null graph database instance
    	Transaction tx = null;
    	GraphDatabase graphDB = null;
    	
        // Extract graph
        try {
        	
            // Open graph databse
            graphDB = GraphFactory.open("Graph.CSV", "config.properties");
        	
            // Start transaction
     		tx = graphDB.beginTransaction(AccessMode.READ);
            for(Vertex vertex : graphDB.getVertices())
            {   
                json = json + "{\"an\":{\"" + vertex.getId() + "\":{\"label\":\"" + vertex.getId() + "\"}}}" + "\r\n";
            }

     		// Commit transaction
     		tx.commit();
 		}
        catch (ConfigurationException cE)
        {
            System.out.println("> Configuration Exception was thrown ... ");
            System.out.println(cE.getMessage());
        }
 		finally
 		{
 			// If the transaction was not committed, complete
 			// will roll it back
 			if (tx != null)
 			{
 				tx.complete();
 			}
 			if (graphDB != null)
 			{
 				graphDB.close();
 			}
 		}
        
        
        System.out.println(json);
        
        return json;
        
    }

}