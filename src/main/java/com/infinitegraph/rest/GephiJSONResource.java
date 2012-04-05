package com.infinitegraph.rest;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.infinitegraph.Vertex;
import com.infinitegraph.Edge;
import com.infinitegraph.BaseEdge;
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
        	
            // Open graph database
            graphDB = GraphFactory.open("Graph.CSV", "config.properties");
        	
            // Start transaction
     		tx = graphDB.beginTransaction(AccessMode.READ);
     		
     		// Extract vertices
            for(Vertex vertex : graphDB.getVertices())
            {   
                json = json + "{\"an\":{\"" + vertex.getId() + "\":{\"label\":\"" + vertex.getId() + "\"}}}" + "\r\n";
            }
            
            // Extract edges
            for(Edge edge : graphDB.getEdges())
            {   
                String source = String.valueOf(edge.getOrigin().getId());
                String target = String.valueOf(edge.getTarget().getId());
                json = json + "{\"ae\":{\"" + source + "_" + target + "\":{\"source\":\"" + source + "\",\"directed\":false,\"target\":\"" + target + "\"}}}" + "\r\n";
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