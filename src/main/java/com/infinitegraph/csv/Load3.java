package com.infinitegraph.csv;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Date;

import org.apache.log4j.Logger;

import java.util.Properties;

import com.infinitegraph.BaseEdge;
import com.infinitegraph.EdgeKind;
import com.infinitegraph.BaseVertex;
import com.infinitegraph.AccessMode;
import com.infinitegraph.Transaction;
import com.infinitegraph.GraphFactory;
import com.infinitegraph.GraphDatabase;
import com.infinitegraph.indexing.GraphIndex;
import com.infinitegraph.indexing.IndexManager;
import com.infinitegraph.indexing.IndexIterable;
import com.infinitegraph.indexing.IndexException;
import com.infinitegraph.ConfigurationException;

import au.com.bytecode.opencsv.CSVReader;

import com.infinitegraph.csv.schema.TripleID;
import com.infinitegraph.csv.schema.FieldName;
import com.infinitegraph.csv.schema.FieldValue;
import com.infinitegraph.csv.schema.DocumentID;
import com.infinitegraph.csv.schema.DocumentEdge;


public class Load3 {
	
	private Transaction tx;
	private Properties props;
	private GraphDatabase graphDB;
	GraphIndex<DocumentID> graphIndex;
	
	public Load3(String graphName, String propsFile) {
	    
	    // Load properties
	    props = new Properties();
        try {
            props.load(new FileReader(propsFile));
		} catch (FileNotFoundException e) {
			System.out.println("Unable to find properties file: " + propsFile);
			e.printStackTrace();		    
		} catch (IOException e) {
		    System.out.println("Unable to find properties file: " + propsFile);
			e.printStackTrace();
		}
	    
	    // Create - Open Graph Database    
		try {
	        graphDB = GraphFactory.open(graphName, propsFile);
	        System.out.println("Opening graphDB: " + graphName);
        } catch (ConfigurationException openEx) {
            System.out.println("Creating graphDB: " + graphName);
            try {
                GraphFactory.create(graphName, propsFile);
                graphDB = GraphFactory.open(graphName, propsFile);
            } catch (ConfigurationException createEx) {
                System.out.println("Issue with creating graphDB: " + graphName);
            	createEx.printStackTrace();
            }
        }
        
        // Get index
        try {
            tx = graphDB.beginTransaction(AccessMode.READ_WRITE);
            graphIndex = IndexManager.getGraphIndex(DocumentID.class.getName(), "id");          
            tx.commit();
        } catch (IndexException e) {
        	e.printStackTrace();
        }
    }
 
    public void ingest() {

        // Load graph
        try {
            
            // Start transaction
    		tx = graphDB.beginTransaction(AccessMode.READ_WRITE);
    		
    		// Parse csv
            String[] row;
            CSVReader csvReader = new CSVReader(new FileReader(props.getProperty("CSV.load3")));
            csvReader.readNext(); // Skip column label row
            System.out.println("Loading data from third CSV ...");
    		while ((row = csvReader.readNext()) != null) 
    		{
		           
    		    // Build graph
    		    String dID = row[0];
    		    String tID = row[1];

                // Retrieve DocumentIDs
                IndexIterable<DocumentID> indexItr = null;          
                try {        
                    indexItr = graphIndex.get("id", dID);       		
                } catch (IndexException e) {
                    e.printStackTrace();
                }
                for(DocumentID documentID : indexItr)
                {
                    DocumentEdge tripleEdge = new DocumentEdge();
        			TripleID tripleID = new TripleID();
        			tripleID.id = tID;
        			graphDB.addVertex(tripleID);
        			documentID.addEdge(tripleEdge, tripleID, EdgeKind.BIDIRECTIONAL);
        			FieldName fn = new FieldName();
        			fn.name = row[2];
        			graphDB.addVertex(fn);
        			tripleID.addEdge(new DocumentEdge(), fn, EdgeKind.BIDIRECTIONAL);
        			FieldValue fv = new FieldValue();
        			fv.value = row[3];
        			graphDB.addVertex(fv);
        			tripleID.addEdge(new DocumentEdge(), fv, EdgeKind.BIDIRECTIONAL);
                }   
                indexItr.close();                
    		}
    		csvReader.close();
    		
    		// Commit transaction
    		tx.commit();
    		
    	} catch (FileNotFoundException e) {
    	    System.out.println("Unable to find raw CSV: " + props.getProperty("CSV.load3"));
			e.printStackTrace();
		} catch (IOException e) {
		    System.out.println("Issue with CSV: " + props.getProperty("CSV.load3"));
			e.printStackTrace();
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
    }
    
    public static void main(String args[]) {
		Load3 loader = new Load3("Graph.CSV", "config.properties");
		loader.ingest();
	}
}