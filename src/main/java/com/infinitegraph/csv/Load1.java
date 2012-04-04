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
import com.infinitegraph.AccessMode;
import com.infinitegraph.Transaction;
import com.infinitegraph.GraphFactory;
import com.infinitegraph.GraphDatabase;
import com.infinitegraph.indexing.IndexManager;
import com.infinitegraph.ConfigurationException;
import com.infinitegraph.indexing.IndexException;

import au.com.bytecode.opencsv.CSVReader;

import com.infinitegraph.csv.schema.DocumentEdge;
import com.infinitegraph.csv.schema.CustomerID;
import com.infinitegraph.csv.schema.RecordID;


public class Load1 {
	
	private Transaction tx;
	private Properties props;
	private GraphDatabase graphDB;
	
	public Load1(String graphName, String propsFile) {
	    
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
        
        // Create index
        try {
            tx = graphDB.beginTransaction(AccessMode.READ_WRITE);
            IndexManager.<RecordID>createGraphIndex(RecordID.class.getName(), "id");
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
            CSVReader csvReader = new CSVReader(new FileReader(props.getProperty("CSV.load1")));
            csvReader.readNext(); // Skip column label row
            System.out.println("Loading data from first CSV ...");
    		while ((row = csvReader.readNext()) != null) 
    		{
		           
    		    // Build graph
    		    String cID = row[0];
    		    String rID = row[1];	              

                // Retrieve or create CustomerIDs
                CustomerID customerID = (CustomerID)graphDB.getNamedVertex(cID);
        		if (customerID == null) 
    			{
    			    customerID = new CustomerID();
    			    customerID.id = cID;
    			    graphDB.addVertex(customerID);
            		graphDB.nameVertex(cID, customerID);
    			}
			
    			// Add records
    			DocumentEdge docEdge = new DocumentEdge();
    			RecordID recordID = new RecordID();
    			recordID.id = rID;
    			graphDB.addVertex(recordID);
    			customerID.addEdge(docEdge, recordID, EdgeKind.BIDIRECTIONAL);
                
    		}
    		csvReader.close();
    		
    		// Commit transaction
    		tx.commit();
    		
    	} catch (FileNotFoundException e) {
    	    System.out.println("Unable to find raw CSV: " + props.getProperty("CSV.load1"));
			e.printStackTrace();
		} catch (IOException e) {
		    System.out.println("Issue with CSV: " + props.getProperty("CSV.load1"));
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
		Load1 loader = new Load1("Graph.CSV", "config.properties");
		loader.ingest();
	}
}