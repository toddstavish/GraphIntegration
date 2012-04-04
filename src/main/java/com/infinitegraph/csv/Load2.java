package com.infinitegraph.csv;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Arrays;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;

import java.util.Properties;

import com.infinitegraph.BaseEdge;
import com.infinitegraph.BaseVertex;
import com.infinitegraph.EdgeKind;
import com.infinitegraph.AccessMode;
import com.infinitegraph.Transaction;
import com.infinitegraph.GraphFactory;
import com.infinitegraph.GraphDatabase;
import com.infinitegraph.indexing.GraphIndex;
import com.infinitegraph.indexing.IndexManager;
import com.infinitegraph.indexing.IndexIterable;
import com.infinitegraph.ConfigurationException;
import com.infinitegraph.indexing.IndexException;

import au.com.bytecode.opencsv.CSVReader;

import com.infinitegraph.csv.schema.DocumentEdge;
import com.infinitegraph.csv.schema.RecordID;
import com.infinitegraph.csv.schema.DocumentID;

public class Load2 {
	
	private Transaction tx;
	private Properties props;
	private GraphDatabase graphDB;
	GraphIndex<RecordID> graphIndex;
	
	public Load2(String graphName, String propsFile) {
	    
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
            IndexManager.<DocumentID>createGraphIndex(DocumentID.class.getName(), "id");
            graphIndex = IndexManager.getGraphIndex(RecordID.class.getName(), "id");          
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
            CSVReader csvReader = new CSVReader(new FileReader(props.getProperty("CSV.load2")));
            row = csvReader.readNext(); // Get the labels row
            String[] typeNames = Arrays.copyOfRange(row, 2, row.length-1);
            System.out.println("Loading data from second CSV ...");
    		while ((row = csvReader.readNext()) != null) 
    		{
		           
    		    // Build graph
    		    String rID = row[0];
    		    String dID = row[1];

                // Retrieve RecordIDs
                IndexIterable<RecordID> indexItr = null;          
                try {        
                    indexItr = graphIndex.get(rID);       		
                } catch (IndexException e) {
                    e.printStackTrace();
                }
                for(RecordID recordID : indexItr)
                {
                    DocumentID documentID = new DocumentID();
                    documentID.id = dID;
                    graphDB.addVertex(documentID);
                    recordID.addEdge(new DocumentEdge(), documentID, EdgeKind.BIDIRECTIONAL);
                    int colNum = 2;
                    for (String className : typeNames)
                    {
                        try {
                            Class classToLoad = Class.forName("com.infinitegraph.csv.schema." + className);
                            BaseVertex schemum = (BaseVertex) classToLoad.newInstance();
                            graphDB.addVertex(schemum);
                            documentID.addEdge(new DocumentEdge(), schemum, EdgeKind.BIDIRECTIONAL);
                            Field field = classToLoad.getFields()[0];
                            Class fieldType = field.getType();
                            if (fieldType.isPrimitive())
                            {
                                field.set(schemum, Integer.parseInt(row[colNum]));
                            }
                            else
                            {
                                field.set(schemum, row[colNum]);
                            }
                            colNum++;
                        } catch (ClassNotFoundException cnfE) {
                            //System.out.println("Class not found: com.infinitegraph.csv.schema." + className); 
                        } catch (InstantiationException iE) {
                            //System.out.println("Instantiation problem: com.infinitegraph.csv.schema." + className);                 
                        } catch (IllegalAccessException iaE) {
                            //System.out.println("Illegal access exception: com.infinitegraph.csv.schema." + className);                 
                        }
                    }    
                }   
                indexItr.close();                
    		}
    		csvReader.close();
    		
    		// Commit transaction
    		tx.commit();
    		
    	} catch (FileNotFoundException e) {
    	    System.out.println("Unable to find raw CSV: " + props.getProperty("CSV.load2"));
			e.printStackTrace();
		} catch (IOException e) {
		    System.out.println("Issue with CSV: " + props.getProperty("CSV.load2"));
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
		Load2 loader = new Load2("Graph.CSV", "config.properties");
		loader.ingest();
	}
}