package com.infinitegraph.csv;

public class Load {
    
    public static void main(String args[]) {
        String graphName = new String("Graph.CSV");
		String propFile =  new String("config.properties");
		Load1 loader1 = new Load1(graphName, propFile);
		loader1.ingest();
		Load2 loader2 = new Load2(graphName, propFile);
		loader2.ingest();
		Load3 loader3 = new Load3(graphName, propFile);
		loader3.ingest();
	}
}