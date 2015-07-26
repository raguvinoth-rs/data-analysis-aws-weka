/** Raguvinoth R S
**/

/*Code:
 * Weka automatically does clustering using the k means algorithm and generates a chart/graph for the same based on the attributes that we select.
 * 
 * Since the classification is done based on the titanic dataset, a clear cluster is seen.
 * Weka creates a cluster and also generates the plot
 * 
*/
import java.awt.BorderLayout;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import weka.clusterers.ClusterEvaluation;
import weka.clusterers.EM;
import weka.clusterers.SimpleKMeans;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.gui.explorer.ClustererPanel;
import weka.gui.visualize.PlotData2D;
import weka.gui.visualize.VisualizePanel; 


public class Wekaclustercode {
  
  public static void main(String[] args) throws Exception {
	 
	// Loading CSV file
    CSVLoader dataloader = new CSVLoader();
    dataloader.setSource(new File("C:/ScannedOutput.csv"));
    Instances indata = dataloader.getDataSet();
 
    // Saving ARFF file
    ArffSaver filesaver = new ArffSaver();
    filesaver.setInstances(indata);
    filesaver.setDestination(new File("C:/ScannedOutput.arff"));
    filesaver.writeBatch();
    
    
    String[] opt = new String[2];
    opt[0] = "-I";                 // max. iterations
    opt[1] = "100";
   
    SimpleKMeans kmeans = new SimpleKMeans();
    kmeans.setOptions(opt);     
    kmeans.buildClusterer(indata);    

    ClusterEvaluation evaluation = new ClusterEvaluation();
    evaluation.setClusterer(kmeans);
    evaluation.evaluateClusterer(indata);
    System.out.println(evaluation.clusterResultsToString());
    
    kmeans.setSeed(15);

    // Set parameters
    kmeans.setPreserveInstancesOrder(true);
    kmeans.setNumClusters(2);
    kmeans.buildClusterer(indata);

 // This array returns the cluster number (starting with 0) for each instance
 // The array has as many elements as the number of instances
 //int[] assign = kmeans.getAssignments();

 
// setup visualization
 // ClustererPanel.startClusterer()
 PlotData2D plData = ClustererPanel.setUpVisualizableInstances(indata, evaluation);
 String name = (new SimpleDateFormat("HH:mm:ss - ")).format(new Date());
 String clname = kmeans.getClass().getName();
 if (clname.startsWith("weka.clusterers."))
   name += clname.substring("weka.clusterers.".length());
 else
   name += clname;

 VisualizePanel vispanel = new VisualizePanel();
 vispanel.setName(name + " (" + indata.relationName() + ")");
 plData.setPlotName(name + " (" + indata.relationName() + ")");
 vispanel.addPlot(plData);
 vispanel.setXIndex(4);
 vispanel.setYIndex(5);

 // Displaying the data
 // taken from: ClustererPanel.visualizeClusterAssignments(VisualizePanel)
 String pName = vispanel.getName();
 final javax.swing.JFrame jframe = 
   new javax.swing.JFrame("Cluster Visualization " + pName);
 jframe.setSize(1000,1000);
 jframe.getContentPane().setLayout(new BorderLayout());
 jframe.getContentPane().add(vispanel, BorderLayout.CENTER);
 jframe.addWindowListener(new java.awt.event.WindowAdapter() {
   public void windowClosing(java.awt.event.WindowEvent e) {
     jframe.dispose();
   }
 });
 jframe.setVisible(true);
}

}