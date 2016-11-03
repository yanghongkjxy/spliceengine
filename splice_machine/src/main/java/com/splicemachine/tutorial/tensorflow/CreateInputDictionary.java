package com.splicemachine.tutorial.tensorflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;


public class CreateInputDictionary {
    
    private static int fileSuffix = 0;
    
    private static final Logger LOG = Logger
            .getLogger(CreateInputDictionary.class);
    
    private static StringBuilder buildDictionaryObject(Connection conn, String modelName, JsonObject inputDict) throws SQLException {
        //Get Columns
        PreparedStatement pstmt = conn.prepareStatement("select COLUMN_NAME from INPUT_DICTIONARY where MODEL = ? and TYPE = ? order by SEQUENCE");
        pstmt.setString(1, modelName);
        pstmt.setString(2, "COLUMN");
        ResultSet rs = pstmt.executeQuery();
               
        //Contains a list of columns to export 
        StringBuilder exportColumns = new StringBuilder();
        
        JsonArray jsonArr = new JsonArray();
        int numColumns = 0;
        while(rs.next()) {            
            String name = rs.getString(1);
            
            if(numColumns > 0) { exportColumns.append(",");}
            exportColumns.append(name);
            
            jsonArr.add(name.toLowerCase());
            numColumns++;
        }
        if(numColumns > 0) {
            inputDict.add("columns", jsonArr);
        }
        
        //Get the categorical columns
        pstmt.clearParameters();
        pstmt.setString(1, modelName);
        pstmt.setString(2, "CATEGORICAL");
        rs = pstmt.executeQuery();        
        jsonArr = new JsonArray();
        numColumns = 0;
        while(rs.next()) {            
            jsonArr.add(rs.getString(1).toLowerCase());
            numColumns++;
        }
        if(numColumns > 0) {
            inputDict.add("categorical_columns", jsonArr);
        }
        
        //Get the continuous columns
        pstmt.clearParameters();
        pstmt.setString(1, modelName);
        pstmt.setString(2, "CONTINUOUS");
        rs = pstmt.executeQuery();        
        jsonArr = new JsonArray();
        numColumns = 0;
        while(rs.next()) {            
            jsonArr.add(rs.getString(1).toLowerCase());
            numColumns++;
        }
        if(numColumns > 0) {
            inputDict.add("continuous_columns", jsonArr);
        }

        //Get label_column
        pstmt.clearParameters();
        pstmt.setString(1, modelName);
        pstmt.setString(2, "LABEL");
        rs = pstmt.executeQuery();
        if(rs.next()) {
            inputDict.addProperty("label_column", rs.getString(1).toLowerCase());
        }
        
        //Get bucketized_columns
        JsonObject buckets = new JsonObject();
        pstmt = conn.prepareStatement("select COLUMN_NAME, LABEL, GROUPING_DETAILS, GROUPING_DETAILS_TYPE from INPUT_DICTIONARY where MODEL = ? and TYPE = ? order by SEQUENCE");
        pstmt.setString(1, modelName);
        pstmt.setString(2, "BUCKET");
        rs = pstmt.executeQuery(); 
        numColumns = 0;
        if(rs.next()) {
            String column = rs.getString(1).toLowerCase();
            String label = rs.getString(2).toLowerCase();
            String groupingDetails = rs.getString(3);
            String groupingDetailsType = rs.getString(4);
            
            JsonObject bucketObject = new JsonObject();
            
            //Add the column label
            JsonArray bucketValues = new JsonArray();
            
            JsonArray bucketDetailsValues = new JsonArray();
            String[] groupVals = groupingDetails.split(",");
            for(String val: groupVals) {               
                if(groupingDetailsType.equals("INTEGER")) {
                    bucketDetailsValues.add(Integer.parseInt(val.trim()));
                } else {
                    bucketDetailsValues.add(val);
                }
            }    
            //Add the bucket groups
            bucketValues.addAll(bucketDetailsValues);
            bucketObject.add(column, bucketValues);
            
            buckets.add(label, bucketObject);
            
            numColumns++;
        }
        if(numColumns > 0) {
            inputDict.add("bucketized_columns", buckets);
        }
        
        //Get the crossed
        pstmt = conn.prepareStatement("select GROUPING, COLUMN_NAME from INPUT_DICTIONARY where MODEL = ? and TYPE = ? order by GROUPING,SEQUENCE");
        pstmt.setString(1, modelName);
        pstmt.setString(2, "CROSSED");
        rs = pstmt.executeQuery();
        
        JsonArray crossed_columns = new JsonArray();
        numColumns = 0;
        int currentGrouping = 0;
        JsonArray group = null;
        while(rs.next()) {
            int grouping = rs.getInt(1);
            if(numColumns == 0 || currentGrouping != grouping) {
                if (numColumns != 0 && currentGrouping != grouping) {
                    crossed_columns.add(group);
                }
                group = new JsonArray();
                group.add(rs.getString(2).toLowerCase());
                currentGrouping = grouping;
            } else {
                group.add(rs.getString(2).toLowerCase());
            }               
            numColumns++;
        }
        if(numColumns > 0) {
            crossed_columns.add(group);
            inputDict.add("crossed_columns", crossed_columns);
        }   
        return exportColumns;
    }
    
    /**
     * Export the data from Splice Machine to the file system
     * 
     * @param conn - Current Connection
     * @param exportCmd - EXPORT command with columns and export location
     * @param exportPath - Used to merge files - it is the directory containing all the files
     * @param exportFileFinal - Used to merge files - indicates what the file final name should be
     * @throws SQLException
     */
    private static void exportData(Connection conn, String exportCmd, String exportPath, String exportFileFinal) throws SQLException {
        LOG.error("Export Command: " + exportCmd);
        Statement stmt = conn.createStatement();
        stmt.executeQuery(exportCmd);
    }
    
    /**
     * Stored procedure used to call a python script.
     * 
     * @param scriptname - Full path to the python script
     */
    public static void generateModel(String fullModelPath, String type, String modelName, String trainTable, String testTable) { 
        try{
            
            Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb;user=splice;password=admin");
            
            File pythonFile = new File(fullModelPath);
            String parentDir = pythonFile.getParent();
            
            String modelOutputDir = parentDir + "/output";
            String dataDir = parentDir + "/data";
            
            String trainingDataFile = dataDir + "/train/part-r-00000.csv";
            String testDataFile = dataDir + "/test/part-r-00000.csv";
            
            JsonObject inputDict = new JsonObject();
            StringBuilder exportColumns = buildDictionaryObject(conn, modelName, inputDict);
            
            
            //Get the train_data_path
            String exportPathTrain = dataDir + "/train";
            inputDict.addProperty("train_data_path", exportPathTrain + "/part-r-00000.csv");
            String exportCmd = "EXPORT('" + exportPathTrain + "', false, null, null, null, null) SELECT " + exportColumns.toString() + " FROM " + trainTable;
            exportData(conn, exportCmd, exportPathTrain + "/* ", exportPathTrain + "/traindata.txt");        
            
            //Get test_data_path
            String exportPathTest = dataDir + "/test";
            inputDict.addProperty("test_data_path", exportPathTest + "/part-r-00000.csv");
            exportCmd = "EXPORT('" + exportPathTest + "', false, null, null, null, null) SELECT " + exportColumns.toString() + " FROM " + testTable;
            exportData(conn, exportCmd, exportPathTest + "/* ", exportPathTest + "/testdata.txt");

            //Build JSON
            Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            String jsonData = gson.toJson(inputDict);
            
            LOG.error("JSON Data: " + jsonData);
                        
            ProcessBuilder pb = new ProcessBuilder("python",fullModelPath,
                    "--model_type=" + type,
                    "--model_dir=" + modelOutputDir,
                    "--inputs=" + jsonData);
            Process p = pb.start();
             
            BufferedReader bfr = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";
            LOG.error("Running Python starts: " + line);
            int exitCode = p.waitFor();
            LOG.error("Exit Code : "+exitCode);
            line = bfr.readLine();
            LOG.error("First Line: " + line);
            while ((line = bfr.readLine()) != null){
                LOG.error("Python Output: " + line);
            }

        }catch(Exception e){
                LOG.error("Exception calling pythong script.", e);
        }
    }
    
    public static void predictModel(String fullModelPath, String type, String modelName, String sourceTable, int sourceId, ResultSet[] returnResultset) {
        try{

            LOG.error("In predictModel: ");
            
            Connection conn = DriverManager.getConnection("jdbc:default:connection");
            
            File pythonFile = new File(fullModelPath);
            String parentDir = pythonFile.getParent();
            
            String modelOutputDir = parentDir + "/output";
            String dataDir = parentDir + "/data";
            
            LOG.error("In predictModel about to build json object: ");
            JsonObject inputDict = new JsonObject();
            StringBuilder exportColumns = buildDictionaryObject(conn, modelName, inputDict);
            
            LOG.error("Export columns: " + exportColumns);
            
            //Retrieve the record from the database
            StringBuilder recordAsCSV = null;
            Statement stmt = conn.createStatement();
            LOG.error("About to select columns: " + sourceTable);
            ResultSet rs = stmt.executeQuery("SELECT " + exportColumns.toString() + " FROM " + sourceTable + " where ID = " + sourceId);
            if(rs.next()) {
                LOG.error("Record found: " + sourceTable);
                recordAsCSV = new StringBuilder();
                int numCols = rs.getMetaData().getColumnCount();
                for(int i=0; i<numCols; i++) {
                    if(i != 0) recordAsCSV.append(",");
                    recordAsCSV.append(rs.getObject(i+1));
                }
                //rs.getOb
            } else {
                LOG.error("No Records found: ");
                returnResultset[0] = stmt.executeQuery("values(-1)");
                return;
            }
            LOG.error("About to build json: ");
            //Build JSON
            Gson gson = new GsonBuilder().setPrettyPrinting().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            String jsonData = gson.toJson(inputDict);
            
            LOG.error("model_type:" + type);
            LOG.error("model_dir:" + modelOutputDir);
            LOG.error("input_record:" + recordAsCSV);
            LOG.error("inputs:" + jsonData);
            
            ProcessBuilder pb = new ProcessBuilder("python",fullModelPath,
                    "--predict=true",
                    "--model_type=" + type,
                    "--model_dir=" + modelOutputDir,
                    "--input_record=" + recordAsCSV,                    
                    "--inputs=" + jsonData);
            
            LOG.error("About to start process builder: ");
            
            Process p = pb.start();
            
            BufferedReader bfr = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String returnVal = "";
            String line = "";
            int exitCode = p.waitFor();
            LOG.error("Running Python starts: ");
            LOG.error("Exit Code : "+exitCode);
            LOG.error("First Line: " + bfr.readLine());
            while ((line = bfr.readLine()) != null){
                LOG.error("Python Output: " + line);
                returnVal = line;
            }
            
            if(exitCode == 0) {    
                LOG.error("return val: " + returnVal);
                int beginIndex = returnVal.indexOf("[");
                int endIndex = returnVal.indexOf("]");
                if(beginIndex > -1 && endIndex > -1) {
                    returnVal = returnVal.substring(beginIndex+1, endIndex);
                    LOG.error("beginIndex: " + beginIndex);
                    LOG.error("endIndex: " + endIndex);
                    LOG.error("return val: " + returnVal);
                    stmt.executeUpdate("UPDATE " + sourceTable + " set LABEL = '" + returnVal +"' where ID = " + sourceId);
                    returnResultset[0] = stmt.executeQuery("select * from "+ sourceTable + " where ID = " + sourceId);
                }
                
                
            } 
            
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Exception calling pythong script.", e);
        }
    }
    
    public static void main(String[] args) {
        predictModel("/Users/erindriggers/anaconda/envs/tensorflow/projects/wide_n_deep/restore_wide_n_deep_model.py","wide_n_deep","CENSUS","CENSUS.LIVE_DATA",1, null);
    }
}