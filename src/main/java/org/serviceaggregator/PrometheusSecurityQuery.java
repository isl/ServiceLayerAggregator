package org.serviceaggregator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.http.protocol.HTTP.USER_AGENT;

/**
 * Created by dmetallidis on 8/25/16.
 */


public class PrometheusSecurityQuery {

    //logIn_request_total
    //failed_login_auth_request_total

    Map<Double,Integer> numberOfFailedAuthentications = new HashMap<Double, Integer>();
    Map<Double,Integer> numberOfTotalAttemptsOfAuthentication = new HashMap<Double, Integer>();



    public static void main(String[] args) throws Exception {


        PrometheusSecurityQuery http = new PrometheusSecurityQuery();

        System.out.println("Send Prometheus Http GET request");
        //simple GET of Request Completion Time
//        http.getFailedAuthentications();
        http.getTotalAttemptsOfAuthentications();
    }

    public Map<Double,Integer> getFailedAuthentications() throws IOException, JSONException {

        String url;
        int failedAuthentications;
        double linuxTime;
        url = "http://localhost:9090/api/v1/query?query=failed_login_auth_request_total" ;

        // Example Queries
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        JSONObject jsonObj = new JSONObject(response.toString());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(jsonObj);
//            System.out.println(json);



        if (!jsonObj.getJSONObject("data").getJSONArray("result").isNull(0)) {
            JSONArray lastEntry = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("value");
            linuxTime = Double.parseDouble(lastEntry.get(0).toString());
            failedAuthentications = Integer.parseInt(lastEntry.get(1).toString());
            // to calculate the acklodeged messages we remove the error one from requestMessages
            numberOfFailedAuthentications.put(linuxTime, failedAuthentications);
        }

          printNumberOfFailedAuthentications();
        return numberOfFailedAuthentications;

    }

    public void printNumberOfFailedAuthentications(){

        for (Map.Entry e : numberOfFailedAuthentications.entrySet()) {
            System.out.println("NumberOfFailedAuthentications values, having date : " + e.getKey() + " me times " + "kai timh: " + e.getValue().toString());
        }
    }

    public Map<Double,Integer>  getTotalAttemptsOfAuthentications() throws IOException, JSONException {

        String url;
        int totalFailedAttemptsAuthentications = 0;
        double linuxTime;
        url = "http://localhost:9090/api/v1/query?query=failed_login_auth_request_total" ;

        // Example Queries
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        JSONObject jsonObj = new JSONObject(response.toString());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(jsonObj);
//            System.out.println(json);



        if (!jsonObj.getJSONObject("data").getJSONArray("result").isNull(0)) {
            JSONArray lastEntry = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("value");
            linuxTime = Double.parseDouble(lastEntry.get(0).toString());
            totalFailedAttemptsAuthentications = Integer.parseInt(lastEntry.get(1).toString());
            // to calculate the acklodeged messages we remove the error one from requestMessages
            int successAttempts = getSuccessTotalAttempts();
            numberOfTotalAttemptsOfAuthentication.put(linuxTime, successAttempts+totalFailedAttemptsAuthentications);
        }


        printNumberOfTotalFailedAuthentications();
        return numberOfTotalAttemptsOfAuthentication;
    }

    private int getSuccessTotalAttempts() throws IOException, JSONException {

        String url;
        int totalSuccessdAttemptsAuthentications = 0;
        url = "http://localhost:9090/api/v1/query?query=success_login_auth_request_total" ;

        // Example Queries
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        JSONObject jsonObj = new JSONObject(response.toString());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String json = gson.toJson(jsonObj);
//            System.out.println(json);



        if (!jsonObj.getJSONObject("data").getJSONArray("result").isNull(0)) {
            JSONArray lastEntry = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("value");
            totalSuccessdAttemptsAuthentications = Integer.parseInt(lastEntry.get(1).toString());
        }

        return  totalSuccessdAttemptsAuthentications;
    }


    public void printNumberOfTotalFailedAuthentications(){

        for (Map.Entry e : numberOfTotalAttemptsOfAuthentication.entrySet()) {
            System.out.println("NumberOfTotal Attempts Authentications values, having date : " + e.getKey() + " me times " + "kai timh: " + e.getValue().toString());
        }
    }
}
