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
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.apache.http.protocol.HTTP.USER_AGENT;

/**
 * Created by dmetallidis on 8/23/16.
 */
public class PrometheusThroughputQuery {



    public Map<String, Map<Double, Long>> averageRequestNumber = new HashMap<String, Map<Double, Long>>();
    public Map<String, Map<Double, Long>> maximumThroughputNumber = new HashMap<String, Map<Double, Long>>();

    public static final String prefixesOfServices [] = {"user", "permAdmin", "object", "ouUser", "role", "userDetail", "perm", "objectAdmin", "roleAdmin"
            , "ouPerm", "sdDynamic", "sdStatic"};

    public static void main(String[] args) throws Exception {


        PrometheusThroughputQuery http = new PrometheusThroughputQuery();

        System.out.println("Send Prometheus Http GET request");
        //simple GET of Request Completion Time
        http.parserThroughput("max");
    }

    public void parserThroughput(String typeOfThroughput) throws IOException, JSONException {

//        for(int i=0; i< prefixesOfServices.length; i++){

        for(int i=0; i<prefixesOfServices.length; i++){
        String url = "http://localhost:9090/api/v1/query?query="+typeOfThroughput+"("+prefixesOfServices[i]+"RequestCompletionTime_latency_milliseconds_count)";

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
        System.out.println(json);
        if (!jsonObj.getJSONObject("data").getJSONArray("result").isNull(0)) {
        JSONArray name = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("value");
        Map<Double,Long> throughputMap= new HashMap<Double,Long>();

        String omilli = (String) name.get(1);
        double dmilli =  Double.parseDouble(omilli);
            System.out.println("equals with : " + dmilli);

            long  olinuxTime = new Double((Double) name.get(0)).longValue();

//        Long llinuxTIme = Long.parseLong(olinuxTime.toString());

        if( dmilli != 0.0){
        throughputMap.put(dmilli, olinuxTime);
        if(typeOfThroughput.equals("avg"))
        averageRequestNumber.put(prefixesOfServices[i]+"AvgThroughput",throughputMap);
        else
        maximumThroughputNumber.put(prefixesOfServices[i]+"MaxThroughput",throughputMap);
        }
        }
       }
        printAverageRequestTimes(typeOfThroughput);
    }


    public Map<String, Map<Double, Long>> getAverageRequestNumber() {
        return averageRequestNumber;
    }

    public Map<String, Map<Double, Long>> getMaximumThroughputNumber() {
        return maximumThroughputNumber;
    }

    // print of avg and max values
    public void printAverageRequestTimes(String typeOfThroughput){

        for(int i=0; i<prefixesOfServices.length; i++){
            Map<Double, Long> map;
            if(typeOfThroughput.equals("avg")){
            map = averageRequestNumber.get(prefixesOfServices[i]+"AvgThroughput");
                if(map != null)
                    for (Map.Entry e : map.entrySet()) {
                        System.out.println("Avg values, having metric name : " + prefixesOfServices[i]+"AvgThroughput me times " +
                                " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
                    }

            }
            else{
            map = maximumThroughputNumber.get(prefixesOfServices[i]+"MaxThroughput");
                if(map != null)
                    for (Map.Entry e : map.entrySet()) {
                        System.out.println("Max values, having metric name : " + prefixesOfServices[i]+"MaxThroughput me times " +
                                " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
                    }

            }

        }

    }


}



