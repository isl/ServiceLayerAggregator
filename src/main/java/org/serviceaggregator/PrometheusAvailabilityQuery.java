package org.serviceaggregator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.apache.http.protocol.HTTP.USER_AGENT;

/**
 * Created by dmetallidis on 8/23/16.
 */
public class PrometheusAvailabilityQuery {



    public Map<String, Map<String, Double>> availabilityPercentangeMetrics = new HashMap<String, Map<String, Double>>();
    public Map<String, Map<String, Double>> downTimePercentageMetrics = new HashMap<String, Map<String, Double>>();
    public static final String prefixesOfServices [] = {"userPage", "permAdminPage", "objectPage", "ouUserPage", "rolePage", "userDetailPage", "permPage", "objectAdminPage", "roleAdminPage"
            , "ouPermPage", "sdDynamicPage", "sdStaticPage"};

    public static void main(String[] args) throws Exception {


        PrometheusAvailabilityQuery http = new PrometheusAvailabilityQuery();

        System.out.println("Send Prometheus Http GET request");
        //simple GET of Request Completion Time
        http.parserAvailability();
    }

    public void parserAvailability() throws IOException, JSONException {

//        for(int i=0; i< prefixesOfServices.length; i++){

        String url = "http://localhost:9090/api/v1/query?query=avg_over_time(up[5m])*100";

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
            Map<String,Double>  upMap;
            Map<String,Double>  downMap;

            for(int i=0; i< jsonObj.getJSONObject("data").getJSONArray("result").length(); i++){

                String name = (String) jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(i).getJSONObject("metric").get("job");
                String upPercentage = (String) jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(i).getJSONArray("value").get(1);
                Double timestamp = Double.parseDouble(String.valueOf(jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(i).getJSONArray("value").get(0)));

                upMap = new HashMap<String, Double>();
                upMap.put(upPercentage, timestamp);
                availabilityPercentangeMetrics.put(name+"_availabilityPercentage",upMap);

                // for the downTime percentage metrics
                double doubleDownPercentage = 100 - Double.parseDouble(upPercentage);
                String downPercentage = Double.toString(doubleDownPercentage);

                downMap = new HashMap<String, Double>();
                downMap.put(downPercentage, timestamp);
                downTimePercentageMetrics.put(name+"_downPercentage",downMap);
            }
        }
        printUpMetrics();
        printDownMetrics();
    }

    public Map<String, Map<String, Double>> getDownTimePercentageMetrics() {
        return downTimePercentageMetrics;
    }

    public Map<String, Map<String, Double>> getAvailabilityPercentangeMetrics() {
        return availabilityPercentangeMetrics;
    }


    private void printUpMetrics(){
        for(int i=0; i<prefixesOfServices.length; i++){
            Map<String, Double> map = availabilityPercentangeMetrics.get(prefixesOfServices[i]+"_service_availabilityPercentage");
            if(map != null)
                for (Map.Entry e : map.entrySet()) {
                    System.out.println("Up values, having metric name : " + prefixesOfServices[i]+"_service_availabilityPercentage" + " me times " +
                            " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
                }

        }

    }

    private void printDownMetrics(){
        for(int i=0; i<prefixesOfServices.length; i++){
            Map<String, Double> map = downTimePercentageMetrics.get(prefixesOfServices[i]+"_service_downPercentage");
            if(map != null)
                for (Map.Entry e : map.entrySet()) {
                    System.out.println("Down values, having metric name : " + prefixesOfServices[i]+"_service_downPercentage" + " me times " +
                            " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
                }

        }
    }

}
