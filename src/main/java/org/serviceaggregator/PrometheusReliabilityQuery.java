package org.serviceaggregator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.org.apache.xerces.internal.impl.dv.xs.DoubleDV;
import com.sun.org.apache.xpath.internal.SourceTree;
import org.apache.http.io.SessionOutputBuffer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.kairosdb.client.builder.DataPoint;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.util.*;

import static org.apache.http.protocol.HTTP.USER_AGENT;

/**
 * Created by dmetallidis on 8/24/16.
 */
public class PrometheusReliabilityQuery {



    public Map<String, Map<Double, Double>> reliabilityRawPage = new HashMap<String, Map<Double, Double>>();// only the up
    Map<String,Double> mTBF = new HashMap<String, Double>(); // mTBF to be also used in WM

    public Map<String, Double> getTotalUpTimes() {
        return totalUpTimes;
    }

    public Map<String, Double> getTotalDownTimes() {
        return totalDownTimes;
    }

    public Map<String, Double> getnBreakDowns() {
        return nBreakDowns;
    }

    Map<String,Double> totalUpTimes = new HashMap<String, Double>();
    Map<String,Double> totalDownTimes = new HashMap<String, Double>();
    Map<String,Double> nBreakDowns = new HashMap<String, Double>();

    Map<Double,Integer> numberOfResponseMessages = new HashMap<Double, Integer>();
    Map<Double,Integer> numberOfRequestMessages = new HashMap<Double, Integer>();
    Map<Double,Integer> numberOfAcknowledgeMessages = new HashMap<Double, Integer>();

    double mTTF; // prepei na toy dwsw kai thn wra

    public static final String prefixesOfServices [] = {"userPage", "permAdminPage", "objectPage", "ouUserPage", "rolePage", "userDetailPage", "permPage", "objectAdminPage", "roleAdminPage"
            , "ouPermPage", "sdDynamicPage", "sdStaticPage"};

    public static final String prefixesReliabilityOfServices [] = {"reliabilityUserPage", "reliabilityPermAdminPage", "reliabilityObjectPage", "reliabilityOuUserPage", "reliabilityRolePage", "reliabilityUserDetailPage", "reliabilityPermPage", "reliabilityObjectAdminPage", "reliabilityRoleAdminPage"
            , "reliabilityOuPermPage", "reliabilitySdDynamicPage", "reliabilitySdStaticPage"};

    public static final String prefixesOfResponseMessages [] ={"userProcess_latency_milliseconds_count", "permAdminProcess_latency_milliseconds_count",
    "objectProcess_latency_milliseconds_count", "ouUserProcess_latency_milliseconds_count", "roleProcess_latency_milliseconds_count", "userDetailProcess_latency_milliseconds_count", "permProcess_latency_milliseconds_count",
    "objectAdminProcess_latency_milliseconds_count", "roleAdminProcess_latency_milliseconds_count", "ouPermProcess_latency_milliseconds_count",
    "sdDynamicProcess_latency_milliseconds_count", "sdStaticProcess_latency_milliseconds_count"};

    public static void main(String[] args) throws Exception {


        PrometheusReliabilityQuery http = new PrometheusReliabilityQuery();

        System.out.println("Send Prometheus Http GET request");
        //simple GET of Request Completion Time
    //    http.getReliabilityRawValuesOfPages();
//        http.getMtbf();
   //     http.getNumberOfResponseMessages();
     //   http.getNumberOfRequestMessages();
     //   http.getNumberOfAcknowledgedMessages();

    }

    public  Map<String, Map<Double, Double>> executeReliabilityRawValuesOfPages() throws IOException, JSONException {

        String url = null;
        for (int i=0 ; i<prefixesOfServices.length; i++) {
            url = "http://localhost:9090/api/v1/query?query=probe_success{job=\"" + prefixesOfServices[i] + "_service\"}[5m]";

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

            Map<Double, Double> map = new HashMap<Double, Double>();
            if (!jsonObj.getJSONObject("data").getJSONArray("result").isNull(0)) {
                int length = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("values").length();
                JSONArray lastEntry = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("values").getJSONArray(length - 1);
                System.out.println(" Length array equals with : " + lastEntry.length());
                Double timestamp = Double.parseDouble(lastEntry.get(0).toString());
                Double value = Double.parseDouble((String) lastEntry.get(1));
                System.out.println("Length equals with: " + length + " and timestamp with : " + timestamp + " also the value equals with :" + value);
                map.put(timestamp, value);
                reliabilityRawPage.put(prefixesOfServices[i] + "_service", map);
            }
        }
        printReliabilityMetrics();
        return  reliabilityRawPage;
    }


    private void printReliabilityMetrics(){
        for(int i=0; i<prefixesOfServices.length; i++){
            Map<Double, Double> map = reliabilityRawPage.get(prefixesOfServices[i]+"_service");
            if(map != null)
                for (Map.Entry e : map.entrySet()) {
                    System.out.println("Raw values, having metric name : " + prefixesOfServices[i]+"_service" + " with values " +
                            " linuxTime: " + e.getKey().toString() + " and value: " + e.getValue().toString());
                }

        }

    }

    // prepei na trextei meta thn prwth methodo toy put
    public Map<String,Double> getMtbf(KairosDbClient kairosClient, KairosDbClient centralKairosClient) throws Exception {

        // first update the rawValuesToCalculate the mtbf
        executeReliabilityRawValuesOfPages();
        // at first take all the reliabilityMetrics for the according service
        int numberOfFailures=0;
        double totalUPTime=0;
        double totalDownTime = 0;
        double mtbf=0;



        for(int j =0 ; j<prefixesReliabilityOfServices.length; j++) {

         //   String exampleMetricUp = "reliabilityUserPage_service3";
            String realiabilityMetric = prefixesReliabilityOfServices[j]+"_service";
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(realiabilityMetric, new Date(0), null);
            System.out.println("DataPoints Size equals with : "  + listDapoint.size() + " name of metric equals with : " + prefixesReliabilityOfServices[j]+"_service") ;
            if(listDapoint.size() >0) {
                System.out.println("Size of it equals to : " + listDapoint.size());
                // here we take the values that we just get
                Map<Double, Double> mapOfMetric = reliabilityRawPage.get(prefixesOfServices[j] + "_service");

                for (int i = 0; i < listDapoint.size(); i++) {
                    Long timestamp = listDapoint.get(i).getTimestamp();
                    Double dtimestamp = Double.parseDouble(timestamp.toString());
                    Object value = listDapoint.get(i).getValue();
                    Double dvalue = Double.parseDouble(value.toString());
                    mapOfMetric.put(dtimestamp, dvalue);
                }

                SortedMap<Double, Double> sorted = new TreeMap<Double, Double>(mapOfMetric);
                List<Double> keyList = new ArrayList<Double>(sorted.keySet());


                Double upLinuxTime;
                Double downLinuxTime;
                System.out.println("For the service equals to : " + prefixesOfServices[j]);
                // calculations for the mtbf
                for (Map.Entry e : sorted.entrySet()) {
                    downLinuxTime = Double.parseDouble(e.getKey().toString());
                    Double downState = Double.parseDouble(e.getValue().toString());
                    System.out.println("Linux time equals to : " + downLinuxTime + " and state equals with : " + downState);
                    if (downState.equals(0.0)) {
                        int idx = keyList.indexOf(downLinuxTime);
                        numberOfFailures++;
                        for (int i = idx; i >= 0; i--) {
                            if (sorted.get(keyList.get(i)).equals(1.0)) {
                                upLinuxTime = keyList.get(i);
                                double diff = downLinuxTime - upLinuxTime;
                                double diffHours = diff / (60 * 60 * 1000) % 24;
                                totalUPTime = totalUPTime + diffHours;
                                System.out.println("The downloadTime equals with : " + downLinuxTime + " the previous LinuxTime equals with: " + upLinuxTime + " and the difference in hours are: " + diffHours);
                                break;
                            }
                        }
                    }
                }

                // In order to calculate the downTime and pass it for the WM layer
                for (Map.Entry e : sorted.entrySet()) {
                    downLinuxTime = Double.parseDouble(e.getKey().toString());
                    Double downState = Double.parseDouble(e.getValue().toString());
                    if (downState.equals(1.0)) {
                        int idx = keyList.indexOf(downLinuxTime);
                        for (int i = idx; i >= 0; i--) {
                            if (sorted.get(keyList.get(i)).equals(0.0)) {
                                upLinuxTime = keyList.get(i);
                                double diff = upLinuxTime - downLinuxTime;
                                double diffHours = diff / (60 * 60 * 1000) % 24;
                                totalDownTime = totalDownTime + diffHours;
                                break;
                            }
                        }
                    }
                }

                // if there were any failures push them on to kairosDB

//                if (numberOfFailures > 0){
                if (numberOfFailures > 0)
                    mtbf = totalUPTime / numberOfFailures;
                else
                    mtbf = 0;
                mTBF.put(prefixesReliabilityOfServices[j] + "_mtbf", mtbf);
                //in case total downTime equals below than zero
                if (totalUPTime < 0)
                    totalUPTime = totalUPTime * -1;
                if (totalDownTime < 0)
                    totalDownTime = totalDownTime * -1;

                String stotalUpTime = prefixesReliabilityOfServices[j] + "totalUpTime";
                String stotalDownTime = prefixesReliabilityOfServices[j] + "totalDownTime";
                String sBreakDowns = prefixesReliabilityOfServices[j] + "nBreakDowns";

                totalUpTimes.put(stotalUpTime, totalUPTime);
                totalDownTimes.put(stotalDownTime, totalDownTime);
                nBreakDowns.put(sBreakDowns, (double) numberOfFailures);
                numberOfFailures = 0;
                totalUPTime = 0.0;
                totalDownTime = 0.0;

//            }
                // the below put is reduntant
                //reliabilityRawPage.put(prefixesOfServices[j] + "_service_mtbf", mapOfMetric);
            }
        }

        printMtbf();
        return mTBF;
    }



    private void printMtbf() {
        for (Map.Entry e : mTBF.entrySet()) {
            System.out.println("MTBF values, having metric name : " + e.getKey() +" with values"  + e.getValue().toString());
        }
    }

    public double getMttf(KairosDbClient kairosClient) throws Exception {

        // first we should run mtbf and then  mttf!!!
        // at first take all the reliabilityMetrics for the according service
        double totalTime=0;

        for(int j =0 ; j<prefixesReliabilityOfServices.length; j++) {

            //   String exampleMetricUp = "reliabilityUserPage_service3";
            List<DataPoint> listDapoint = kairosClient.QueryDataPointsAbsolute(prefixesReliabilityOfServices[j]+"_service", new Date(0), null);
            if(listDapoint.size() >0) {
                System.out.println("The size equals with: " + listDapoint.size());
                // edw pernoyme ayta poy vrikame twra
                Map<Double, Double> mapOfMetric = reliabilityRawPage.get(prefixesOfServices[j] + "_service");

                for (int i = 0; i < listDapoint.size(); i++) {
                    Long timestamp = listDapoint.get(i).getTimestamp();
                    Double dtimestamp = Double.parseDouble(timestamp.toString());
                    Object value = listDapoint.get(i).getValue();
                    Double dvalue = Double.parseDouble(value.toString());
                    mapOfMetric.put(dtimestamp, dvalue);
                }

                SortedMap<Double, Double> sorted = new TreeMap<Double, Double>(mapOfMetric);
                List<Double> keyList = new ArrayList<Double>(sorted.keySet());


                Double upLinuxTime;
                Double downLinuxTime;
                System.out.println("for the service equals with : " + prefixesOfServices[j]);
                for (Map.Entry e : sorted.entrySet()) {
                    downLinuxTime = Double.parseDouble(e.getKey().toString());
                    Double downState = Double.parseDouble(e.getValue().toString());
//                    System.out.println("Linux time  : " + downLinuxTime + " and the state equals with : " + downState);
                    if (downState.equals(0.0)) {
                        int idx = keyList.indexOf(downLinuxTime);
                        for (int i = idx; i >= 0; i--) {
                            if (sorted.get(keyList.get(i)).equals(1.0)) {
                                upLinuxTime = keyList.get(i);
                                double diff = downLinuxTime - upLinuxTime;
                                double diffHours = diff / (60 * 60 * 1000) % 24;
                                totalTime = totalTime + diffHours;
//                                System.out.println("The downLinuxTime equals with: " + downLinuxTime + " the previous LinuxTime equals with: " + upLinuxTime + " and the difference in hours equals with: " + diffHours);
                                break;
                            }
                        }
                    }
                }
            }
        }

        mTTF = totalTime / 12;
        System.out.println("MTTF equals to: " + mTTF + " hours/service ");
        return mTTF;
    }


    public Map<Double,Integer> getNumberOfResponseMessages() throws IOException, JSONException {

        String url = null;
        int responseMessages;
        int dnumberOfResponseMessages = 0;
        for (int i = 0; i < prefixesOfServices.length; i++) {
            url = "http://localhost:9090/api/v1/query?query=" + prefixesOfResponseMessages[i];

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
//                System.out.println("to length toy last entry einai iso me : " + lastEntry.length());
//                System.out.println(lastEntry.get(0).toString());
//                System.out.println(lastEntry.get(1).toString());
                responseMessages = Integer.parseInt(lastEntry.get(1).toString());
                dnumberOfResponseMessages = dnumberOfResponseMessages + responseMessages;
            }

        }
        // pushing the current time to the metric
        Date date = new Date();
        numberOfResponseMessages.put((double)date.getTime(), dnumberOfResponseMessages);
        printNumberOfResponseMessages();
        return numberOfResponseMessages;
    }

    public void printNumberOfResponseMessages() throws IOException, JSONException {
        for (Map.Entry e : numberOfResponseMessages.entrySet()) {
            System.out.println("NumberOfResponseMessages values, having date : " + e.getKey() + " with values: " + e.getValue().toString());
        }
    }

      public Map<Double,Integer> getNumberOfRequestMessages() throws IOException, JSONException {

            String url = null;
            double linuxTime = 0;
            int requestMessages = 0;
            url = "http://localhost:9090/api/v1/query?query=request_messages_total" ;

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
              requestMessages = Integer.parseInt(lastEntry.get(1).toString());
              numberOfRequestMessages.put(linuxTime, requestMessages);
          }

          printNumberOfRequestMessages();
          return numberOfRequestMessages;

        }

    public void printNumberOfRequestMessages() throws IOException, JSONException {
        for (Map.Entry e : numberOfRequestMessages.entrySet()) {
            System.out.println("NumberOfRequestMessages values, having date : " + e.getKey() + " and value :" + e.getValue().toString());
        }

    }

        public Map<Double,Integer> getNumberOfAcknowledgedMessages() throws IOException, JSONException {

            String url = null;
            double linuxTime = new Date().getTime();
            int requestMessages = 0;
            int errorMessages= 0;
            int acknowledged = 0;
            Map<Double,Integer> numberOfRequestMessagesInternal = new HashMap<Double, Integer>();
            numberOfRequestMessagesInternal = getNumberOfRequestMessages();

            url = "http://localhost:9090/api/v1/query?query=error_request_total" ;

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
                errorMessages = Integer.parseInt(lastEntry.get(1).toString());
                for (Map.Entry e : numberOfRequestMessagesInternal.entrySet()) {
                   requestMessages = Integer.parseInt(e.getValue().toString());
                }
                // to calculate the acknoledged messages we remove the error one from requestMessages
                acknowledged = requestMessages - errorMessages;
                numberOfAcknowledgeMessages.put(linuxTime, acknowledged);
            }

          printNumberOfAcknowledgedMessages();
            return numberOfAcknowledgeMessages;

        }


    public void printNumberOfAcknowledgedMessages() throws IOException, JSONException {
        for (Map.Entry e : numberOfAcknowledgeMessages.entrySet()) {
            System.out.println("numberOfAcknowledgeMessages values, having date : " + e.getKey() + " and value " + e.getValue().toString());
        }
    }


}
