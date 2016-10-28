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
import java.net.URL;
import java.util.*;

import static org.apache.http.protocol.HTTP.USER_AGENT;


public class PrometheusResponseTimesParserQuery {



    public Map<String, Map<Double, Long>> sumMetricsNotToBeInserted = new HashMap<String, Map<Double, Long>>();
    public Map<String, Map<Double, Long>> rawMetricsToBeInsert = new HashMap<String, Map<Double, Long>>();
    public Map<String, Map<Double, Long>> compositeMetricsToBeInsert = new HashMap<String, Map<Double, Long>>();
    public Map<String, Map<Double, Long>> maxResponseTimeMetricsToBeInsert = new HashMap<String, Map<Double, Long>>();

    public static final String rawMetricNames[] = {"userRequestCompletionTime_latency_milliseconds", "userProcess_latency_milliseconds",
            "userDelay_latency_milliseconds", "userAnswerDelay_latency_milliseconds", "userNetworkLatency_latency_milliseconds",

            "permAdminRequestCompletionTime_latency_milliseconds", "permAdminProcess_latency_milliseconds",
            "permAdminDelay_latency_milliseconds", "permAdminAnswerDelay_latency_milliseconds", "permAdminNetworkLatency_latency_milliseconds",

            "objectRequestCompletionTime_latency_milliseconds", "objectProcess_latency_milliseconds",
            "objectDelay_latency_milliseconds", "objectAnswerDelay_latency_milliseconds", "objectNetworkLatency_latency_milliseconds",

            "ouUserRequestCompletionTime_latency_milliseconds", "ouUserProcess_latency_milliseconds",
            "ouUserDelay_latency_milliseconds", "ouUserAnswerDelay_latency_milliseconds", "ouUserNetworkLatency_latency_milliseconds",

            "roleRequestCompletionTime_latency_milliseconds", "roleProcess_latency_milliseconds",
            "roleDelay_latency_milliseconds", "roleAnswerDelay_latency_milliseconds", "roleNetworkLatency_latency_milliseconds",

            "userDetailRequestCompletionTime_latency_milliseconds", "userDetailProcess_latency_milliseconds",
            "userDetailDelay_latency_milliseconds", "userDetailAnswerDelay_latency_milliseconds", "userDetailNetworkLatency_latency_milliseconds",

            "permRequestCompletionTime_latency_milliseconds", "permProcess_latency_milliseconds",
            "permDelay_latency_milliseconds", "permAnswerDelay_latency_milliseconds", "permNetworkLatency_latency_milliseconds",

            "objectAdminRequestCompletionTime_latency_milliseconds", "objectAdminProcess_latency_milliseconds",
            "objectAdminDelay_latency_milliseconds", "objectAdminAnswerDelay_latency_milliseconds", "objectAdminNetworkLatency_latency_milliseconds",

            "roleAdminRequestCompletionTime_latency_milliseconds", "roleAdminProcess_latency_milliseconds",
            "roleAdminDelay_latency_milliseconds", "roleAdminAnswerDelay_latency_milliseconds", "roleAdminNetworkLatency_latency_milliseconds",

            "ouPermRequestCompletionTime_latency_milliseconds", "ouPermProcess_latency_milliseconds",
            "ouPermDelay_latency_milliseconds", "ouPermAnswerDelay_latency_milliseconds", "ouPermNetworkLatency_latency_milliseconds",

            "sdDynamicRequestCompletionTime_latency_milliseconds", "sdDynamicProcess_latency_milliseconds",
            "sdDynamicDelay_latency_milliseconds", "sdDynamicAnswerDelay_latency_milliseconds", "sdDynamicNetworkLatency_latency_milliseconds",

            "sdStaticRequestCompletionTime_latency_milliseconds", "sdStaticProcess_latency_milliseconds",
            "sdStaticDelay_latency_milliseconds", "sdStaticAnswerDelay_latency_milliseconds", "sdStaticNetworkLatency_latency_milliseconds"
    };

    public static final String compositeMetricNames[] = {"userExecutionTime_latency_milliseconds", "userRawResponseTime_latency_milliseconds",
            "permAdminExecutionTime_latency_milliseconds", "permAdminRawResponseTime_latency_milliseconds",
            "objectExecutionTime_latency_milliseconds", "objectRawResponseTime_latency_milliseconds",
            "ouUserExecutionTime_latency_milliseconds", "ouUserRawResponseTime_latency_milliseconds",
            "roleExecutionTime_latency_milliseconds", "roleRawResponseTime_latency_milliseconds",
            "userDetailExecutionTime_latency_milliseconds", "userDetailRawResponseTime_latency_milliseconds",
            "permExecutionTime_latency_milliseconds", "permRawResponseTime_latency_milliseconds",
            "objectAdminExecutionTime_latency_milliseconds", "objectAdminRawResponseTime_latency_milliseconds",
            "roleAdminExecutionTime_latency_milliseconds", "roleAdminRawResponseTime_latency_milliseconds",
            "ouPermExecutionTime_latency_milliseconds", "ouPermRawResponseTime_latency_milliseconds",
            "sdDynamicExecutionTime_latency_milliseconds", "sdDynamicRawResponseTime_latency_milliseconds",
            "sdStaticExecutionTime_latency_milliseconds", "sdStaticRawResponseTime_latency_milliseconds",
    };


    public static final String prefixesOfServices [] = {"user", "permAdmin", "object", "ouUser", "role", "userDetail", "perm", "objectAdmin", "roleAdmin"
    , "ouPerm", "sdDynamic", "sdStatic"};

    public static void main(String[] args) throws Exception {


        PrometheusResponseTimesParserQuery http = new PrometheusResponseTimesParserQuery();

        System.out.println("Send Prometheus Http GET request");
        //simple GET of Request Completion Time
        http.parser();
    }

    public void parser() throws IOException, JSONException {
        //String urlForGettingMetrics = "http://localhost:8088/api/v1/datapoints/query?query={\"metrics\":[{\"tags\":{},\"name\":\"exampleMetric39\"}],\"cache_time\":0,\"start_absolute\":18000000,\"time_zone\":\"GMT\"}";
        //String url = "http://localhost:9090/api/v1/query?query=userRequestCompletionTime_latency_milliseconds_sum[5m]";
        //String rangeUrl = "http://localhost:9090/api/v1/query_range?query=userRequestCompletionTime_latency_milliseconds_sum&start=2016-08-08T00:00:00.781Z&end=2016-08-08T23:59:59.781Z&step=20m";


        // ------------- Apo ayto edw to meros prepei kathe fora na peroyne ta metrics  apo to prometheus-----------------------------------------
        for (int j = 0; j <rawMetricNames.length; j++) {
            String url = "http://localhost:9090/api/v1/query?query=" + rawMetricNames[j] + "_sum[5m]";
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

            // DataPoint 1
            // to array ayto exei mesa sto value to timestamp sto 0 kai sto 1 exei to value
            // Kathe 5 lepta tha ginete to parakatw iteration gia to ekastote query
            if (!jsonObj.getJSONObject("data").getJSONArray("result").isNull(0)) {
                int lengthRequestCompletionTime = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("values").length();
                for (int i = 0; i < lengthRequestCompletionTime; i++) {
                    JSONArray name = jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("values").getJSONArray(i);
//            System.out.println("TO name einai iso me  : " + name.toString() + " kai length iso me : " + name.length());
                    double unixTime = (Double) name.get(0);
                    long lunixTime = (long) unixTime;
//            System.out.println("To unix time einai iso me : " + lunixTime);
                    String stringMilliseconds = (String) name.get(1);
                    double milliseconds;
//                    try{
                    milliseconds = Double.parseDouble(stringMilliseconds);//}
//                    catch (NumberFormatException ex){
//                        double dmilliseconds = Double.parseDouble(stringMilliseconds);
//                        milliseconds = (long) Math.round(dmilliseconds);
//                        System.out.println("Rounded milliseconds are " + milliseconds );
//                    }
//            System.out.printf("kai ta milliseconds einai isa me : " + milliseconds);
                    if (!(sumMetricsNotToBeInserted.containsKey(rawMetricNames[j]))) {
                        Map<Double, Long> valueOfMetric = new HashMap<Double, Long>();
                        valueOfMetric.put(milliseconds, lunixTime);
                        sumMetricsNotToBeInserted.put(rawMetricNames[j], valueOfMetric);
                    } else {
                        if (!(sumMetricsNotToBeInserted.get(rawMetricNames[j]).containsKey(milliseconds))) {
                            Map<Double, Long> mapToUpdate = sumMetricsNotToBeInserted.get(rawMetricNames[j]);
                            mapToUpdate.put(milliseconds, lunixTime);
                            sumMetricsNotToBeInserted.put(rawMetricNames[j], mapToUpdate);
//                        System.out.println("Evala tis times");
                        }
                    }
                }

            }
        }
//        printNotToBeInserteMetricsWithValues();
        rawMetricsToBeInserted();
//        printRawMetricsToBeInsertedWithValues();
        compositeMetricsToBeInserted();
//        printCompositeMetricsToBeInsertedWithValues();
        putMaximumResponseTimes();
//        printMaximumResponseTimes();
    }

    public Map<String, Map<Double, Long>> getRawMetricsToBeInsert() {
        return rawMetricsToBeInsert;
    }

    public Map<String, Map<Double, Long>> getCompositeMetricsToBeInsert() {
        return compositeMetricsToBeInsert;
    }

    public Map<String, Map<Double, Long>> getMaxResponseTimeMetricsToBeInsert() {
        return maxResponseTimeMetricsToBeInsert;
    }

    private void putMaximumResponseTimes() {

        Map<Double,Long> tmp;
        Map.Entry<Double,Long> maxEntry;
        Map<Double,Long> maxToSet;
        for(int i=0; i<prefixesOfServices.length; i++){
            tmp = compositeMetricsToBeInsert.get(prefixesOfServices[i]+"RawResponseTime_latency_milliseconds");
            maxEntry = null;
            maxToSet = new HashMap<Double, Long>();
            if(tmp!=null){
                for(Map.Entry<Double,Long> entry : tmp.entrySet())
                {
                    if( maxEntry == null || entry.getKey() > maxEntry.getKey()){
                    maxEntry = entry;
                    maxToSet.clear();
                    maxToSet.put(entry.getKey(), entry.getValue());
                    }
                }
                if (!maxToSet.containsKey(0.0)){
                    maxResponseTimeMetricsToBeInsert.put(prefixesOfServices[i]+"RawResponseTime_latency_milliseconds_max",maxToSet);
                }
            }
        }
    }

    private void printMaximumResponseTimes(){

        for(int i=0; i<prefixesOfServices.length; i++){
            Map<Double, Long> map = maxResponseTimeMetricsToBeInsert.get(prefixesOfServices[i]+"RawResponseTime_latency_milliseconds");
            if(map != null)
                for (Map.Entry e : map.entrySet()) {
                    System.out.println("Max values, having metric name : " + prefixesOfServices[i]+"RawResponseTime_latency_milliseconds me times " +
                            " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
                }

        }

    }

    private void printCompositeMetricsToBeInsertedWithValues() {
        for(int i=0; i<compositeMetricNames.length; i++){
            Map<Double, Long> map = compositeMetricsToBeInsert.get(compositeMetricNames[i]);
            if(map != null)
            for (Map.Entry e : map.entrySet()) {
                System.out.println("Composite values, having metric name : " + compositeMetricNames[i] + " me times " +
                        " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
            }
        }
    }

    public void printNotToBeInserteMetricsWithValues() {
        for(int i=0; i<sumMetricsNotToBeInserted.size();i++) {
            Map<Double, Long> map = sumMetricsNotToBeInserted.get(rawMetricNames[i]);
            if(map != null)
            for (Map.Entry e : map.entrySet()) {
                System.out.println("Unsorted values, having metric name : " + rawMetricNames[i] + " me times " +
                        " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
            }
        }
    }

    public void printRawMetricsToBeInsertedWithValues() {
        for(int i=0; i<rawMetricsToBeInsert.size();i++) {
            Map<Double, Long> map = rawMetricsToBeInsert.get(rawMetricNames[i]);
            if(map != null)
            for (Map.Entry e : map.entrySet()) {
                System.out.println("Raw values, having metric name : " + rawMetricNames[i] + " me times " +
                        " milli: " + e.getKey().toString() + " kai linuxTime: " + e.getValue().toString());
            }
        }
    }

    public void rawMetricsToBeInserted() {

        //flag in order to put the first the metric as it is
        int k = 0;
        double milliBefore = 0;
        double milliCurrent;
        long linuxTimeCurrent;
        double rawValue;
        for (int i = 0; i < sumMetricsNotToBeInserted.size(); i++) {
            Map<Double, Long> map = sumMetricsNotToBeInserted.get(rawMetricNames[i]);
            if (map != null) {
                System.out.println("Raw metric of " + rawMetricNames[i]);
                SortedMap<Double, Long> sorted = new TreeMap<Double, Long>(map);
                //sorting of the values to be put
                for (Map.Entry e : sorted.entrySet()) {
                    milliCurrent = Double.parseDouble(e.getKey().toString());
                    linuxTimeCurrent = Long.parseLong(e.getValue().toString());
                       System.out.println("Sorted milli equals with : " + milliCurrent + " linuxTime equals with : " + linuxTimeCurrent + " with rawMetricName : " +rawMetricNames[i]);
                    if (k == 0) {
                        Map<Double, Long> toPut = new HashMap<Double, Long>();
                        toPut.put(milliCurrent, linuxTimeCurrent);
                        rawMetricsToBeInsert.put(rawMetricNames[i], toPut);
                        milliBefore = milliCurrent;
                    } else {
                        rawValue = milliCurrent - milliBefore;
                        Map<Double, Long> toPutRaw = rawMetricsToBeInsert.get(rawMetricNames[i]);
                        toPutRaw.put(rawValue, linuxTimeCurrent);
                        rawMetricsToBeInsert.put(rawMetricNames[i], toPutRaw);
                        milliBefore = milliCurrent;
                    }
                    k++;
                }
                k = 0;
            }
        }
    }

    private void compositeMetricsToBeInserted() {

        List<Long> linuxTimes = new ArrayList<Long>();
        Map<Double, Long> map1;
        List<Double> listMap1 = new ArrayList<Double>();
        Map<Double, Long> map2;
        List<Double> listMap2 = new ArrayList<Double>();
        Map<Double, Long> map3;
        List<Double> listMap3 = new ArrayList<Double>();
        Map<Double, Long> map4;
        List<Double> listMap4 = new ArrayList<Double>();
        boolean networkLatencyNull = false;
        long linuxTime =0;
        SortedMap<Double, Long> sorted4 = null;


        for(int j=0; j<prefixesOfServices.length; j++){
        if(rawMetricsToBeInsert.containsKey(prefixesOfServices[j]+"Process_latency_milliseconds") &&
               rawMetricsToBeInsert.containsKey(prefixesOfServices[j]+"Delay_latency_milliseconds") &&
               rawMetricsToBeInsert.containsKey(prefixesOfServices[j]+"AnswerDelay_latency_milliseconds")) {
            map1 = rawMetricsToBeInsert.get(prefixesOfServices[j]+"Process_latency_milliseconds");
            SortedMap<Double, Long> sorted1 = new TreeMap<Double, Long>(map1);
            map2 = rawMetricsToBeInsert.get(prefixesOfServices[j]+"Delay_latency_milliseconds");
            SortedMap<Double, Long> sorted2 = new TreeMap<Double, Long>(map2);
            map3 = rawMetricsToBeInsert.get(prefixesOfServices[j]+"AnswerDelay_latency_milliseconds");
            SortedMap<Double, Long> sorted3 = new TreeMap<Double, Long>(map3);
            map4 = rawMetricsToBeInsert.get(prefixesOfServices[j]+"NetworkLatency_latency_milliseconds");
            if(map4 == null)
                 networkLatencyNull = true;
            else
                sorted4 = new TreeMap<Double, Long>(map4);


            for (Map.Entry<Double, Long> entry : sorted1.entrySet()) {
                listMap1.add(entry.getKey());
                linuxTimes.add(entry.getValue());
            }
            for (Map.Entry<Double, Long> entry : sorted2.entrySet()) {
                listMap2.add(entry.getKey());
            }
            for (Map.Entry<Double, Long> entry : sorted3.entrySet()) {
                listMap3.add(entry.getKey());
            }
            if(!networkLatencyNull)
            for (Map.Entry<Double, Long> entry : sorted4.entrySet()) {
                listMap4.add(entry.getKey());
            }


//            System.out.println("to listmap1 exei size iso me : " + listMap1.size()
//            + " to listmap2 exei size iso me : " + listMap2.size()
//            + " to listmap3 exei size iso me : " + listMap3.size()
//            + " to listmap4 exei size iso me : " + listMap4.size());
            for (int i = 0; i < listMap1.size(); i++) {
                // prwta gia to execution time
//                System.out.println("gia to prefix " + prefixesOfServices[j] + " tha exoyme to parakatw executionTime to opoio einai iso me : " );
                double executionTime = listMap1.get(i) + listMap2.get(i) + listMap3.get(i);
                linuxTime = linuxTimes.get(i);
//                System.out.println(" listMap1 einai iso me : " + listMap1.get(i) + " listMap2 einai iso me : " + listMap2.get(i) + " kai listMap3 einai iso me : " + listMap3.get(i));
//                System.out.println("executionTime to be pushed is : " + executionTime);
                if (!(compositeMetricsToBeInsert.containsKey(prefixesOfServices[j]+"ExecutionTime_latency_milliseconds"))) {
//                   System.out.println("Entered here ");
                    Map<Double, Long> valueOfMetric = new HashMap<Double, Long>();
                    valueOfMetric.put(executionTime, linuxTime);
                    compositeMetricsToBeInsert.put(prefixesOfServices[j]+"ExecutionTime_latency_milliseconds", valueOfMetric);
                } else {
                    if (!(compositeMetricsToBeInsert.get(prefixesOfServices[j]+"ExecutionTime_latency_milliseconds").containsKey(executionTime))) {
//                        System.out.println(" OR Entered here ");
                        Map<Double, Long> mapToUpdate = compositeMetricsToBeInsert.get(prefixesOfServices[j]+"ExecutionTime_latency_milliseconds");
                        mapToUpdate.put(executionTime, linuxTime);
                        compositeMetricsToBeInsert.put(prefixesOfServices[j]+"ExecutionTime_latency_milliseconds", mapToUpdate);
//                        System.out.println("Evala tis times");
                    }
                }
//                System.out.println("gia to networklatency poy einai iso me : " + listMap4.get(i));

                double networkLatency;
                double rawResponseTime;
                System.out.println("For the prefix of " + prefixesOfServices[j]);
                // in case where network latency equals to 0
                if(!networkLatencyNull && listMap4.size() == listMap1.size()){
                    networkLatency = listMap4.get(i);
                    rawResponseTime= executionTime + networkLatency;
                }
                else
                rawResponseTime = executionTime;

                if (!(compositeMetricsToBeInsert.containsKey(prefixesOfServices[j]+"RawResponseTime_latency_milliseconds"))) {
                    Map<Double, Long> valueOfMetric = new HashMap<Double, Long>();
                    valueOfMetric.put(rawResponseTime, linuxTime);
                    compositeMetricsToBeInsert.put(prefixesOfServices[j]+"RawResponseTime_latency_milliseconds", valueOfMetric);
                } else {
                    if (!(compositeMetricsToBeInsert.get(prefixesOfServices[j]+"RawResponseTime_latency_milliseconds").containsKey(rawResponseTime))) {
                        Map<Double, Long> mapToUpdate = compositeMetricsToBeInsert.get(prefixesOfServices[j]+"RawResponseTime_latency_milliseconds");
                        mapToUpdate.put(rawResponseTime, linuxTime);
                        compositeMetricsToBeInsert.put(prefixesOfServices[j]+"RawResponseTime_latency_milliseconds", mapToUpdate);
//                        System.out.println("Evala tis times");
                    }
                }
            }
            // we need to clean the values each time we have a composite metric
            networkLatencyNull = false;
            linuxTime =0;
            listMap1.clear();
            listMap2.clear();
            listMap3.clear();
            listMap4.clear();
            linuxTimes.clear();
        }

       }



    }

}