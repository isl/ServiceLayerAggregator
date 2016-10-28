package org.serviceaggregator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.kairosdb.client.builder.DataPoint;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ServiceAggregator {

    private final String USER_AGENT = "Mozilla/45.3.0";
    PrometheusResponseTimesParserQuery responseTimeMetric = new PrometheusResponseTimesParserQuery();
    PrometheusThroughputQuery throughputMetrics = new PrometheusThroughputQuery();
    PrometheusAvailabilityQuery availabilityMetrics = new PrometheusAvailabilityQuery();
    PrometheusReliabilityQuery reliabilityMetrics = new PrometheusReliabilityQuery();
    PrometheusSecurityQuery securityMetrics = new PrometheusSecurityQuery();

    //Metrics to be inserted
    public Map<String, Map<Double, Long>> rawResponseTimeMetricsToBeInsert; // milli,linuxTime
    public Map<String, Map<Double, Long>> compositeResponseTimeMetricsToBeInsert;
    public Map<String, Map<Double, Long>> maxResponseTimes;
    public Map<String, Map<Double, Long>> averageRequestNumber;
    public Map<String, Map<Double, Long>> maximumThroughputNumber;
    public Map<String, Map<String, Double>> availabilityPercentangeMetrics ; // percentage, linuxTime
    public Map<String, Map<String, Double>> downTimePercentageMetrics;
    public Map<String, Double> mTBF;
    public Map<Double, Integer> numberOfResponseMessages;
    public Map<Double, Integer> numberOfRequestMessages;
    public Map<Double, Integer> numberOfAcknowledgedMessages;
    public Map<Double,Integer> numberOfFailedAuthentications;
    public Map<Double,Integer> numberOfTotalAttemptsOfAuthentication;

    public double mTTF;

    public  int k =-1;
    public static void main(String[] args) throws Exception {


       ServiceAggregator http = new ServiceAggregator();


       KairosDbClient kairosDbClient = new KairosDbClient("http://localhost:8088/");
       KairosDbClient centralKairosDbClient = new KairosDbClient("http://147.52.82.63:8088/");

       // initialize with the already pushed metrics
       kairosDbClient = kairosDbClient.initializeFullBuilder(kairosDbClient);
       centralKairosDbClient = centralKairosDbClient.initializeFullBuilder(centralKairosDbClient);


        while(true){
        System.out.println("Send Prometheus Http GET request");
        http.retrievingMetricsLocalhost(kairosDbClient, centralKairosDbClient);
        Thread.sleep(10000);
        System.out.printf("Sending them to central TSDB");
        http.retrievingMetricsCentral(centralKairosDbClient);
        // take the values every one minute
        Thread.sleep(60000);
        }

    }

    private void deleteMetrics() throws IOException {

        KairosDbClient kairosClient = new KairosDbClient("http://localhost:8088");
        kairosClient.deleteMetrics();

    }

    // EXAMPLE OF HTTP GET request
    private void sendGet() throws Exception {

        String urlForGettingMetrics = "http://localhost:8088/api/v1/datapoints/query?query={\"metrics\":[{\"tags\":{},\"name\":\"exampleMetric39\"}],\"cache_time\":0,\"start_absolute\":18000000,\"time_zone\":\"GMT\"}";
        String url = "http://localhost:9090/api/v1/query?query=userRequestCompletionTime_latency_milliseconds_sum";

        // a range value http query is the below
        //String rangeUrl = "http://localhost:9090/api/v1/query_range?query=userRequestCompletionTime_latency_milliseconds_sum&start=2016-08-08T00:00:00.781Z&end=2016-08-08T23:59:59.781Z&step=20m";

        // ------------- Get the metrics from Prometheus ------------------------
        //gettingOfAggregatedMetrics
        //gettingOfMetrics();
        // TA MTTF kai MTBF tha ta paroyme afou valoyme tis paranw times sthn vash
        // calculation of MTBF and MTTF will be done after the insertion of availabilitiMetrics
        // : DEFINITION OF A variableThatHas the Ups and Downs of each of the pages and is going to be put on kairosDB

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
        System.out.println(json);


        // ----------------------------------- TO OPOIO FTANEI MEXRI EDW ---------------------------------------------------------------------------

        //example putting of downs and ups
       // insertionOfDatesUpsAndDowns();

    }

    // This is used for
    // (a) responseTime metrics, (b) throughput metrics,
    private void insertToDbDoubleLong(Map<String, Map<Double, Long>> metricsToInsert, KairosDbClient client) throws Exception {

        Map<Double, Long> metricsMap;
        String metricName;

        for (Map.Entry e : metricsToInsert.entrySet()) {
            metricName = e.getKey().toString();
            metricsMap = (Map) e.getValue();
            for (Map.Entry p : metricsMap.entrySet()) {
                //  dvalue = (Double) p.getValue();
                Date date = toLinuxDate((Long) p.getValue());
                System.out.println("The metric name to be put is : " + metricName);
                metricName.replace("_milliseconds","");
                client.putAlreadyInstantiateMetric(metricName, date.getTime(), p.getKey());
            }
        }
    }

    // This is used for
    // (a) availability metrics
    private void insertToDbStringDouble(Map<String, Map<String, Double>> metricsToInsert, KairosDbClient client) throws Exception {
        Map<Double, Long> metricsMap;
        String metricName;
        Long lvalue;
        double dvalue;

        for (Map.Entry e : metricsToInsert.entrySet()) {
            metricName = e.getKey().toString();
            metricsMap = (Map) e.getValue();
            for (Map.Entry p : metricsMap.entrySet()) {
//                dvalue = (Double) p.getValue();
                try{
                Date date = toLinuxDate((Long.parseLong(p.getValue().toString())));
                client.putAlreadyInstantiateMetric(metricName, date.getTime(), p.getKey());}
                catch (java.lang.NumberFormatException e1){
                    System.out.println("NumberFormatExecption Continue with the rest metrics");
                    continue;
                }
            }
        }

    }

    // This is used for
    // (a) mtbf metrics
    private void insertToDbStringDoubleMtbf(Map<String, Double> metricsToInsert, KairosDbClient kairosClient) throws Exception {

        String metricName;
        for (Map.Entry e : metricsToInsert.entrySet()) {
            metricName = e.getKey().toString();
            kairosClient.putAlreadyInstantiateMetric(metricName, new Date().getTime(), e.getValue());
        }
    }


    // This is used for
    // (a) mttf metric
    private void insertToDbMttf(double mTTF, KairosDbClient kairosClient ) throws Exception {
        kairosClient.putAlreadyInstantiateMetric("mttf", (Long) new Date().getTime(), mTTF);
    }

    // This is used for
    // (a) responseMessages (b) requestMessages (c) acknowledgedMessages (d) failedAuthentications (e) totalAttemptsOfAuthentication
    private void insertToDbDoubleInteger(Map<Double, Integer> metricsToInsert, String metricName, KairosDbClient kairosClient ) throws Exception {

        kairosClient.deleteMetrics();
        double linuxTime ;
        long lLinuxTime;

        for (Map.Entry e : metricsToInsert.entrySet()) {
            linuxTime = (Double) e.getKey();
            lLinuxTime = (long) linuxTime;
            Date date = toLinuxDate(lLinuxTime);
            kairosClient.putAlreadyInstantiateMetric(metricName, date.getTime(), e.getValue());
        }
    }



    private void insertionOfDatesUpsAndDowns( KairosDbClient kairosClient) throws Exception {


        String exampleMetricUp = "reliabilityUserPage_service";

       // String exampleMetricDown = "userPage_Down12";
        Date Date1;
        Date Date2;
        Date Date3;
        Date Date4;
        Date Date5 = null;
        Date Date6 = null;
        Date Date7 =null;

        if(k == 0){
        Date1 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(9));
        Date2 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(8));
        Date3 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4));
        Date4 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));}
        else
        {
            Date1 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5));
            Date2 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3));
            Date3 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
            Date4 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
            Date5 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
            Date6 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
            Date7 = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));


        }
        Object up = 1;
        Object down = 0;

        if ( k== 0){

                kairosClient.putAlreadyInstantiateMetric(exampleMetricUp,  Date1.getTime(), up);

                //metric exists
                kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date2.getTime(), down);
                Thread.sleep(1000);

                kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date3.getTime(), up);
                Thread.sleep(1000);

                kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date4.getTime(), down);}

        else{
            kairosClient.putAlreadyInstantiateMetric(exampleMetricUp,  Date1.getTime(), up);

            //metric exists
            kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date2.getTime(), down);
            Thread.sleep(1000);

            kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date3.getTime(), up);
            Thread.sleep(1000);

            kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date4.getTime(), down);

            Thread.sleep(1000);

            kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date5.getTime(), down);
            Thread.sleep(1000);

            kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date6.getTime(), up);
            Thread.sleep(1000);

            kairosClient.putAlreadyInstantiateMetric(exampleMetricUp, Date7.getTime(), down);

        }


    }


    // pushing the metrics to the central TSDB
    private void retrievingMetricsCentral(KairosDbClient client) throws Exception{


        insertToDbDoubleLong(rawResponseTimeMetricsToBeInsert, client);
        insertToDbDoubleLong(compositeResponseTimeMetricsToBeInsert, client);
        insertToDbDoubleLong(maxResponseTimes, client);
        insertToDbDoubleLong(maximumThroughputNumber, client);
        insertToDbDoubleLong(averageRequestNumber,client);
        insertToDbStringDouble(availabilityPercentangeMetrics, client);
        insertToDbStringDouble(downTimePercentageMetrics, client);
        insertToDbStringDoubleMtbf(mTBF, client);
        insertToDbStringDoubleMtbf(reliabilityMetrics.getTotalUpTimes(), client);
        insertToDbStringDoubleMtbf(reliabilityMetrics.getTotalDownTimes(), client);
        insertToDbStringDoubleMtbf(reliabilityMetrics.getnBreakDowns(), client);
        insertToDbMttf(mTTF, client);
        insertToDbDoubleInteger(numberOfResponseMessages, "responseMessages", client);
        insertToDbDoubleInteger(numberOfRequestMessages, "requestMessages", client);

        insertToDbDoubleInteger(numberOfAcknowledgedMessages, "acknowledgedMessages", client);
        insertToDbDoubleInteger(numberOfFailedAuthentications, "failedAuthentications", client);
        insertToDbDoubleInteger(numberOfTotalAttemptsOfAuthentication, "totalAttemptsOfAuthentication", client);



    }


    private void retrievingMetricsLocalhost(KairosDbClient client, KairosDbClient centralClient) throws Exception {

        // BELOW WILL RUN STRICTLY WITH THE SEQUENCE SHOWN!!!

//      here we take the metrics for the responseTimes
        responseTimeMetric.parser();
        rawResponseTimeMetricsToBeInsert = responseTimeMetric.getRawMetricsToBeInsert();
        insertToDbDoubleLong(rawResponseTimeMetricsToBeInsert, client); // OTAN VALW AYTA EDW DEN M VRISKEI TO MTBF META
        compositeResponseTimeMetricsToBeInsert = responseTimeMetric.getCompositeMetricsToBeInsert();
        insertToDbDoubleLong(compositeResponseTimeMetricsToBeInsert, client);
        maxResponseTimes = responseTimeMetric.getMaxResponseTimeMetricsToBeInsert();
        insertToDbDoubleLong(maxResponseTimes, client);


//      here we take the metrics of throughput
        throughputMetrics.parserThroughput("avg");
        throughputMetrics.parserThroughput("max");
        maximumThroughputNumber = throughputMetrics.getMaximumThroughputNumber();
        insertToDbDoubleLong(maximumThroughputNumber, client);
        averageRequestNumber = throughputMetrics.getAverageRequestNumber();
        insertToDbDoubleLong(averageRequestNumber,client);


// here we take the availability metrics
        availabilityMetrics.parserAvailability();
        availabilityPercentangeMetrics = availabilityMetrics.getAvailabilityPercentangeMetrics();
        insertToDbStringDouble(availabilityPercentangeMetrics, client);

        downTimePercentageMetrics = availabilityMetrics.getDownTimePercentageMetrics();
        insertToDbStringDouble(downTimePercentageMetrics, client);


        // EMEINA EDW GIA NA VALW TOYS CLIENTS



// here we take the security metrics
        numberOfFailedAuthentications = securityMetrics.getFailedAuthentications(); // metric with name failedAuthentications
        insertToDbDoubleInteger(numberOfFailedAuthentications, "failedAuthentications", client);

        numberOfTotalAttemptsOfAuthentication = securityMetrics.getTotalAttemptsOfAuthentications(); // metric with name totalAttemptsOfAuthentication
        insertToDbDoubleInteger(numberOfTotalAttemptsOfAuthentication, "totalAttemptsOfAuthentication", client);


        // here we take the reliabilityRawMetrics
        // Example put of some failures in order to check the mtbfs
        k++;
        if(k==0 || k ==2){
            insertionOfDatesUpsAndDowns(client);
            //   insertionOfDatesUpsAndDowns(centralClient);
        }
        mTBF = reliabilityMetrics.getMtbf(client, centralClient); // query first mTBF and then MTTF!!
        insertToDbStringDoubleMtbf(reliabilityMetrics.getTotalUpTimes(), client);
        insertToDbStringDoubleMtbf(reliabilityMetrics.getTotalDownTimes(), client);
        insertToDbStringDoubleMtbf(reliabilityMetrics.getnBreakDowns(), client);
        mTTF = reliabilityMetrics.getMttf(client); // have to give the current hour that has been computed // metric with name : mttf, will be given by here
        insertToDbStringDoubleMtbf(mTBF, client);

        insertToDbMttf(mTTF, client);

        numberOfResponseMessages = reliabilityMetrics.getNumberOfResponseMessages(); // metric with name : responseMessages, will be given by here
        insertToDbDoubleInteger(numberOfResponseMessages, "responseMessages", client);
        numberOfRequestMessages = reliabilityMetrics.getNumberOfRequestMessages(); // metric with name : requestMessages, will be given by here
        insertToDbDoubleInteger(numberOfRequestMessages, "requestMessages", client);
        numberOfAcknowledgedMessages = reliabilityMetrics.getNumberOfAcknowledgedMessages(); // metric with name : acknowledgedMessages, will be given by here
        insertToDbDoubleInteger(numberOfAcknowledgedMessages, "acknowledgedMessages", client);

    }


    private void exampleMetricsToInsert(JSONObject jsonObj) throws JSONException, InterruptedException {


        // DataPoint 1
        double unixTime = (Double) jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("value").get(0);
        long lUnixTime = (long) unixTime;
        Object milli =  jsonObj.getJSONObject("data").getJSONArray("result").getJSONObject(0).getJSONArray("value").get(1);
        System.out.println("Milliseconds are " + milli.toString());

        // DataPoint 2
        Object  milli2 = 25000;
        Thread.sleep(3000);
        // einai hdh etoimo den xreiazete allagh
        long unixTime2 = new Date().getTime();



        // DataPoint 3
        Object  milli3 = 28000;
        Thread.sleep(4000);
        // einai hdh etoimo den xreiazete allagh
        long unixTime3 = new Date().getTime();

    }


    private String toDate(Object unixTime) {
        long unixSeconds = ((Number) unixTime).longValue();
        Date date = new Date(unixSeconds * 1000L);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"); // the format of your date
        sdf.setTimeZone(TimeZone.getTimeZone("GMT")); // give a timezone reference for formating (see comment at the bottom
        String formattedDate = sdf.format(date);
        System.out.println(formattedDate);

        return formattedDate;
    }

    private Date toLinuxDate(long unixTimeTimestamp){
        java.util.Date time= new java.util.Date(unixTimeTimestamp*1000);
        System.out.println("\n Converter of date will return : " + time.toString() + " \n");

        return time;
    }

}
