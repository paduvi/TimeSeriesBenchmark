package io.dogy;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dogy.utility.Util;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.client.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class AnalyticResult {

    private final RestHighLevelClient client;

    public AnalyticResult() {
        Map<String, Integer> hosts = new HashMap<>();
        hosts.put("10.8.7.172", 9200);
        hosts.put("10.8.5.166", 9200);
        hosts.put("10.8.5.206", 9200);

        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        int i = 0;
        for (Map.Entry<String, Integer> entry : hosts.entrySet()) {
            httpHosts[i++] = new HttpHost(entry.getKey(), entry.getValue(), "http");
        }
        RestClientBuilder builder = RestClient.builder(httpHosts);
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setMaxConnPerRoute(10)
                .setMaxConnTotal(100)
                .setKeepAliveStrategy(Util.createKeepAliveStrategy())
        );
        builder.setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(60000)
        );
        this.client = new RestHighLevelClient(builder);
    }

    private void clearScrollContext(String scrollID) throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollID);
        client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
    }

    private String buildSearchRequest(Long minTime, Long maxTime, String type) throws JSONException {
        JSONObject query = new JSONObject();
        query.put("query", new JSONObject().put("bool", new JSONObject()));

        JSONArray filters = new JSONArray();
        if (minTime != null) {
            filters.put(new JSONObject().put("range", new JSONObject().put("timestamp", new JSONObject().put("gt", minTime))));
        }
        if (maxTime != null) {
            filters.put(new JSONObject().put("range", new JSONObject().put("timestamp", new JSONObject().put("lt", maxTime))));
        }
        filters.put(new JSONObject().put("term", new JSONObject().put("step", type)));

        query.getJSONObject("query").getJSONObject("bool").put("filter", filters);

        return query.toString();
    }

    public void export(String fileName, Long minTime, Long maxTime, String type) throws IOException, JSONException {
        final String[] columns = new String[]{"requestID", "duration", "size", "distance"};
        final String scrollContext = "2m";
        final String nodePrefix = "Kinghub_";
        final String index = "notify-monitor-time-logs-*";

        try (FileOutputStream out = new FileOutputStream(new File(fileName + ".xlsx"))) {
            try (Workbook workbook = new XSSFWorkbook()) {
                Sheet sheet = workbook.createSheet(fileName);

                Font headerFont = workbook.createFont();
                headerFont.setBold(true);
                headerFont.setFontHeightInPoints((short) 14);
                headerFont.setColor(IndexedColors.RED.getIndex());

                CellStyle headerCellStyle = workbook.createCellStyle();
                headerCellStyle.setFont(headerFont);

                Row headerRow = sheet.createRow(0);

                for (int i = 0; i < columns.length; i++) {
                    Cell cell = headerRow.createCell(i);
                    cell.setCellValue(columns[i]);
                    cell.setCellStyle(headerCellStyle);
                }
                CellStyle rowStyle = workbook.createCellStyle();
                rowStyle.setVerticalAlignment(VerticalAlignment.TOP);

                AtomicInteger rowNum = new AtomicInteger(1);

                String scrollID = null;
                while (true) {
                    JSONObject query;
                    Request request;
                    if (scrollID == null) {
                        query = new JSONObject(buildSearchRequest(minTime, maxTime, type));
                        request = new Request("GET", "/" + index + "/_search?scroll=" + scrollContext);
                    } else {
                        query = new JSONObject();
                        query.put("scroll", scrollContext);
                        query.put("scroll_id", scrollID);

                        request = new Request("POST", "/_search/scroll");
                    }

                    RestClient restClient = this.client.getLowLevelClient();
                    request.setJsonEntity(query.toString());

                    Response response = restClient.performRequest(request);
                    String jsonString = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    JSONObject data = new JSONObject(jsonString);
                    if (!data.has("hits")) {
                        break;
                    }
                    scrollID = data.getString("_scroll_id");
                    JSONArray listResult = data.getJSONObject("hits").getJSONArray("hits");
                    if (listResult.length() == 0) {
                        break;
                    }

                    for (int i = 0; i < listResult.length(); i++) {
                        JSONObject obj = listResult.getJSONObject(i);
                        final MonitorTimeLog log = Util.OBJECT_MAPPER.readValue(obj.getJSONObject("_source").toString(), MonitorTimeLog.class);
                        if (!log.getNodeName().contains(nodePrefix)) {
                            continue;
                        }

                        //export map to excel
                        Row row = sheet.createRow(rowNum.getAndIncrement());
                        row.createCell(0).setCellValue(log.getRequestID());
                        row.createCell(1).setCellValue(log.getDuration());
                        row.createCell(2).setCellValue(log.getExtension().path("size").asInt());

                        long distance = log.getTimestamp() - log.getExtension().path("requestedTimestamp").asLong();
                        double distanceInHours = distance * 1.0 / Duration.ofHours(1).toMillis();
                        row.createCell(3).setCellValue(String.format("%.2f", distanceInHours));

                        row.setHeight((short) -1);
                        row.setRowStyle(rowStyle);

                        Util.clearCurrentConsoleLine();
                        System.out.print("\r" + rowNum);
                    }
                }
                if (scrollID != null) {
                    clearScrollContext(scrollID);
                }

                // Resize all columns to fit the content size
                for (int i = 0; i < columns.length; i++) {
                    sheet.autoSizeColumn(i);
                }

                System.out.println("\rNumber of rows: " + rowNum.get() + ". Writing to file...");
                workbook.write(out);
                out.flush();

                System.out.println("\nDone write to file " + fileName + ".xlsx\n");
            }
        }
    }

    public static class MonitorTimeLog {

        private String requestID;
        private String userID;
        private String groupID;
        private String step;
        private long duration;
        private String nodeName;
        private long timestamp = System.currentTimeMillis();
        private ObjectNode extension = Util.OBJECT_MAPPER.createObjectNode();

        public MonitorTimeLog() {
        }

        public MonitorTimeLog(String requestID, String userID, String groupID, String step) {
            this.requestID = requestID;
            this.userID = userID;
            this.groupID = groupID;
            this.step = step;
        }

        public void end() {
            this.duration = System.currentTimeMillis() - this.timestamp;
        }

        public String getRequestID() {
            return requestID;
        }

        public void setRequestID(String requestID) {
            this.requestID = requestID;
        }

        public String getUserID() {
            return userID;
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }

        public String getGroupID() {
            return groupID;
        }

        public void setGroupID(String groupID) {
            this.groupID = groupID;
        }

        public String getStep() {
            return step;
        }

        public void setStep(String step) {
            this.step = step;
        }

        public long getDuration() {
            return duration;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }

        public String getNodeName() {
            return nodeName;
        }

        public void setNodeName(String nodeName) {
            this.nodeName = nodeName;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public ObjectNode getExtension() {
            return extension;
        }

        public void setExtension(ObjectNode extension) {
            this.extension = extension;
        }
    }

    public static void main(String[] args) {
        long minTime = 1592031600000L;
        long maxTime = System.currentTimeMillis();

        AnalyticResult analyticResult = new AnalyticResult();
        try {
            analyticResult.export("hbase", minTime, maxTime, "hbase.listRecentChangelogByCheckpoints");
            analyticResult.export("timescale", minTime, maxTime, "timescale.listRecentChangelogByCheckpoints");
        } catch (IOException | JSONException e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

}
