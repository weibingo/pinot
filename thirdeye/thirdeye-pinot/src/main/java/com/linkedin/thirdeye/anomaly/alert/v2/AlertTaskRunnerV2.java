package com.linkedin.thirdeye.anomaly.alert.v2;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskInfo;
import com.linkedin.thirdeye.anomaly.alert.AlertTaskRunner;
import com.linkedin.thirdeye.anomaly.alert.grouping.AlertGroupKey;
import com.linkedin.thirdeye.anomaly.alert.grouping.AlertGrouper;
import com.linkedin.thirdeye.anomaly.alert.grouping.AlertGrouperFactory;
import com.linkedin.thirdeye.anomaly.alert.grouping.GroupedAnomalyResults;
import com.linkedin.thirdeye.anomaly.alert.template.pojo.MetricDimensionReport;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.alert.util.AnomalyReportGenerator;
import com.linkedin.thirdeye.anomaly.alert.util.DataReportHelper;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertTaskRunnerV2 implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AlertTaskRunner.class);
  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");
  public static final String CHARSET = "UTF-8";

  private final MergedAnomalyResultManager anomalyMergedResultDAO;
  private final AlertConfigManager alertConfigDAO;
  private final MetricConfigManager metricConfigManager;

  private AlertConfigDTO alertConfig;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;
  private AlertFilterFactory alertFilterFactory;

  public AlertTaskRunnerV2() {
    anomalyMergedResultDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    alertConfigDAO = DAORegistry.getInstance().getAlertConfigDAO();
    metricConfigManager = DAORegistry.getInstance().getMetricConfigDAO();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext)
      throws Exception {
    List<TaskResult> taskResult = new ArrayList<>();
    AlertTaskInfo alertTaskInfo = (AlertTaskInfo) taskInfo;
    alertConfig = alertTaskInfo.getAlertConfigDTO();
    thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();
    alertFilterFactory = new AlertFilterFactory(thirdeyeConfig.getAlertFilterConfigPath());

    try {
      LOG.info("Begin executing task {}", taskInfo);
      runTask();
    } catch (Exception t) {
      LOG.error("Task failed with exception:", t);
      sendFailureEmail(t);
      // Let task driver mark this task failed
      throw t;
    }
    return taskResult;
  }

  // TODO : separate code path for new vs old alert config !
  private void runTask() throws Exception {
    LOG.info("Starting email report for id : {}, name : {} ", alertConfig.getId(),
        alertConfig.getName());
    sendAnomalyReport();
    sendScheduledDataReport();
    ThirdeyeMetricsUtil.alertTaskSuccessCounter.inc();
  }

  private void sendAnomalyReport() throws Exception {
    AlertConfigBean.EmailConfig emailConfig = alertConfig.getEmailConfig();
    if (emailConfig != null && emailConfig.getFunctionIds() != null) {
      List<Long> functionIds = alertConfig.getEmailConfig().getFunctionIds();
      List<MergedAnomalyResultDTO> mergedAnomaliesAllResults = new ArrayList<>();
      long lastNotifiedAnomaly = emailConfig.getAnomalyWatermark();
      for (Long functionId : functionIds) {
        List<MergedAnomalyResultDTO> resultsForFunction = anomalyMergedResultDAO
            .findUnNotifiedByFunctionIdAndIdGreaterThan(functionId, lastNotifiedAnomaly);
        if (CollectionUtils.isNotEmpty(resultsForFunction)) {
          mergedAnomaliesAllResults.addAll(resultsForFunction);
        }
        // fetch anomalies having id lesser than the watermark for the same function with notified = false & endTime > last one day
        // these anomalies are the ones that did not qualify filtration rule and got modified.
        // We should add these again so that these could be included in email if qualified through filtration rule
        List<MergedAnomalyResultDTO> filteredAnomalies = anomalyMergedResultDAO
            .findUnNotifiedByFunctionIdAndIdLesserThanAndEndTimeGreaterThanLastOneDay(functionId,
                lastNotifiedAnomaly);
        if (CollectionUtils.isNotEmpty(filteredAnomalies)) {
          mergedAnomaliesAllResults.addAll(filteredAnomalies);
        }
      }

      // apply filtration rule
      List<MergedAnomalyResultDTO> results =
          AlertFilterHelper.applyFiltrationRule(mergedAnomaliesAllResults, alertFilterFactory);

      if (results.isEmpty()) {
        LOG.info("Zero anomalies found, skipping sending email");
      } else {
        // TODO: Add dimensional alert grouping before the stage of task runner?
        // There are two approaches to solve the problem of alert grouping:
        // 1. Anomaly results from detection --> Grouped anomalies from grouper --> Alerter sends emails on grouped anomalies
        // 2. Anomaly results from detection --> Alerter performs simple grouping and sends alerts in one go

        // Current implementation uses the second approach for experimental purpose. We might need to move to
        //     approach 1 in order to consider multi-metric grouping.
        // Input: a list of anomalies.
        // Output: lists of anomalies; each list contains the anomalies of the same group.
        AlertGrouper alertGrouper = AlertGrouperFactory.fromSpec(alertConfig.getGroupByConfig());
        Map<AlertGroupKey, GroupedAnomalyResults> groupedAnomalyResultsMap = alertGrouper.group(results);

        for (Map.Entry<AlertGroupKey, GroupedAnomalyResults> entry : groupedAnomalyResultsMap.entrySet()) {
          // Anomaly results for this group
          List<MergedAnomalyResultDTO> resultsForThisGroup = entry.getValue().getAnomalyResults();
          // Append auxiliary recipients for this group
          String recipientsForThisGroup = alertConfig.getRecipients();
          String auxiliaryRecipients = alertGrouper.groupEmailRecipients(entry.getKey());
          if (StringUtils.isNotBlank(auxiliaryRecipients)) {
            recipientsForThisGroup = recipientsForThisGroup + EmailHelper.EMAIL_ADDRESS_SEPARATOR + auxiliaryRecipients;
          }
          // Append group name after config name if the group name is not an empty string or group
          String emailName = alertConfig.getName();
          String groupName = entry.getKey().toGroupName();
          if (StringUtils.isNotBlank(groupName)) {
            emailName = emailName + " " + groupName;
          }
          // Generate and send out an anomaly report for this group
          AnomalyReportGenerator.getInstance()
              .buildReport(resultsForThisGroup, thirdeyeConfig, recipientsForThisGroup, alertConfig.getFromAddress(),
                  emailName);
          updateNotifiedStatus(resultsForThisGroup);
        }

        // update anomaly watermark in alertConfig
        long lastNotifiedAlertId = emailConfig.getAnomalyWatermark();
        for (MergedAnomalyResultDTO anomalyResult : results) {
          if (anomalyResult.getId() > lastNotifiedAlertId) {
            lastNotifiedAlertId = anomalyResult.getId();
          }
        }
        if (lastNotifiedAlertId != emailConfig.getAnomalyWatermark()) {
          alertConfig.getEmailConfig().setAnomalyWatermark(lastNotifiedAlertId);
          alertConfigDAO.update(alertConfig);
        }
      }
    }
  }

  private void sendScheduledDataReport() throws Exception {
    AlertConfigBean.ReportConfigCollection reportConfigCollection =
        alertConfig.getReportConfigCollection();

    if (reportConfigCollection != null && reportConfigCollection.isEnabled()) {
      if (reportConfigCollection.getReportMetricConfigs() != null
          && reportConfigCollection.getReportMetricConfigs().size() > 0) {

        List<MetricDimensionReport> metricDimensionValueReports;
        // Used later to provide collection for a metric to help build the url link in report

        Map<String, MetricConfigDTO> metricMap = new HashMap<>();

        List<ContributorViewResponse> reports = new ArrayList<>();
        for (int i = 0; i < reportConfigCollection.getReportMetricConfigs().size(); i++) {
          AlertConfigBean.ReportMetricConfig reportMetricConfig =
              reportConfigCollection.getReportMetricConfigs().get(i);
          MetricConfigDTO metricConfig =
              metricConfigManager.findById(reportMetricConfig.getMetricId());

          List<String> dimensions = reportMetricConfig.getDimensions();
          if (dimensions != null && dimensions.size() > 0) {
            for (String dimension : dimensions) {
              ContributorViewResponse report = EmailHelper
                  .getContributorDataForDataReport(metricConfig.getDataset(),
                      metricConfig.getName(), Arrays.asList(dimension),
                      reportMetricConfig.getCompareMode(),
                      alertConfig.getReportConfigCollection().getDelayOffsetMillis(),
                      alertConfig.getReportConfigCollection().isIntraDay());
              if (report != null) {
                metricMap.put(metricConfig.getName(), metricConfig);
                reports.add(report);
              }
            }
          }
        }
        if (reports.size() == 0) {
          LOG.warn("Could not fetch report data for " + alertConfig.getName());
          return;
        }
        long reportStartTs = reports.get(0).getTimeBuckets().get(0).getCurrentStart();
        metricDimensionValueReports =
            DataReportHelper.getInstance().getDimensionReportList(reports);
        for (int i = 0; i < metricDimensionValueReports.size(); i++) {
          MetricDimensionReport report = metricDimensionValueReports.get(i);
          report.setDataset(metricMap.get(report.getMetricName()).getDataset());
          long metricId = metricMap.get(report.getMetricName()).getId();
          report.setMetricId(metricId);
          for (AlertConfigBean.ReportMetricConfig reportMetricConfig : reportConfigCollection
              .getReportMetricConfigs()) {
            if (reportMetricConfig.getMetricId() == metricId) {
              metricDimensionValueReports.get(i)
                  .setCompareMode(reportMetricConfig.getCompareMode().name());
            }
          }
        }
        Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
        freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector/");
        freemarkerConfig.setDefaultEncoding(CHARSET);
        freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        Map<String, Object> templateData = new HashMap<>();
        DateTimeZone timeZone = DateTimeZone.forTimeZone(DEFAULT_TIME_ZONE);
        DataReportHelper.DateFormatMethod dateFormatMethod =
            new DataReportHelper.DateFormatMethod(timeZone);
        templateData.put("timeZone", timeZone);
        templateData.put("dateFormat", dateFormatMethod);
        templateData.put("dashboardHost", thirdeyeConfig.getDashboardHost());
        templateData.put("fromEmail", alertConfig.getFromAddress());
        templateData.put("contactEmail", alertConfig.getReportConfigCollection().getContactEmail());
        templateData.put("reportStartDateTime", reportStartTs);
        templateData.put("metricDimensionValueReports", metricDimensionValueReports);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
          Template template = freemarkerConfig.getTemplate("data-report-by-metric-dimension.ftl");
          template.process(templateData, out);

          // Send email
          HtmlEmail email = new HtmlEmail();
          String alertEmailSubject =
              String.format("Thirdeye data report : %s", alertConfig.getName());
          String alertEmailHtml = new String(baos.toByteArray(), CHARSET);
          EmailHelper
              .sendEmailWithHtml(email, thirdeyeConfig.getSmtpConfiguration(), alertEmailSubject,
                  alertEmailHtml, alertConfig.getFromAddress(), alertConfig.getRecipients());

        } catch (Exception e) {
          throw new JobExecutionException(e);
        }
      }
    }
  }

  private void updateNotifiedStatus(List<MergedAnomalyResultDTO> mergedResults) {
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setNotified(true);
      anomalyMergedResultDAO.update(mergedResult);
    }
  }

  private void sendFailureEmail(Throwable t) throws JobExecutionException {
    HtmlEmail email = new HtmlEmail();
    String subject = String
        .format("[ThirdEye Anomaly Detector] FAILED ALERT ID=%d for config %s", alertConfig.getId(),
            alertConfig.getName());
    String textBody = String
        .format("%s%n%nException:%s", alertConfig.toString(), ExceptionUtils.getStackTrace(t));
    try {
      EmailHelper
          .sendEmailWithTextBody(email, thirdeyeConfig.getSmtpConfiguration(), subject, textBody,
              thirdeyeConfig.getFailureFromAddress(), thirdeyeConfig.getFailureToAddress());
    } catch (EmailException e) {
      throw new JobExecutionException(e);
    }
  }
}
