import bill_calculator_hep.graphite
import logging

import csv
from io import BytesIO
from io import StringIO

import string, re
import datetime, time
import sys, os, socket
import configparser
import pprint
import time
import datetime
import yaml
import traceback
from datetime import timedelta
from google.cloud import bigquery
from collections import defaultdict
import pandas as pd

class GCEBillCalculator(object):
    def __init__(self, account, globalConfig, constants, logger, sumToDate = None):
        self.logger = logger
        self.globalConfig = globalConfig
        # Configuration parameters
        self.project_id = constants['projectId']
        # Expect lastKnownBillDate as '%m/%d/%y %H:%M' : validated when needed
        self.lastKnownBillDate = constants[ 'lastKnownBillDate']
        self.balanceAtDate = constants['balanceAtDate']
        # Expect sumToDate as '%m/%d/%y %H:%M' : validated when needed
        self.sumToDate = constants['sumToDate'] # '08/31/16 23:59'

        self.logger.debug('Loaded account configuration successfully')

    def setLastKnownBillDate(self, lastKnownBillDate):
        self.lastKnownBillDate = lastKnownBillDate

    def setBalanceAtDate(self, balanceAtDate):
        self.balanceAtDate = balanceAtDate

    def setSumToDate(self, sumToDate):
        self.sumToDate = sumToDate

    def CalculateBill(self):
        # define necessary constants
        query_costs_credits, query_adjustments = initializeConstantsForBillCalculation()
        # totalCsvHeaderString = 'Total'
        # adjustedSupportCostKeyString = 'AdjustedSupport'
        # BillSummaryDict = { totalCsvHeaderString : 0.0 , adjustedSupportCostKeyString : 0.0 }
        # print(f"MyDebug: BillSummaryDict: {BillSummaryDict}")
        
        # TODO: substitute the static date with the variables for usage from above
        # using the BigQuery API to fetch Cloud Billing Data
        bq_client = bigquery.Client()
        costs_and_credits_result = bq_client.query(query_costs_credits).to_dataframe()
        # dataframe columns, including numeric, have dtype object; convert to float type
        # costs_and_credits_result['rawCost'] = costs_and_credits_result['rawCost'].astype('float64')
        # costs_and_credits_result['rawCredits'] = costs_and_credits_result['rawCredits'].astype('float64')
        costs_and_credits_result = costs_and_credits_result.astype({'rawCost': 'float64', 'rawCredits': 'float64'})
        # group data based on service category
        costs_and_credits = costs_and_credits_result.groupby('Service')[['Sku', 'rawCost', 'rawCredits']].apply(lambda x: x.set_index('Sku').to_dict(orient='index')).to_dict()
        # costs_and_credits is a dictionary of the form:
        # {service: {sku1: {rawCost: 1.00, rawCredits: 2.00}, sku2: {rawCost: 3.00, rawCredits: 0.00}}}
        costSubtotals = defaultdict(float)
        for service_name, sku in costs_and_credits.items():
            for sku_name, usage_info in sku.items():
                service = "-".join(service_name.lower().split())
                sku = "".join(sku_name.split())
                lineItem = f"{service}.{sku}"
                total_usage_cost_float = float(sum(usage_info.values()))
                costSubtotals[lineItem] = total_usage_cost_float
                costSubtotals['TotalCost'] += total_usage_cost_float
        costs = pd.DataFrame([costSubtotals])
        self.logger.info(f"GCE costs: {costs}")

        # using similar logic to gather data about adjustments issued
        adjustments_result = bq_client.query(query_adjustments).to_dataframe()
        # dataframe columns, including numeric, have dtype object; convert to float type
        # adjustments_result['rawAdjustments'] = adjustments_result['rawAdjustments'].astype('float64')
        # adjustments_result['rawCredits'] = adjustments_result['rawCredits'].astype('float64')
        adjustments_result = adjustments_result.astype({'rawAdjustments': 'float64', 'rawCredits': 'float64'})
        # again group data based on service category
        adjustments = adjustments_result.groupby('Service')[['Sku', 'rawAdjustments', 'rawCredits']].apply(lambda x: x.set_index('Sku').to_dict(orient='index')).to_dict()
        # adjustments is a dictionary of the form:
        # {service: {sku1: {rawAdjustments: 1.00, rawCredits: 2.00}, sku2: {rawAdjustments: 3.00, rawCredits: 0.00}}}
        adjSubtotals = defaultdict(float)
        for service_name, sku in adjustments.items():
            for sku_name, usage_info in sku.items():
                service = "-".join(service_name.lower().split())
                sku = "".join(sku_name.split())
                lineItem = f"{service}.{sku}"
                total_adj_float = float(sum(usage_info.values()))
                adjSubtotals[lineItem] = total_adj_float
                adjSubtotals['TotalAdjustments'] += total_adj_float
        adjustments = pd.DataFrame([adjSubtotals])
        self.logger.info(f"GCE adjustments: {adjustments}")
        
        # self.logger.info('Bill Computation for %s Account Finished at %s' % ( self.project_id, time.strftime("%c") ))
        # self.logger.info('Last Start Date Billed Considered : ' + lastStartDateBilledConsideredDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info(f"Last Known Balance : {self.balanceAtDate}")
        self.logger.info(f"Date of Last Known Balance : {self.lastKnownBillDate}")
        self.logger.debug(f"Costs (incl. credits) Summary: {costs}")
        self.logger.debug(f"Adjustments (incl. credits) Summary: {adjustments}")
        
        # return lastStartDateBilledConsideredDatetime, CorrectedBillSummaryDict
        return costs, adjustments

    def sendDataToGraphite(self, CorrectedBillSummaryDict ):
        graphiteHost=self.globalConfig['graphite_host']
        graphiteContext=self.globalConfig['graphite_context_billing'] + str(self.project_id)

        graphiteEndpoint = graphite.Graphite(host=graphiteHost)
        graphiteEndpoint.send_dict(graphiteContext, CorrectedBillSummaryDict, send_data=True)

def initializeConstantsForBillCalculation(self):
    # defining required constants
    # Google Cloud Billing project for HEPCloud Decision Engine is 'hepcloud-fnal' which is in GCE channel config
    billingProjectId = self.project_id 
    # as of May 2023, cloud billing was exported to BigQuery and the table containing this data is the standard usage cost table
    billingDataset = f"{billingProjectId}.hepcloud_fnal_bigquery_billing"
    billingDataTable = f"{billingDataset}.gcp_billing_export_v1_0175D2_253B59_AB11A7"
    self.logger.info(f"billingProjectId = {billingProjectId}")
    self.logger.info(f"billingDataset = {billingDataset}")
    self.logger.info(f"billingDataTable = {billingDataTable}")
    
    # sumFromDate used previously in _sumUpBillFromDateToDate is the lastKnownBillDate
    fromDate = self.lastKnownBillDate    # class 'str'
    toDate = self.sumToDate              # class 'NoneType' unless `sumToDate` attribute is defined in the channel's configuration (jsonnet file)
    # datetime.strptime converts a string to a datetime object
    sumBillFromDate = datetime.datetime.strptime(self.lastKnownBillDate, '%m/%d/%y %H:%M')
    self.logger.info(f"sumBillFromDate: {sumBillFromDate}")
    if self.sumToDate != None:
        sumBillToDate = datetime.datetime.strptime(self.sumToDate, '%m/%d/%y %H:%M')
    else:
        # TODO: change this to datetime.datetime.now
        sumBillToDate = datetime.datetime.strptime("05/03/24 00:00", '%m/%d/%y %H:%M')
    self.logger.info(f"sumBillToDate: {sumBillToDate}")
    
    usageStartFromDate = sumBillFromDate
    self.logger.info(f"usageStartDate: {usageStartFromDate}")
    usageEndToDate = sumBillToDate
    self.logger.info(f"usageEndDate: {usageEndToDate}")
    
    # queries to query cloud billing data in BigQuery
    costs_query = f"""
    SELECT sku.description as Sku, service.description as Service, 
    ROUND(SUM(CAST(cost AS NUMERIC)), 8) as rawCost, 
    ROUND(SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC)) 
        FROM UNNEST(credits) AS c), 0)), 8) as rawCredits 
    FROM `hepcloud-fnal.hepcloud_fnal_bigquery_billing.gcp_billing_export_v1_0175D2_253B59_AB11A7` 
    WHERE project.id = "hepcloud-fnal" AND 
    DATE(usage_start_time) BETWEEN "2024-02-01" AND "2024-02-29" AND 
    DATE(usage_end_time) BETWEEN "2024-02-01" AND "2024-02-29"
    GROUP BY 1, 2
    """
    adjustments_query = f"""
    SELECT sku.description as Sku, service.description as Service,  
    ROUND(SUM(CAST(cost AS NUMERIC)), 8) as rawAdjustments, 
    ROUND(SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC))
        FROM UNNEST(credits) AS c), 0)), 8) as rawCredits 
    FROM `hepcloud-fnal.hepcloud_fnal_bigquery_billing.gcp_billing_export_v1_0175D2_253B59_AB11A7` 
    WHERE project.id = "hepcloud-fnal" AND 
    DATE(usage_start_time) BETWEEN "2024-02-01" AND "2024-02-29" AND 
    DATE(usage_end_time) BETWEEN "2024-02-01" AND "2024-02-29" AND 
    adjustment_info.id IS NOT NULL 
    GROUP BY 1, 2
    """
    
    return costs_credits_query, adjustments_query


class GCEBillAlarm(object):

    def __init__(self, calculator, account, globalConfig, constants, logger):
        # Configuration parameters
        self.globalConfig = globalConfig
        self.logger = logger
        self.constants = constants
        self.projectId = calculator.project_id
        self.calculator = calculator
        self.costRatePerHourInLastDayAlarmThreshold = constants['costRatePerHourInLastDayAlarmThreshold']
        self.burnRateAlarmThreshold = constants['burnRateAlarmThreshold']
        self.timeDeltaforCostCalculations = constants['timeDeltaforCostCalculations']

    def EvaluateAlarmConditions(self, publishData = True):
        """Compare the alarm conditions with the set thresholds.

           Returns: alarmMessage
                If no alarms are triggered, alarmMessage = None
        """

        # Extracts alarm conditions from billing data
        alarmConditionsDict = self.ExtractAlarmConditions()

        # Publish data to Graphite
        if publishData:
            self.sendDataToGraphite(alarmConditionsDict)

        # Compare alarm conditions with thresholds and builds alarm message
        alarmMessage = None
        messageHeader = 'GCE Billing Alarm Message for project %s - %s\n' % ( self.projectId, time.strftime("%c") )
        messageHeader += 'GCE Billing Dashboard - %s\n\n' % ( os.environ.get('GRAPHITE_HOST' ))

        if alarmConditionsDict['costRatePerHourInLastDay'] > self.costRatePerHourInLastDayAlarmThreshold:
            if alarmMessage is None:
                alarmMessage = messageHeader
            alarmMessage += 'Alarm threshold surpassed for cost rate per hour in the last day\n'
            alarmMessage += "Cost in the last day: $ %f\n" % alarmConditionsDict['costInLastDay']
            alarmMessage += 'Cost rate per hour in the last day: $%f / h\n' %  alarmConditionsDict['costRatePerHourInLastDay']
            alarmMessage += 'Set Alarm Threshold on one day cost rate: $%f / h\n' % self.costRatePerHourInLastDayAlarmThreshold

        if alarmConditionsDict['currentBalance'] - \
                self.timeDeltaforCostCalculations*alarmConditionsDict['costRatePerHourInLastDay'] <= \
                self.burnRateAlarmThreshold:
            if alarmMessage is None:
                alarmMessage = messageHeader
            alarmMessage += 'Alarm: account is approaching the balance\n'
            alarmMessage += "Current balance: $ %f\n" % (alarmConditionsDict['currentBalance'],)
            alarmMessage += 'Cost rate per hour: $%f / h for last %s hours\n' %  (alarmConditionsDict['costRatePerHourInLastDay'], self.timeDeltaforCostCalculations)
            alarmMessage += 'Set Alarm Threshold on burn rate: $%f\n' % (self.burnRateAlarmThreshold,)

        return alarmMessage

    def ExtractAlarmConditions(self):
        """Extract the alarm conditions from the billing data. For now, focusing on cost
        rates.

        Returns: alarmConditionsDict
           Example alarmConditionsDict:
            {
               'costRatePerHourInLastDay': 0.7534264869301031,
               'costRatePerHourInLastDayAlarmThreshold': 20,
               'costInLastDay': 18.082235686322473
            }
        """

        # Get total and last date billed
        lastStartDateBilledDatetime, CorrectedBillSummaryNowDict = self.calculator.CalculateBill()
        dateNow = datetime.datetime.now()

        # Get cost in the last 24 hours
        oneDayBeforeLastDateBilledDatetime = lastStartDateBilledDatetime - timedelta(hours=24)
        self.calculator.setLastKnownBillDate(oneDayBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        newLastStartDateBilledDatetime, CorrectedBillSummaryOneDayBeforeDict = self.calculator.CalculateBill()

        costInLastDay = CorrectedBillSummaryOneDayBeforeDict['AdjustedTotal']
        costRatePerHourInLastDay = costInLastDay / 24

        dataDelay = int((time.mktime(dateNow.timetuple()) - time.mktime(lastStartDateBilledDatetime.timetuple())) / 3600)
        self.logger.info('---')
        self.logger.info('Alarm Computation for {0} Project Finished at {1}'.format(self.projectId,time.strftime("%c")))
        self.logger.info('Last Start Date Billed Considered: ' + lastStartDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info('Now '+dateNow.strftime('%m/%d/%y %H:%M'))
        self.logger.info('Delay between now and Last Start Date Billed Considered in hours '+str(dataDelay))
        self.logger.info('One day before that: ' + oneDayBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info('Adjusted Total Now from Date of Last Known Balance: $' + str(CorrectedBillSummaryNowDict['AdjustedTotal']))
        self.logger.info('Cost In the Last Day: $' + str(costInLastDay))
        self.logger.info('Cost Rate Per Hour In the Last Day: $'+str(costRatePerHourInLastDay)+' / h')
        self.logger.info('Alarm Threshold: $'+str(self.constants['costRatePerHourInLastDayAlarmThreshold']))
        self.logger.info('---')

        alarmConditionsDict = { 'costInLastDay' : costInLastDay, \
                                'costRatePerHourInLastDay' : costRatePerHourInLastDay, \
                                'costRatePerHourInLastDayAlarmThreshold' : self.costRatePerHourInLastDayAlarmThreshold, \
                                'delayTolastStartDateBilledDatetime': dataDelay, \
                                'currentBalance': CorrectedBillSummaryNowDict['Balance'], \
                                'timeDeltaforCostCalculations': self.timeDeltaforCostCalculations, \
                                'burnRateAlarmThreshold': self.burnRateAlarmThreshold

                                }

        self.logger.debug('alarmConditionsDict'.format(alarmConditionsDict))
        return alarmConditionsDict

    def sendDataToGraphite(self, alarmConditionsDict):
        """Send the alarm condition dictionary to the Graphana dashboard

        Args:
            alarmConditionsDict: the alarm data to send Graphite.
                 Example dict:
                    {
                       'costRatePerHourInLastDay': 0.7534264869301031,
                       'costRatePerHourInLastDayAlarmThreshold': 20,
                       'costInLastDay': 18.082235686322473
                    }

        Returns:
            none
        """

        #Constants
        graphiteHost=self.globalConfig['graphite_host']
        graphiteContext=self.globalConfig['graphite_context_alarms'] + str(self.projectId)

        graphiteEndpoint = graphite.Graphite(host=graphiteHost)
        graphiteEndpoint.send_dict(graphiteContext, alarmConditionsDict, send_data=True)
    
    def submitAlert(message, snowConfig):
        sendAlarmByEmail(alarmMessageString = message, 
                         emailReceipientString = AWSCMSAccountConstants.emailReceipientForAlarms,
                         subject = '[GCE Billing Alarm] Alarm threshold surpassed for cost rate for %s account'%(alarm.accountName,),
                         sender = 'GCEBillAlarm@%s'%(socket.gethostname(),),
                         verbose = alarm.verboseFlag)
        submitAlarmOnServiceNow(usernameString = ServiceNowConstants.username, 
                                passwordString = ServiceNowConstants.password, 
                                messageString = message, 
                                eventAssignmentGroupString = ServiceNowConstants.eventAssignmentGroup,
                                eventSummary = AlarmSummary,
                                event_cmdb_ci = ServiceNowConstants.event_cmdb_ci,
                                eventCategorization = ServiceNowConstants.eventCategorization,
                                eventVirtualOrganization = ServiceNowConstants.eventVirtualOrganization,
                                instanceURL = ServiceNowConstants.instanceURL)


if __name__ == "__main__":

    os.setuid(53431)
    logger = logging.getLogger("GGE_UNIT_TEST")
    logger.handlers=[]

    try:
        init = '/etc/hepcloud/bill-calculator.ini'
        config = configparser.ConfigParser()
        config.read(init)

        # Setting up logger level from config spec
        debugLevel = config.get('Env','LOG_LEVEL')
        logger.setLevel(debugLevel)

        # Not interested in actually writing logs
        # Redirecting to stdout is enough
        fh = logging.StreamHandler(sys.stdout)
        fh.setLevel(debugLevel)
        FORMAT='%(asctime)s %(name)-2s %(levelname)-4s %(message)s'
        #FORMAT="%(asctime)s: i[%(levelname)s:] %(message)s"
        fh.setFormatter(logging.Formatter(FORMAT))
        logger.addHandler(fh)

        logger.info("Reading configuration file at %s" % init)

        for section in config.sections():
            for key, value in config.items(section):
                if 'Env' in section:
                    if "LOG" in key.upper():
                       continue
                    os.environ[key.upper()] = value
                    logger.debug("Setting Env variable {0}={1}".format(key.upper(),os.environ.get(key.upper())))
                else:
                    os.environ[key.upper()] = value
                    logger.debug("Setting Env variable for {0} as {1}={2}".format(section,key.upper(),os.environ.get(key.upper())))
    except Exception as error:
        traceback.print_exc()
        logger.exception(error)

    GCEconstants = "/etc/hepcloud/config.d/GCE.yaml"
    with open(GCEconstants, 'r') as stream:
        config = yaml.safe_load(stream)
    globalConfig = config['global']
    logger.info("--------------------------- Start of calculation cycle {0} ------------------------------".format(time.strftime("%c")))

    for constantsDict in config['accounts']:
        account = constantsDict['accountName']
        try:
            os.chdir(os.environ.get('BILL_DATA_DIR'))
            logger.info("[UNIT TEST] Starting Billing Analysis for GCE {0} account".format(account))
            calculator = GCEBillCalculator(account, globalConfig, constantsDict, logger)
            lastStartDateBilledConsideredDatetime, CorrectedBillSummaryDict = calculator.CalculateBill()
            calculator.sendDataToGraphite(CorrectedBillSummaryDict)

            logger.info("[UNIT TEST] Starting Alarm calculations for GCE {0} account".format(account))
            alarm = GCEBillAlarm(calculator, account, globalConfig, constantsDict, logger)
            message = alarm.EvaluateAlarmConditions(publishData = True)
        except Exception as error:
            logger.exception(error)
            continue

