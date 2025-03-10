# standard library imports
import sys, os, socket
import datetime
import time
import logging
import configparser
import time
import yaml
import traceback
from datetime import timedelta
from collections import defaultdict
# related third party imports
import pandas as pd
from google.cloud import bigquery
from google.auth.exceptions import RefreshError, DefaultCredentialsError

class GCEBillCalculator(object):
    def __init__(self, account, globalConfig, constants, logger, sumToDate = None):
        self.logger = logger
        self.globalConfig = globalConfig
        # Configuration parameters
        self.project_id = constants['projectId']
        # Expect lastKnownBillDate as '%m/%d/%y %H:%M' : validated when needed
        self.lastKnownBillDate = constants[ 'lastKnownBillDate']
        self.balanceAtDate = constants['balanceAtDate']
        self.applyDiscount = constants['applyDiscount']
        # Expect sumToDate as '%m/%d/%y %H:%M' : validated when needed
        self.sumToDate = sumToDate #  '08/31/16 23:59'
        self.logger.info('Loaded account configuration successfully')

        # defining additional constants that will be used to store cloud billing
        # data from BigQuery...
        self.adj_key = "Adjustments"
        self.adjusted_total_key = "AdjustedTotal"
        self.balance_at_date_key = "Balance"
        self.total_key = "Total"
        self.adjusted_support_cost_key = "AdjustedSupport"

    def set_last_known_bill_date(self, lastKnownBillDate):
        self.lastKnownBillDate = lastKnownBillDate

    def set_balance_at_date(self, balanceAtDate):
        self.balanceAtDate = balanceAtDate

    def set_sum_to_date(self, sumToDate):
        self.sumToDate = sumToDate

    # TODO: check for possible refactoring of the following methods to redefine them as instance methods, class method or static methods
    def calculate_bill(self):
        """
        This method calculates the bill amount for the Google Cloud Services
        used across a specific time period (as defined in the GCE billing
        channel configuration).
        """
        # defining a few constants...
        query_costs_credits, query_adjustments, last_start_date_billed_considered = self.initialize_constants_for_bill_calculation()

        # invoking bigquery client APIs to query BigQuery for cloud billing
        # data...
        try:
            bq_client = bigquery.Client()
        except DefaultCredentialsError:
            self.logger.error("**** AUTHENTICATION FAILED: Incorrect/Corrupted private key! Please verify. ****")
            raise

        line_items_costs = self.calculate_sub_totals(bq_client, query_costs_credits, cost_query=True)
        self.logger.info(f"Line item costs: {line_items_costs}")

        # using similar logic to gather data about adjustments issued...
        adj_issued = self.calculate_sub_totals(bq_client, query_adjustments)
        self.logger.info(f"Line Item adjustments: {adj_issued}")

        # adding information from the adjustments dictionary to the costs
        # dictionary...
        for adj_line_item in adj_issued:
            # if the key is 'Total', skip the iteration
            if adj_line_item == self.total_key:
                continue
            # if the key is not found in the list of keys for the dictionary
            # containing line items costs, then report an error/throw a warning
            if adj_line_item not in line_items_costs:
                self.logger.error("something is wrong")
            # get the value for the key in line items costs dictionary
            value = line_items_costs[adj_line_item]
            # add a new key-pair to the line item entry in the line items costs
            # dictionary; the key-pair represents the adjustments info
            line_items_costs[adj_line_item][self.adj_key] = adj_issued[adj_line_item]
        # after the adjustments info is added, calculate the adjusted total
        line_items_costs[self.adjusted_total_key] = line_items_costs[self.total_key] + line_items_costs[self.adjusted_support_cost_key]
        # calculating the remaining balance (after the costs are deducted)
        line_items_costs[self.balance_at_date_key] = self.balanceAtDate - line_items_costs[self.adjusted_total_key]

        self.logger.info(f"Bill computation for '{self.project_id}' account finished at {time.strftime('%c')}")
        self.logger.info(f"Last Start Date Billed Considered : {last_start_date_billed_considered.strftime('%m/%d/%y %H:%M')}")
        self.logger.info(f"Last Known Balance : {str(self.balanceAtDate)}")
        self.logger.info(f"Date of Last Known Balance : {self.lastKnownBillDate}")
        self.logger.info(f"Bill Summary: {line_items_costs}")
        # converting the dictionary containing line items costs to a dataframe
        # before returning from here since the calling function requires a
        # dataframe...
        costs = pd.DataFrame([line_items_costs])

        return costs

    def send_data_to_graphite(self, CorrectedBillSummaryDict ):
        graphiteHost=self.globalConfig['graphite_host']
        graphiteContext=self.globalConfig['graphite_context_billing'] + str(self.project_id)

        graphiteEndpoint = graphite.Graphite(host=graphiteHost)
        graphiteEndpoint.send_dict(graphiteContext, CorrectedBillSummaryDict, send_data=True)

    def initialize_constants_for_bill_calculation(self):
        """
        This method initializes several constants necessary for calculating the bill amount for using Google Cloud services
        """
        # Google Cloud Billing project for HEPCloud Decision Engine is
        # 'hepcloud-fnal' which is in GCE channel config
        billing_project_id = self.project_id
        # as of May 2023, cloud billing was exported to BigQuery and the table
        # containing this data is the standard usage cost table
        billing_dataset = f"{billing_project_id}.hepcloud_fnal_bigquery_billing"
        billing_data_table = f"{billing_dataset}.gcp_billing_export_v1_0175D2_253B59_AB11A7"
        self.logger.info(f"Billing project id = {billing_project_id}")
        self.logger.info(f"Billing dataset = {billing_dataset}")
        self.logger.info(f"Billing data table = {billing_data_table}")

        # sumFromDate used previously in _sumUpBillFromDateToDate is the lastKnownBillDate; this is the last time that we compared the billing info we had versus the actual bill. As an example, if lastKnownBillDate is defined to be 04/01/22, download everything from april 22 until now and not go back farther than that
        bill_from = self.lastKnownBillDate    #  class 'str'
        bill_to = self.sumToDate              #  class 'NoneType' unless `sumToDate` attribute is defined in the channel's configuration (jsonnet file)
        # datetime.strptime converts a string to a datetime object
        sum_bill_from = datetime.datetime.strptime(bill_from, '%m/%d/%y %H:%M')
        self.logger.info(f"Calculate cloud billing expenses from: {sum_bill_from}")
        last_start_date = sum_bill_from
        if bill_to is not None:
            sum_bill_to = datetime.datetime.strptime(bill_to, '%m/%d/%y %H:%M')
        else:
            # TODO: change this to datetime.datetime.now
            sum_bill_to = datetime.datetime.strptime("05/03/24 00:00", '%m/%d/%y %H:%M')
        self.logger.info(f"Calculate cloud billing expenses to: {sum_bill_to}")

        usage_start_from = sum_bill_from        #  class datetime.datetime
        self.logger.info(f"usageStartDate: {usage_start_from}")
        usage_end_to = sum_bill_to              #  class datetime.datetime
        self.logger.info(f"usageEndDate: {usage_end_to}")

        # queries to query cloud billing data in BigQuery
        # date format in bigquery: YYYY-MM-DD HH:MM:SS:MS TZ
        costs_query = f"""
        SELECT sku.description as Sku, service.description as Service, ROUND(SUM(CAST(cost AS NUMERIC)), 8) as rawCost, ROUND(SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC)) FROM UNNEST(credits) AS c), 0)), 8) as rawCredits FROM `{billing_data_table}` WHERE project.id = '{billing_project_id}' AND DATE(usage_start_time) BETWEEN '{usage_start_from.date()}' AND '{usage_end_to.date()}' AND DATE(usage_end_time) BETWEEN '{usage_start_from.date()}' AND '{usage_end_to.date()}'
        GROUP BY 1, 2
        """
        adjustments_query = f"""
        SELECT sku.description as Sku, service.description as Service,
        ROUND(SUM(CAST(cost AS NUMERIC)), 8) as rawAdjustments,
        ROUND(SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC)) FROM
        UNNEST(credits) AS c), 0)), 8) as rawCredits FROM
        `{billing_data_table}` WHERE project.id = '{billing_project_id}' AND DATE(usage_start_time) BETWEEN '{usage_start_from.date()}' AND '{usage_end_to.date()}' AND DATE(usage_end_time) BETWEEN '{usage_start_from.date()}' AND '{usage_end_to.date()}' AND adjustment_info.id IS NOT NULL
        GROUP BY 1, 2
        """

        return costs_query, adjustments_query, last_start_date

    def query_cloud_billing_data(self, bigquery_client, query, cost_query=None):
        """
        This method queries BigQuery to fetch costs,
        credits and adjustments data from cloud billing data.
        """
        try:
            query_result = bigquery_client.query(query).to_dataframe()
        except RefreshError as rEx:
            if rEx.args[1]['error_description'] == "Invalid grant: account not found":
                self.logger.error("**** AUTHENTICATION FAILED: One/more fields in the credential might be incorrect/corrupted! ****")
            elif rEx.args[1]['error_description'] == "Invalid JWT Signature.":
                self.logger.error("**** AUTHENTICATION FAILED: Invalid credential file!! ****")
            raise

        # check the query flag to determine whether query for costs or
        # adjustments was run
        if cost_query:
            target_column = 'rawCost'
        else:
            target_column = 'rawAdjustments'
        # dataframe columns, including numeric, have dtype object; convert to
        # float type
        query_result = query_result.astype({target_column: 'float64', 'rawCredits': 'float64'})
        # group data based on service category
        result = query_result.groupby('Service')[['Sku', target_column, 'rawCredits']].apply(lambda x: x.set_index('Sku').to_dict(orient='index')).to_dict()
        # 'result' is a dictionary of the form: {service: {sku1: {rawCost: 1.00,
        # rawCredits: 2.00}, sku2: {rawCost: 3.00, rawCredits: 0.00}}}, (OR)
        # {service: {sku1: {rawAdjustments: 1.00, rawCredits: 2.00}, sku2:
        # {rawAdjustments: 3.00, rawCredits: 0.00}}}

        return result, target_column

    def calculate_sub_totals(self, bigquery_client, query, cost_query = None):
        """
        This method computes the total cost and total adjustments
        issued individually.
        """
        # defining additional constants that will be used to store cloud billing data from BigQuery...
        raw_cost_key = "rawCost"
        credit_key = "Credits"
        cost_key = "Cost"
        if cost_query:
            query_result, cost_column = self.query_cloud_billing_data(bigquery_client, query, cost_query)
            sub_totals = dict()
            sub_totals[self.total_key] = 0.0
            sub_totals[self.adjusted_support_cost_key] = 0.0
        else:
            query_result, adjColumn = self.query_cloud_billing_data(bigquery_client, query)
            sub_totals = defaultdict(float)

        for service_name, sku in query_result.items():
            for sku_name, usage_info in sku.items():
                service = "-".join(service_name.lower().split())
                sku = "".join(sku_name.split())
                line_item = f"{service}.{sku}"
                if cost_query:
                    total = float(sum(usage_info.values()))
                    # cost is before credits; so add credits to cost to get the
                    # actual costs for the sku/service
                    sub_totals[line_item] = {
                        raw_cost_key: usage_info[cost_column],
                        credit_key: usage_info['rawCredits'],
                        cost_key: total
                    }
                else:
                    total = float(sum(usage_info.values()))
                    sub_totals[line_item] = total
                # add the line item cost to the cumulative costs for the time
                # window for which the bill is being calculated
                sub_totals[self.total_key] += total

        return sub_totals


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

