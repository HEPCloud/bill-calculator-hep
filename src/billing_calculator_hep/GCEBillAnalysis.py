import json
import boto
import gcs_oauth2_boto_plugin

import billing_calculator_hep.graphite
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
from boto.exception import NoAuthHandlerFound


class GCEBillCalculator(object):
    def __init__(self, account, globalConfig, constants, logger, sumToDate = None):
        self.logger = logger
        self.globalConfig = globalConfig
        # Configuration parameters
        self.outputPath = globalConfig['outputPath']
        self.project_id = constants['projectId']
        self.accountProfileName = constants['credentialsProfileName']
        self.accountNumber = constants['accountNumber']
        self.bucketBillingName = constants['bucketBillingName']
        # Expect lastKnownBillDate as '%m/%d/%y %H:%M' : validated when needed
        self.lastKnownBillDate = constants[ 'lastKnownBillDate']
        self.balanceAtDate = constants['balanceAtDate']
        self.applyDiscount = constants['applyDiscount']
        # Expect sumToDate as '%m/%d/%y %H:%M' : validated when needed
        self.sumToDate = sumToDate # '08/31/16 23:59'

        # Do not download the files twice for repetitive calls e.g. for alarms
        self.fileNameForDownloadList = None
        self.logger.debug('Loaded account configuration successfully')

    def setLastKnownBillDate(self, lastKnownBillDate):
        self.lastKnownBillDate = lastKnownBillDate

    def setBalanceAtDate(self, balanceAtDate):
        self.balanceAtDate = balanceAtDate

    def setSumToDate(self, sumToDate):
        self.sumToDate = sumToDate

    def CalculateBill(self):

        # Load data in memory
        if self.fileNameForDownloadList == None:
            self.fileNameForDownloadList = self._downloadBillFiles()

        lastStartDateBilledConsideredDatetime, BillSummaryDict = self._sumUpBillFromDateToDate( self.fileNameForDownloadList, self.lastKnownBillDate, self.sumToDate );

        CorrectedBillSummaryDict = self._applyBillCorrections(BillSummaryDict);

        self.logger.info('Bill Computation for %s Account Finished at %s' % ( self.project_id, time.strftime("%c") ))
        self.logger.info('Last Start Date Billed Considered : ' + lastStartDateBilledConsideredDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info('Last Known Balance :' + str(self.balanceAtDate))
        self.logger.info('Date of Last Known Balance : ' + self.lastKnownBillDate)
        self.logger.debug('BillSummaryDict:'.format(BillSummaryDict))
        self.logger.debug('CorrectedBillSummaryDict'.format(CorrectedBillSummaryDict))
        return lastStartDateBilledConsideredDatetime, CorrectedBillSummaryDict

    def sendDataToGraphite(self, CorrectedBillSummaryDict ):
        graphiteHost=self.globalConfig['graphite_host']
        graphiteContext=self.globalConfig['graphite_context_billing'] + str(self.project_id)

        graphiteEndpoint = graphite.Graphite(host=graphiteHost)
        graphiteEndpoint.send_dict(graphiteContext, CorrectedBillSummaryDict, send_data=True)


    def _downloadBillFiles(self):
        # Identify what files need to be downloaded, given the last known balance date
        # Download the files from google storage

        # Constants
        # URI scheme for Cloud Storage.
        GOOGLE_STORAGE = 'gs'
        LOCAL_FILE = 'file'
        header_values = {"x-goog-project-id": self.project_id}

        gcs_oauth2_boto_plugin.SetFallbackClientIdAndSecret("32555940559.apps.googleusercontent.com","ZmssLNjJy2998hD4CTg2ejr2")


        # Access list of files from Goggle storage bucket
# HK> this try statement is needed to make DE unit test work
        try:
            uri = boto.storage_uri(self.bucketBillingName, GOOGLE_STORAGE)
            file_obj = uri.get_bucket()
        except NoAuthHandlerFound:
            self.logger.error(
                "Unable to download GCE billing file names because auth is not set up")
            return []
        except Exception:
            self.logger.error(
                "Able to auth but unable to download GCE billing files")
            return []
        filesList = []
        for obj in uri.get_bucket():
          filesList.append(obj.name)
        # Assumption: sort files by date using file name: this is true if file name convention is maintained
        filesList.sort()

        # Extract file creation date from the file name
        # Assume a format such as this: Fermilab Billing Export-2016-08-22.csv
        # billingFileNameIdentifier = 'Fermilab\ Billing\ Export\-20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9].csv'
        billingFileNameIdentifier = 'hepcloud\-fnal\-20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9].csv'
        billingFileMatch = re.compile(billingFileNameIdentifier)
        billingFileDateIdentifier = '20[0-9][0-9]\-[0-9][0-9]\-[0-9][0-9]'
        dateExtractionMatch = re.compile(billingFileDateIdentifier)
        lastKnownBillDateDatetime = datetime.datetime(*(time.strptime(self.lastKnownBillDate, '%m/%d/%y %H:%M')[0:6]))

        self.logger.debug('lastKnownBillDate ' +  self.lastKnownBillDate)
        fileNameForDownloadList = []
        previousFileForDownloadListDateTime = None
        previousFileNameForDownloadListString = None
        noFileNameMatchesFileNameIdentifier = True
        for file in filesList:
           self.logger.debug('File in bucket ' + self.bucketBillingName + ' : ' +  file)
           # Is the file a billing file?
           if billingFileMatch.search(file) is None:
               continue
           else:
               noFileNameMatchesFileNameIdentifier = False
           # extract date from file
           dateMatch = dateExtractionMatch.search(file)
           if dateMatch is None:
             self.logger.exception('Cannot identify date in billing file name ' + file + ' with regex = "' + billingFileDateIdentifier + '"')
             #raise Exception('Cannot identify date in billing file name ' + file + ' with regex = "' + billingFileDateIdentifier + '"')
           date = dateMatch.group(0)
           billDateDatetime = datetime.datetime(*(time.strptime(date, '%Y-%m-%d')[0:6]))
           self.logger.debug('Date extracted from file: ' + billDateDatetime.strftime('%m/%d/%y %H:%M'))

           # Start by putting the current file and file start date in the previous list
           if not previousFileNameForDownloadListString:
               previousFileNameForDownloadListString = file
               previousFileForDownloadListDateTime = billDateDatetime
               self.logger.debug('previousFileForDownloadListDateTime ' + previousFileForDownloadListDateTime.strftime('%m/%d/%y %H:%M'))
               self.logger.debug('previousFileNameForDownloadListString ' + previousFileNameForDownloadListString)
               self.logger.debug('fileNameForDownloadList: '.format(fileNameForDownloadList))

           # if the last known bill date is past the start date of the previous file...
           if lastKnownBillDateDatetime > previousFileForDownloadListDateTime:
               self.logger.debug('lastKnownBillDateDatetime > previousFileForDownloadListDateTime: ' + lastKnownBillDateDatetime.strftime('%m/%d/%y %H:%M') + ' > ' + previousFileForDownloadListDateTime.strftime('%m/%d/%y %H:%M'))
               # if the previous file starts and end around the last known bill date,
               # add previous and current file name to the list
               if lastKnownBillDateDatetime < billDateDatetime:
                   fileNameForDownloadList = [ previousFileNameForDownloadListString, file ];
                   self.logger.debug('lastKnownBillDateDatetime < billDateDatetime: ' + lastKnownBillDateDatetime.strftime('%m/%d/%y %H:%M') + ' < ' + billDateDatetime.strftime('%m/%d/%y %H:%M'))
                   self.logger.debug('fileNameForDownloadList:'.format(fileNameForDownloadList))

               previousFileForDownloadListDateTime = billDateDatetime
               previousFileNameForDownloadListString = file
               self.logger.debug('previousFileForDownloadListDateTime ' + previousFileForDownloadListDateTime.strftime('%m/%d/%y %H:%M'))
               self.logger.debug('previousFileNameForDownloadListString ' + previousFileNameForDownloadListString)
           else:
               if not fileNameForDownloadList:
                  fileNameForDownloadList = [ previousFileNameForDownloadListString ]
               # at this point, all the files have a start date past the last known bill date: we want those files
               fileNameForDownloadList.append(file)
               self.logger.debug('fileNameForDownloadList:'.format(fileNameForDownloadList))

        if noFileNameMatchesFileNameIdentifier:
           self.logger.exception('No billing files found in bucket ' + self.bucketBillingName + ' looking for patterns containing ' + billingFileNameIdentifier)
           #raise Exception('No billing files found in bucket ' + self.bucketBillingName + ' looking for patterns containing ' + billingFileNameIdentifier)

        # After looking at all the files, if their start date is always older than the last known billing date,
        # we take the last file
        if fileNameForDownloadList == []:
            fileNameForDownloadList = [ file ]

        self.logger.debug('fileNameForDownloadList:'.format(fileNameForDownloadList))

        # Download files to the local directory
        new_fileNameForDownloadList = []
        for fileNameForDownload in fileNameForDownloadList:
            src_uri = boto.storage_uri(self.bucketBillingName + '/' + fileNameForDownload, GOOGLE_STORAGE)

            # Create a file-like object for holding the object contents.
            object_contents = BytesIO()

            # The unintuitively-named get_file() doesn't return the object
            # contents; instead, it actually writes the contents to
            # object_contents.
            src_uri.get_key().get_file(object_contents)

            outputfile = os.path.join(self.outputPath, fileNameForDownload)
            local_dst_uri = boto.storage_uri(outputfile, LOCAL_FILE)
            object_contents.seek(0)
            local_dst_uri.new_key().set_contents_from_file(object_contents)
            object_contents.close()
            new_fileNameForDownloadList.append(outputfile)

        return new_fileNameForDownloadList


    def _sumUpBillFromDateToDate(self, fileList , sumFromDate, sumToDate = None):
        # CSV Billing file format documentation:
        # https://support.google.com/cloud/answer/6293835?rd=1
        # https://cloud.google.com/storage/pricing
        #
        # Cost : the cost of each item; no concept of "unblended" cost in GCE, it seems.
        #
        # Line Item : The URI of the specified resource. Very fine grained. Need to be grouped
        #
        # Project ID : multiple project billing in the same file
        #
        #  Returns:
        #               BillSummaryDict: (Keys depend on services present in the csv file)


        # Constants
        itemDescriptionCsvHeaderString = 'ItemDescription'
        ProductNameCsvHeaderString = 'Line Item'
        costCsvHeaderString = 'Cost'
        usageStartDateCsvHeaderString = 'Start Time'
        totalCsvHeaderString = 'Total'
        ProjectID = 'Project ID'
        adjustedSupportCostKeyString = 'AdjustedSupport'

        sumFromDateDatetime = datetime.datetime(*(time.strptime(sumFromDate, '%m/%d/%y %H:%M')[0:6]))
        lastStartDateBilledConsideredDatetime = sumFromDateDatetime
        if sumToDate != None:
            sumToDateDatetime = datetime.datetime(*(time.strptime(sumToDate, '%m/%d/%y %H:%M')[0:6]))
        BillSummaryDict = { totalCsvHeaderString : 0.0 , adjustedSupportCostKeyString : 0.0 }


        for fileName in fileList:
            file = open(fileName, 'r')
            csvfilereader = csv.DictReader(file)
            rowCounter=0

            for row in csvfilereader:
                # Skip if there is no date (e.g. final comment lines)
                if row[usageStartDateCsvHeaderString] == '' :
                   self.logger.exception("Missing Start Time in row: ", row)

                if row[ProjectID] != self.project_id:
                    continue

                # Skip rows whose UsageStartDate is prior to sumFromDate and past sumToDate
                # Remove timezone info, as python 2.4 does not support %z and we consider local time
                # Depending on standard vs. daylight time we have a variation on that notation.
                dateInRowStr = re.split('-0[7,8]:00',row[usageStartDateCsvHeaderString])[0]
                usageStartDateDatetime = datetime.datetime(*(time.strptime(dateInRowStr, '%Y-%m-%dT%H:%M:%S')[0:6]))
                if usageStartDateDatetime < sumFromDateDatetime :
                   continue;

                if sumToDate != None:
                    if usageStartDateDatetime > sumToDateDatetime :
                        continue;

                if usageStartDateDatetime > lastStartDateBilledConsideredDatetime:
                   lastStartDateBilledConsideredDatetime = usageStartDateDatetime

                # Sum up the costs
                try:
                    rowCounter+=1
                    key = row[ProductNameCsvHeaderString]
                    if key == '':
                        self.logger.exception("Missing Line Item in file %s, row: %s" % (fileName, row))

                    # For now we do not calculate support costs as they depend on Onix services only

                    # Add up cost per product (i.e. key) and total cost
                    # totalCsvHeaderString already exists within the dictionary: it is added first
                    # as it is guaranteed not to throw a KeyError exception.
                    BillSummaryDict[ totalCsvHeaderString ] += float(row[costCsvHeaderString])
                    BillSummaryDict[ key ] += float(row[costCsvHeaderString])


                # If it is the first time that we encounter this key (product), add it to the dictionary
                except KeyError:
                    BillSummaryDict[ key ] = float(row[costCsvHeaderString])
                except Exception as e:
                    logger.error("An exception was thrown while reading row: "+row)
                    logger.exception(e)
                   # raise e

        return lastStartDateBilledConsideredDatetime, BillSummaryDict;

    def _applyBillCorrections(self, BillSummaryDict):
        # This function aggregates services according to these rules:
        #
        #     SpendingCategory, ItemPattern, Example, Description
        #     compute-engine/instances, compute-engine/Vmimage*, com.google.cloud/services/compute-engine/VmimageN1Standard_1, Standard Intel N1 1 VCPU running in Americas
        #     compute-engine/instances, compute-engine/Licensed*, com.google.cloud/services/compute-engine/Licensed1000206F1Micro, Licensing Fee for CentOS 6 running on Micro instance with burstable CPU
        #     compute-engine/network, compute-engine/Network*, com.google.cloud/services/compute-engine/NetworkGoogleEgressNaNa, Network Google Egress from Americas to Americas
        #     compute-engine/network, compute-engine/Network*, com.google.cloud/services/compute-engine/NetworkInterRegionIngressNaNa, Network Inter Region Ingress from Americas to Americas
        #     compute-engine/network, compute-engine/Network*, com.google.cloud/services/compute-engine/NetworkInternetEgressNaApac, Network Internet Egress from Americas to APAC
        #     compute-engine/storage, compute-engine/Storage*, com.google.cloud/services/compute-engine/StorageImage, Storage Image
        #     compute-engine/storage, compute-engine/Storage*, com.google.cloud/services/compute-engine/StoragePdCapacity, Storage PD Capacity
        #     compute-engine/other, , , everything else w/o examples
        #     cloud-storage/storage, cloud-storage/Storage*, com.google.cloud/services/cloud-storage/StorageStandardUsGbsec, Standard Storage US
        #     cloud-storage/network, cloud-storage/Bandwidth*, com.google.cloud/services/cloud-storage/BandwidthDownloadAmerica, Download US EMEA
        #     cloud-storage/operations, cloud-storage/Class*, com.google.cloud/services/cloud-storage/ClassARequest, Class A Operation Request e.g. list obj in bucket ($0.10 per 10,000)
        #     cloud-storage/operations, cloud-storage/Class*, com.google.cloud/services/cloud-storage/ClassBRequest, Class B Operation Request e.g. get obj ($0.01 per 10,000)
        #     cloud-storage/other, , , everything else w/o examples
        #     pubsub, pubsub/*, com.googleapis/services/pubsub/MessageOperations, Message Operations
        #     services, services/*, , Any other service under com.google.cloud/services/* not currently in the examples

        # Constants
        adjustedSupportCostKeyString = 'AdjustedSupport'
        adjustedTotalKeyString = 'AdjustedTotal'
        balanceAtDateKeyString = 'Balance'
        totalKeyString = 'Total'
        ignoredEntries = ['Total', 'AdjustedSupport']

        # using an array of tuples rather than a dictionary to enforce an order
        # (as soon as there's a match, no other entries are checked: higher priority
        # (i.e. more detailed) categories should be entered first
        # (using regex in case future entries need more complex parsing;
        # (there shouldn't be any noticeable performance loss (actually, regex may even be faster than find()!
        # '/' acts as '.' in graphite (i.e. it's a separator)
        spendingCategories = [
                                ('compute-engine.instances', re.compile('com\.google\.cloud/services/compute-engine/(Vmimage|Licensed)')),
                                ('compute-engine.network'  , re.compile('com\.google\.cloud/services/compute-engine/Network')),
                                ('compute-engine.storage'  , re.compile('com\.google\.cloud/services/compute-engine/Storage')),
                                ('compute-engine.other'    , re.compile('com\.google\.cloud/services/compute-engine/')),
                                ('cloud-storage.storage'   , re.compile('com\.google\.cloud/services/cloud-storage/Storage')),
                                ('cloud-storage.network'   , re.compile('com\.google\.cloud/services/cloud-storage/Bandwidth')),
                                ('cloud-storage.operations', re.compile('com\.google\.cloud/services/cloud-storage/Class')),
                                ('cloud-storage.other'     , re.compile('com\.google\.cloud/services/cloud-storage/')),
                                ('pubsub'                  , re.compile('com\.googleapis/services/pubsub/')),
                                ('services'                , re.compile('')) # fallback category
                             ]
        
        egressCategories = [
                                ('compute-engine.egresstotal'  , re.compile('com\.google\.cloud/services/compute-engine/Network.*Egress.')),
                                ('compute-engine.egressoutsideNa'  , re.compile('com\.google\.cloud/services/compute-engine/Network.*Egress((?!NaNa).)')),
                             ]

        CorrectedBillSummaryDict = dict([ (key, 0) for key in [ k for k,v in spendingCategories ] ])
        # use the line above if dict comprehensions are not yet supported
        #CorrectedBillSummaryDict = { key: 0.0 for key in [ k for k,v in spendingCategories ] }

        for entryName, entryValue in BillSummaryDict.items():
            if entryName not in ignoredEntries:
                for categoryName, categoryRegex in spendingCategories:
                    if categoryRegex.match(entryName):
                        try:
                            CorrectedBillSummaryDict[categoryName] += entryValue
                        except KeyError:
                            CorrectedBillSummaryDict[categoryName] = entryValue
                        break
                for categoryName, categoryRegex in egressCategories:
                    if categoryRegex.match(entryName):
                        try:
                            CorrectedBillSummaryDict[categoryName] += entryValue
                        except KeyError:
                            CorrectedBillSummaryDict[categoryName] = entryValue

        # Calculate totals
        CorrectedBillSummaryDict[adjustedSupportCostKeyString] = BillSummaryDict[ adjustedSupportCostKeyString ]
        CorrectedBillSummaryDict[adjustedTotalKeyString] = BillSummaryDict[ totalKeyString ] + BillSummaryDict[ adjustedSupportCostKeyString ]
        CorrectedBillSummaryDict[balanceAtDateKeyString] = self.balanceAtDate - CorrectedBillSummaryDict[adjustedTotalKeyString]

        return CorrectedBillSummaryDict

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

