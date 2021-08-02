import boto3
from boto3.session import Session
from zipfile import ZipFile
import csv
import pprint
import os
from io import StringIO
import re
import datetime, time
import datetime
from datetime import timedelta
import logging
import sys
import traceback
import billing_calculator_hep.graphite
#import graphite
import configparser
import yaml

class AWSBillCalculator(object):
    def __init__(self, account, globalConfig, constants, logger, sumToDate = None):
        self.logger = logger
        self.globalConfig = globalConfig
        # Configuration parameters
        self.outputPath = globalConfig['outputPath']
        # Now, we require AWS.yaml to have a new line in global section, accountDirs to be 0 or 1
        # 1 means bill files are saved in their account subdirs e.g. /home/awsbilling/bill-data/RnD or so
        self.accountDirs = False
        if ("accountDirs" in globalConfig.keys()) and (globalConfig['accountDirs'] != 0):
            self.accountDirs = True
        self.accountName = account
        self.accountProfileName = constants['credentialsProfileName']
        self.accountNumber = constants['accountNumber']
        self.bucketBillingName = constants['bucketBillingName']
        # Expect lastKnownBillDate as '%m/%d/%y %H:%M' : validated when needed
        self.lastKnownBillDate = constants['lastKnownBillDate']
        self.balanceAtDate = constants['balanceAtDate'] # $
        self.applyDiscount = constants['applyDiscount']
        # Expect sumToDate as '%m/%d/%y %H:%M' : validated when needed
        self.sumToDate = sumToDate
        self.logger.debug('Loaded account configuration successfully')

        # Can save state for repetitive calls e.g. for alarms
        self.billCVSAggregateStr = None

        boto3.setup_default_session(profile_name=self.accountProfileName)

    def setLastKnownBillDate(self, lastKnownBillDate):
        self.lastKnownBillDate = lastKnownBillDate

    def setBalanceAtDate(self, balanceAtDate):
        self.balanceAtDate = balanceAtDate

    def setSumToDate(self, sumToDate):
        self.sumToDate = sumToDate

    def CalculateBill(self):
        """Select and download the billing file from S3; aggregate them; calculates sum and
        correct for discounts, data egress waiver, etc.; send data to Graphite

        Args:
            none
        Returns:
            ( lastStartDateBilledConsideredDatetime, BillSummaryDict )
                Example BillSummaryDict:
                 {'AdjustedSupport': 24.450104610658975, 'AWSKeyManagementService': 0.0,
                  'AmazonRoute53': 7.42, 'AmazonSimpleNotificationService': 0.0,
                  'AmazonElasticComputeCloud': 236.5393058537243,
                  'AmazonSimpleQueueService': 0.0, 'TotalDataOut': 0.0,
                  'AmazonSimpleStorageService': 0.15311901797500035,
                  'Balance': 299731.0488492827, 'Total': 244.50104610658974,
                  'AWSSupportBusiness': 0.38862123489039674,
                  'AdjustedTotal': 268.9511507172487
                 }
        """

        # Load data in memory
        if self.billCVSAggregateStr == None:
            fileNameForDownloadList = self._downloadBillFiles()
            self.billCVSAggregateStr = self._aggregateBillFiles( fileNameForDownloadList );

        lastStartDateBilledConsideredDatetime, BillSummaryDict = self._sumUpBillFromDateToDate( self.billCVSAggregateStr, self.lastKnownBillDate, self.sumToDate );
        

        CorrectedBillSummaryDict = self._applyBillCorrections(BillSummaryDict);
        if "AccountName" not in CorrectedBillSummaryDict:
            CorrectedBillSummaryDict["AccountName"] = self.accountName

        self.logger.info('Bill Computation for %s Account Finished at %s' % ( self.accountName, time.strftime("%c") ))
        self.logger.info('Last Start Date Billed Considered : ' + lastStartDateBilledConsideredDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info('Last Known Balance :' + str(self.balanceAtDate))
        self.logger.info('Date of Last Known Balance : ' + self.lastKnownBillDate)
        self.logger.debug('BillSummaryDict:'.format(BillSummaryDict))
        self.logger.debug('CorrectedBillSummaryDict'.format(CorrectedBillSummaryDict))

        return lastStartDateBilledConsideredDatetime, CorrectedBillSummaryDict


    def sendDataToGraphite(self, CorrectedBillSummaryDict ):
        """Send the corrected bill summary dictionary to the Graphana dashboard for the
        bill information
        Args:
            CorrectedBillSummaryDict: the billing data to send Graphite.
                 Example dict:
                 {'AdjustedSupport': 24.450104610658975, 'AWSKeyManagementService': 0.0,
                  'AmazonRoute53': 7.42, 'AmazonSimpleNotificationService': 0.0,
                  'AmazonElasticComputeCloud': 236.5393058537243,
                  'AmazonSimpleQueueService': 0.0, 'TotalDataOut': 0.0,
                  'AmazonSimpleStorageService': 0.15311901797500035,
                  'Balance': 299731.0488492827, 'Total': 244.50104610658974,
                  'AWSSupportBusiness': 0.38862123489039674,
                  'AdjustedTotal': 268.9511507172487
                 }

        Returns:
            none
        """

        graphiteHost=self.globalConfig['graphite_host']
        graphiteContext=self.globalConfig['graphite_context_billing'] + str(self.accountName)

        graphiteEndpoint = graphite.Graphite(host=graphiteHost)
        graphiteEndpoint.send_dict(graphiteContext, CorrectedBillSummaryDict, send_data=True)


    def _obtainRoleBasedSession(self):
        """ Obtain a short-lived role-based token
        """

        roleNameString = 'CalculateBill'
        fullRoleNameString = 'arn:aws:iam::' + str(self.accountNumber) + ':role/' + roleNameString

        # using boto3 default session to obtain temporary token
        # long term credentials have ONLY the permission to assume role CalculateBill
        client = boto3.client('sts')
        response = client.assume_role( RoleArn=fullRoleNameString, RoleSessionName='roleSwitchSession'  )
        pprint.pprint(response)

        role_AK_id = response['Credentials']['AccessKeyId']
        role_AK_sc = response['Credentials']['SecretAccessKey']
        role_AK_tk = response['Credentials']['SessionToken']

        self.logger.debug('Opening Role-based Session for account %s with temporary key for role %s' % (self.accountName, fullRoleNameString))
        session = Session(aws_access_key_id=role_AK_id, aws_secret_access_key=role_AK_sc, aws_session_token=role_AK_tk)
        return session


    def _downloadBillFiles(self ):
        # Identify what files need to be downloaded, given the last known balance date
        # Download the files from S3

        session = self._obtainRoleBasedSession()

        s3 = session.client('s3')
        filesObjsInBucketDict = s3.list_objects(Bucket=self.bucketBillingName)
        filesDictList = filesObjsInBucketDict['Contents']
        # Assumption: sort files by date using file name: this is true if file name convention is maintained
        filesDictList.sort(key=lambda filesDict: filesDict['Key'])

        # Extract file creation date from the file name
        # Assume a format such as this: 950490332792-aws-billing-detailed-line-items-2015-09.csv.zip
        billingFileNameIdentifier = 'aws\-billing.*\-20[0-9][0-9]\-[0-9][0-9].csv.zip'
        billingFileMatch = re.compile(billingFileNameIdentifier)
        billingFileDateIdentifier = '20[0-9][0-9]\-[0-9][0-9]'
        dateExtractionMatch = re.compile(billingFileDateIdentifier)
        lastKnownBillDateDatetime = datetime.datetime(*(time.strptime(self.lastKnownBillDate, '%m/%d/%y %H:%M')[0:6]))

        self.logger.debug('lastKnownBillDate ' +  self.lastKnownBillDate)
        fileNameForDownloadList = []
        previousFileForDownloadListDateTime = None
        previousFileNameForDownloadListString = None
        noFileNameMatchesFileNameIdentifier = True
        for filesDict in filesDictList:
           self.logger.debug('File in bucket ' + self.bucketBillingName + ' : ' +  filesDict['Key'])
           # Is the file a billing file?
           if billingFileMatch.search(filesDict['Key']) is None:
               continue
           else:
               noFileNameMatchesFileNameIdentifier = False
           # extract date from file
           dateMatch = dateExtractionMatch.search(filesDict['Key'])
           if dateMatch is None:
             logger.exception('Cannot identify date in billing file name ' + filesDict['Key'] + ' with regex = "' + billingFileDateIdentifier + '"')
             raise Exception('Cannot identify date in billing file name ' + filesDict['Key'] + ' with regex = "' + billingFileDateIdentifier + '"')
           date = dateMatch.group(0)
           billDateDatetime = datetime.datetime(*(time.strptime(date, '%Y-%m')[0:6]))
           self.logger.debug('Date extracted from file: ' + billDateDatetime.strftime('%m/%d/%y %H:%M'))

           # Start by putting the current file and file start date in the previous list
           if not previousFileNameForDownloadListString:
               previousFileNameForDownloadListString = filesDict['Key']
               previousFileForDownloadListDateTime = billDateDatetime
               self.logger.debug('previousFileForDownloadListDateTime ' + previousFileForDownloadListDateTime.strftime('%m/%d/%y %H:%M'))
               self.logger.debug('previousFileNameForDownloadListString ' + previousFileNameForDownloadListString)
               self.logger.debug('fileNameForDownloadList:'.format(fileNameForDownloadList))
               continue

           # if the last known bill date is past the start date of the previous file...
           if lastKnownBillDateDatetime > previousFileForDownloadListDateTime:
               self.logger.debug('lastKnownBillDateDatetime > previousFileForDownloadListDateTime: ' + lastKnownBillDateDatetime.strftime('%m/%d/%y %H:%M') + ' > ' + previousFileForDownloadListDateTime.strftime('%m/%d/%y %H:%M'))
               # if the previous file starts and end around the last known bill date,
               # add previous and current file name to the list
               if lastKnownBillDateDatetime < billDateDatetime:
                   fileNameForDownloadList = [ previousFileNameForDownloadListString, filesDict['Key'] ];
                   self.logger.debug('lastKnownBillDateDatetime < billDateDatetime: ' + lastKnownBillDateDatetime.strftime('%m/%d/%y %H:%M') + ' < ' + billDateDatetime.strftime('%m/%d/%y %H:%M'))
                   self.logger.debug('fileNameForDownloadList:'.format(fileNameForDownloadList))
               previousFileForDownloadListDateTime = billDateDatetime
               previousFileNameForDownloadListString = filesDict['Key']
               self.logger.debug('previousFileForDownloadListDateTime ' + previousFileForDownloadListDateTime.strftime('%m/%d/%y %H:%M'))
               self.logger.debug('previousFileNameForDownloadListString ' + previousFileNameForDownloadListString)

           else:
               if not fileNameForDownloadList:
                  fileNameForDownloadList = [ previousFileNameForDownloadListString ]
               # at this point, all the files have a start date past the last known bill date: we want those files
               fileNameForDownloadList.append(filesDict['Key'])
               self.logger.debug('fileNameForDownloadList:'.format(fileNameForDownloadList))

        if noFileNameMatchesFileNameIdentifier:
           self.logger.exception('No billing files found in bucket ' + self.bucketBillingName + ' looking for patterns containing ' + billingFileNameIdentifier)
           raise Exception('No billing files found in bucket ' + self.bucketBillingName + ' looking for patterns containing ' + billingFileNameIdentifier)

        # After looking at all the files, if their start date is always older than the last known billing date,
        # we take the last file
        if fileNameForDownloadList == []:
            fileNameForDownloadList = [ filesDict['Key'] ]
        self.logger.debug('fileNameForDownloadList:'.format(fileNameForDownloadList))

        new_fileNameForDownloadList = []
        for fileNameForDownload in fileNameForDownloadList:
            outputfile = os.path.join(self.outputPath, fileNameForDownload) if self.accountDirs is False else os.path.join(self.outputPath, self.accountName, fileNameForDownload)
            s3.download_file(self.bucketBillingName, fileNameForDownload, outputfile)
            new_fileNameForDownloadList.append(outputfile)
        return new_fileNameForDownloadList


    def _aggregateBillFiles(self, zipFileList ):
       # Unzip files and aggregate billing info in a single dictionary

       # Since Feb 2016, the csv file has two new field: RecordId (as new 5th column) and
       # ResourceId (last column)
       # If we are merging files with old and new format, we need to add empty
       # columns to preserve the format and allow the cvs module to work properly
       # Here we add the new columns to the old format in any case

       # Constants
       billingFileNameNewFormatIdentifiew = '.*with\-resources\-and\-tags\-.*.csv.zip'
       billingFileNameNewFormatMatch = re.compile(billingFileNameNewFormatIdentifiew)
       newLastColumnHeaderString = 'ResourceId'
       new5thColumnHeaderString = 'RecordId'
       old4thColumnHeaderString = 'RecordType'
       billCVSAggregateStr = ''
       newFormat = True
       for zipFileName in zipFileList:
         zipFileNameBase = os.path.basename( zipFileName )
         # Check if file is in new or old format
         if billingFileNameNewFormatMatch.search(zipFileName) is None:
             newFormat = False
         else:
             newFormat = True

         # Read in files for the merging
         zipFile = ZipFile(zipFileName, 'r')
         billingFileName = zipFileNameBase.rstrip('.zip')
         billCSVStr = zipFile.read(billingFileName)
         billCSVStr = billCSVStr.decode("utf-8")

         # Remove the header for all files except the first
         if billCVSAggregateStr != '':
             billCSVStr = re.sub('^.*\n','',billCSVStr,count=1)

         # If the file is in the old format, add the missing fields for every row
         if not newFormat:
             lineArray = billCSVStr.splitlines()
             firstLine = True
             for line in lineArray:
                # If the file is in the old format, add the new columns to the header
                if firstLine and billCVSAggregateStr == '':
                  firstLine = False
                  billCSVStr = re.sub(old4thColumnHeaderString,old4thColumnHeaderString+','+new5thColumnHeaderString,line) +\
                      ','+newLastColumnHeaderString+'\n'

                  continue

                #Put lines back together adding missing fields
                recordList=line.split(',')
                billCSVStr = billCSVStr + ','.join(recordList[0:4]) + ',,' + ','.join(recordList[4:]) + ',\n'

         # aggregate data from all files
         billCVSAggregateStr = billCVSAggregateStr + billCSVStr
       return billCVSAggregateStr;

    def _sumUpBillFromDateToDate(self, billCVSAggregateStr , sumFromDate, sumToDate = None):
        # CSV Billing file format documentation:
        #
        # UnBlendedCost : the corrected cost of each item; unblended from the accounts under
        # single master / payer account
        #
        # ProductName : S3, EC2, etc
        #
        # ItemDescription = contains("data transferred out") holds information about
        # charges due to data transfers out
        #
        #  Returns:
        #               BillSummaryDict: (Keys depend on services present in the csv file)
        #                    {'AmazonSimpleQueueService': 0.0,
        #                     'AmazonSimpleNotificationService': 0.0,
        #                     'AWSKeyManagementService': 0.0,
        #                     'EstimatedTotalDataOut': 0.0033834411000000018,
        #                     'AmazonElasticComputeCloud': 0.24066755999999997,
        #                     'AWSCloudTrail': 0.0,
        #                     'AmazonSimpleStorageService': 0.38619119999999818,
        #                     'TotalDataOut': 0.0,
        #                     'Total': 0.62769356699999868,
        #                     'AWSSupportBusiness': 0.00083480700000000642}


        # Constants
        itemDescriptionCsvHeaderString = 'ItemDescription'
        ProductNameCsvHeaderString = 'ProductName'
        totalDataOutCsvHeaderString = 'TotalDataOut'
        estimatedTotalDataOutCsvHeaderString = 'EstimatedTotalDataOut'
        usageQuantityHeaderString = 'UsageQuantity'
        unBlendedCostCsvHeaderString = 'UnBlendedCost'
        usageStartDateCsvHeaderString = 'UsageStartDate'
        totalCsvHeaderString = 'Total'

        adjustedSupportCostKeyString = 'AdjustedSupport'
        awsSupportBusinessCostKeyString = 'AWSSupportBusiness'

        educationalGrantRowIdentifyingString = 'EDU_'
        unauthorizedUsageString = 'Unauthorized Usage'
        costOfGBOut = 0.09 # Assume highest cost of data transfer out per GB in $

        sumFromDateDatetime = datetime.datetime(*(time.strptime(sumFromDate, '%m/%d/%y %H:%M')[0:6]))
        lastStartDateBilledConsideredDatetime = sumFromDateDatetime
        if sumToDate != None:
            sumToDateDatetime = datetime.datetime(*(time.strptime(sumToDate, '%m/%d/%y %H:%M')[0:6]))
        BillSummaryDict = { totalCsvHeaderString : 0.0 , totalDataOutCsvHeaderString : 0.0, \
                            estimatedTotalDataOutCsvHeaderString : 0.0, adjustedSupportCostKeyString : 0.0 }

        # Counters to calculate tiered support cost
        totalForPreviousMonth = 0
        currentMonth = ''

        # The seek(0) resets the csv iterator, in case of multiple passes e.g. in alarm calculations
        billCVSAggregateStrStringIO = StringIO(billCVSAggregateStr)
        billCVSAggregateStrStringIO.seek(0)
        for row in csv.DictReader(billCVSAggregateStrStringIO):
            # Skip if there is no date (e.g. final comment lines)
            if row[usageStartDateCsvHeaderString] == '' :
               continue;

            # Skip rows whose UsageStartDate is prior to sumFromDate and past sumToDate
            usageStartDateDatetime = datetime.datetime(*(time.strptime(row[usageStartDateCsvHeaderString], '%Y-%m-%d %H:%M:%S')[0:6]))
            if usageStartDateDatetime < sumFromDateDatetime :
               continue;

            if sumToDate != None:
                if usageStartDateDatetime > sumToDateDatetime :
                    continue;

            if usageStartDateDatetime > lastStartDateBilledConsideredDatetime:
               lastStartDateBilledConsideredDatetime = usageStartDateDatetime

            # Sum up the costs
            try:
                # Don't add up lines that are corrections for the educational grant, the unauthorized usage, or the final Total
                if row[itemDescriptionCsvHeaderString].find(educationalGrantRowIdentifyingString) == -1 and \
                   row[itemDescriptionCsvHeaderString].find(unauthorizedUsageString) == -1 and \
                   row[itemDescriptionCsvHeaderString].find(totalCsvHeaderString) == -1 :
                    #Py2.7: string.translate(row[ProductNameCsvHeaderString], None, ' ()')
                    #Ported to py3 is: str.maketrans('','',' ()'))
                    key = row[ProductNameCsvHeaderString].translate(str.maketrans('','',' ()'))

                    # Don't add up lines that don't have a key e.g. final comments in the csv file
                    if key != '':
                        # Calculate support cost at the end of the month
                        # For the first row, we initialize the current month
                        if currentMonth == '':
                             currentMonth = usageStartDateDatetime.month
                        else:
                            # If this row is for a new month, then we calculate the support cost
                            if  currentMonth != usageStartDateDatetime.month:
                                monthlySupportCost = self._calculateTieredSupportCost( BillSummaryDict[ totalCsvHeaderString ] - totalForPreviousMonth )
                                BillSummaryDict[ adjustedSupportCostKeyString ] += monthlySupportCost
                                currentMonth = usageStartDateDatetime.month
                                self.logger.debug('New month: %d. Calculated support at %f for total cost at %f. Total support at %f Last row considered:' % \
                                    (usageStartDateDatetime.month, monthlySupportCost, BillSummaryDict[ totalCsvHeaderString ], BillSummaryDict[ adjustedSupportCostKeyString ] ))
                                self.logger.debug(row)
                                totalForPreviousMonth = BillSummaryDict[ totalCsvHeaderString ]

                        # Add up cost per product (i.e. key) and total cost
                        BillSummaryDict[ key ] += float(row[unBlendedCostCsvHeaderString])
                        # Do not double count support from AWS billing
                        if key != awsSupportBusinessCostKeyString:
                            BillSummaryDict[ totalCsvHeaderString ] += float(row[unBlendedCostCsvHeaderString])

                        # Add up all data transfer charges separately
                        if row[itemDescriptionCsvHeaderString].find('data transferred out') != -1:
                           BillSummaryDict[ totalDataOutCsvHeaderString ] += float(row[unBlendedCostCsvHeaderString])
                           BillSummaryDict[ estimatedTotalDataOutCsvHeaderString ] += float(row[usageQuantityHeaderString]) * costOfGBOut


            # If it is the first time that we encounter this key (product), add it to the dictionary
            except KeyError:
                BillSummaryDict[ key ] = float(row[unBlendedCostCsvHeaderString])
                if key != awsSupportBusinessCostKeyString:
                    BillSummaryDict[ totalCsvHeaderString ] += float(row[unBlendedCostCsvHeaderString])

        # Calculates the support for the last part of the month
        monthlySupportCost = self._calculateTieredSupportCost( BillSummaryDict[ totalCsvHeaderString ] - totalForPreviousMonth )
        BillSummaryDict[ adjustedSupportCostKeyString ] += monthlySupportCost
        self.logger.info('Final support calculation. Month: %d. Calculated support at %f for total cost at %f. Total support at %f' % \
                (usageStartDateDatetime.month, monthlySupportCost, BillSummaryDict[ totalCsvHeaderString ], BillSummaryDict[ adjustedSupportCostKeyString ] ))

        return lastStartDateBilledConsideredDatetime, BillSummaryDict;


    def _calculateTieredSupportCost(self, monthlyCost):
        """ Calculate support cost FOR A GIVEN MONTH, using tiered definition below
            As of Mar 3, 2016:
                 10% of monthly AWS usage for the first $0-$10K
                  7% of monthly AWS usage from $10K-$80K
                  5% of monthly AWS usage from $80K-$250K
                  3% of monthly AWS usage over $250K
        Args:
            monthlyCost: the cost incurred in a given month
        Returns:
            supportCost
        """
        adjustedSupportCost = 0
        if monthlyCost < 10000:
            adjustedSupportCost = 0.10 * monthlyCost
        else:
            adjustedSupportCost = 0.10 * 10000
            if monthlyCost < 80000:
                adjustedSupportCost += 0.07 * (monthlyCost - 10000)
            else:
                adjustedSupportCost += 0.07 * (80000 - 10000)
                if monthlyCost < 250000:
                    adjustedSupportCost += + 0.05 * (monthlyCost - 80000)
                else:
                    adjustedSupportCost += + 0.05 * (250000 - 80000)
                    adjustedSupportCost += + 0.03 * (monthlyCost - 250000)
        return adjustedSupportCost

    def _applyBillCorrections(self, BillSummaryDict):
        # Need to apply corrections from the csv files coming from Amazon to reflect the final
        # bill from DLT
        # 1) The S3 .csv never includes support charges because it isn't available in the
        #    source data. It can be calculated at the 10% of spend, before applying any
        #    discounts
        # 2) the .csv does not include the DLT discount of 7.25%. For all of the non-data
        #    egress charges, it shows LIST price (DLT Orbitera reflects the discount)
        # 3) Currently (Nov 2015), the .csv files zero out all data egress costs.
        #    According to the data egress waiver contract, it is supposed to zero out up to
        #    15% of the total cost. This correction may need to be applied in the
        #    future

        # Constants
        vendorDiscountRate = 0.0725 # 7.25%
        adjustedSupportCostKeyString = 'AdjustedSupport'
        adjustedTotalKeyString = 'AdjustedTotal'
        balanceAtDateKeyString = 'Balance'
        totalKeyString = 'Total'


        # Apply vendor discount if funds are NOT on credit
        if self.applyDiscount:
            reductionRateDueToDiscount = 1 - vendorDiscountRate
        else:
            reductionRateDueToDiscount = 1

        CorrectedBillSummaryDict = { }
        for key in BillSummaryDict:
            # Discount does not apply to business support
            if key != adjustedSupportCostKeyString:
                CorrectedBillSummaryDict[key] = reductionRateDueToDiscount * BillSummaryDict[key]
            else:
                CorrectedBillSummaryDict[key] = BillSummaryDict[key]
        # Calculate total
        CorrectedBillSummaryDict[adjustedTotalKeyString] = CorrectedBillSummaryDict['Total'] + CorrectedBillSummaryDict['AdjustedSupport']

        CorrectedBillSummaryDict['Balance'] = self.balanceAtDate - CorrectedBillSummaryDict['AdjustedTotal']

        return CorrectedBillSummaryDict

class AWSBillAlarm(object):
    
    def __init__(self, calculator, account, globalConfig, constants, logger):
        self.logger = logger
        self.globalConfig = globalConfig
        self.accountName = account
        self.calculator = calculator
        self.costRatePerHourInLastSixHoursAlarmThreshold = constants['costRatePerHourInLastSixHoursAlarmThreshold']
        self.costRatePerHourInLastDayAlarmThreshold = constants['costRatePerHourInLastDayAlarmThreshold']
        self.burnRateAlarmThreshold = constants['burnRateAlarmThreshold']
        self.timeDeltaforCostCalculations = constants['timeDeltaforCostCalculations']
        self.graphiteHost=globalConfig['graphite_host']
        self.grafanaDashboard=globalConfig['grafana_dashboard']


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
        messageHeader = 'AWS Billing Alarm Message for account %s - %s\n' % ( self.accountName, time.strftime("%c") )
        messageHeader += 'AWS Billing Dashboard - %s\n\n' % ( self.grafanaDashboard )

        if alarmConditionsDict['costRatePerHourInLastDay'] > \
           self.costRatePerHourInLastSixHoursAlarmThreshold:
            alarmMessage = messageHeader
            alarmMessage += 'Alarm threshold surpassed for cost rate per hour in the last six hours\n'
            alarmMessage += "Cost in the last six hours: $ %f\n" % alarmConditionsDict['costInLastSixHours']
            alarmMessage += 'Cost rate per hour in the last six hours: $%f / h\n' %  alarmConditionsDict['costRatePerHourInLastSixHours']
            alarmMessage += 'Set Alarm Threshold on six hours cost rate: $%f / h\n\n' % self.costRatePerHourInLastSixHoursAlarmThreshold

        if alarmConditionsDict['costRatePerHourInLastDay'] > \
           self.costRatePerHourInLastDayAlarmThreshold:
            if alarmMessage is None:
                alarmMessage = messageHeader
            alarmMessage += 'Alarm threshold surpassed for cost rate per hour in the last day\n'
            alarmMessage += "Cost in the last day: $ %f\n" % alarmConditionsDict['costInLastDay']
            alarmMessage += 'Cost rate per hour in the last day: $%f / h\n' %  alarmConditionsDict['costRatePerHourInLastDay']
            alarmMessage += 'Set Alarm Threshold on one day cost rate: $%f / h\n' % self.costRatePerHourInLastDayAlarmThreshold
        if alarmConditionsDict['Balance'] - \
                self.timeDeltaforCostCalculations*alarmConditionsDict['costRatePerHourInLastSixHours'] <= \
                self.burnRateAlarmThreshold:
            if alarmMessage is None:
                alarmMessage = messageHeader
            alarmMessage += 'Alarm: account is approaching the balance\n'
            alarmMessage += "Current balance: $ %f\n" % (alarmConditionsDict['Balance'],)
            alarmMessage += 'Cost rate per hour: $%f / h for last %s hours\n' %  (alarmConditionsDict['costRatePerHourInLastSixHours'], self.timeDeltaforCostCalculations)
            alarmMessage += 'Set Alarm Threshold on burn rate: $%f\n' % (self.burnRateAlarmThreshold,)

        return alarmMessage

    def ExtractAlarmConditions(self):
        """Extract the alarm conditions from the billing data. For now, focusing on cost
        rates.

        Returns: alarmConditionsDict
           Example alarmConditionsDict:
            {  'costInLastSixHours': 9.889187795409999,
               'costRatePerHourInLastSixHoursAlarmThreshold': 20,
               'costRatePerHourInLastDay': 0.7534264869301031,
               'costRatePerHourInLastDayAlarmThreshold': 20,
               'costRatePerHourInLastSixHours': 1.6481979659016666,
               'costInLastDay': 18.082235686322473
            }
        """

        # Get total and last date billed
        lastStartDateBilledDatetime, CorrectedBillSummaryNowDict = self.calculator.CalculateBill()
        dateNow = datetime.datetime.now()

        # Get cost in the last 6 hours
        sixHoursBeforeLastDateBilledDatetime = lastStartDateBilledDatetime - timedelta(hours=6)
        self.calculator.setLastKnownBillDate(sixHoursBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        newLastStartDateBilledDatetime, CorrectedBillSummarySixHoursBeforeDict = self.calculator.CalculateBill()

        costInLastSixHours = CorrectedBillSummarySixHoursBeforeDict['AdjustedTotal']
        costRatePerHourInLastSixHours = costInLastSixHours / 6

        # Get cost in the last 24 hours
        oneDayBeforeLastDateBilledDatetime = lastStartDateBilledDatetime - timedelta(hours=24)
        self.calculator.setLastKnownBillDate(oneDayBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        newLastStartDateBilledDatetime, CorrectedBillSummaryOneDayBeforeDict = self.calculator.CalculateBill()

        costInLastDay = CorrectedBillSummaryOneDayBeforeDict['AdjustedTotal']
        costRatePerHourInLastDay = costInLastDay / 24

        dataDelay = int((time.mktime(dateNow.timetuple()) - time.mktime(lastStartDateBilledDatetime.timetuple())) / 3600)

        self.logger.info('Alarm Computation for %s Account Finished at %s' % ( self.accountName, time.strftime("%c") ))
        self.logger.info('Last Start Date Billed Considered: ' + lastStartDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info('Now' + dateNow.strftime('%m/%d/%y %H:%M'))
        self.logger.info( 'delay between now and Last Start Date Billed Considered in hours'+ str(dataDelay))
        self.logger.info( 'Six hours before that: ' + sixHoursBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info( 'One day before that: ' + oneDayBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info( 'Adjusted Total Now from Date of Last Known Balance: $'+ str(CorrectedBillSummaryNowDict['AdjustedTotal']))
        self.logger.info( 'Cost In the Last Six Hours: $'+ str(costInLastSixHours))
        self.logger.info( 'Cost Rate Per Hour In the Last Six Hours: $'+ str(costRatePerHourInLastSixHours) + ' / h')
        self.logger.info( 'Alarm Threshold on that: $'+ str(self.costRatePerHourInLastSixHoursAlarmThreshold))
        self.logger.info( 'Cost In the Last Day: $'+ str(costInLastDay))
        self.logger.info( 'Cost Rate Per Hour In the Last Day: $'+ str(costRatePerHourInLastDay)+ ' / h')
        self.logger.info( 'Alarm Threshold on that: $'+ str(self.costRatePerHourInLastDayAlarmThreshold))

        alarmConditionsDict = { 'costInLastSixHours' : costInLastSixHours, \
                                'costRatePerHourInLastSixHours' : costRatePerHourInLastSixHours, \
                                'costRatePerHourInLastDayAlarmThreshold' : self.costRatePerHourInLastSixHoursAlarmThreshold, \
                                'costInLastDay' : costInLastDay, \
                                'costRatePerHourInLastDay' : costRatePerHourInLastDay, \
                                'costRatePerHourInLastSixHoursAlarmThreshold' : self.costRatePerHourInLastDayAlarmThreshold,
                                'delayTolastStartDateBilledDatetime': dataDelay,
                                'Balance': CorrectedBillSummaryNowDict['Balance'],
                                'timeDeltaforCostCalculations': self.timeDeltaforCostCalculations,
                                'burnRateAlarmThreshold': self.burnRateAlarmThreshold
                                 }

        self.logger.debug("alarmConditionsDict".format(alarmConditionsDict))

        return alarmConditionsDict

    def sendDataToGraphite(self, alarmConditionsDict ):
        """Send the alarm condition dictionary to the Graphana dashboard

        Args:
            alarmConditionsDict: the alarm data to send Graphite.
                 Example dict:
                    {  'costInLastSixHours': 9.889187795409999,
                       'costRatePerHourInLastSixHoursAlarmThreshold': 20,
                       'costRatePerHourInLastDay': 0.7534264869301031,
                       'costRatePerHourInLastDayAlarmThreshold': 20,
                       'costRatePerHourInLastSixHours': 1.6481979659016666,
                       'costInLastDay': 18.082235686322473
                    }

        Returns:
            none
        """

        graphiteContext=self.globalConfig['graphite_context_alarms'] + str(self.accountName)

        graphiteEndpoint = graphite.Graphite(host=self.graphiteHost)
        graphiteEndpoint.send_dict(graphiteContext, alarmConditionsDict, send_data=True)

class AWSBillDataEgress(object):

    #alarm = GCEBillAlarm(calculator, account, config, logger)

    def __init__(self, calculator, account, globalConfig, constants, logger):
        self.globalConfig = globalConfig
        # Configuration parameters
        self.accountName = account
        self.calculator = calculator
        self.logger = logger
        self.graphiteHost = globalConfig['graphite_host']
        
        
    def ExtractDataEgressConditions(self):
        """Extract the data egress conditions from the billing data.   
        
        Returns: dataEgressConditionsDict
           Example dataEgressConditionsDict:
            { 'costInLastTwoDays': 188.09057763476676, 
              'costOfDataEgressInLastTwoDays': 0.019326632849999987, 
              'percentageOfEgressInLastTwoDays': 0.010275173319701498, 
              'costFromFirstOfMonth': 5840.722959302295, 
              'costOfDataEgressFromFirstOfMonth': 949.5988685657911, 
              'percentageOfEgressFromFirstOfMonth': 16.25824191940831
            }
        """

        # Get total and last date billed 
        lastStartDateBilledDatetime, CorrectedBillSummaryNowDict = self.calculator.CalculateBill()
        
        # Get costs in the last 48 hours
        twoDaysBeforeLastDateBilledDatetime = lastStartDateBilledDatetime - timedelta(hours=48)
        self.calculator.setLastKnownBillDate(twoDaysBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        newLastStartDateBilledDatetime, CorrectedBillSummaryTwoDaysBeforeDict = self.calculator.CalculateBill()
        
        costOfDataEgressInLastTwoDays = CorrectedBillSummaryTwoDaysBeforeDict['EstimatedTotalDataOut']
        costInLastTwoDays = CorrectedBillSummaryTwoDaysBeforeDict['AdjustedTotal'] + costOfDataEgressInLastTwoDays
        percentageDataEgressOverTotalCostInLastTwoDays = costOfDataEgressInLastTwoDays / costInLastTwoDays * 100

        # Get costs since the first of the month
        lastStartDateBilledFirstOfMonthDatetime = datetime.datetime(lastStartDateBilledDatetime.year, lastStartDateBilledDatetime.month, 1)
        self.calculator.setLastKnownBillDate(lastStartDateBilledFirstOfMonthDatetime.strftime('%m/%d/%y %H:%M'))
        newLastStartDateBilledDatetime, CorrectedBillSummaryFirstOfMonthDict = self.calculator.CalculateBill()
        
        costOfDataEgressFromFirstOfMonth = CorrectedBillSummaryFirstOfMonthDict['EstimatedTotalDataOut']
        costFromFirstOfMonth = CorrectedBillSummaryFirstOfMonthDict['AdjustedTotal'] + costOfDataEgressFromFirstOfMonth
        percentageDataEgressOverTotalCostFromFirstOfMonth = costOfDataEgressFromFirstOfMonth / costFromFirstOfMonth * 100


        self.logger.info( 'Account: ' + self.accountName)
        self.logger.info( 'Last Start Date Billed: ' + lastStartDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info( 'Two days before that: ' + twoDaysBeforeLastDateBilledDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info( 'First of the month: ' +  lastStartDateBilledFirstOfMonthDatetime.strftime('%m/%d/%y %H:%M'))
        self.logger.info( 'Adjusted Total Now from Date of Last Known Balance: $' + str(CorrectedBillSummaryNowDict['AdjustedTotal']))
        self.logger.info( 'Adjusted Estimated Data Egress Now from Date of Last Known Balance: $'+ str(CorrectedBillSummaryNowDict['EstimatedTotalDataOut']))
        self.logger.info( 'Adjusted Cost (estimtated as Total + Data Egress costs) In the Last Two Days: $'+str(costInLastTwoDays))
        self.logger.info( 'Adjusted Cost Of Data Egress (Estimated) In the Last Two Days: $'+str(costOfDataEgressInLastTwoDays))
        self.logger.info( 'Percentage In the Last Two Days:'+ str(percentageDataEgressOverTotalCostInLastTwoDays)+'%')
        self.logger.info( 'Adjusted Cost (estimtated as Total + Data Egress costs) From The First Of The Month: $' + str(costFromFirstOfMonth))
        self.logger.info( 'Adjusted Cost Of Data Egress (Estimated) From The First Of The Month: $' + str(costOfDataEgressFromFirstOfMonth))
        self.logger.info( 'Percentage From The First Of The Month:' + str(percentageDataEgressOverTotalCostFromFirstOfMonth)+ '%')

        dataEgressConditionsDict = { 'costInLastTwoDays' : costInLastTwoDays, \
                                     'costOfDataEgressInLastTwoDays' : costOfDataEgressInLastTwoDays, \
                                     'percentageOfEgressInLastTwoDays' : percentageDataEgressOverTotalCostInLastTwoDays, \
                                     'costFromFirstOfMonth' : costFromFirstOfMonth, \
                                     'costOfDataEgressFromFirstOfMonth' : costOfDataEgressFromFirstOfMonth, \
                                     'percentageOfEgressFromFirstOfMonth' : percentageDataEgressOverTotalCostFromFirstOfMonth }

        self.logger.debug('dataEgressConditionsDict'.format(dataEgressConditionsDict))

        return dataEgressConditionsDict

    def sendDataToGraphite(self, dataEgressConditionsDict ):
        """Send the data egress condition dictionary to the Graphana dashboard 
        
        Args: 
           dataEgressConditionsDict: the data egress costs and calculations to send Graphite
                Example dataEgressConditionsDict:
                    { 'costInLastTwoDays': 188.09057763476676, 
                      'costOfDataEgressInLastTwoDays': 0.019326632849999987, 
                      'percentageOfEgressInLastTwoDays': 0.010275173319701498, 
                      'costFromFirstOfMonth': 5840.722959302295, 
                      'costOfDataEgressFromFirstOfMonth': 949.5988685657911, 
                      'percentageOfEgressFromFirstOfMonth': 16.25824191940831
                    }

        Returns: 
            none
        """
        
        graphiteContext=self.globalConfig['graphite_context_egress'] + str(self.accountName)
        graphiteEndpoint = graphite.Graphite(host=self.graphiteHost)
        graphiteEndpoint.send_dict(graphiteContext, dataEgressConditionsDict,  send_data=True)



if __name__ == "__main__":

    os.setuid(53431)
    logger = logging.getLogger("AWS-UNIT-TEST")
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
        FORMAT='%(asctime)s %(levelname)-4s %(message)s'
        #FORMAT="%(asctime)s:%(levelname)s:%(message)s"
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

    AWSconstants = '/etc/hepcloud/config.d/AWS_test.yaml'
    with open(AWSconstants, 'r') as stream:
        config = yaml.safe_load(stream)
    
    globalDict = config['global']

    logger.info("--------------------------- Start of calculation cycle {0} ------------------------------".format(time.strftime("%c")))

    for constantDict in config['accounts']:
        account = constantDict['accountName']
        try:
            os.chdir(os.environ.get('BILL_DATA_DIR'))
            logger.info("[UNIT TEST] Starting Billing Analysis for AWS {0} account".format(account))
            calculator = AWSBillCalculator(account, globalDict, constantDict, logger)
            lastStartDateBilledConsideredDatetime, \
            CorrectedBillSummaryDict = calculator.CalculateBill()

            logger.info("[UNIT TEST] Starting Alarm calculations for AWS {0} account".format(account))
            alarm = AWSBillAlarm(calculator, account, globalDict, constantDict, logger)
            message = alarm.EvaluateAlarmConditions(publishData = True)

            logger.info("[UNIT TEST] Starting Data Egress calculations for AWS {0} account".format(account))
            billDataEgress = AWSBillDataEgress(calculator, account, globalDict, constantDict, logger)
            dataEgressConditionsDict = billDataEgress.ExtractDataEgressConditions()

            calculator.sendDataToGraphite(CorrectedBillSummaryDict)
        except Exception as error:
            logger.info("--------------------------- End of calculation cycle {0} with ERRORS ------------------------------".format(time.strftime("%c")))
            logger.exception(error)
            continue

    logger.info("--------------------------- End of calculation cycle {0} ------------------------------".format(time.strftime("%c")))
