#!/usr/bin/python3.4

import logging
import logging.handlers
import sys
import os
import time
import schedule
import configparser
import pwd
import socket
import traceback
import threading
import yaml

from bill-calculator-hep import GCEBillAnalysis, GCEBillCalculator, GCEBillAlarm
from bill-calculator-hep import AWSBillAnalysis, AWSBillCalculator, AWSBillAlarm, AWSBillDataEgress
from bill-calculator-hep import submitAlarm, sendAlarmByEmail, submitAlarmOnServiceNow

class hcfBillingCalculator():

    def start(self):
        self.logger = logging.getLogger("billing-calculator-main")
        self.logger.handlers=[]
     
        try:
            init = '/etc/hepcloud/bill-calculator.ini'
            config = configparser.ConfigParser()
            config.read(init)

            # Setting up logger level from config spec
            debugLevel = config.get('Env','LOG_LEVEL')
            self.logger.setLevel(debugLevel)

            # Creating a rotating file handler and adding it to our logger
            fh=logging.handlers.RotatingFileHandler(config.get('Env','LOG_DIR')+"billing-calculator.log",maxBytes=536870912,backupCount=5)
            fh.setLevel(debugLevel)
            FORMAT="%(asctime)s:%(levelname)s:%(message)s"
            fh.setFormatter(logging.Formatter(FORMAT))

            self.logger.addHandler(fh)

            self.logger.info("Starting hcf-billing-calculator at {0}".format(time.time()))
            self.logger.info("Reading configuration file at %s" % init)
            
            for section in config.sections():
                for key, value in config.items(section):
                    if "LOG" in key.upper():
                        continue
                    else:
                        os.environ[key.upper()] = value
                        self.logger.debug("Setting Env variable {0}={1}".format(key.upper(),os.environ.get(key.upper())))
        except Exception as error:
            traceback.print_exc()
            self.logger.exception(error)

        self.logger.info("Initialized successfully")
        os.chdir(os.environ.get('BILL_DATA_DIR'))
        self.run(self.logger)

    def run(self, log):
        log.info("Scheduling daemons")
        #os.chdir(os.environ.get('BILL_DATA_DIR'))
        schedule.every().day.at("01:05").do(self.AWSBillAnalysis, logger=log)
        schedule.every().day.at("07:05").do(self.AWSBillAnalysis, logger=log)
        schedule.every().day.at("13:05").do(self.AWSBillAnalysis, logger=log)
        schedule.every().day.at("19:05").do(self.AWSBillAnalysis, logger=log)
        schedule.every().day.at("03:05").do(self.GCEBillAnalysis, logger=log)
        schedule.every().day.at("15:05").do(self.GCEBillAnalysis, logger=log)
        #TEsting scheduling
        #schedule.every(2).minutes.do(self.AWSBillAnalysis, logger=log)
        #schedule.every(1).minutes.do(self.GCEBillAnalysis, logger=log)
        #self.GCEBillAnalysis(logger=log)

        while True:
            schedule.run_pending()
            time.sleep(1)

    def GCEBillAnalysis(self, logger):
        GCEconstants = "/etc/hepcloud/config.d/GCE.yaml"
        with open(GCEconstants, 'r') as stream:
            config = yaml.safe_load(stream)
        logger.info("--------------------------- Start GCE calculation cycle {0} ------------------------------".format(time.time()))
        globalConf = config['global']
        snowConf = config['snow']

        for constantsDict in config['accounts']:
            account = constantsDict['accountName']
            try:
                os.chdir(globalConf['outputPath'])
                logger.info(" ---- Billing Analysis for GCE {0} account".format(account))
                calculator = GCEBillCalculator(account, globalConf, constantsDict, logger)
                lastStartDateBilledConsideredDatetime, CorrectedBillSummaryDict = calculator.CalculateBill()
                calculator.sendDataToGraphite(CorrectedBillSummaryDict)

                logger.info(" ---- Alarm calculations for GCE {0} account".format(account))
                alarm = GCEBillAlarm(calculator, account, globalConf, constantsDict, logger)
                message = alarm.EvaluateAlarmConditions(publishData = True)
                if message:
                  sendAlarmByEmail(message,
                                   emailReceipientString = constantsDict['emailReceipientForAlarms'],
                                   subject = '[GCE Billing Alarm] Alarm threshold surpassed for cost rate for %s account'%(account,),
                                   sender = 'GCEBillAlarm@%s'%(socket.gethostname(),),
                                   verbose = False)
                  submitAlarmOnServiceNow (snowConf, message, "GCE Bill Spending Alarm")

                logger.debug(message)
                logger.debug(message)
            except Exception as error:
                logger.info("--------------------------- End of GCE calculation cycle {0} with ERRORS ------------------------------".format(time.time()))
                logger.exception(error)
                continue 

    def AWSBillAnalysis(self, logger):
        AWSconstants = '/etc/hepcloud/config.d/AWS.yaml'
        with open(AWSconstants, 'r') as stream:
            config = yaml.safe_load(stream)

        logger.info("--------------------------- Start AWS calculation cycle {0} ------------------------------".format(time.time()))
        globalConf = config['global']
        snowConf = config['snow']
    
        for constantsDict in config['accounts']:
            account = constantsDict['accountName']
            try:
                os.chdir(globalConf['outputPath'])
                logger.info(" ---- Billing Analysis for AWS {0} account".format(account))
                calculator = AWSBillCalculator(account, globalConf, constantsDict, logger)
                lastStartDateBilledConsideredDatetime, \
                CorrectedBillSummaryDict = calculator.CalculateBill()
                calculator.sendDataToGraphite(CorrectedBillSummaryDict)
    
                logger.info(" ---- Alarm calculations for AWS {0} account".format(account))
                alarm = AWSBillAlarm(calculator, account, globalConf, constantsDict, logger)
                message = alarm.EvaluateAlarmConditions(publishData = True)
                if message:
                  sendAlarmByEmail(message,
                                   emailReceipientString = constantsDict['emailReceipientForAlarms'],
                                   subject = '[AWS Billing Alarm] Alarm threshold surpassed for cost rate for %s account'%(account,),
                                   sender = 'AWSBillAlarm@%s'%(socket.gethostname(),),
                                   verbose = False)
                  submitAlarmOnServiceNow (snowConf, message, "AWS Bill Spending Alarm")
                  
                logger.debug(message)
                logger.info(" ---- Data Egress calculations for AWS {0} account".format(account))
                billDataEgress = AWSBillDataEgress(calculator, account, globalConf, constantsDict, logger)
                dataEgressConditionsDict = billDataEgress.ExtractDataEgressConditions()
                billDataEgress.sendDataToGraphite(dataEgressConditionsDict)
    
            except Exception as error:
                logger.info("--------------------------- End of AWS calculation cycle {0} with ERRORS ------------------------------".format(time.time()))
                logger.exception(error)
                continue
    
        logger.info("--------------------------- End of AWS calculation cycle {0} ------------------------------".format(time.time()))
    

if __name__== "__main__":
    billingCalc = hcfBillingCalculator()
    billingCalc.start()
