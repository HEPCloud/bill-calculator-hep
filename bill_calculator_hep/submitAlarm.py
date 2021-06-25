import smtplib
from email.mime.text import MIMEText
from ServiceNowHandler import ServiceNowHandler
from ServiceDeskProxy import *

def sendAlarmByEmail(messageString, emailReceipientString, subject=None, sender=None, verbose=False):
    """Send the alarm message via email

        Args:
            alarmMessageString
            emailReceipientString

        Returns:
            none
    """
    # Constants
    smtpServerString = 'smtp.fnal.gov'

    # Create and send email from message
    emailMessage = MIMEText(messageString)
  
    #SMTPServer = 'smtp.fnal.gov'
    emailMessage['Subject'] = subject
    emailMessage['From'] = sender
    emailMessage['To'] = emailReceipientString

    if verbose:
        print(emailMessage)
  
    smtpServer = smtplib.SMTP(smtpServerString)
    smtpServer.sendmail(emailMessage['From'], emailMessage['To'], emailMessage.as_string())
    smtpServer.quit()
  
def submitAlarmOnServiceNow(
                            config,
                             
                            messageString, 
                              
                            eventSummary = 'AWS Billing Alarm',
                            
                            ):
    """ Submit incident on ServiceNow.
  
        Args:
              usernameString
              passwordString
              messageString
              eventAssignmentGroupString
              eventSummary
              event_cmdb_ci
              eventCategorization
              eventVirtualOrganization
              instanceURL
  
          Returns:
              none
      """
    instanceURL = config['instance_url']
    serviceNowHandler = ServiceNowHandler('WARN', instanceURL=instanceURL)
 
      # Create Incident on ServiceNow
    proxy = ServiceDeskProxy(instanceURL, config['username'], config['password'])
    argdict = {
                'impact': serviceNowHandler.eventImpact,
                'priority': serviceNowHandler.eventPriority,
                'short_description': eventSummary,
                'description': messageString,
                'assignment_group': config['assignment_group'],
                'cmdb_ci': config['cmdb_ci'],
                'u_monitored_categorization': config['categorization'],
                'u_virtual_organization': config['virtual_organization'],
                }
  
    # create incident:
    this_ticket = proxy.createServiceDeskTicket(argdict)
    print(this_ticket)
  
    return
  

