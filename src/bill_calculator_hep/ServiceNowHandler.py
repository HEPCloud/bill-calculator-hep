#!/usr/bin/env python

event_map = {'INFO': ('4 - Low', '4 - Minor/Localized'),
             'WARN': ('3 - Medium', '4 - Minor/Localized'),
             'ERROR': ('3 - Medium', '3 - Moderate/Limited'),
             'FAIL': ('2 - High', '2 - Significant/Large'),
             'CRITICAL': ('2 - High', '1 - Extensive/Widespread'),
             'TEST': ('2 - High', '1 - Extensive/Widespread'),
             }

class ServiceNowHandler(object):
    instanceURL = 'https://fermidev.service-now.com/'
    eventSummary = 'AWS Activity regarding Users and Roles.'

    def __init__(self, eventClassification,
                 eventSummary=eventSummary,
                 instanceURL=instanceURL):

        self.eventSummary = eventSummary
        self.instanceURL = instanceURL
        if eventClassification in event_map:
            self.eventClassification = eventClassification
            self.eventPriority,  self.eventImpact = event_map[eventClassification]
        else:
            self.eventClassification = 'UNKNOWN'
            self.eventPriority = '4 - Low'
            self.eventImpact = '4 - Minor/Localized'

        self.eventShortDescription = '[%s] : %s'%(self.eventClassification, eventSummary)
