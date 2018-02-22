#!/usr/bin/env python3

import argparse
import os.path
import cherrypy
import json

from bs4 import BeautifulSoup
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from pysnmp.hlapi import NotificationType, ObjectIdentity, sendNotification, \
                         SnmpEngine, CommunityData, UdpTransportTarget, \
                         ContextData, OctetString

def replaceUTF8(s):
    """ replaces utf-8 chars with '?' """
    return s.encode('latin-1', 'replace').decode('latin-1', 'replace')

class SpectrumTrap:
    """
    Send a SNMP Trap to Spectrum

    We are reusing the Seyren MIB, see for the Seyren implementation:
    https://github.com/LibertyGlobal/seyren/blob/master/seyren-core/src/main/java/com/seyren/core/domain/AlertType.java

    Fields in shown in Spectrum UI

        Alarm Name
        Formula for Calculation
        Severity
        Value
        Threshold error
        Threshold warning
        Alarm id seyren
        Alarm URL
        Alarm Info

    Severity accepted by Spectrum

        UNKNOWN(0)
        OK(1)
        WARN(2)
        ERROR(3)
        EXCEPTION(4)
    """
    DTV_OID = '1.3.6.1.4.1.13424.15.2.4'
    OBSERVER_OID = '.1.3.6.1.4.1.13424.13.3' + '.2010'
    SNMP_SOURCE = '172.0.0.100'

    # Map of custom variables
    oidmap = {
        'name': 1,
        'metric': 2,
        'state': 3,
        'value': 4,
        'error': 5,
        'warn': 6,
        'id': 7,
        'checkUrl': 8,
        'description': 9,
    }

    def __init__(self, base_oid=DTV_OID, community='community',
                 destination='127.0.0.1', source=SNMP_SOURCE):
        self.base_oid = base_oid
        self.community = community
        self.destination = destination
        self.source = source

    def send(self, alert):
        """ send a SNMP alert """

        # Set Agent source address
        a = [('1.3.6.1.6.3.18.1.3.0', self.source),
             ('1.3.6.1.6.3.1.1.4.3.0', self.base_oid + '.0')]

        # Generic-trap = 0
        # Specific-trap = 1

        # 1.3.6.1.6.3.1.1.5.1 = cold start
        # 1.3.6.1.6.3.1.1.7.1 = Enterprise-Specific 1

        for name, i in self.oidmap.items():
            if name in alert.body:
                v = OctetString(replaceUTF8(alert.body[name]))
                a.append(("{0}.{1}".format(self.base_oid, i), v))

        n = NotificationType(
            ObjectIdentity(self.base_oid + '.0.0.1')).addVarBinds(*a)

        error_indication, error_status, error_index, var_binds = next(
            sendNotification(
                SnmpEngine(),
                CommunityData(self.community, mpModel=0),
                UdpTransportTarget((self.destination, 162)),
                ContextData(),
                'trap',
                n,
                lookupMib=False
            )
        )

        if error_indication:
            raise SnmpError(error_indication)
        else:
            cherrypy.log("Sent SNMP Trap {0} to {1}".format(
                         alert.body, self.destination))

class SnmpError(Exception):
    def __init__(self, message):
        self.message = message


class SourceTypeError(Exception):
    def __init__(self, message, source_type=""):
        self.message = message
        self.sourceType = source_type
