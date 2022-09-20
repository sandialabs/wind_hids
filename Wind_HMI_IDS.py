"""
Copyright 2022 National Technology & Engineering Solutions of Sandia, LLC (NTESS). 
Under the terms of Contract DE-NA0003525 with NTESS, the U.S. Government retains 
certain rights in this software.

MIT License: 

Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in 
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS 
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER 
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION 
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import time
import argparse
import xml.etree.ElementTree as ET
import requests
import urllib.request
import math
import pprint
import csv 
from collections import OrderedDict

parser = argparse.ArgumentParser()
parser.add_argument("-ip", "--ipaddr", dest="ip", default="10.10.10.10", help="HMI IP", type=str)
parser.add_argument("-p", "--port", dest="port", default=80, help="HMI Port", type=int)
parser.add_argument("-t", "--srv_time", dest="srv_time", default=103453815, help="Webserver Time", type=int)
parser.add_argument("-o", "--offline", dest="offline", default=True, help="Get data from offline XML", type=bool)
parser.add_argument("-d", "--debug_level", dest="debug_level", default='high', help="Debug level (low, high)", type=str)
args = parser.parse_args()

class IDS():
    def __init__(self):

        print("Running with arguments: IP: '{}', port: {}, server time: {}, offline: {}"
              "".format(args.ip, args.port, args.srv_time, args.offline))

        self.addr = args.ip
        self.port = args.port
        self.srv_time = args.srv_time
        self.offline = args.offline
        self.debug_level = args.debug_level
        self.data_fields = []
        self.alarm_fields = []

        self.full_data = {}
        self.alarm_data = OrderedDict()
        self.var_info = {}

        self.csv_file = None

        self.sec_idle = 0.  # time turbine has been in idle state
        self.sec_powered = 0.  # time turbine has been powered
        self.last_check_time = 0.  # time.time() of last check - only useful for real time operation

        self.data = OrderedDict()  # simplified subset for analysis

        self.temp_gearbox_bearing_max = 60.  # TbGbxBearingFastShaftA
        self.v_rated = 11.  # m/s
        self.v_cut_in = 3.5  # m/s
        self.v_cut_out = 25.  # m/s
        self.p_rated = 1500.  # kW
        self.torque_rated = 11.  # kNm
        self.w_max_rated = 18.39  # rpm
        # Rated generator speed is 1440 rpm (that is, the gearbox ratio is slightly over 78).

        self.blade_angle_tol = 5  # deg
        self.v_tol = 0.5  # m/s
        self.p_tol = 100.  # kW
        self.torque_tol = 1.  # kNm
        self.w_tol = 1.  # rpm
        self.temp_tol = 1.  # deg C

        # self.parse_variables()
        # self.run_once()

    def run_once(self):

        if self.offline:
            self.get_alarms_offline()
        else: 
            self.get_alarms()

        if self.offline:
            files = ['mk6e-readdynamicxml.xml', 'mk6e-readdynamicxml1Sec.xml']
            for f in files:
                self.get_data_offline(file=f)
        else: 
            self.get_data()

        ids_alarms = self.check_data(debug_level=self.debug_level)
        return ids_alarms

    def run_continuous(self, poll_rate=1, timeout=None):
        """
        Get updated turbine data at the poll_rate with a timeout period

        :param poll_rate: data collection rate in sec
        :param timeout: amount of time to pull data in sec
        """
        if timeout is None:
            timeout = 1e10  # run forever

        self.csv_file = '%s.csv' % math.floor(time.time())
        t_start = time.time()
        printed_header = False
        while (time.time() - t_start) < timeout:
            t_loop_start = time.time()
            print('\n*** Run time = %0.2f sec of %0.2f sec total.' % (time.time() - t_start, timeout))

            try:
                ids_alarms = self.run_once()

                # Save results to csv
                csv_data_file = open(self.csv_file, 'a+', newline='')
                if not printed_header:
                    print(list(self.data.keys()) + list(self.alarm_data.keys()))
                    csv.writer(csv_data_file).writerow(list(self.data.keys()) + list(self.alarm_data.keys()) + ['IDS Alarms'])
                    printed_header = True
                csv.writer(csv_data_file).writerow(list(self.data.values()) + list(self.alarm_data.values()) + ids_alarms)
                csv_data_file.close()

            except Exception as e:
                print('Data collection failure. %s' % e)
                continue

            # display calculation time
            run_time = (time.time() - t_loop_start)
            print('Loop time = %0.2f' % run_time)

            try:
                time.sleep(poll_rate - run_time)
            except ValueError as e:
                print("Warning: Loop time greater than poll_rate.")


    def get_data(self):
        """
        :param file: file with xml wind data
        :return: dict with turbine data
        """
        t_now = math.floor(time.time()-1)
        url = 'http://%s/cgi-bin/mk6e-readdynamicxml?file=cdl.xml&type=4&data=1&p1=%s&p2=%s' % (self.addr, t_now, t_now)
        response = requests.get(url)
        root = ET.fromstring(response.content)
        self.parse_xml(root)

    def get_data_offline(self, file='mk6e-readdynamicxml.xml'):
        """
        :param file: file with xml wind data
        :return: dict with turbine data
        """
        print('Using XML file: %s' % file)
        xml = ET.parse(file)
        root = xml.getroot()
        self.parse_xml(root)

    def parse_xml(self, root=None):
        """
        Converts the turbine data in XML format into self.data dict
        """
        if root is None:
            print('ERROR - No xml file')
            return None

        self.data['Turbine Status'] = []  # refresh status list
        latest_idx = root[0][0].attrib.get('Index')  # Don't populate with old data
        for child in root[0]:
            
            # terminate search when getting to older indeces
            if child.attrib.get('Index') != latest_idx:
                break

            # add new names to data_fields
            if child.attrib.get('Name') not in self.data_fields:
                self.data_fields.append(child.attrib.get('Name'))

            self.full_data[child.attrib.get('Name')] = float(child.attrib.get('Value'))
            # print(child.attrib.get('Name'), child.attrib.get('Value'), child.attrib.get('Desc'))

            if child.attrib.get('Name') == 'In_WindSpd':
                self.data['Wind Speed'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'In_RotorSpd':
                self.data['Rotor Speed'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'AI_In_GridMonReacPowerAct':
                self.data['Reactive Power'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'AI_In_GridMonRealPowerAct':
                self.data['Active Power'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'AI_In_PitchAngleCurrent1':
                self.data['Blade Pitch 1'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'AI_In_PitchAngleCurrent2':
                self.data['Blade Pitch 2'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'AI_In_PitchAngleCurrent3':
                self.data['Blade Pitch 3'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'AI_CuTorqueAct':
                self.data['Torque'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'In_TbGbxBearingFastShaftA':
                self.data['Gearbox Bearing Temp'] = float(child.attrib.get('Value'))

            if child.attrib.get('Name') == 'DynCtl_Blad1AngleSetpt':
                self.data['Blade 1 Angle Setpoint'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'DynCtl_Blad2AngleSetpt':
                self.data['Blade 2 Angle Setpoint'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'DynCtl_Blad3AngleSetpt':
                self.data['Blade 3 Angle Setpoint'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'DynCtl_RotorSpeedSetpoint':
                self.data['Rotor Speed Setpoint'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'DynCtl_PowerSetpoint':
                self.data['Power Setpoint'] = float(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'AI_In_TbTowerAcceleration':
                self.data['Tower Acceleration'] = float(child.attrib.get('Value'))

            if child.attrib.get('Name') == 'Yaw_Mode':
                self.data['Yaw Mode'] = int(child.attrib.get('Value'))
            if child.attrib.get('Name') == 'Pitch_Mode':
                self.data['Pitch Mode'] = int(child.attrib.get('Value'))

            if child.attrib.get('Name') == 'OpCtl_TurbineStatus':
                bitfield = int(child.attrib.get('Value'))
                if (bitfield & (1 << 0)) == (1 << 0):
                    self.data['Turbine Status'].append('Turbine OK')
                if (bitfield & (1 << 1)) == (1 << 1):
                    self.data['Turbine Status'].append('Turbine with Grid Connection')
                if (bitfield & (1 << 2)) == (1 << 2):
                    self.data['Turbine Status'].append('Run Up / Idling')
                if (bitfield & (1 << 3)) == (1 << 3):
                    self.data['Turbine Status'].append('Maintenance')
                if (bitfield & (1 << 4)) == (1 << 4):
                    self.data['Turbine Status'].append('Repair')
                if (bitfield & (1 << 5)) == (1 << 5):
                    self.data['Turbine Status'].append('Grid loss')
                if (bitfield & (1 << 6)) == (1 << 6):
                    self.data['Turbine Status'].append('Weather conditions')
                if (bitfield & (1 << 7)) == (1 << 7):
                    self.data['Turbine Status'].append('Stop extern')
                if (bitfield & (1 << 8)) == (1 << 8):
                    self.data['Turbine Status'].append('Stopped (manual Stop, if turbine ok)')
                if (bitfield & (1 << 9)) == (1 << 9):
                    self.data['Turbine Status'].append('Stopped (remote Stop, if turbine ok)')
                if (bitfield & (1 << 10)) == (1 << 10):
                    self.data['Turbine Status'].append('Emergency STOP')
                if (bitfield & (1 << 11)) == (1 << 11):
                    self.data['Turbine Status'].append('External Stop regarding Energy Curtailment')
                if (bitfield & (1 << 12)) == (1 << 12):
                    self.data['Turbine Status'].append('Customer Stop')
                if (bitfield & (1 << 13)) == (1 << 13):
                    self.data['Turbine Status'].append('Manual Idle Stop')
                if (bitfield & (1 << 14)) == (1 << 14):
                    self.data['Turbine Status'].append('Remote Idle Stop')
                if (bitfield & (1 << 15)) == (1 << 15):
                    self.data['Turbine Status'].append('Wind Direction Curtailment')

        if self.debug_level == 'high':
            pprint.pprint(self.data)

    def get_alarms(self):
        """
        Get alarm data from the HMI system and store in self.alarm_data

        :return: None
        """

        url = 'http://%s/cgi-bin/mk6e-readdynamicxml?file=alarms.xml&type=16&p1=0&p2=0' % self.addr
        response = requests.get(url)
        root = ET.fromstring(response.content)
        self.parse_alarms(root)

    def get_alarms_offline(self, file='mk6e-readdynamicxml_alarms.xml'):
        """
        :param file: file with xml wind data
        :return: None 
        """

        print('Using XML file: %s' % file)
        xml = ET.parse(file)
        root = xml.getroot()
        self.parse_alarms(root)

    def parse_alarms(self, root=None):
        """
        populate self.alarm_data dict with turbine alarms
        """

        if root is None:
            print('ERROR - No xml file')
            return None

        self.alarm_fields = []  # reset data fields for latest collection
        for child in root[0]:
            self.alarm_fields.append(child.attrib.get('Name'))
            self.alarm_data[child.attrib.get('Name')] = 'Status: ' + child.attrib.get('Status')

    def check_data(self, debug_level='high'):
        """
        Validate data from the turbine

        """
        check_period = time.time() - self.last_check_time
        self.last_check_time = time.time()

        alerts = []

        w = self.data['Rotor Speed']
        p = self.data['Active Power']
        q = self.data['Reactive Power']
        v = self.data['Wind Speed']
        p1 = self.data['Blade Pitch 1']
        p2 = self.data['Blade Pitch 2']
        p3 = self.data['Blade Pitch 3']
        torque = self.data['Torque']
        gearbox_temp = self.data.get('Gearbox Bearing Temp')
        status = self.data['Turbine Status']

        # calculate time powered or idle.  If in a transitory state, ignore some alarms.
        if self.offline:
            if p > 0 + self.p_tol:  # powered
                self.sec_powered += 1.  # assume 1 sec data
                self.sec_idle = 0.
            else:
                self.sec_idle += 1. # assume 1 sec data
                self.sec_powered = 0.
        else:
            # todo - add turbine status to this logic
            if p > 0 + self.p_tol:  # powered
                self.sec_powered += check_period
                self.sec_idle = 0.
            else:
                self.sec_idle += check_period
                self.sec_powered = 0.

        '''
        Turbine Warnings - By themselves aren't IDS alerts, but could be useful information
        '''

        # Pitch Modes: 1 manual pitch, 2 battery test, 0 otherwise
        if self.data['Pitch Mode'] is not None:
            if self.data['Pitch Mode'] != 0:
                alert = 'Warning: Unusual Pitch Mode! Mode = %s, where 1 = manual pitch, 2 = battery test, 0 = otherwise' % \
                        self.data['Pitch Mode']
                alerts.append(alert) 
                if debug_level == 'high':
                    print(alert)

        turbine_ok = True
        if ('Turbine OK' not in status) and ('Turbine with Grid Connection' not in status):
            if debug_level == 'high':
                print('Warning: Turbine not running or OK. Status: %s' % status)
            turbine_ok = False

        turbine_e_stop = False
        if 'Emergency STOP' in status:
            if debug_level == 'high':
                print('Warning: Turbine in Emergency Stop mode. Status: %s' % status)
            turbine_ok = True

        '''
        Global Alerts - when turbine running or not
        '''

        # Idled blades with not in "idle" status
        if 'Run Up / Idling' not in status:
            if p1 > (80.0 - self.blade_angle_tol) or p2 > (80.0 - self.blade_angle_tol) or p3 > (80.0 - self.blade_angle_tol):
                if self.sec_powered > 120.:  # need to be powered for 2 min before alerting
                    alert = 'Alert: Blade pitches set to idle when turbine status is not "Idle"! Status = %s and Pitches = %s ' \
                            'degrees' % (status, [p1, p2, p3])
                    alerts.append(alert) 
                    if debug_level == 'high':
                        print(alert)

        # High temp will cause turbine shutdown
        if gearbox_temp is not None: 
            if gearbox_temp > self.temp_gearbox_bearing_max: 
                alert = 'Alert: High Gearbox Temperature! Gearbox Temperature = %s C' % gearbox_temp
                alerts.append(alert) 
                if debug_level=='high':
                    print(alert)
                        
        # Rotor speed should be < 20 rpm (rated is 18.39 rpm)
        if w is not None:
            if w > self.w_max_rated + self.w_tol: 
                alert = 'Alert! Rotor overspeed! Rotor Speed = %s rpm and Power = %s kW' % (w, p)
                alerts.append(alert) 
                if debug_level=='high':
                    print(alert)

        '''
        Operational Alerts - when turbine running
        '''

        if (turbine_ok or len(status) == 0) and not turbine_e_stop:  # Generator OK or no status data, and not in emergency stop mode
            if debug_level == 'high':
                print('Turbine Status: %s. Checking operational data.' % status)

            if w is not None and p is not None: 
                # Generator should not create power below about 11 rpm.
                if w > self.torque_rated - self.torque_tol: 
                    if p > self.p_tol and self.sec_powered > 60.: 
                        alert = 'Alert: Falsified Data! Rotor Speed = %s rpm and Power = %s kW' % (w, p)
                        alerts.append(alert) 
                        if debug_level=='high':
                            print(alert)
                        

                # If rotor speed > 12 rpm, then power should be > 0
                if w > self.torque_rated + self.torque_tol: 
                    if p > self.p_tol and self.sec_powered > 60.: 
                        alert = 'Alert: Brake Failure! Rotor Speed = %s rpm and Power = %s kW' % (w, p)
                        alerts.append(alert) 
                        if debug_level == 'high':
                            print(alert)
                        
            try:
                # v > v_cut_out: Very high wind speed (above v_cut-out), but turbine still operating
                if v >= self.v_cut_out and self.sec_powered > 60.:
                    alert = 'Alert: Operation above cut out! Wind Speed = %0.2f m/s and cutout = %0.2f m/s' % (v, self.v_cut_out)
                    alerts.append(alert) 
                    if debug_level == 'high':
                        print(alert)

                # v_cut_out < v <= v_rated: Turbine operating above rated wind speed for more than 60 seconds
                elif v >= self.v_rated and self.sec_powered > 60.:
                    # If wind speed >= v_rated, then blade pitch should be > 0
                    if p1 > (0.0 - self.blade_angle_tol) or p2 < (0.0 - self.blade_angle_tol) or p3 < (0.0 - self.blade_angle_tol):
                        alert = 'Alert: Blade pitches do not match wind speed! Wind speed = %s m/s and Pitches = %s degrees' % (v, [p1, p2, p3])
                        alerts.append(alert) 
                        if debug_level == 'high':
                            print(alert)
                    # If wind speed > v_rated, power should be >= p_rated and torque >= q_rated
                    if p < self.p_rated - self.p_tol: 
                        alert = 'Alert: Power should be higher at this wind speed! Wind Speed = %s m/s and Power = %s kW' % (v, p)
                        alerts.append(alert) 
                        if debug_level=='high':
                            print(alert)
                    if torque < self.torque_rated - self.torque_tol:
                        alert = 'Alert: Torque should be higher at this wind speed! Wind Speed = %s m/s and Torque = %s kNm' % (v, torque)
                        alerts.append(alert) 
                        if debug_level=='high':
                            print(alert)

                # v < v_cut_in: Turbine OK and producing power below cut-in wind speed
                elif v < self.v_cut_in:
                    if p > 0. + self.p_tol:
                        alert = 'Alert: Power generation below cut in wind speed! Wind speed = %0.2f m/s and cut-in= %0.2f m/s' % (v, self.v_cut_in)
                        alerts.append(alert) 
                        if debug_level == 'high':
                            print(alert)

                # v_cut_in < v < v_rated: normal operation envelop
                elif v < self.v_rated:
                    # If wind speed < v_rated, then blade pitch should be 0 (or maybe like -2), when operating. 
                    if self.sec_powered > 30.: 
                        if (p1 > (self.blade_angle_tol) or p2 > (self.blade_angle_tol) or p3 > (self.blade_angle_tol) or \
                            p1 < -self.blade_angle_tol or p2 < -self.blade_angle_tol or p3 < -self.blade_angle_tol):  #  blades > 5 deg or < -5 deg
                            alert = 'Alert: Blade pitches strange for v_cut_in < v < v_rated! Wind Speed = %s m/s, Pitches = %s degrees, Status: %s, ' \
                                    'P = %0.2f, Run Time = %s' % (v, [p1, p2, p3], status, p, self.sec_powered)
                            alerts.append(alert) 
                            if debug_level == 'high':
                                print(alert)

                    # If wind speed < v_rated (about 11 m/s), power should be < p_rated (1,500 kW) 
                    if p > self.p_rated and self.sec_powered > 60.: 
                        alert = 'Alert: Power does not match wind speed! Wind Speed = %s m/s and Power = %s kW' % (v, p)
                        alerts.append(alert) 
                        if debug_level == 'high':
                            print(alert)
                    
                    # If wind speed < v_rated (about 11 m/s), torque should be < q_rated (about 11 kNm)
                    if torque > self.torque_rated and self.sec_powered > 60.:
                        alert = 'Alert: Torque does not match wind speed! Wind Speed = %s m/s and Torque = %s kNm' % (torque, p)
                        alerts.append(alert) 
                        if debug_level == 'high':
                            print(alert)

            except Exception as e:
                print('Warning could not check wind speed vs power/torque rules: %s' % e)

            try:
                # Imbalanced blade pitches
                if abs(p1-p2) > 5.0 or abs(p1-p3) > 5.0 or abs(p2-p3) > 5.0:
                    if self.sec_powered > 60.:  # give control system time to orient blades when powering turbine
                        alert = 'Alert: Blade pitch angles are not the same! Pitches = %s degrees' % [p1, p2, p3]
                        alerts.append(alert) 
                        if debug_level == 'high':
                            print(alert)

            except Exception as e:
                print('Warning could not check wind speed vs blade pitch rules: %s' % e)

        return alerts
        

    def parse_variables(self, file='.\\mk6e-listallvars.xml'):
        """
        :param file: file with xml wind data
        """
        print('Using XML file: %s' % file)
        xml = ET.parse(file)
        root = xml.getroot()
        self.var_info = {}
        fields = ['Name', 'Desc', 'EntryHigh', 'EntryLow', 'DisplayHigh', 'DisplayLow', 'Prec', 'Access', 'Units', 'HMIResource', 'AlarmClass']

        for child in root:
            # print(child.attrib.get('Name'))
            self.var_info[child.attrib.get('Name')] = {'Desc': child.attrib.get('Desc'),
                                                       'Prec': child.attrib.get('Prec'),
                                                       'Access': child.attrib.get('Access'),
                                                       'Units': child.attrib.get('Units'),
                                                       'HMIResource': child.attrib.get('HMIResource'),
                                                       'AlarmClass': child.attrib.get('AlarmClass'),
                                                      }

    def inspect_data(self):

        # for k, v in self.var_info.items():
        #     if v['AlarmClass'] != '':
        #         print('Name: %s.  Alarm: %s' % (k, v['AlarmClass']))

        for k in self.data_fields:
            try: 
                print('%s [%s] = %s' % (self.var_info.get(k).get('Desc'), k, self.full_data[k]))
            except Exception as e: 
                print('[%s] = %s' % (k, self.full_data[k]))

        # for k, v in self.var_info.items():
        #     if k in self.data_fields:
        #         print('%s [%s] = %s' % (v['Desc'], k, self.full_data[k]))
        #     if k in self.alarm_fields:
        #         print('%s [%s] = %s (Alarm: %s)' % (v.get('Desc'), k, self.alarm_data[k], v.get('AlarmClass')))


def main():
    sim = IDS()
    # sim.run_once()
    # sim.inspect_data()
    sim.run_continuous(poll_rate=1, timeout=5)

if __name__ == "__main__":
    main()