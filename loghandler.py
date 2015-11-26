# -*- coding: utf-8 -*-
#
# Copyright 2014 - StackStorm, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd
import glob
import config
import time

__author__ = 'techbk,thang,nhat'


class LogHandler(object):
    def __init__(self, path=None):
        if not path:
            self._path = config.LOG_PATH
        else:
            self._path = path
        self._list_of_project = None
        self._log = None #dat_frame
        self._list_of_path = None


    def _understand_rource(resource):
        for project in self._list_of_project:
            if resource.find(project):
                return project.upper(project)

    def _fomat_log(self, dataframe):
        for row in dataframe: 
            row['project'] = self._understand_rouce(row['resource'])


    def _log_path(self):
        path = []

        for file in glob.glob(self._path + 'n-*.log*'):
            path.append(file)
        for file in glob.glob(self._path + 'g-*.log*'):
            path.append(file)
        #for file in glob.glob(self._path + 'key.log*'):
            #path.append(file)
        for file in glob.glob(self._path + 'horizon.log*'):
            path.append(file)
        for file in glob.glob(self._path + 'q-*.log*'):
            path.append(file)

        return path

    def _filter_log(project, level, date_start, date_finish):         

        filtered_log = self._log[(log['time'] >= date_start) & (log['time'] <= date_finish)]
        if project != 'all':
            filtered_log = filtered_log[(log['project'] == project)]
        if level != 'all':
            filtered_log = filtered_log[(log['level'])]
        return filtered_log.to_json(orient="index")

    def _statistic_log():
        #nhat viet
        count_per_level = pd.DataFrame({'count' : sort.groupby( ["level"] ).size()}).reset_index()
        sum_each = pd.DataFrame({'sum' : gr.groupby(["level"])["count"].sum()}).reset_index()
        total = gr['count'].sum()
        summary = {}
        summary['Total'] = total
        for index, col in sum_each.iterrows():
            summary[col['level']] = col['sum']
        
        statistic['summary'] = summary
        return statistic
    

    def _read_log(self):
        path = self._log_path()
        # print(path)
        cols = ['time', 'level', 'resource', 'message']  # Set columns for DataFrame

        log = pd.DataFrame()
        for log_file in path[log_name]:
            # print(log_file)
            rl = pd.read_csv(log_file, sep=',', names=cols)  # Read file log and display to dataframe's format
            # print(rl)
            log = log.append(rl, ignore_index=True)
        # print(log)
        sorted_log = log.sort_values(['dates'])  # sort time
        sorted_log = sort.reset_index(drop=True)
        sorted_log = _fomat_log(sort)
        # print(sort)
        # print(sort.to_json(orient='index'))
        return sorted_log

    def project_log(self, list_of_project,project,level,start,end):

        if not self._list_of_project or not self._log_path:
            self._list_of_project = list_of_project
            self._log_path = self._log_path()
        self._log = self._read_log()
        log = filtered_log(list_of_project,project,level,start,end)
        return log.to_json(orient='index')

    def tong_hop(self, list_of_project,project,level,start,end):
        result = self._statistic_log()

        return json.dumps(result)

if __name__ == "__main__":
    handler = LogHandler()

    for i in handler._log_path():
        print(i)

    for l in handler.project_log(['nova','horizon','neutron'], 'all', 'all', '2015-11-25 00:00:00', '2015-11-27 00:00:00'):
        print(l)