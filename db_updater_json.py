# -*- coding: utf-8 -*-
"""Read and update json data into Infrastructure test Database.

    author: Saverio Brancaccio
    last update: 2018-05-15
"""

# Import system libraries
import ftplib
import json
import os
import sys

# Build and add EPG framework path to sys.path
currentDir = os.getcwd()
for i in range(2):
    currentDir = os.path.split(currentDir)[0]
sys.path.append(currentDir + "\\_EPG_Framework")
sys.path.append(os.path.join(currentDir, "_EPG_Framework\\_Framework\\_Lib"))
sys.path.append(
    os.path.join(currentDir, "_EPG_Framework\\_Framework\\_Lib\\mysql"))

# Import EPG framework libraries
# from _Framework.DB import InfrastructureSetup as IS
# from _Framework.DB.Utils import Utils
import _Framework.TestEnvironment as tenv
import MySQLdb


class DBUpdaterJson:
    """
    """

    def __init__(self):

        try:

            # json files path on source server (e.g. corvetto server)
            self.src_server_jsonfiles_path = 'P:\\Automation_Script_Runs\\to_rogoredo'

            # json files path on teleplan server
            self.teleplan_jsonfiles_path = './automation_script_runs/to_rogoredo'

            # json files path on target server (rogoredo), used to update the test database
            self.tgt_server_jsonfiles_path = 'P:\\Automation_Script_Runs\\to_corvetto'

            if tenv.tp_source == 'rogoredo':

                # Set data structures used to handle data:
                # - to save data coming from json files
                # - to create insertion and updating database queries
                self.query_data = {
                    'as_id': 0,
                    'end_time': None,
                    'facility_id': 0,
                    'img_folder': None,
                    'local_run': 0,
                    'platform_id': None,
                    'res_url': None,
                    'result': 0,
                    'server_name': None,
                    'slot_label': None,
                    'slot_number': None,
                    'start_time': None,
                    'step_number': 0,
                    'test_name': None,
                    'tester': None
                }

                self.query_data_pf = {
                    'as_test_run_id': 0,
                    'avg': 0.0,
                    'avg_g': 0.0,
                    'avg_r': 0.0,
                    'avg_y': 0.0,
                    'deviation': 0.0,
                    'facility_id': 0,
                    'fus_dev': 0.0,
                    'fusion_avg': 0.0,
                    'leg_avg': 0.0,
                    'max': 0.0,
                    'min': 0.0,
                    'platform_id': None,
                    'run_date': None,
                    'server_id': None,
                    'slot_id': 0,
                    'std_dev': 0.0,
                    'std_dst': 0.0,
                    'test_name': None,
                    'test_status': None,
                    'threshold_1': 0.0,
                    'threshold_2': 0.0,
                    'upper_limit': 0.0
                }

                # Connect to test DB
                self.db_connection = MySQLdb.connect(
                    host=tenv.db_infr_host,
                    user=tenv.db_infr_user,
                    passwd=tenv.db_infr_passwd,
                    db='testdb')

                print "DBUpdaterJson: testdb connection etabilished"

                # Get DB cursor
                self.db_cursor = self.db_connection.cursor(
                    MySQLdb.cursors.DictCursor)
                print "DBUpdaterJson: DB cursor fetched"

        except Exception as e:
            print "--- ERROR initializing DB updater! ---"
            print(e)

    def _set_data_relations(self):
        self.query_data_pf['facility_id'] = self.query_data['facility_id']
        self.query_data_pf['platform_id'] = self.query_data['platform_id']
        self.query_data_pf['run_date'] = self.query_data['start_time']
        self.query_data_pf['server_id'] = self.query_data['server_name']
        self.query_data_pf['slot_id'] = self.query_data['slot_number']
        self.query_data_pf['test_name'] = self.query_data['test_name']

    def remove_json_file_local_folder(self, filepath):
        os.remove(filepath)

    def _get_platformid_infrastructuredb(self, json_platformid='Samsung 990'):
        """Get specific id from platform id name

            @return: id, if it is found.
                     None, if it is not found.
        """

        # Build query
        search_query = 'SELECT id '
        search_query += 'FROM ' + tenv.db_infr_selDb + '.tabstb '
        search_query += 'WHERE ' + 'platformName' + '=\'' + json_platformid + '\';'

        # Execute query
        self.db_cursor.execute(search_query)
        result_record = self.db_cursor.fetchone()

        if result_record['id'] is None:
            print "error - _get_data_from_infrastructuredb(): id not found."
            return None
        else:
            return result_record['id']

    def _reset_querydata_values(self):
        """Reset query data member variables with initial values,
           as reported in _init_ method
        """
        # Reset query_daya
        for k in self.query_data:
            if (type(self.query_data[k]) is int) or (type(self.query_data[k])
                                                     is bool):
                self.query_data[k] = 0
            elif type(self.query_data[k]) is float:
                self.query_data[k] = 0.0
            else:
                self.query_data[k] = None

        # Reset query_data_pf
        for k in self.query_data_pf:
            if (type(self.query_data_pf[k]) is int) or (type(
                    self.query_data_pf[k]) is bool):
                self.query_data_pf[k] = 0
            elif type(self.query_data_pf[k]) is float:
                self.query_data_pf[k] = 0.0
            else:
                self.query_data_pf[k] = None

    def check_data_in_db(self, json_file_name=None):
        """Check if test data is already defined into 'testb'.
           The function returns True if data is already defined, False otherwise.

            @param: json_file_name: (str) The json file absolute path,
                                    used to open the file and get its data.

            @return: True, if test data is already defined as a record into db
                     False, if json data is not stored into db.
        """
        if json_file_name is None:
            if len(self.json_file_paths) > 0:
                json_file_name = self.json_file_paths[0]
            else:
                print "error: no json file found at the specified path."
                return False

        with open(json_file_name, "r") as read_file:
            print 'Checking json data in: ' + json_file_name

            # Get data from json file
            deserialized_data = json.load(read_file)

            # Set query data with data obtained from json file
            search_query = 'SELECT server_name, slot_number, start_time '
            search_query += 'FROM testdb.as_test_run '
            search_query += 'WHERE server_name=%(server_name)s '
            search_query += 'AND slot_number=%(slot_number)s '
            search_query += 'AND start_time=%(start_time)s;'

            query_fields = {
                'server_name': deserialized_data['server_name'],
                'slot_number': deserialized_data['slot_number'],
                'start_time': deserialized_data['start_time']
            }

            # print "data to be used for the db query: "
            # print " -server_name: " + str(query_fields['server_name'])
            # print " -slot_number: " + str(query_fields['slot_number'])
            # print " -start_time: " + str(query_fields['start_time'])

            # Query the DB
            self.db_cursor.execute(search_query, query_fields)

            # Verify DB results
            fetched_db_record = self.db_cursor.fetchone()
            if fetched_db_record is None:
                print "_check_data_in_db(): no record found in DB."
                return False
            else:
                print "_check_data_in_db(): record already defined in DB!"
                return True

    def insert_data_in_db(self, json_file_name=None):
        """Insert json file data into database

            @parm json_file_name: (str) The json file absolute path,
                                  used to open the file and get its data.
        """

        # Open file
        with open(json_file_name, "r") as read_file:

            # Get data from json file
            deserialized_data = json.load(read_file)

            # Build query for 'as_test_run' db table
            query_field_names = self.query_data.keys()

            insert_query = 'INSERT INTO testdb.as_test_run ('

            # Add field names section in the query string
            for name in query_field_names:
                insert_query += name + ', '
            insert_query = insert_query[:len(insert_query) - 2]
            insert_query += ') '

            # Add VALUES section in the query string
            insert_query += 'VALUES ('
            for name in query_field_names:
                insert_query += '%(' + name + ')s, '
            insert_query = insert_query[:len(insert_query) - 2]
            insert_query += ');'

            # -------------------------------------
            # Build query with available json nodes

            # Set the value of 'as_id' field
            try:
                splitPath = deserialized_data['script_path']
                testName = splitPath[-1]
                testPath = "\\".join(splitPath[-3:-1])
                firstQuery = "select id from testdb.automation_script where path = %(path)s and file_name = %(filename)s;"
                args = {'path': testPath, 'filename': testName}
                self.db_cursor.execute(firstQuery, args)
                queryResult = self.db_cursor.fetchone()
                as_id = queryResult['id']
            except:
                as_id = 0

            self.query_data['as_id'] = as_id

            # set self.query_data only if related data is available
            # in deserialized json structure
            for key in self.query_data:
                if key in deserialized_data:
                    self.query_data[key] = deserialized_data[key]

            # Correctly set the field platform_id
            platf_id = self._get_platformid_infrastructuredb(
                deserialized_data['platform_id'])
            self.query_data['platform_id'] = platf_id

            # Perform insertion query
            print "Performing insertion query with json data in db..."
            self.db_cursor.execute(insert_query, self.query_data)
            self.db_cursor.execute("commit;")

            # -----------------------------------------------------------------------
            # Check if file contains performance data, if so build performance query:
            if len(deserialized_data['performanceData']) > 0:
                # Performance data defined, update also the table 'pf_test_run'
                print "--- json contains performanceData here."
                print "--- Setting performance data..."

                # Build query for 'pf_test_run' db table

                # Set the list of field names
                query_field_names_pf = self.query_data_pf.keys()

                insert_query_pf = 'INSERT INTO testdb.pf_test_run ('

                # Add field NAMES section in the query string
                for name in query_field_names_pf:
                    insert_query_pf += name + ', '
                insert_query_pf = insert_query_pf[:len(insert_query_pf) - 2]
                insert_query_pf += ') '

                # Add VALUES section in the query string
                insert_query_pf += 'VALUES ('
                for name in query_field_names_pf:
                    insert_query_pf += '%(' + name + ')s, '
                insert_query_pf = insert_query_pf[:len(insert_query_pf) - 2]
                insert_query_pf += ');'

                # Set performance data in self.query_data_pf coming from json
                self._set_data_relations()

                just_pf_names = deserialized_data['performanceData'].keys()
                for name in query_field_names_pf:
                    if name in just_pf_names:
                        self.query_data_pf[name] = deserialized_data[
                            'performanceData'][name]

                print "query_fields: "
                print str(self.query_data_pf)

                # Perform INSERT query
                print "Performing insertion query with performance data in db..."
                self.db_cursor.execute(insert_query_pf, self.query_data_pf)
                self.db_cursor.execute("commit;")

        self.remove_json_file_local_folder(json_file_name)

        return

    def update_data_in_db(self, json_file_name=None):

        self._reset_querydata_values()

        # Open file
        with open(json_file_name, "r") as read_file:

            # Get data from json file
            deserialized_data = json.load(read_file)

            # --------------
            # Set query data

            # Set the value of 'as_id' field
            try:
                splitPath = deserialized_data['script_path']
                testName = splitPath[-1]
                testPath = "\\".join(splitPath[-3:-1])
                firstQuery = "select id from testdb.automation_script where path = %(path)s and file_name = %(filename)s;"
                args = {'path': testPath, 'filename': testName}
                self.db_cursor.execute(firstQuery, args)
                queryResult = self.db_cursor.fetchone()
                as_id = queryResult['id']
            except:
                as_id = 0

            self.query_data['as_id'] = as_id

            # set self.query_data only if related data is available
            # in deserialized json structure
            for key in self.query_data:
                if key in deserialized_data:
                    self.query_data[key] = deserialized_data[key]

            # Correctly set the field platform_id
            platf_id = self._get_platformid_infrastructuredb(
                deserialized_data['platform_id'])
            self.query_data['platform_id'] = platf_id

            # Build query

            # Set the list of field names
            query_field_names = self.query_data.keys()

            update_query = 'UPDATE testdb.as_test_run set '

            # Add field names section in the query string
            for name in query_field_names:
                update_query += name + '=%(' + name + ')s, '
            update_query = update_query[:len(update_query) - 2]
            update_query += ' '

            # Add WHERE section in the query string
            update_query += 'WHERE server_name=%(server_name)s '
            update_query += 'AND slot_number=%(slot_number)s '
            update_query += 'AND start_time=%(start_time)s;'

            # Perform update
            # Perform insertion query
            print "Performing update query with json data in db..."
            # print "update query: " + update_query
            # print "self.query_data: " + str(self.query_data)
            self.db_cursor.execute(update_query, self.query_data)
            self.db_cursor.execute("commit;")

            # ---------------------------------
            # Set performance query data
            if len(deserialized_data['performanceData']) > 0:
                # Set performance data in self.query_data_pf coming from json

                # Set the list of field names
                query_field_names_pf = self.query_data_pf.keys()

                # set self.query_data_pf only if related data is available
                # in deserialized json structure
                just_pf_names = deserialized_data['performanceData'].keys()
                for name in query_field_names_pf:
                    if name in just_pf_names:
                        self.query_data_pf[name] = deserialized_data[
                            'performanceData'][name]

                # Build query for performance data
                self._set_data_relations()

                update_query = 'UPDATE testdb.pf_test_run set '

                # Add field names section in the query string
                for name in query_field_names_pf:
                    update_query += name + '=%(' + name + ')s, '
                update_query = update_query[:len(update_query) - 2]
                update_query += ' '

                # Add WHERE section in the query string
                update_query += 'WHERE server_id=%(server_id)s '
                update_query += 'AND slot_id=%(slot_id)s '
                update_query += 'AND run_date=%(run_date)s;'

                # Perform update
                # Perform insertion query
                print "Performing update query pf with json data in db..."
                # print "update query: " + update_query
                # print "self.query_data: " + str(self.query_data)
                self.db_cursor.execute(update_query, self.query_data_pf)
                self.db_cursor.execute("commit;")

        self.remove_json_file_local_folder(json_file_name)

    def upload_local_jsons(self):
        """Sends a copy of the json files with the test result to a remote FTP server.
        """

        res = True
        print "Starting Upload of local results to Remote Server..."

        try:
            fileListToOpen = os.listdir(self.src_server_jsonfiles_path)
            ftp = ftplib.FTP(tenv.tp_hostname)
            ftp.login(tenv.tp_username, tenv.tp_password)
            ftp.cwd(self.teleplan_jsonfiles_path)

            for filename in fileListToOpen:
                with open(self.src_server_jsonfiles_path + "\\" + filename,
                          'rb') as fd:
                    ftp.storbinary('STOR %s' % filename, fd)
                os.remove(self.src_server_jsonfiles_path + "\\" + filename)
            ftp.quit()
            ftp.close()

        except Exception, e:
            print("An error occoured in file Upload..." + str(e))
            res = False

        print "Upload of local results to Remote Server completed!"

        return res

    def download_remote_jsons(self):
        """Download json files with the test from remote FTP server.
        """
        res = True

        print "Starting Download of remote results to Local Server..."
        try:
            ftp = ftplib.FTP(tenv.tp_hostname)
            ftp.login(tenv.tp_username, tenv.tp_password)
            ftp.cwd(self.teleplan_jsonfiles_path)
            list_files = []
            ftp.retrlines('NLST', list_files.append)

            if not os.path.exists(self.tgt_server_jsonfiles_path):
                os.makedirs(self.tgt_server_jsonfiles_path)

            for dlFile in list_files:
                print dlFile
                with open(self.tgt_server_jsonfiles_path + "\\" + dlFile,
                          "wb") as localFile:
                    ftp.retrbinary("RETR " + dlFile, localFile.write)
                ftp.delete(dlFile)
                # os.remove(self.tgt_server_jsonfiles_path + dlFile)

            ftp.quit()
            ftp.close()

            # Get the list of file paths contained in local log folder
            file_list = os.listdir(self.tgt_server_jsonfiles_path)
            self.json_file_paths = []
            self.json_file_paths = [
                self.tgt_server_jsonfiles_path + os.sep + filename
                for filename in file_list
            ]

            # print self.json_file_paths

        except Exception, e:
            print("An error occoured during downloading of json files..." +
                  str(e))
            res = False

        return res


##############################################################################
# Footer
##############################################################################
if __name__ == '__main__':

    db_updater = DBUpdaterJson()

    # If script is run on external server (e.g.: corvetto, not rogoredo):
    # - just upload json files on teleplan server
    if tenv.tp_source != 'rogoredo':
        db_updater.upload_local_jsons()

    else:
        # here this script is run in rogoredo, therefore:
        # - download json files from teleplan server
        # - update infrastructure test database with json data

        # ----------------------------------------
        # - Download json files from teleplan server
        db_updater.download_remote_jsons()

        # ----------------------------------------
        # - update json data into database

        # For each json file contained in log folder
        if len(db_updater.json_file_paths) < 1:
            print "Info: Json files no found, DB not updated."
            return

        for file in db_updater.json_file_paths:

            try:
                # Check if file data is stored into testdb as_test_run
                result = db_updater.check_data_in_db(file)
                # data already stored into db
                if result:
                    print "-> update_data_in_db"
                    db_updater.update_data_in_db(file)
                    print " "
                # data not stored into db
                else:
                    print "-> insert_data_in_db"
                    db_updater.insert_data_in_db(file)
                    print " "
            except:
                print "error -- the file: "
                print str(file)
                print "was not correctly handled!"
