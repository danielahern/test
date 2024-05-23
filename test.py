"""

DWHTestInit.py

 

Usage:   python DWHTestInit.py -h

Purpose: python library for initializing a folder structure and files to test an EDW PRD or development

         This script first looks up info in an .ini file with multiple 'PRD' and 'DWH' sections:

        

         Sample PRD section (http://omeg6703/PTTR/screens/ProjectHome):

         --------------------------------------------------------------

         [1234-EDW-01]

         prd_description = PTSB Application

         prd_folder      = \\OMEG6711\GroupIT\Group Change\PRDs\1234 - PTSB - Application\4. PRD1234 - Sub PRDs\1234-EDW-01 PTSB Application

         jira_test_plan  = DWH-0123

 

         Sample DWH section:

         -------------------

         [DWH-2345]

         url         = https://jira.omega.internal/browse/DWH-2345

         summary     = 1234-EDW-01 PTSB Application 1.0

         developer   = Jane Doeveloper

         svn         = https://omeg6999/svn/PTSBNet/DataWarehouse/teradata/tag/Release/1234_1.0_PTSB_Application_DWH-2345

         prd_number  = 1234-EDW-01

         environment = T05

 

Maintaining in: https://omeg6999/svn/PTSBNet/DataWarehouse/EDW/UserSandbox/LIDG582/Utilities (not yet maintaining)

 

Revision History:

Date        Version Author Description

2020/01/01  1.0     JF     initial version

"""

 

import argparse

import codecs

import configparser

import datetime

import DWHTestDocGenerator

import filecmp

import os

import pathlib

import re

import shutil

import teradata_funcs

 

class DWHTestInit:

    '''Initialize testing for an EDW JIRA development or PRD'''

    def __init__(self):

        '''(1) Checkout code

           (2) Validate/create deploy_items.txt

           (3) Do more things'''

        #hardcoded values

        self.g_drive='\\\\ilife400\home'

        self.test_doc_templates_dir = pathlib.Path(f'{self.g_drive}\Projects\_templates')

        assert(self.test_doc_templates_dir.is_dir())

        self.default_inifilename, self.general_config_name = 'EDWTAU.ini', 'EDWTAU'

       

        inifilename, configname, self.post_load_test = self.get_args()

        if inifilename == None: inifilename = self.default_inifilename

        self.ini = self.read_ini(inifilename, configname)

        self.current_dir = pathlib.Path.cwd()

        self.session = teradata_funcs.teradata_funcs('DWHDR')

 

        svnkeys, multi_svn_id = [], '' #multiple SVN locations

        for key in self.ini:

            if key.startswith('svn'): svnkeys.append(key)

        for svnkey in svnkeys:

            multi_svn_id = f'_{pathlib.Path(self.ini[svnkey]).name}'

            if self.ini[svnkey].startswith('https'):

                if not self.post_load_test:

                    self.checkout(self.ini[svnkey], self.ini['work_folder'])

               

                # get paths, file lists etc from checked out application code

                checkout_target_dir = self.ini['work_folder'] + '\\' + self.ini[svnkey].split('/')[-1:][0]

                self.dir_list, self.file_list = self.svn_directory_and_file_lists(checkout_target_dir)

                self.teradata_path, self.teradata_parent_path = self.get_teradata_paths(self.dir_list)

                #print(f'{self.teradata_path}\n{self.teradata_parent_path}')

                if self.teradata_path is not None and self.teradata_parent_path is not None:

                    self.deploy_items_path, self.synopsis_list = self.validate_create_deploy_items(multi_svn_id)

                    if self.synopsis_list is not None:

                        if not self.post_load_test:

                            synopsis_file = pathlib.Path(self.ini['work_folder']) / pathlib.Path(f"{self.ini['dwh']}_synopsis{multi_svn_id}.txt")

                            self.synopsize(self.synopsis_list, synopsis_file, multi_svn_id)

                            self.check_databases_exist()

                            #self.remove_BOM()

                            #self.semicolon()

                            self.DDL_replace_text()

                            self.init_deploy(multi_svn_id)

                        else:

                            # this only runs if -z switch used

                            query_row_counts_filename = pathlib.Path(self.ini['work_folder']) / 'Test Scripts' / pathlib.Path(f"{self.ini['dwh']}_row_counts{multi_svn_id}.sql")

                            self.create_query_row_counts(self.synopsis_list, query_row_counts_filename)

                            query_data_checks_filename = pathlib.Path(self.ini['work_folder']) / 'Test Scripts' / pathlib.Path(f"{self.ini['dwh']}_data_checks{multi_svn_id}.sql")

                            self.create_query_data_checks(self.synopsis_list, query_data_checks_filename)

            else:

                if self.ini[svnkey] == '': print('No SVN location has been specifed')

                else:

                    print(f"SVN: '{self.ini[svnkey]}'")

                    for i in range(2, 20):

                        if svnkey+1 in self.ini: print(f"SVN{i}: '{self.ini[svnkey+1]}'")

                        else: break

        self.copy_test_doc_templates(self.ini['work_folder'])

        self.prd_folder_cmd()

        print('\nScript complete.')

 

   def check_databases_exist(self):

        '''check that databases exist'''

        AELO_dict = self.get_AELO_dict(self.file_list)

        dbs, dbs_not_found = [], []

        for table, database_list in AELO_dict.items():

            for db in database_list:

                if db not in dbs: dbs.append(db)

        for db in sorted(dbs):

            query = f"SELECT DatabaseName FROM DBC.Databases WHERE DatabaseName = '{db}';"

            results = self.session.Teradata_query(query)

            if results is not None:

                if len(results) == 0: #0 rows returned, db not found

                    print(f'WARNING: db does not exist: {db}')

                    dbs_not_found.append(db)

                elif len(results) == 1: #db found

                    db_found = results[0][0].strip().upper()

                    assert db_found in dbs

                elif len(results) > 1: #more than 1 db found? impossible

                    print(f'These results are most unusual:\n{results}');exit()

            else:

                print(f'CRITICAL: db does not exist: {db}');

        if len(dbs_not_found) == 0: print('All dbs exist.')

 

    def prd_folder_cmd(self):

        '''create a .cmd file that opens the prd_folder'''

        if 'prd_folder' not in self.ini: return

        prd_folder_cmd_filename = pathlib.Path(self.ini['work_folder']) / pathlib.Path('prd_folder.cmd')

        if prd_folder_cmd_filename.exists(): return

        file = open(prd_folder_cmd_filename, "w")

        file.write(f'explorer "{self.ini["prd_folder"]}\c. Systems Testing"')

        file.close()

 

    def copy_test_doc_templates(self, checkout_target_dir):

        '''copy test doc templates to target folder only if the folder does not exist already

           filenames in the template directoy must have a "template_" prefix to be recognized'''

        def template_rename(d, filename_root):

            '''return 2 paths: (1) template file name (2) DWH-NNNNN file name'''

            template_name = pathlib.Path(d) / f'template_{filename_root}'

            DWH_name = pathlib.Path(d) / f'{self.ini["dwh"]}_{filename_root}'

            if template_name.is_file() and not DWH_name.exists():

                print(f'Renaming {template_name} to {DWH_name}')

                template_name.rename(DWH_name)

                with open(DWH_name) as f:

                    s = f.read()

                with open(DWH_name, 'w') as f:

                    s = s.replace('DWH-NNNNN', self.ini["dwh"]).replace('$$ENV$$', self.ini['environment'])

                    f.write(s)

            #check if .docx files exist in their respective folders

            for dt in ['Test Approach', 'Test Plan', 'Test Report']:

                if dt == d.name and dt.replace(' ','_') in DWH_name.name and not DWH_name.is_file():

                    for target_dir in fullpath_test_doc_dirs:

                        if dt in str(target_dir):

                            target_file = target_dir / DWH_name.name

                            if target_file.is_file():

                                print(f'{target_file} already exists and will not be overwritten.')

                            if not target_file.is_file():

                                print(f'{target_file} does not exist. Creating...')

                                doctypes.append(dt)

                            break

       

        test_doc_dirs = ['Test Approach', 'Test Evidence', 'Test Plan', 'Test Report', 'Test Scripts']

        test_documents = ['Test_Approach.docx','Test_Plan.docx','Test_Report.docx']

        test_scripts_files = ['01_SELECT.template.sql','02_DELETE.sql','03_SELECT_NON_KEY_VALUES.template.sql','04_UPDATE_NON_KEY_VALUES.sql', \

                            '05_SELECT_KEY_VALUES.template.sql','06_UPDATE_KEY_VALUES.sql','07_SELECT_DELETED_KEY_VALUES.template.sql', \

                            'associated_objects.sql','duplicates.template.sql','ExtractionType_Initial.sql','ExtractionType_Regular.sql','JIRATestScript.csv']

        fullpath_test_doc_dirs = [pathlib.Path(checkout_target_dir) / x for x in test_doc_dirs]

        fullpath_test_doc_templates = [pathlib.Path(self.test_doc_templates_dir) / x for x in test_doc_dirs]

        doctypes = [] #should contain 0 or more of 'Test Approach', 'Test Plan', 'Test Report'

        for d, template in zip(fullpath_test_doc_dirs, fullpath_test_doc_templates):

            if not d.is_dir(): shutil.copytree(template, d) #only create directory (and copy files) if the directory doesn't exist

            #filename_root values should exist as "template_[filename_root]" in the self.test_doc_templates_dir directory

            for filename_root in test_documents+test_scripts_files:

                #print(f'renaming template_{filename_root} to {filename_root}')

                #template_rename(template, filename_root)

                template_rename(d, filename_root)

        if doctypes: DWHTestDocGenerator.DWHTestDocGenerator(doctypes, self.ini)

 

    def init_deploy(self, multi_svn_id):

        '''get ready to (but don't actually) deploy the application'''

        def write_install_ini(deploy_directory):

            filename = deploy_directory / "install.ini"

            try:

                file = open(filename, "w")

                file.seek(0)

                file.truncate()

                lines_of_text = ["[CONFIG]\n", "SOURCE_DIR=.\n", f"dataSourceName={self.session.DSN}\n",

                    f"TARGET_ENV={self.ini['environment']}\n", "TARGET_ENVS=T02\n", "TARGET_ENVP=B00\n",

                    "BTEQ=True\n", "FIX_DOLLAR=True\n", "logLevel=INFO\n", "logConsole=True\n", "logDir=.\Log\n"]

                file.writelines(lines_of_text)

                file.close()

            except:

                raise NameError(f"unable to create {filename}")

 

        deploy_directory = pathlib.Path(self.ini['work_folder']) / pathlib.Path("deploy")

        pathlib.Path(deploy_directory).mkdir(parents=True, exist_ok=True) # make target directory

        write_install_ini(deploy_directory)

        td_install_path = self.current_dir / 'td_install.py'

        shutil.copy(td_install_path, deploy_directory)

        td_install_command = f'python td_install.py --appName={self.ini["dwh"]} --SOURCE_DIR="{self.teradata_path}" --OBJECT_LIST="{self.deploy_items_path}"'

        td_install_command = td_install_command.replace(self.g_drive, 'G:')

        deploy_cmd_filename = deploy_directory / pathlib.Path(f"{self.ini['dwh']}_deploy{multi_svn_id}.cmd")

       

        GCFR_Log_directory = deploy_directory / 'Log'

        Deployment_directory = pathlib.Path(self.ini['work_folder']) / 'Test Evidence' / 'Deployment'

        copy_log_command = f'IF %ERRORLEVEL% EQU 0 copy "{GCFR_Log_directory}\\{self.ini["dwh"]}*.log" "{Deployment_directory}"'

        copy_log_command = copy_log_command.replace(self.g_drive, 'G:')

        deploy_cmd_filename.write_text(td_install_command + '\n' + copy_log_command)

        print(f'Deploy using: {deploy_cmd_filename}')

       

    def DDL_replace_text(self):

        '''replace production references with test references'''

        def find_replace(findtxt, replacetxt, changelist):

            def replace_text(filename, old_text, new_text):

                with open(filename) as f:

                    s = f.read()

                    if old_text not in s:

                        print('"{old_text}" not found in {filename} ... WARNING'.format(**locals()))

                        return

                with open(filename, 'w') as f:

                    print("Replace\t\"{old_text}\" with \"{new_text}\" in {filename} ... ".format(**locals()), end='')

                    s = s.replace(old_text, new_text)

                    f.write(s)

                    print("OK")

                   

            askreplace = False

            for f in self.file_list:

                if str(f).lower().endswith('.sql') or str(f).lower().endswith('.ddl'):

                    file = open(f, "r")

                    filedata = file.read()

                    if findtxt in filedata:

                        print(f)

                        askreplace = True

                        file.close()

                        break

            if askreplace and self.ask_YNQ(f"Replace {findtxt} with {replacetxt}", "n"):

                for f in self.file_list:

                    if str(f).lower().endswith('.sql') or str(f).lower().endswith('.ddl'):

                        file = open(f, "r")

                        filedata = file.read()

                        if findtxt in filedata:

                            print()

                            replace_text(f, findtxt, replacetxt)

                            if not f in changelist: changelist[f] = 1

                            else: changelist[f] += 1

                        file.close()

                if len(changelist.keys()) == 0: print('None found')

               

        changelist = {}

        find_replace('C:', 'J:', changelist)

        find_replace('P00', '$$Env$$', changelist)

        find_replace('OMEG8844', 'OMEG8770', changelist)

        find_replace('7019', '7018', changelist)

 

        if len(changelist.keys()) > 0:

            print(f'DDL/SQL updated in {len(changelist.keys())} files:')

            for k in sorted(changelist.keys()):

                print(k)

 

    def semicolon(self):

        '''check last char in file ends in a semicolon'''

        def last_char_in_file(filename):

            file = open(filename, "r")

            filecontents = file.read().strip()

            file.close()

            return filecontents[-1:]

        def last_line_in_file(filename):

            def list_from_file(filename):

                lines = []

                with open(filename) as file:

                    for line in file:

                        lines.append(line.strip())

                return lines

            lines = list_from_file(filename)

            if len(lines) == 0:

                print(f'{filename} is empty')

                return ''

            lastline = -1

           while lines[lastline] == '': lastline -= 1 # ignore blank lines

            return lines[lastline]

        def append_to_file(filename, txt):

            file = open(filename, "a")

            file.write(txt)

            file.close()

        #for f in self.file_list:

        for f in self.synopsis_list:

            if str(f).lower().endswith('.sql') or str(f).lower().endswith('.ddl'):

                if last_char_in_file(f) != ';':

                    if '.IF ERRORCODE' not in last_line_in_file(f):

                        print(f'{f} does not end with a semicolon')

                        append_to_file(f, ';')

 

    def remove_BOM(self):

        '''Remove BOM from file if exists'''

        def remove_BOM_from_file(path):

            size = os.path.getsize(path)

            BOM_length = len(codecs.BOM_UTF8)

            with open(path, "r+b") as file:

                chunk = file.read(size)

                if chunk.startswith(codecs.BOM_UTF8):

                    chunk = chunk[BOM_length:]

                    while chunk:

                        file.seek(1)

                        file.write(chunk)

                        file.seek(BOM_length, os.SEEK_CUR)

                        chunk = file.read(size)

                    file.seek(-BOM_length, os.SEEK_CUR)

                    file.truncate()

                    return True

                else:

                    return False

       

        for f in self.file_list:

            if remove_BOM_from_file(f):

                print(f'BOM removed from {f}')

 

    def ask(self, question):

        '''ask a question and return the answer'''

        return input(f'{question} ')

 

    def ask_YNQ(self, question, default_enter):

        '''ask a YES/NO/QUIT question'''

        default = ''

        if default_enter != '': default = f'[ENTER = {default_enter.upper()}] '

        while True:

            result = input(f'{question} (Y/N/Q) {default}? ')

            result = result.strip().upper()

            if result == '':

                if default_enter != '': result = default_enter.strip().upper()

            if result == 'Y': return True

            if result == 'N': return False

            if result == 'Q':

                print('\nScript terminated.')

                os._exit(0)

 

    def svn_directory_and_file_lists(self, checkout_target_dir):

        '''get list of dirs and files from target dir'''

        def read(directory, dir_list, file_list):

            '''recursive function: read dirs and files in a directory'''

            for item in directory.iterdir():

                #ignore these directories

                if str(item).endswith('.svn'): continue

                if str(item).endswith('Rollback'): continue

                if str(item).endswith('Test Approach'): continue

                if str(item).endswith('Test Evidence'): continue

                if str(item).endswith('Test Plan'): continue

                if str(item).endswith('Test Report'): continue

                if str(item).endswith('Test Scripts'): continue

                #add files and directories to lists

                if item.is_file(): file_list.append(item)

                if item.is_dir():

                    dir_list.append(item)

                    read(item, dir_list, file_list)

            return dir_list, file_list

       

        dir_list, file_list = read(pathlib.Path(str(checkout_target_dir)), [], [])

        if not dir_list: dir_list.append(checkout_target_dir)

        return sorted(dir_list), sorted(file_list)

 

    def get_AELO_dict(self, filelist):

        '''get dictionary of all database.[table|view]s in release

        "[table|view]name":["db1", "db2", "db3", "db4", "db5"]'''

        summary_text = ['CREATE TABLE','CREATE MULTISET TABLE','CREATE SET TABLE','REPLACE VIEW','RENAME VIEW']

        AELO_dict = {} #key = database, value = table or view

        for filename in filelist:

            if filename.name == 'deploy_items.tmp': continue #might be unlinked (deleted)

            if filename.suffix.lower() not in ['.ddl','.sql']: continue

            #print(filename.name)

            #file = open(filename, "r", encoding="utf-8")

            file = open(filename, "r")

            if filename.stat().st_size == 0: continue #firstlines.append('(empty file)')

           

            for line in file:

                append_line = line.upper().strip()

                if append_line.startswith('--'): continue

                for summary in summary_text:

                    if append_line.find(summary) is not -1:

                        append_line = append_line.lstrip(summary)

                        if summary == 'RENAME VIEW':

                            m = re.compile('(?<=TO).*(?=;)', re.IGNORECASE).search(append_line)

                            #m = re.compile('(?<=TO).*(?=[ ;])', re.IGNORECASE).search(append_line)

                            if m is not None:

                                append_line = m.group().strip().rstrip(';').rstrip()

                            else:

                                print(f'There is some kind of issue (A) parsing this line that should be investigated: {append_line}')

                        else:

                            #append_line = append_line.lstrip().split(' ')[0].replace('"','').replace(',','').replace('NO FALLBACK','').replace('FALLBACK','')

                            append_line = append_line.lstrip().replace('"','').replace(',','').replace('NO FALLBACK','').replace('FALLBACK','').rstrip()

                            if ' AS ' in append_line.upper():

                                as_index = append_line.upper().index(" AS ")

                                append_line = f'{append_line[:as_index].strip()}'

                            if append_line.endswith('_N') or append_line.endswith('_O'): continue

                        if '.' in append_line:

                            db, ob = append_line.split('.')[0].replace('$$ENV$$', self.ini['environment']), append_line.split('.')[1]

                            if ob not in AELO_dict: AELO_dict[ob] = []

                            if db not in AELO_dict[ob]: AELO_dict[ob].append(db)#;print(ob,db)

                            break

                        else:

                            print(f'ERROR: Cannot read database.object in {filename.name} from string: {append_line}')

                            break

            file.close()

        #for k,v in AELO_dict.items(): print(k,v)

        #exit()

        return AELO_dict

       

    def create_query_data_checks(self, filelist, query_filename):

        '''generate AELO queries to be run post-load for ODS releases 2

        use the -z switch to run this'''

        print('Data check queries : ', end='')

       

        def find_values_in_all_tables(tablename, dbnames):

            '''find a value in a column common to all tables, return that value or None'''

            num_values_to_check = 50000

            find_common_value = []

            max_values_to_find = 20

            for db in dbnames:

                if first_key is None: continue

                query = f"SEL TOP {num_values_to_check} {first_key} FROM {db}.{tablename} ORDER BY 1 ASC;"

                results = self.session.Teradata_query(query)

                clean_results = []

                for result in results:

                    clean_results.append(result[0].strip(' '))

                find_common_value.append(clean_results)

                print(query+' --'+str(len(clean_results))+' rows returned')

            found_values = []

            if find_common_value[0] == []:

                print('return (1)')

                return []

            for value in find_common_value[0]:

                for i in range(1, len(find_common_value)):

                    if value not in find_common_value[i]:

                        break

                    else:

                        if value not in found_values:

                            found_values.append(value)

                if len(found_values) == max_values_to_find:

                    print('return (2)')

                    return found_values

            print('return (3)')

            return found_values

       

        AELO_dict = self.get_AELO_dict(filelist) #key:value = "[table|view]name":["db1", "db2", "db3", "db4", "db5"]

        #for tablename, dbnames in AELO_dict.items(): print(f'-->{tablename}|{dbnames}')

        AELO_query = open(query_filename, "w", encoding="utf-8")

      

        column_query = "SEL CAST(columnname AS VARCHAR(100)) FROM dbc.COLUMNS WHERE databasename='%s' AND TABLENAME='%s' AND columnname NOT IN ('start_date','end_date','start_ts','end_ts','record_deleted_flag','ctl_id','process_name','process_id','update_process_name','update_process_id') ORDER BY columnid;"

        counter = 0

        for tablename, dbnames in AELO_dict.items():

            counter += 1

            column_list, columns, first_key = ['name'], {}, None

           

            #GET PRIMARY KEY

            query= f"SELECT COALESCE(Key_Column, '') FROM DW{self.ini['environment']}V_GCFR.GCFR_Transform_KeyCol WHERE Out_DB_Name = 'DW{self.ini['environment']}V_ODS_IN' AND Out_Object_Name = '{tablename}';"

            primary_key_list = self.session.Teradata_query(query)

            primary_key = []

            for item in primary_key_list:

                primary_key.append(item[0])

            first_key = primary_key[0]

           

            for db in dbnames:

                #print(f'-->{tablename}|{db}')

                query = column_query % (db, tablename)

                results = self.session.Teradata_query(query)

                for result in results:

                    key = result[0].strip(' ')

                    if key is None: continue

                    if not first_key: first_key = key

                    #print(first_key)

                    if key not in columns:

                        column_list.append(key)

                        columns[key] = 0

                    columns[key] += 1

                    #print(f'columns[{key}]={columns[key]}')

           

            #delete columns that are not in every db

            delete_columns = []

            for column_name, num in columns.items():

                if num < len(dbnames): #column name is common to every identically named table/view in every database, so we can query it

                    if column_name not in delete_columns:

                        delete_columns.append(column_name)

                if len(column_name) > 29:

                    #returned column names have a max length of 30 (for some unknown reason...)

                    #therefore, any column name that is 30 characters long MAY be truncated, so, as a precaution, remove it

                    if column_name not in delete_columns:

                        delete_columns.append(column_name)

            for column_name in delete_columns:

                del columns[column_name]

                column_list.remove(column_name)

           

            found_values = find_values_in_all_tables(tablename, dbnames)

            if found_values == []:

                print(f'{counter:03}/{len(AELO_dict):03}:{tablename} has no data')

            else:

                for found_value in found_values:

                    cast, query, end_line = "Cast(\"%s\" AS VARCHAR(50))", "", ' UNION ALL '

                    count = 0

                    for db in dbnames:

                        count += 1

                        dbobject = f"'{db}.{tablename}'"

                        query += f"SELECT Cast({dbobject} AS VARCHAR(50))"

                        for column in columns:

                            query += f", {cast % column}"

                        if found_value == 'None':

                            query += f" FROM {db}.{tablename} WHERE {first_key} IS NULL"

                        else:

                            query += f" FROM {db}.{tablename} WHERE {first_key} = '{found_value}'"

                        if count == len(dbnames): end_line = ';'

                        query += end_line

                    AELO_query.write(query.replace('SELECT ', '\nSELECT ')+'\n')     #write query to file

                    results = self.session.Teradata_query(query)                     #run query

                    AELO_query.write('/*\n'+teradata_funcs.teradata_funcs.format_results(column_list, results)+'*/\n')

            #if counter == 2: exit()

       

        AELO_query.close()

        print(query_filename)

   

    def create_query_row_counts(self, filelist, query_filename):

        '''generate and run queries to count rows'''

        print('Row count queries  : ', end='')

        AELO_dict = self.get_AELO_dict(filelist)

        AELO_query = open(query_filename, "w")

        items = []

        for k, v in AELO_dict.items():

            query = ''

            items = AELO_dict[k]

 

            alias, end_line = [' #', ' name', ' total'], ' UNION '

            count = 0

            for item in items:

                count += 1

                if count == 2: alias1, alias2, alias3 = ('',)*3

                if count == len(items): end_line = ';'

                query += f"SELECT {count}{alias[0]}, CAST('{item}.{k}' AS VARCHAR(100)){alias[1]}, CAST(COUNT(*) AS BIGINT){alias[2]} FROM {item}.{k}%s" % end_line

                if count == len(items):

                    AELO_query.write(query.replace('SELECT ', '\nSELECT ')+'\n')     #write query to file

                    results = self.session.Teradata_query(query)                     #run query

                    AELO_query.write('/*\n'+teradata_funcs.teradata_funcs.format_results(alias, results)+'*/\n')

        AELO_query.close()

        print(query_filename)

   

    def create_query_row_counts_ORIGINAL(self, filelist, query_filename):

        '''generate and run queries to count rows'''

        print('Row count queries  : ', end='')

        AELO_dict = self.get_AELO_dict(filelist)

        AELO_query = open(query_filename, "w")

        items = []

        for k, v in AELO_dict.items():

            query = ''

            if not items: items = AELO_dict[k]

            #print(f'=>{k}|{items}')

            if items == AELO_dict[k]: #verify same views/tables in each db

                alias, end_line = [' #', ' name', ' total'], ' UNION '

                count = 0

                for item in items:

                    count += 1

                    if count == 2: alias1, alias2, alias3 = ('',)*3

                    if count == len(items): end_line = ';'

                    query += f"SELECT {count}{alias[0]}, CAST('{item}.{k}' AS VARCHAR(100)){alias[1]}, CAST(COUNT(*) AS BIGINT){alias[2]} FROM {item}.{k}%s" % end_line

                    if count == len(items):

                        AELO_query.write(query.replace('SELECT ', '\nSELECT ')+'\n')     #write query to file

                        results = self.session.Teradata_query(query)                     #run query

                        AELO_query.write('/*\n'+teradata_funcs.teradata_funcs.format_results(alias, results)+'*/\n')

            else:

                print(f'\n WARNING: issue generating post-load AELO verification query: {query_filename}')

                print(f' items:{items}')

                print(f' AELO_dict[{k}]:{AELO_dict[k]}')

                #break

        AELO_query.close()

        print(query_filename)

 

    def synopsize(self, filelist, synopsis_filename, multi_svn_id):

        '''make synopsis file showing first line in each ddl/sql file'''

        summary_text = ['COLLECT STATISTICS','COLLECT STATS','ALTER TABLE','CREATE TABLE','CREATE MULTISET TABLE','CREATE SET TABLE','RENAME TABLE','REPLACE VIEW','CREATE VIEW','REPLACE RECURSIVE VIEW','RENAME VIEW','EXEC','INSERT INTO','DROP TABLE','DROP VIEW','DELETE','UPDATE','SELECT']

        firstlines = []

        maxlen = 50

        synopsis = open(synopsis_filename, "w")

        for filename in filelist:

            isSummarized = False

            file = open(filename, "r")

            if filename.stat().st_size == 0:

                firstlines.append('(empty file)')

                file.close()

                continue

           

            #count no. of statements (= no. of semicolons)

            num_semicolons = 0

            for line in file:

                if line.strip().startswith('--'): continue

                if line.strip().startswith('/*'): continue

                num_semicolons += line.count(';')

            file.close()

           

            file = open(filename, "r")

            block_comment_mode = False

            for line in file:

                while '  ' in line:

                    line = line.replace('  ',' ')

                if isSummarized: break

                append_line = line.strip()

                if append_line.startswith('--'): continue

                if append_line.startswith('/*'): block_comment_mode = True

                if block_comment_mode:

                    if '*/' in line:

                        line = f'{line[line.index("*/")+2:].strip()}'

                        block_comment_mode = False

                    else:

                        continue

                for summary in summary_text:

                    if line.upper().find(summary) is not -1:

                        if summary in ['COLLECT STATISTICS','COLLECT STATS','EXEC','INSERT INTO','DELETE','UPDATE','SELECT']:

                            append_line = line[:maxlen].strip()

                        if num_semicolons > 1:

                            append_line += f' (+{num_semicolons-1} more)'

                        firstlines.append(append_line)

                        if len(append_line) > maxlen: maxlen = len(append_line)

                        isSummarized = True

                        break

            file.close()

       

        if len(firstlines) > 0: maxlen = len(max(firstlines, key=len))

        for line, filename in zip(firstlines, filelist):

            #synopsis.write(line+(' '*(maxlen-len(line)+1))+str(filename)+'\n')

            synopsis.write(line+(' '*(maxlen-len(line)+1))+str(filename).replace(str(self.teradata_path), '')+'\n')

        synopsis.close()

       

        redeploy_init = pathlib.Path(self.ini['work_folder']) / pathlib.Path(f"{self.ini['dwh']}_redeploy_init{multi_svn_id}.sql")

        redeploy_init.touch()

        #write to redeploy_init

        today = datetime.date.today().strftime('%Y-%m-%d')

        delete = []

        delete.append(f"/*")

        delete.append(f"DELETE FROM DW{self.ini['environment']}T_GCFR.GCFR_SSIS_File WHERE Ctl_Id = 123 AND File_Id IN (1,2,3);")

        delete.append(f"DELETE FROM DW{self.ini['environment']}T_GCFR.SSIS_File_Config WHERE TableType LIKE 'SOMETHING%';")

        delete.append(f"DELETE FROM DW{self.ini['environment']}T_GCFR.GCFR_System_File_Extract WHERE Ctl_Id = 123  AND File_Id IN (1,2,3) AND Business_Date IN (DATE '{today}', DATE '{today}');")

        add_lines = True

       

        #file = open(redeploy_init, "r")

        #for line in file:

        #    if delete[0] == line.strip():

        #        add_lines = False

        #        break

        with open(redeploy_init, "a+") as file:

            for line in file:

                if delete[0] == line.strip():

                    add_lines = False

                    file.write(f"--should exist\n\n--should not exist\n\n--?\n\n")

                    break

       

        if add_lines:

            with open(redeploy_init, 'a+') as file:

                for line in delete:

                    file.write('\n'+line)

 

    def validate_create_deploy_items(self, multi_svn_id):

        '''create deploy_items.txt based on directory structure

        validate with existing deploy_items.txt, if it exists'''

       

        def get_deploy_items_path():

            '''return path to deploy_items.txt or None if not found'''

            print('\nFUNCTION get_deploy_items_path()')

            deploy_items_path = None

            for f in self.file_list:

                print(f' ==> str(f) = {str(f)}')

                if str(f).lower().endswith('deploy_items.txt'):

                    deploy_items_path = f

                    break

            if deploy_items_path: print(f'deploy_items.txt found   : {deploy_items_path}')

            else: print('deploy_items.txt not found')

            return deploy_items_path

           

        def write_deploy_items(filename):

            '''write deploy_items.txt to a target path'''

            filepath = self.teradata_parent_path / filename

            lines = []

            line_count = 0

            synopsis_list = []

            for line in self.file_list:

                p = pathlib.PurePath(str(line).replace(str(self.teradata_parent_path), ''))

                if len(p.parts) > 1:

                    if p.parts[1].upper() == 'TERADATA':

                    #if p.parts[1].upper() == str(self.teradata_path).split('\\')[-1].upper():

                        if str(line).lower().endswith('.sql') or str(line).lower().endswith('.ddl'):

                            line_count += 1

                            lines.append("{0:0=3d}".format(line_count) + "|.." + str(line).replace(str(self.teradata_parent_path), ''))

                            synopsis_list.append(line)

           

            #"Pathlib.iterdir style" ("Pathlib.iterdir style" is where e.g. a folder called 03A comes AFTER a folder called 03)

            #with filepath.open("w", encoding ="utf-8") as f:

            #    for line in lines:

            #        f.write(line + '\n')

            wdi_SARATH(filename)

           

            print(f'{filename} written : {self.teradata_parent_path}\{filename}')

            return synopsis_list #list: full paths of all SQL & DDL files in TERADATA folder

       

        def wdi_SARATH(filename):

            '''write deploy_items.txt to a target path in "os.walk style"

               "os.walk style" is where e.g. a folder called 03A comes BEFORE a folder called 03'''

            filepath = self.teradata_parent_path / filename

            seq_no = 0

            with open(filepath,'wt') as out_f:

                for dir_name, subdirs, files in os.walk(self.teradata_path):

                    for file_name in files:

                        if file_name.lower().endswith(filename): continue

                        full_file_name = os.path.join(dir_name, file_name)

                        if not file_name.lower().endswith(('.ddl', '.sql')): continue #print("Ignoring file:", full_file_name)

                        seq_no += 1

                        #print("Including: ", full_file_name)

                        out_f.write("{0:03}|{1}\n".format(seq_no, os.path.join(dir_name.replace(str(self.teradata_parent_path), '..'), file_name)))

       

        verify_deploy_items = True

        deploy_items_path = get_deploy_items_path()

        #print(f'deploy_items_path={deploy_items_path}')

        #deploy_items.txt not found

        if deploy_items_path == None: # create deploy_items.txt

            synopsis_list = write_deploy_items('deploy_items.txt')

            deploy_items_path = self.teradata_parent_path / 'deploy_items.txt'

            verify_deploy_items = False

        deploy_items_parent_path = pathlib.Path(deploy_items_path).resolve().parents[0]

        #print(f'deploy_items_parent_path={deploy_items_parent_path}')

       

        #deploy_items.txt in the wrong place

        if deploy_items_parent_path != self.teradata_parent_path:

            #copy deploy_items.txt to self.teradata_parent_path

            shutil.copy(deploy_items_path, self.teradata_parent_path)

            #rename deploy_items.txt to deploy_items.original

            deploy_items_path.rename(deploy_items_path.with_suffix('.original'))

            deploy_items_path = get_deploy_items_path()

            deploy_items_parent_path = pathlib.Path(deploy_items_path).resolve().parents[0]

       

        #deploy_items.txt verification

        if verify_deploy_items:

            synopsis_list = write_deploy_items('deploy_items.tmp')

            tmp_file = self.teradata_parent_path / 'deploy_items.tmp'

            print(f'\n --> deploy_items_path=|{deploy_items_path}|\n --> tmp_file=|{tmp_file}|')

            if filecmp.cmp(deploy_items_path, tmp_file):

                tmp_file.unlink()

                print(f'PASS|IDENTICAL {deploy_items_path.name}={tmp_file.name} ({tmp_file.name} deleted)')

            else: (f'WARNING|DIFFERENT {deploy_items_path}!={tmp_file} <-- investigate')

           

            #compare = fcomp.FComp(False) # compare existing and temporary deploy_items.txt

            #compare_output_path = pathlib.Path(self.ini['work_folder']) / pathlib.Path(f'compare_deploy_items{multi_svn_id}.html')

            #compare_result = compare.compare_files(deploy_items_path, tmp_file, compare_output_path, True, False, False)

            #if compare_result != 'identical':

            #    print(f'Review {compare_result}')

 

        return deploy_items_path, synopsis_list

 

    def get_teradata_paths(self, dir_list):

        '''return paths to teradata and teradata parent directories or None if not found'''

        teradata_path = None

        for d in dir_list:

            if str(d).lower().endswith('teradata') and 'rollback' not in str(d).lower() and not str(d).lower().endswith('ods_vteradata'):

                teradata_path = d

                break

        if not teradata_path: # user selects "TERADATA" path

            for n, d in list(enumerate(dir_list)):

                print(f'[{n}] {d}')

            teradata_path_select_number = self.ask('Select directory for deploy_items.txt (ENTER to skip):')

            if teradata_path_select_number == '': return None, None

            teradata_path = dir_list[int(teradata_path_select_number)]

        teradata_parent_path = pathlib.Path(teradata_path).resolve().parents[0]

        return teradata_path, teradata_parent_path

 

    def checkout(self, svn_url, checkout_target_dir):

        '''checkout files from a url to a target directory'''

        pathlib.Path(checkout_target_dir).mkdir(parents=True, exist_ok=True) # make target directory

        os.chdir(str(checkout_target_dir).replace(self.g_drive, 'G:').lstrip('\\'))

        if 'â€“' in svn_url: svn_url = svn_url.replace('â€“', '–')

        return os.system(f'svn checkout "{svn_url}"')

 

    def read_ini(self, config_file_name, config_name):

        '''read an .ini file and return results in a dictionary called ini'''

        config = configparser.ConfigParser(interpolation=None)

        try: config.read(config_file_name)

        except: raise NameError(f'{config_file_name} contains duplicate section names.')

        if config_name not in config:

            print(f'[{config_name}] is not in {config_file_name}');exit()

        nosection = True

        try:

            ini = {}

            try: #read tester info from ini file

                config.read(self.general_config_name)

                if config[self.general_config_name]['tester_name'] is not None: ini['tester_name'] = config[self.general_config_name]['tester_name']

                if config[self.general_config_name]['tester_id'] is not None: ini['tester_id'] = config[self.general_config_name]['tester_id']

                if config[self.general_config_name]['base_folder'] is not None: ini['base_folder'] = config[self.general_config_name]['base_folder']

            except: print(f'{config_file_name} contains duplicate PRD info.');exit()

           

            ini['dwh'] = config_name

            ini_keys, ini_keys_optional = ['url', 'summary', 'developer', 'svn', 'prd_number', 'environment'], []

            for i in range(2, 20):

                ini_keys_optional.append('svn'+str(i))

            max_ini_key_len = len(max(ini_keys, key=len))

            if config[config_name] is not None:

                nosection = False

                for ini_key in ini_keys:

                    if config[config_name][ini_key] is not None:

                        if config[config_name][ini_key] == '':

                            print(f"{config_file_name} [{config_name}] section {ini_key} key has no value")

                            if ini_key not in ['svn']: exit()

                        ini[ini_key] = config[config_name][ini_key]

                for ini_key2 in ini_keys_optional:

                    if ini_key2 in config[config_name]:

                        if config[config_name][ini_key2] is not None:

                            if config[config_name][ini_key2] == '':

                                print(f"{config_file_name} [{config_name}] section {ini_key2} optional key has no value")

                                exit()

                            ini[ini_key2] = config[config_name][ini_key2]

            #ini['work_folder'] = ini['base_folder'] + '\\' + ini['prd_number'] + '\\' + ini['dwh']

            ini['work_folder'] = ini['base_folder'] + '\\' + ini['prd_number'][:4] + '\\' + ini['prd_number'] + '\\' + ini['dwh']

            ini_keys.append('work_folder')

            print(f"{config_file_name}: [{ini['dwh']}]")

            for ini_key in ini_keys:

                print(f"{ini_key}{' '*(max_ini_key_len-len(ini_key)+1)}: {ini[ini_key]}")

                if ini_key == 'svn' and ini_keys_optional:

                    for ini_key_opt in ini_keys_optional:

                        if ini_key_opt in ini: print(f"{ini_key_opt}{' '*(max_ini_key_len-len(ini_key_opt)+1)}: {ini[ini_key_opt]}")

                       else: break

           

            try: #read PRD info

                prd_config_name = ini['prd_number'] #prd_config_name = 'PRD_'+ini['prd_number']

                config.read(prd_config_name)

                if config[prd_config_name]['prd_description'] is not None:

                    ini['prd_description'] = config[prd_config_name]['prd_description']

                if config[prd_config_name]['prd_folder'] is not None:

                    ini['prd_folder'] = config[prd_config_name]['prd_folder']

                if config[prd_config_name]['jira_test_plan'] is not None:

                    ini['jira_test_plan'] = config[prd_config_name]['jira_test_plan']

            except: print(f'{config_file_name} contains duplicate PRD info.');exit()

 

            return ini

        except:

            if nosection: print(f'{config_file_name} has no [{config_name}] section');exit()

            else: print(f"{config_file_name} [{config_name}] section has a missing key");exit()

 

    def get_args(self):

        '''return args'''

        parser = argparse.ArgumentParser(description='Initialize testing for an EDW JIRA development')

        parser.add_argument("--i", help=".ini filename")

        parser.add_argument("config", help="configuration in .ini file to use")

        parser.add_argument("-z", help="Run post-load testing only", action='store_true')

        args = parser.parse_args()

        return args.i, args.config, args.z

 

def main():

    x = DWHTestInit()

 

if __name__ == '__main__':

    main()

 

'''

TEST TIPS:

Test (1) the load (2) the process

Do ad-hoc testing (smoke test?)

- compare final out view with tables\views at start

- look for truncations, look for types (e.g. money) staying the same

 

do diff back to source db

 

Where tables/views have no data, do they have data in Prod?

Where they have no data in Prod, are they redundant? Either way, no testing can be done on them.

Where they do have data in Prod, is it quite sparse and old?

    Are they synced to DR? If not this would further suggest they are not used (DBAs can add tables to sync if requested but I would question the need/value in this case).

'''
