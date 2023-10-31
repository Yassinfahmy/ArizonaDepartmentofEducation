# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:13:11 2023

@author: yfahmy
"""
import pandas as pd
from datetime import date
from termcolor import colored
from CONNECTION import CONNECTION as con
import traceback
import numpy as np
import math


class DB:
    
    def __init__(self,  fiscal_year=None, run='Prelim', schema=None, database=None, live_server=False):
        # define fiscalYear
        if fiscal_year is None:
           self.fiscal_year = date.today().year
        else:
            self.fiscal_year = fiscal_year
        # print('Fiscal year defined as {}'.format(self.fiscal_year))        
        self.previous_fiscal_year = self.fiscal_year-1
        
        #set run type
        self.run = run.capitalize()
        ##define server name
        self.server_name = 'AACTASTPDDBVM02'
        #if live server grab data from 01
        self.live_server = live_server
        if self.live_server:
            self.live_connection = '[ACCOUNTABILITY_LS].'
        else:
            self.live_connection = ''
            
        #define graduation cohort to snapshot (we report graduation one year in lag)
        self.latest_cohort = self.previous_fiscal_year
        self.earliest_cohort = self.latest_cohort-5
        
        #define static db
        if database is None:
            self.static_db = 'AccountabilityArchive'
        else:
            self.static_db = database
        
        if schema is None:
            self.static_schema = 'Static'
        else:
            self.static_schema = schema
        
    def take_snapshot(self):
        self.census_to_db()
        self.edorg_to_db()
        self.azella_to_db()
        self.assessments_to_db()
        self.enrollment_to_db()
        self.grad_to_db()
        self.dropout_to_db()
        self.trad_ccri_to_db()
        self.alt_ccri_to_db()
        self.persistance_to_db()
        self.ce_to_db()
        self.otg_to_db()
    
    def drop_tables_syntax(self, table_name):
        sql_statment = F'DROP TABLE {table_name}'
        return sql_statment

    def drop_tables_in_run(self, *tables, table_prefix=None):
        if table_prefix is None:
            raise ValueError('Please provide Prefix of table name to "table_prefix" such as "Prelim" or "Final"')
        # setup a connection to db
        cnxn = con().__call__(server_name = self.server_name)
        # start an instance of cursor object
        cursor = cnxn.cursor()
        
        #use list of tables to delete
        for table in tables:
            table_name=F'{self.static_db}.{self.static_schema}.{table_prefix}{table}' + str(self.fiscal_year)
            sql_statment = self.drop_tables_syntax(table_name)
            try:
                cursor.execute(sql_statment)
                cnxn.commit()
                print(F'{table_name} was successfully deleted')
            except Exception:
                print('Table not found or could not be deleted')
                traceback.print_exc()
        cnxn.close()
        
    def excute_sql(self, sql_statment, new_table_name):
        try:
            # setup a connection to db
            cnxn = con().__call__(server_name = self.server_name)
            # start an instance of cursor object
            cursor = cnxn.cursor()
            print(F'Uploading {new_table_name} to {self.server_name}')
            # execute sql statment
            cursor.execute(sql_statment)
            cnxn.commit()
            # close Connection
            cnxn.close()
            # print satus statment
            print(colored(f'{new_table_name} Snapshot (from Live Server -->{self.live_server}) ==> successful', 'green'))
            
        except Exception:
            # print satus statment
            print(colored(f'\033[1mWARNING: \n{new_table_name} Snapshot (from Live Server -->{self.live_server}) ==> FAILED\033[0m', 'red'))
            traceback.print_exc()
    
    def persistance_to_db(self):
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}PersistRate' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[dbo].[PersistenceRate9-12]
                            WHERE FiscalYear = {self.fiscal_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
        
    def ce_to_db(self):
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}CE' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[ccr].[CreditsEarned]
                            WHERE FiscalYear = {self.fiscal_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
        
    def otg_to_db(self):
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}OTG' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[ccr].[OnTracktoGraduate]
                            WHERE FiscalYear = {self.fiscal_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
        
        
    def trad_ccri_to_db(self):
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}TradCCRI' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[ccr].[TraditionalCCRISelfReporting]
                            WHERE FiscalYear = {self.fiscal_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
        
    def alt_ccri_to_db(self):
        
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}AltCCRI' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[ccr].[AlternateCCRISelfReporting]
                            WHERE FiscalYear = {self.fiscal_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
        
    def grad_to_db(self):
        
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}GradRate' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[dbo].[GradRate]
                            WHERE CohortYear >= {self.earliest_cohort} 
                                AND  CohortYear <= {self.latest_cohort}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
    
    def dropout_to_db(self):
        ## do we need the previous fiscalyear
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}DropOut' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[dbo].[DropoutRate9-12]
                            WHERE FiscalYear IN ({self.fiscal_year}, {self.previous_fiscal_year})'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)

            
    def enrollment_to_db(self):
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}FiscalYearEnrollment' + str(self.fiscal_year)

        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[dbo].[FiscalYearEnrollment]
                            WHERE FiscalYear Between {self.fiscal_year-4} AND {self.fiscal_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)

    def assessments_to_db(self):
        begining_year = self.fiscal_year-2
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}Assessments' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[assessment].[StudentAssessment]
                            WHERE FiscalYear >= {begining_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)

    def azella_to_db(self):
        #define the name of the table to create
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}Azella' + str(self.fiscal_year)

        ## setup the sql statment for table creation                                
        sql_statment = F'''SELECT *, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[Legacy].[AZELLAStudentOverallAssessment]
                            WHERE FiscalYear IN ({self.fiscal_year}, {self.previous_fiscal_year})'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
            
    def edorg_to_db(self):

        #define the name of the table to create
        new_table_name =F'{self.static_db}.{self.static_schema}.{self.run}EdOrg' + str(self.fiscal_year)
        ## setup the sql statment for table creation            
        #####--------------- this  produces the DistrictCounty col for each school depending on where the district is located (not where the school is located==> important for public files)             
        sql_statment = F'''SELECT sch.*, lea.DistrictName, lea.CTDS as DistrictCTDS, lea.CountyTypeDesc AS DistrictCounty, GETDATE() as UploadDate
                            INTO {new_table_name}
                            FROM {self.live_connection}[Accountability].[EdOrg].[School] sch
                            INNER JOIN (SELECT a.FiscalYear, DistrictKey, DistrictName, CTDS , CountyTypeDesc
                            			FROM {self.live_connection}[Accountability].[EdOrg].[LEA] a
                            			inner join (select distinct FiscalYear, CountyTypeKey, CountyTypeDesc 
                                                       From {self.live_connection}[Accountability].[EdOrg].[School]) b
                            			 ON a.FiscalYear = b.FiscalYear AND a.CountyTypeKey=b.CountyTypeKey) lea
                                  ON lea.DistrictKey = sch.DistrictKey AND lea.FiscalYear = sch.FiscalYear
                            WHERE sch.FiscalYear = {self.fiscal_year}'''
        # excute the snapshot code
        self.excute_sql(sql_statment, new_table_name)
    
    def census_to_db(self):
        #define the name of the server to connect to (the finance server)
        finance_server = 'AESSPRDDBVM01'
        
        # --------------------------------------------------------------------get census data from finance server
        sql_statment = F'''SELECT 
                              [ServiceType]
                              ,[SAISID]
                              ,[Grade]
                              ,[DependentID]
                          FROM [EssCensus_v2].[dbo].[CenUnduplicatedCount]
                          WHERE FiscalYear = {self.fiscal_year}'''
        #setup a connection to read the data into pandas
        cnxn =  con().__call__(server_name = finance_server)
        print(F'Retrieving data from {finance_server}')
        census = pd.read_sql(sql_statment, cnxn)
        #structure data and de-duplicate and re-upload
        census['FiscalYear'] = self.fiscal_year
        census['UploadDate'] = pd.Timestamp.today()
        census = census[~census.duplicated()]
        #close connection to conserve resources
        cnxn.close()
        # ------------------------------------------------------------------- move census data to 02 server
        #define the name of the table to create on the 02 database
        new_table_name = F'{self.static_db}.{self.static_schema}.{self.run}Census' + str(self.fiscal_year)
        ## setup the sql statment for table creation                                
        sql_create_table = self.create_new_table_syntax(census, new_table_name, all_nvarchar=False)
        #create and upload the census table to 02
        try:
            # setup a connection to db
            cnxn =  con().__call__(server_name = self.server_name)
            # start an instance of cursor object
            cursor = cnxn.cursor()
            # execute sql statment
            cursor.execute(sql_create_table)
            cnxn.commit()
            print(F'Uploading {new_table_name} to {self.server_name}')
            # fill the new table
            self.upload_to_server(census, new_table_name, cnxn)
            # close Connection
            cnxn.close()
            # print satus statement
            print(colored(f'Snapshot of [EssCensus_v2].[dbo].[CenUnduplicatedCount] uploaded to {new_table_name} successfully', 'green'))
            
        except Exception:
            # print satus statment
            print(colored(f'\033[1mWARNING: \nSnapshot of [EssCensus_v2].[dbo].[CenUnduplicatedCount] uploaded to {new_table_name} FAILED\033[0m', 'red'))
            traceback.print_exc()
        
    def create_new_table_syntax(self, df, new_table_name, all_nvarchar=False):
        #--------------------------------------------------this section builds cols according to pandas types assignment
        # df = df.where(pd.notnull(df), None)
        #map python dataypes to sql datatypes
        col_types = df.dtypes.astype(str).to_frame()
        types_map = {'float':'float Null'
                     ,'object':'nvarchar (max) Null'
                     ,'category':'nvarchar (max) Null'
                     ,'bool':'bit NULL'
                     ,'int': 'int Null'
                     ,'datetime': 'DATETIME Null'}
        col_assignment =''
        for col in enumerate(col_types.itertuples()):
            name = ''
            if col[0] != 0:
                name = ', '
            for i in types_map.keys():
                if i in col[1][1]:
                    name += str('"' + col[1][0]) + '" ' + types_map[i]              
            col_assignment += name
        
        
        #--------------------------------------------------this section builds all cols as varchar including dates
        if all_nvarchar:
            col_types = df.columns.to_frame().reset_index(drop=True)
            col_types['type'] = 'nvarchar (max) NULL'
            ##make sure ENtityID and Fiscal year cols stay integers
            mask = (col_types[0].astype(str).str.contains('EntityID', case=False)) | (col_types[0].astype(str).str.contains('FiscalYear', case=False)) | (col_types[0].astype(str).str.contains('SchoolCode', case=False))
            col_types.loc[mask, 'type'] = 'int NULL'
            #--------------------------------------------------
            
            #loop throught to build sql syntax
            col_assignment =''
            for col in col_types.itertuples():
                name = ''
                if col[0] != 0:
                    name = ', '
                name += str('"' + col[1]) + '" ' + str(col[2])
                col_assignment += name
            
        ##----------------------------------------------------- setup the sql statment for table creation                                
        sql_create_table = F'''CREATE TABLE {new_table_name} ({col_assignment})'''
        
        return sql_create_table
    
    def upload_table_to_db (self, df, table_name, all_nvarchar=False):
        

        # 
        #define the name of the table to create
        new_table_name =F'{self.static_db}.{self.static_schema}.{self.run}{table_name}' + str(self.fiscal_year)
        sql_create_table = self.create_new_table_syntax(df, new_table_name, all_nvarchar)

        try:
            # setup a connection to db
            cnxn =  con().__call__(server_name = self.server_name)
            # start an instance of cursor object
            cursor = cnxn.cursor()
            # execute sql statment
            cursor.execute(sql_create_table)
            cnxn.commit()
            print(F'Uploading {new_table_name} to {self.server_name}')

            # upload the new table
            self.upload_to_server(df, new_table_name, cnxn)
            # close Connection
            cnxn.close()
            # print satus statement
            print(colored(f'{table_name} uploaded to {new_table_name} successfully', 'green'))
            
        except Exception:
            # print satus statment
            print(colored(f'\033[1mWARNING: \n{table_name} upload to {new_table_name} FAILED\033[0m', 'red'))
            traceback.print_exc()
            
    def read_table(self, table_name):
        new_table_name =F'{self.static_db}.{self.static_schema}.{self.run}{table_name}' + str(self.fiscal_year)
        sql = F'SELECT * FROM {new_table_name}'
        try:
            # setup a connection to db
            cnxn =  con().__call__(server_name = self.server_name)
            #read data
            df = pd.read_sql(sql,cnxn)
            # close Connection
            cnxn.close()
            # print satus statement
            print(colored(f'{new_table_name} retrieved successfully', 'green'))
            
        except Exception:
            # print satus statment
            print(colored(f'\033[1mWARNING: \nUnable to retrieve {new_table_name}\033[0m', 'red'))
            traceback.print_exc()
        return df
            
    def upload_to_server(self, df, sql_table_name, cnxn):
        cursor = cnxn.cursor()
        cursor.fast_executemany = True
        #convert nan to none (because sql server doesnt accept nan values)
        df = df.replace({np.nan:None})
        ### get the column names in the data frame
        ### also make a list of '?' with same len as column names
        names = ''
        values = ''
        for i in  df.columns:
            if i == df.columns[-1]:
                names = names+'"'+i+'"'
                values = values+'?'
            else:
                names = names+'"'+i+'", '
                values = values+'?,'
        try:
            chunksize = 100000
            for n in range(0,math.ceil(df.shape[0]/chunksize)):
                cursor.executemany(F'''INSERT INTO {sql_table_name} ({names}) values ({values})'''
                                   , df.iloc[n*chunksize:(n+1)*chunksize, :].values.tolist())
                print(f"Total cumulative rows uploaded: {(n+1)*chunksize:,}")
            ### insert our dataframe into the table with fast excution
            # df =list(df.itertuples(index=False))
            # cursor.executemany(f'''INSERT INTO {sql_table_name} ({names}) values ({values})''', df)
            cnxn.commit()
        except Exception:
            print('***ExcuteMany failed***')
            traceback.print_exc()
            ## insert our dataframe into the table
            # for row in df.itertuples(index=False):
            #     cursor.execute(f'''INSERT INTO {sql_table_name} ({names}) values ({values})''', row)