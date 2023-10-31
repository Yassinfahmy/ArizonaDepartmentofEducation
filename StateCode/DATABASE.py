# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 15:55:55 2023

@author: yfahmy
"""

import pyodbc
import pandas as pd
from termcolor import colored
import traceback
import numpy as np
import math
from datetime import date

class DATABASE:
    
    def __init__(self,  fiscal_year=None, database='AccountabilityArchive', schema='Static', run='Prelim', server_name='AACTASTPDDBVM02'):
        '''
        
        Parameters
        ----------
        fiscal_year : TYPE, optional
            DESCRIPTION. The default is None.
        database : TYPE, optional
            DESCRIPTION. The default is 'AccountabilityArchive'.
        schema : TYPE, optional
            DESCRIPTION. The default is 'Static'.
        run : TYPE, optional
            DESCRIPTION. The default is 'Prelim'.

        Returns
        -------
        None.

        '''
        # define fiscalYear
        if fiscal_year is None:
           self.fiscal_year = date.today().year
        else:
            self.fiscal_year = fiscal_year
        # print('Fiscal year defined as {}'.format(self.fiscal_year))        
        self.previous_fiscal_year = self.fiscal_year-1
        
        #set run type
        self.run = run.capitalize()
        #set schema to write to
        self.schema = schema
        #set database name
        self.database = database

        ##define server name
        self.server_name = server_name
        
    def connect_to_db(self):
        '''
        Setup up a method to connect to DB using your preffered way. then alter this function to connect to DB by calling
        self."your method" instead of self.connect_with_pyodbc()

        Returns
        -------
        cnxn : object
            DESCRIPTION.

        '''
        cnxn = self.connect_with_pyodbc()
        return cnxn
    
    def  connect_with_pyodbc(self):
        # setup a connection using pyODBC
        cnxn_str = (f'''Driver=ODBC Driver 17 for SQL Server;
                    Server={self.server_name};
                    Trusted_Connection=yes;''')
        cnxn = pyodbc.connect(cnxn_str)
        
        return cnxn
    
    def drop_tables_syntax(self, table_name):
        sql_statment = F'DROP TABLE {table_name}'
        return sql_statment

    def drop_tables_in_run(self, *tables):
        # setup a connection to db
        cnxn = self.connect_to_db()
        # start an instance of cursor object
        cursor = cnxn.cursor()
        
        #use list of tables to delete
        for table in tables:
            table_name=F'{self.database}.{self.schema}.{self.run}{table}' + str(self.fiscal_year)
            sql_statment = self.drop_tables_syntax(table_name)
            try:
                cursor.execute(sql_statment)
                cnxn.commit()
                print(F'{table_name} was successfully deleted')
            except Exception:
                print(f'{table_name} NOT found or could not be deleted')
                traceback.print_exc()
        cnxn.close()
        
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
            mask = (col_types[0].astype(str).str.contains('EntityID', case=False)) | (col_types[0].astype(str).str.contains('FiscalYear', case=False))
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
    
    def upload_table_to_db (self, df, table_name, suffix_fy=True, all_nvarchar=False):
        '''
        This is the right method to use from outside of class
        Parameters
        ----------
        df : pandas DF to upload
            DESCRIPTION.
        table_name : str
            DESCRIPTION: The table name in the server without prefix or suffix
        all_nvarchar : bool
            DESCRIPTION. The default is False.

        Returns
        -------
        None.

        '''
        #define the name of the table to create
        if suffix_fy:
            year=str(self.fiscal_year)
        else:
            year=''
        new_table_name =F'{self.database}.{self.schema}.{self.run}{table_name}' + year
        sql_create_table = self.create_new_table_syntax(df, new_table_name, all_nvarchar)


        # setup a connection to db
        cnxn =  self.connect_to_db()
        # start an instance of cursor object
        cursor = cnxn.cursor()
        try:
            # execute sql statment
            cursor.execute(sql_create_table)
            cnxn.commit()
            print(colored(f'\n{new_table_name} Created Successfully', 'green'))
        except Exception:
            # print satus statment
            print(colored(f'\033[1mWARNING: \n{sql_create_table} FAILED\033[0m', 'red'))
            traceback.print_exc()
        else:
            if all_nvarchar:
                df.fillna('.', inplace=True)
            else:
                #convert nan to none (because sql server doesnt accept nan values)
                df = df.replace({np.nan:None})
            # upload the new table
            self.upload_to_server(df, new_table_name, cnxn)
            
    def fill_table (self, dataframe, table_name, clear_table=True, all_data=False, copy=False, cols_to_ignore=None):
        '''
        This is the right method to use from outside of class
        Parameters
        ----------
        df : pandas DF to upload
            DESCRIPTION.
        table_name : str
            DESCRIPTION: The table name in the server without prefix or suffix
        all_nvarchar : bool
            DESCRIPTION. The default is False.

        Returns
        -------
        None.

        '''
        ## make a list of cols to ignore in the target table
        if cols_to_ignore is None:
            cols_to_ignore = ['Key', 'CreatedBy', 'CreatedDate', 'LastModifiedBy', 'LastModifiedDate', 'KThru8', 'AThruF', 'NTSCLGID']
        # setup a connection to db
        cnxn =  self.connect_to_db()
        cursor = cnxn.cursor()
        #clear table if indicated
        if clear_table:
            try:
                if all_data is False:
                    delete_cy = f'WHERE FiscalYear = {self.fiscal_year}'
                else:
                    delete_cy = ''
                sql_clear = f'Delete {table_name} {delete_cy}'
                cursor.execute(sql_clear)
                print(colored(f'"{sql_clear}" ==> Executed Successfully', 'green'))
            except Exception as ex:
                print(ex)
                print(colored(f'Warning:\n"Delete {table_name} WHERE FiscalYear = {self.fiscal_year}" ==> Failed', 'red'))
                
        try:
            ## read empty table cols
            sql_read = f'SELECT top(1)* FROM {table_name}'
            target_table = pd.read_sql_query(sql_read, cnxn)
            print(colored(f'"{table_name}" ==> read Successfully', 'green'))
        except Exception as ex:
            print(colored(f'"{sql_read}" ==> Failed to read target table', 'red'))
            print(ex)
            print('******************************************\n')
        else:
            # an option as to wether the upload modifies the source table or not
            if copy:
                df = dataframe.copy()
            else:
                df = dataframe
                
            ## get a dataframe with all the cols from the target table thats missing from the source df
            missing_cols = target_table.loc[:, ~target_table.columns.astype(str).str.lower().isin(df.columns.astype(str).str.lower())]
            ## drop the cols that need to be ignored
            for col in cols_to_ignore:
                mask = missing_cols.columns.astype(str).str.contains(col, regex =True, case=False)
                missing_cols = missing_cols.loc[:, ~mask].copy()
            
            ##outline missing cols and print them for user
            if missing_cols.shape[1]>0:
                print(colored(f'****Warning****:\n{missing_cols.shape[1]} columns in "{table_name}" Not found in source DataFrame:', 'red'))
                print(*missing_cols.columns.to_list())
                print(colored('Columns will be filled with "."', 'red'))
                #recreat misssing cols as empty fields
                df[missing_cols.columns.to_list()] = np.nan
            ##=================================================upload table
            ##select relevant cols
            df = df.loc[:, df.columns.astype(str).str.lower().isin(target_table.columns.astype(str).str.lower())].copy()
            #convert nan to '.'
            # df = df.replace({np.nan:'.'})
            df.fillna('.', inplace=True)
            df = df.astype(str)
            # upload the new table
            self.upload_to_server(df, table_name, cnxn)
            
            
    def read_table(self, table_name, cy_data_only=False, suffix_fy=True, prefix_run=True):
        '''
        Parameters
        ----------
        table_name : str
            DESCRIPTION: The table name ithout prefix or suffix

        Returns
        -------
        df : padas DF
            DESCRIPTION.

        '''
        if prefix_run:
            run = self.run
        else:
            run=''
            
        if suffix_fy:
            new_table_name =F'[{self.database}].[{self.schema}].[{run}{table_name}' + str(self.fiscal_year)+']'
        else:
            new_table_name =F'[{self.database}].[{self.schema}].[{run}{table_name}]'
        
        if cy_data_only:
            sql = F'SELECT * FROM {new_table_name} WHERE FiscalYear={self.fiscal_year}'
        else:
            sql = F'SELECT * FROM {new_table_name}'
            
        try:
            # setup a connection to db
            cnxn =  self.connect_to_db()
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
    
    def read_sql_query(self, sql):

        try:
            # setup a connection to db
            cnxn =  self.connect_to_db()
            #read data
            df = pd.read_sql(sql, cnxn)
            # close Connection
            cnxn.close()
            # print satus statement
            print(colored('Data retrieved successfully', 'green'))
            
        except Exception:
            # print satus statment
            print(colored(f'\033[1mWARNING: \nUnable to run: \n{sql}\033[0m', 'red'))
            traceback.print_exc()
        return df
            
    def upload_to_server(self, df, sql_table_name, cnxn):
        cursor = cnxn.cursor()
        cursor.fast_executemany = True
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
            print(f'Uploading {sql_table_name} to {self.server_name}')
            chunksize=100000
            for n in range(0,math.ceil(df.shape[0]/chunksize)):
                cursor.executemany(F'''INSERT INTO {sql_table_name} ({names}) values ({values})'''
                                   , df.iloc[n*chunksize:(n+1)*chunksize, :].values.tolist())
                print(f"Total cumulative rows uploaded up to: {(n+1)*chunksize:,}")
                
            # print satus statement
            print(colored(f'{sql_table_name} uploaded successfully', 'green'))
            print('******************************************\n')
            ### insert our dataframe into the table with fast excution
            # df =list(df.itertuples(index=False))
            # cursor.executemany(f'''INSERT INTO {sql_table_name} ({names}) values ({values})''', df)
            cnxn.commit()
            cnxn.close()
        except Exception as ex:
            print(colored(f'\033[1mWARNING: \n{sql_table_name} upload FAILED\033[0m', 'red'))
            print(ex)
            print('******************************************\n')
            ## insert our dataframe into the table
            # for row in df.itertuples(index=False):
            #     cursor.execute(f'''INSERT INTO {sql_table_name} ({names}) values ({values})''', row)