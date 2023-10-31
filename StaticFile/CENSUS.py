# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 10:31:56 2022

@author: yfahmy
"""
import pandas as pd
from CONNECTION import CONNECTION as con
import os
from datetime import date
from SOURCES import SOURCES
from termcolor import colored

class CENSUS(SOURCES):
    
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
     
    def save_data(self, df, path_to_save, file_name):
        try:
            path_to_save = os.path.join(path_to_save, file_name)
            df.to_csv(path_to_save, index=False, na_rep='')
        except:
            print('You must provide a valid file path')
    
    def get_cesnsus_from_database(self):
        print('\nGetting Census data from database')
        #define table name
        if self.server_name == 'AACTASTPDDBVM02':
            table_name = F'AccountabilityArchive.Static.{self.run}Census{self.fiscal_year}'
            server_name = 'AACTASTPDDBVM02'
        else:
            table_name = '[EssCensus_v2].[dbo].[CenUnduplicatedCount]'
            server_name = 'AESSPRDDBVM01'
        
        sql_statment = F'''SELECT
                              [FiscalYear]
                              ,[ServiceType]
                              ,[SAISID]
                              ,[DependentID]
                          FROM {table_name}
                        WHERE FiscalYear = {self.fiscal_year}'''
        cnxn = con().__call__(server_name)
        census = pd.read_sql(sql_statment, cnxn)
        cnxn.close()
        
        if self.raw_folder is not None:
            file_name = f'{self.run}Census'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(census, self.raw_folder, file_name)
            
        return census
        
    def format_census(self, census_raw=None):
        
        if census_raw is None:
            
            census = self.get_cesnsus_from_database()
        else:   
            census = census_raw
            
        print(colored('Starting Census data formating', 'green'))
        #rename variable
        census.rename({'DependentID':'SchoolCode'}, axis=1, inplace=True)
        #deduplicate
        census = census[~census.duplicated()]
        ## if service type =A that means that student is sped on oct 1 and spent 80% or more in regular classroom
        census['SPEDInclusion'] = census.ServiceType.apply(lambda x: 1 if x=='A' else 0)
        ## drop service type col
        census.drop('ServiceType', inplace=True, axis=1)
        #convert to numeric
        for i in census.columns:
            census[i] = pd.to_numeric(census[i], errors='coerce')  
                 
        
        if self.static_folder is not None:
            file_name = 'Census'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(census, self.static_folder, file_name)
            
        return census
