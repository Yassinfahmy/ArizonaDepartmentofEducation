# -*- coding: utf-8 -*-
"""
Created on Mon May  1 14:09:56 2023

@author: YFahmy
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 15:26:30 2023

@author: YFahmy
For historical data on graduation, he current behavior is to pull the data from previous gui tables not from live server data.
"""

from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np
from TABLES import TABLES

class DROPOUT(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        ## these cols are checked to be numeric prior to any wrangling
        self.numeric_cols = ['EntityId', 'FiscalYear', 'NumEnrolled', 'NumDropouts', 'DropoutRate']
        ## this is used to rename the cols for CSi_drilldown
        #values are used as values in pivot operation after dropping EntityID
        self.dd_cols = {'NumDropouts':'NumberDO'
                        ,'NumEnrolled':'NumberEnrolled'
                        ,'DropoutRate':'DR'
                        ,'EntityId':'EntityID'}
        ## this is used to filter out to keys only and rename cols for csi_summary
        self.summary_cols = {'DropoutRate':'DropoutRate'+ str(self.fiscal_year) + 'Pct'
                            ,'EntityId':'EntityID'
                            ,'NumEnrolled':'NumCohort'
                            ,'SchoolTypeF':'SchoolTypeF'}
        ## used to rename cols for atsi and also filter out to keys only
        self.atsi_cols = {'NumDropouts':'NumDrpdOut'
                        ,'NumEnrolled':'NumEnrolled'
                        ,'DropoutRate':'DR'
                        ,'EntityId':'EntityID'
                        ,'SchoolTypeF':'SchoolTypeF'
                        ,'SubGroup':'SubGroup'}
        ## this is used to construct the points cols to match sql server for csi_summary
        self.csi_summary_points_cols = ['DropoutRate'+str(self.fiscal_year)+'Points'
                                        ,'Dropout'
                                        ,'DropoutRatePoints'+str(self.fiscal_year) 
                                        ]
        #retrieve model weights for models that have dropout rate
        self.dropout_rate_weights = {}
        for model, components in self.federal_model_weights.items():
            for component, weight in components.items():
                if 'dropout' in component.lower():
                    self.dropout_rate_weights[model] = weight/100
        
    def calculate_component(self, dropout, schooltype):
        ##------------------------------------------- Data Wrangling
        dropout = dropout.copy()
        ##only keep entries for the right school types
        schools = tuple(self.dropout_rate_weights.keys())
        schooltype = schooltype.query(f'SchoolTypeF in {schools}')
        dropout = pd.merge(dropout, schooltype[['SchoolCode','SchoolTypeF']], left_on='EntityId', right_on='SchoolCode', how='inner')
        
        #make sure numeric cols are numeric
        dropout = self.columns_to_numeric(dropout, self.numeric_cols)
        ## change subgroup names in Type
        dropout['SubGroup'] = dropout.Type.map(self.db_subgroups)
        #filter to right cohort and grad rate type
        mask = (dropout.FiscalYear==self.fiscal_year) & (dropout.SubGroup.notnull())
        dropout = dropout[mask].copy()

        #--------------------------------------------- get results tables
        csi_drilldown = self.get_csi_drilldown(dropout)
        csi_summary = self.get_csi_summary(dropout)
        atsi_1, atsi_2 = self.get_atsi(dropout)
        
        ##merge atsi_1 and atsi_2
        atsi = atsi_1.merge(atsi_2, how='outer', on=[ self.atsi_cols['EntityId'],  self.atsi_cols['SchoolTypeF']])
        
        return csi_summary, csi_drilldown, atsi
        
    def get_atsi(self, dropout):
        ##rename cols
        do = dropout.rename(self.atsi_cols, axis=1).copy()
        
        #-----------------------------------assign points in the top table of atsi only
        do['Dropout'] = (do[ self.atsi_cols['SchoolTypeF']].map(self.dropout_rate_weights) * (100-do[self.atsi_cols['DropoutRate']])).round(2)
        ## --------------apply n-count
        do.loc[do[self.atsi_cols['NumEnrolled']] < self.n_count, 'Dropout'] = np.nan
            
        #build a copy of DropoutRate for extra col
        cy_do = 'FY'+str(self.fiscal_year)+'DR'
        do[cy_do] = do[self.atsi_cols['DropoutRate']].copy()
        
        ##build atsi cols in wide format
        index= [self.atsi_cols['SchoolTypeF'], self.atsi_cols['EntityId']]
        values = [self.atsi_cols['NumDropouts'], self.atsi_cols['NumEnrolled'], self.atsi_cols['DropoutRate'], cy_do, 'Dropout']
        columns = 'SubGroup'
        atsi = pd.pivot(do, index=index, columns=columns, values=values)
        #resolve multi index
        atsi.columns = [i[0]+i[1] for i in atsi.columns]
        
        ##----------------------------- get and merge historic atsi data
        ##----------------------------- split atsi and atsi_2 tables
        # extract all cols containg graduation to atsi_1
        atsi_1_cols = [i for i in  atsi.columns if 'Dropout' in i]
        atsi_1 = atsi[atsi_1_cols].copy().reset_index()
        atsi_2 = atsi.drop(atsi_1_cols, axis=1).reset_index()
        
        return atsi_1, atsi_2
        
    def get_csi_summary(self, dropout):
        #filter out to values of interest only
        mask = (dropout.SubGroup==self.db_subgroups['All'])
        cols_to_keep = dropout.columns.isin(list(self.summary_cols.keys()))
        csi_summary = dropout.loc[mask, cols_to_keep].copy()
        
        # rename cols
        csi_summary.rename(self.summary_cols, axis=1, inplace=True)
        
        ## --------------apply n-count
        csi_summary.loc[csi_summary[self.summary_cols['NumEnrolled']] < self.n_count, self.summary_cols['DropoutRate']] = np.nan
        
        #-----------------------------------assign points
        for i in self.csi_summary_points_cols:
            csi_summary[i] = (csi_summary.SchoolTypeF.map(self.dropout_rate_weights) * (100-csi_summary[self.summary_cols['DropoutRate']])).round(2)
        
        #--- drop un-needed cols
        cols_to_drop = [self.summary_cols['NumEnrolled']]
        csi_summary.drop(cols_to_drop, axis=1, inplace=True)
        return csi_summary
        
    def get_csi_drilldown(self, dropout):
        #rename cols
        do = dropout.rename(self.dd_cols, axis=1).copy()
        
        ##make a copy of dropout rate col and name it by fy for duplicate numbers in server
        do_by_year_col_name = 'FY'+str(self.fiscal_year)+'DR'
        do[do_by_year_col_name] = do[self.dd_cols['DropoutRate']].copy()
        
        ##apply n-count to DR only (They want to see the data else where still) 
        do.loc [do[self.dd_cols['NumEnrolled']] < self.n_count,  self.dd_cols['DropoutRate']] = np.nan
        
        ##--------------------------------------------- create wide data
        index= [self.dd_cols['EntityId'], 'SchoolTypeF']
        columns = 'SubGroup'
        values = [self.dd_cols['NumDropouts'], self.dd_cols['NumEnrolled'], self.dd_cols['DropoutRate'], do_by_year_col_name]
        do_wide = pd.pivot(do, index=index, columns=columns, values=values).reset_index()
        ##resolve multiindex
        do_wide.columns = [i[1]+i[0] for i in do_wide.columns]
        
        ##------------------------------------------ bring in historic data
        historic_dd_do = self.get_historic_csi_dd_data()
        ##merge with data
        csi_drilldown = pd.merge(do_wide, historic_dd_do, on=self.dd_cols['EntityId'], how='left')
        
        ## add fiscal year
        csi_drilldown['FiscalYear'] = self.fiscal_year
        ## add federal model
        csi_drilldown['FederalModel'] = csi_drilldown.SchoolTypeF.map(self.federal_school_type_map)
        
        return csi_drilldown
        
    def get_historic_csi_dd_data(self):
        ##---------------------------------------------------------------- get historic data
        ##connect to db
        db = DATABASE(fiscal_year = self.previous_fiscal_year
                      ,run = ''
                      ,schema = 'ssi'
                      ,database = 'REDATA_UAT')
        
        #setup an empty dataframe to read data into
        historic_dd_do = pd.DataFrame()
        for model, table in self.py_dropout_tables.items():
            data = db.read_table(table_name = table)
            data['SchoolTypeF'] = model
            historic_dd_do = pd.concat([historic_dd_do, data])
        
            
        ##---------------------- filter out the cols of interest only
        ## pull entity id into index
        historic_dd_do = historic_dd_do.set_index('EntityID')
        ## identify cols with cohort years of interest
        regex_exp = f'FY{self.fiscal_year-1}|FY{self.fiscal_year-2}|FY{self.fiscal_year-3}'
        cols_to_keep = historic_dd_do.columns.str.contains(regex_exp, regex=True)
        ##keep only cols of interest
        historic_dd_do = historic_dd_do.loc[:, cols_to_keep].copy().reset_index()
        
        ##---------------------- convert cols to numeric
        historic_dd_do = self.columns_to_numeric(historic_dd_do, list(historic_dd_do.columns))
        
        ##---------------------- change ELFEP1thru4 to  ELFEP14
        historic_dd_do.columns = historic_dd_do.columns.str.replace('ELFEP1thru4', 'ELFEP14')
        ##deduplicate
        historic_dd_do = historic_dd_do[~historic_dd_do.EntityID.duplicated()].copy()
        
        return historic_dd_do
        
            
        
# self = DROPOUT(2022)
# #%%
# fy= 2022
# run = 'Prelim'
# db = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive')
# dropout_raw = db.read_table(table_name ='DropOut')
# schooltype = db.read_table(table_name ='SchoolType')
# #%% calculate EL component
# fy= 2022
# self = DROPOUT(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(dropout_raw, schooltype)

# # Upload data

# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_do_drilldown_columns()
# #find and select cols missing from results tables
# missing_cols = []
# for i in cols:
#     if i not in csi_drilldown.columns:
#         missing_cols.append(i)
#         print(i, 'is missing from results')
# ##make empty cols to replace missing cols (temporarily)
# csi_drilldown.loc[:,missing_cols] = np.nan
# ## select cols and produce ssi.graduation
# csi_drilldown = csi_drilldown[cols]


# data = {'DropoutRate': [csi_drilldown, '', 'ssi']
#         ,'CSIDropoutRate':[csi_summary, 'Prelim', 'Results']
#         ,'ATSIDropoutRate': [atsi, 'Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][1]
#             ,schema = data[i][2]
#             ,database = 'AccountabilityArchive').upload_table_to_db(df=data[i][0], table_name=i)

# #%%upload 2023 table for Mayank
# fy= 2023
# self = DROPOUT(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(dropout_raw, schooltype)
# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_do_drilldown_columns()
# ## select cols and produce ssi.CA
# #find and select cols missing from results tables
# missing_cols = []
# for i in cols:
#     if i not in csi_drilldown.columns:
#         missing_cols.append(i)
#         print(i, 'is missing & will be recreated as an empty column')
# ##make empty cols to replace missing cols (temporarily)
# csi_drilldown.loc[:,missing_cols] = np.nan
# ## select all returned cols and upload to db
# csi_drilldown = csi_drilldown[cols]

# DATABASE(fiscal_year = fy
#         ,run = ''
#         ,schema = 'ssi'
#         ,database = 'AccountabilityArchive').upload_table_to_db(df=csi_drilldown, table_name='DropoutRate')

# #%%drop tables
# fy= 2022
# data = {'CSIDropoutRate':['Prelim', 'Results']
#         ,'DropoutRate': ['', 'ssi']
#         ,'ATSIDropoutRate': ['Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][0]
#             ,schema = data[i][1]
#             ,database = 'AccountabilityArchive').drop_tables_in_run(i, table_prefix=data[i][0])