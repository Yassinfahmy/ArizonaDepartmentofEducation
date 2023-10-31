# -*- coding: utf-8 -*-
"""
Created on Fri May 12 12:54:15 2023

@author: YFahmy
"""

from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np



class CCRI(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.naming_convention = {'SchoolID':'EntityID'
                             ,'FiscalYear':'FiscalYear'
                             ,'IsEligible': 'Eligible'
                             ,'Points':'CollegeandCareerReady_SRSS'
                             ,'BonusPoint':'CCRIBonusPoint'}
        self.numeric_cols = [self.naming_convention['FiscalYear'], self.naming_convention['SchoolID'], self.naming_convention['Points'], self.naming_convention['BonusPoint']]
        
        self.points_cap = {}
        for model, components in self.models_component_weights.items():
            for component, weight in components.items():
                if 'CollegeandCareerReady_SRSS'.lower() in component.lower():
                        self.points_cap[model]=weight

    def calculate_component(self, trad, alt, schooltype):
        #====================== Some Data Wrangling ===========================
        if self.include_late_submissions:
            trad, alt= self.update_ccri_raw_data(trad, alt)
            
        data = pd.DataFrame()
        for df, schooltypes in zip([trad, alt], [0, 1]):
            #change col names
            df_temp = df.rename(self.naming_convention, axis=1).copy()
            #add bonus point col for alt schools and fill it with zeros (they don't get bonus points)
            if schooltypes ==1:
                df_temp[self.naming_convention['BonusPoint']] = np.nan
            #convert cols to numeric
            df_temp = self.columns_to_numeric(df_temp, self.numeric_cols)
            
            #keep relevant cols only
            df_temp = df_temp[list(self.naming_convention.values())]
            # add type indicator
            if schooltypes ==1:
                mask = schooltype.Alternative==True
                df_temp = pd.merge(df_temp, schooltype.loc[mask,['SchoolCode', 'Alternative']], left_on ='EntityID', right_on='SchoolCode')
                df_temp['Model'] ='Alt 9-12'
            else:
                mask = schooltype.Alternative==False
                df_temp = pd.merge(df_temp, schooltype.loc[mask,['SchoolCode', 'Alternative']], left_on ='EntityID', right_on='SchoolCode')
                df_temp['Model'] = '9-12'
            
            #convert all missing eligibility (those who did not submit) to eligible
            df_temp.loc[df_temp.Eligible.isnull(), 'Eligible'] = True
            # convert eligibility col to bool
            df_temp['Eligible'] = df_temp.Eligible.astype('bool')
            #keep current fiscalyear data only
            df_temp = df_temp[df_temp.FiscalYear==self.fiscal_year].copy()
            #drop fiscalyear
            df_temp.drop('FiscalYear', inplace=True, axis=1)
            #concat to data
            data = pd.concat([data, df_temp], axis=0)
        #======================================================================

        ##-----------------------------------------------------get summary cols
        ### remove points for in-eligible schools
        ineligible_mask = data.Eligible==False
        data.loc[ineligible_mask, ['CollegeandCareerReady_SRSS', self.naming_convention['BonusPoint']]] = np.nan
        ## all those eligible with missing points are assigned zero points
        didnt_submit_mask = (data.Eligible==True) & (data.CollegeandCareerReady_SRSS.isnull())
        data.loc[didnt_submit_mask, 'CollegeandCareerReady_SRSS'] = 0
        
        ##cap points
        for model, cap in self.points_cap.items():
            mask = (data.Model.str.lower() == model.lower()) & (data.CollegeandCareerReady_SRSS > cap)
            data.loc[mask, 'CollegeandCareerReady_SRSS'] = cap

        
        ##make SelfReportCCRPoints col
        data['SelfReportCCRPoints'] = data['CollegeandCareerReady_SRSS'].copy()
        ##make fiscalyear col
        data['FiscalYear'] = self.fiscal_year
        
        # data = self.add_suffices(data)
        return data
        
    def add_suffices(self, data):
        ## add suffices
        trad_cols = ['CCRIBonusPoint']
        extra_cols = ['Eligible', 'Alternative']
        new_names=[]
        for col in data.columns:
            added=0
            for tcol in trad_cols:
                if tcol.lower() in col.lower():
                    new_names.append(col+'_Trad')
                    added =1
                    break
            #check if they are in extra col list
            for ecol in extra_cols:
                if ecol.lower() in col.lower():
                    new_names.append(col+'_Extra')
                    added =1
                    break
            #check if name has been added already, if so skip to next iteration
            if added ==1:
                continue
            else:
                new_names.append(col+'_All')
        ##assign names to data
        data.columns = new_names
        return data
    
    def fetch_late_ccri_submissions(self):
        db = DATABASE(fiscal_year=self.fiscal_year, database='AccountabilityArchive', schema='dbo', run='', server_name='AACTASTPDDBVM02')
        sql = f"""Select [FiscalYear]
                  ,[SchoolID]
                  ,[IsEligible]
                  ,[Points]
                  ,[BonusPoint]
                  ,[StateModel]
                  ,[CreatedBy]
                  ,[LastModifiedByEmail]
                  ,[LastModifiedDate]
                  FROM [AccountabilityArchive].[dbo].[LateLateSelfReportedData]
                 Where FiscalYear={self.fiscal_year} and type='CCRI'"""
        late_ccri = db.read_sql_query(sql=sql)
        return late_ccri
    
    def update_ccri_raw_data(self, trad, alt):
        
        late_ccri = self.fetch_late_ccri_submissions()
        late_ccri['IsEligible'] = late_ccri['IsEligible'].apply(lambda x: True if x==1 else False)
        
        late_trad = late_ccri[late_ccri.StateModel.str.contains('trad', regex=True, case=False)].copy()
        late_alt = late_ccri[late_ccri.StateModel.str.contains('alt', regex=True, case=False)].copy()
        
        #delete the late submission value from original table and add new data
        trad = trad[~trad.SchoolID.isin(late_trad.SchoolID)].copy()
        trad = pd.concat([trad, late_trad], axis=0)
        
        alt = alt[~alt.SchoolID.isin(late_alt.SchoolID)].copy()
        alt = pd.concat([alt, late_alt], axis=0)
        return trad, alt
                    
            
self = CCRI(2023, 'prelimv6')

#%% bring in data
# fy = 2023
# run = 'PrelimV6'

# db = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive')
# trad_table_name = 'TradCCRI'
# trad = db.read_table(table_name =trad_table_name, cy_data_only=True, suffix_fy=True)
# alt_table_name = 'AltCCRI'
# alt = db.read_table(table_name =alt_table_name, cy_data_only=True, suffix_fy=True)
# schooltype_table_name = 'SchoolType'
# schooltype = db.read_table(table_name =schooltype_table_name, cy_data_only=True, suffix_fy=True)

# #%% Make instance of class and calc CCRI
# fy=2022
# self = CCRI(fy)
# data = self.calculate_component(trad, alt)

# # Upload data
# table_name='StateCCRI'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#         ,run = run
#         ,schema = 'Results'
#         ,database = 'AccountabilityArchive').upload_table_to_db(df=data, table_name=table_name)
# #%%drop table

# fy= 2022
# table_name='StateCCRI'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').drop_tables_in_run(table_name, table_prefix=run)