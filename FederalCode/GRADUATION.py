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

class GRADUATION(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        ## these cols are checked to be numeric prior to any wrangling
        self.numeric_cols = ['EntityId', 'CohortYear', 'NumCohort', 'NumGraduates', 'GradRateType', 'GradRate']
        ## this is used to filter the grad rate type to 4 year gard rate as per CSI-LA
        self.grade_rate_type = 4
        ## this is used to rename the cols for CSi_drilldown
        #values are used as values in pivot operation after dropping EntityID
        self.dd_cols = {'NumGraduates':'NG'
                        ,'NumCohort':'NC'
                        ,'GradRate':'GR'
                        ,'EntityId':'EntityID'}
        ## this is used to filter out to keys only and rename cols for csi_summary
        self.summary_cols = {'GradRate':'Cohort'+ str(self.previous_fiscal_year) + '5YearGradtRatePct'
                            ,'EntityId':'EntityID'
                            ,'NumCohort':'NumCohort'
                            ,'SchoolTypeF':'SchoolTypeF'}
        ## used to rename cols for atsi and also filter out to keys only
        self.atsi_cols = {'NumGraduates':'NumGR'
                        ,'NumCohort':'NumCohort'
                        ,'GradRate':'GR'
                        ,'EntityId':'EntityID'
                        ,'SchoolTypeF':'SchoolTypeF'
                        ,'SubGroup':'SubGroup'}
        ## this is used to construct the points cols to match sql server for csi_summary
        self.csi_summary_points_cols = ['Cohort'+ str(self.previous_fiscal_year) +'5YearGradtRatePoints', 'Graduation', 'GraduationRatePoints'+str(self.fiscal_year)]
        #retrieve model weight for those that have grad rate
        self.grad_rate_weights = {}
        for model, components in self.federal_model_weights.items():
            for component, weight in components.items():
                if 'graduation' in component.lower():
                    self.grad_rate_weights[model] = weight/100
        
    def calculate_component(self, gradrates, schooltypes):
        ##------------------------------------------- Data Wrangling
        ##only keep entries for the right school types
        schools = tuple(self.grad_rate_weights.keys())
        schooltype = schooltypes.query(f'SchoolTypeF in {schools}').copy()
        grad_rates = pd.merge(gradrates, schooltype[['SchoolCode','SchoolTypeF']], left_on='EntityId', right_on='SchoolCode', how='inner')
        
        #make sure numeric cols are numeric
        grad_rates = self.columns_to_numeric(grad_rates, self.numeric_cols)
        ## change col name types
        grad_rates['SubGroup'] = grad_rates.Type.map(self.db_subgroups)
        #filter to right cohort and grad rate type
        mask = (grad_rates.GradRateType==self.grade_rate_type) & (grad_rates.CohortYear==self.previous_fiscal_year) & (grad_rates.SubGroup.notnull())
        grad_rates = grad_rates[mask].copy()

        #--------------------------------------------- get results tables
        csi_drilldown = self.get_csi_drilldown(grad_rates)
        csi_summary = self.get_csi_summary(grad_rates)
        atsi_1, atsi_2 = self.get_atsi(grad_rates)
        
        ##merge atsi_1 and atsi_2
        atsi = atsi_1.merge(atsi_2, how='outer', on=[ self.atsi_cols['EntityId'],  self.atsi_cols['SchoolTypeF']])
        
        return csi_summary, csi_drilldown, atsi
        
    def get_atsi(self, grad_rates):
        ##rename cols
        gr = grad_rates.rename(self.atsi_cols, axis=1).copy()
        
        #-----------------------------------assign points in the top table of atsi only
        gr['Graduation'] = (gr[ self.atsi_cols['SchoolTypeF']].map(self.grad_rate_weights) * (gr[self.atsi_cols['GradRate']])).round(2)
        ## --------------apply n-count
        gr.loc[gr[self.atsi_cols['NumCohort']] < self.n_count, 'Graduation'] = np.nan
            
        #build a copy of grad rate for extra col
        cy_gr = 'CY'+str(self.previous_fiscal_year)+'GR'
        gr[cy_gr] = gr[self.atsi_cols['GradRate']].copy()
        
        ##build atsi cols in wide format
        index= [self.atsi_cols['SchoolTypeF'], self.atsi_cols['EntityId']]
        values = [self.atsi_cols['NumGraduates'], self.atsi_cols['NumCohort'], self.atsi_cols['GradRate'], cy_gr, 'Graduation']
        columns = 'SubGroup'
        atsi = pd.pivot(gr, index=index, columns=columns, values=values)
        #resolve multi index
        atsi.columns = [i[0]+i[1] for i in atsi.columns]
        
        ##----------------------------- split atsi and atsi_2 tables
        # extract all cols containg graduation to atsi_1
        atsi_1_cols = [i for i in  atsi.columns if 'Graduation' in i]
        atsi_1 = atsi[atsi_1_cols].copy().reset_index()
        atsi_2 = atsi.drop(atsi_1_cols, axis=1).reset_index()
        
        return atsi_1, atsi_2
        
    def get_csi_summary(self, grad_rates):
        #filter out to values of interest only
        mask = (grad_rates.SubGroup==self.db_subgroups['All'])
        cols_to_keep = grad_rates.columns.isin(list(self.summary_cols.keys()))
        csi_summary = grad_rates.loc[mask, cols_to_keep].copy()
        
        # rename cols
        csi_summary.rename(self.summary_cols, axis=1, inplace=True)
        
        ## --------------apply n-count
        csi_summary.loc[csi_summary[self.summary_cols['NumCohort']] < self.n_count, self.summary_cols['GradRate']] = np.nan
        
        #-----------------------------------assign points
        for i in self.csi_summary_points_cols:
            csi_summary[i] = (csi_summary.SchoolTypeF.map(self.grad_rate_weights) * (csi_summary[self.summary_cols['GradRate']])).round(2)
            
        #--- drop un-needed cols
        cols_to_drop = ['NumCohort']
        csi_summary.drop(cols_to_drop, axis=1, inplace=True)
        return csi_summary
        
    def get_csi_drilldown(self, grad_rates):
        #rename cols
        gr = grad_rates.rename(self.dd_cols, axis=1).copy()
        
        ##make a copy of graduation rate col and name it by cohortyear for duplicate numbers in server
        gr_by_cohort_col_name = 'Cohort'+str(self.previous_fiscal_year)+'GR'
        gr[gr_by_cohort_col_name] = gr.GR.copy()
        
        ##apply n-count to GR only (They want to see the data else where still) 
        gr.loc [gr.NC < self.n_count,  self.dd_cols['GradRate']] = np.nan
        
        ##--------------------------------------------- create wide data
        index= [self.dd_cols['EntityId'], 'SchoolTypeF']
        columns = 'SubGroup'
        values = [self.dd_cols['NumGraduates'], self.dd_cols['NumCohort'], self.dd_cols['GradRate'], gr_by_cohort_col_name]
        gr_wide = pd.pivot(gr, index=index, columns=columns, values=values).reset_index()
        ##resolve multiindex
        gr_wide.columns = [i[1]+i[0] for i in gr_wide.columns]
        
        ##------------------------------------------ bring in historic data
        historic_dd_gr = self.get_historic_csi_dd_data()
        ##merge with data
        csi_drilldown = pd.merge(gr_wide, historic_dd_gr, on=self.dd_cols['EntityId'], how='left')
        
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
        historic_dd_gr = pd.DataFrame()
        for model, table in self.py_gradrate_tables.items():
            data = db.read_table(table_name = table)
            data['SchoolTypeF'] = model
            historic_dd_gr = pd.concat([historic_dd_gr, data])
        
            
        ##---------------------- filter out the cols of interest only
        ## pull entity id into index
        historic_dd_gr = historic_dd_gr.set_index('EntityID')
        ## identify cols with cohort years of interest
        regex_exp = f'Cohort{self.fiscal_year-2}|Cohort{self.fiscal_year-3}|Cohort{self.fiscal_year-4}'
        cols_to_keep = historic_dd_gr.columns.str.contains(regex_exp, regex=True)
        ##keep only cols of interest
        historic_dd_gr = historic_dd_gr.loc[:, cols_to_keep].copy().reset_index()
        
        ##---------------------- convert cols to numeric
        historic_dd_gr = self.columns_to_numeric(historic_dd_gr, list(historic_dd_gr.columns))
        
        ##---------------------- change ELFEP1thru4 to  ELFEP14
        historic_dd_gr.columns = historic_dd_gr.columns.str.replace('ELFEP1thru4', 'ELFEP14')
        ##deduplicate
        historic_dd_gr = historic_dd_gr[~historic_dd_gr.EntityID.duplicated()].copy()
        
        return historic_dd_gr
    
    
    def calculate_csi_G(self, gradrates, schooltypes, oct1_filepath:str=None, oct1_sheetname:str=None, new_identification:bool=False, **kwargs):
        gradrates = gradrates.rename(columns={"EntityId":"EntityID"})

        # get Series of schools for which we want csi-G results
        models_being_used = tuple(self.grad_rate_weights.keys())
        schools_included = schooltypes.query(f'SchoolTypeF in {models_being_used}')["SchoolCode"].copy()
        

        # load in historical csiG data from REDATA_UAT
        db = DATABASE(fiscal_year = self.previous_fiscal_year, run = '', schema = 'ssi', database = 'REDATA_UAT')
        csi_G_history = db.read_table(table_name="SummaryCSIG")

        # define historical columns to get
        historical_columns = [f"AllStudentsCohort{db.fiscal_year-3}", f"AllStudentsCohort{db.fiscal_year-2}", f"AllStudentsCohort{db.fiscal_year-1}"]

        # filter for relevant schools and columns 
        csi_G_history = csi_G_history[csi_G_history["EntityID"].isin(schools_included)][["EntityID"] + historical_columns].set_index("EntityID")


        # load in 5-year grad rates for relevant schools
        sql_subgroups = ['All', 'American Indian or Alaska Native', 'Asian', 'Black/African American', 'Hispanic or Latino', 
            'Native Hawaiian or Pacific Islander', 'White', 'Multiple Races', 'ELL Fep', 'SPED', 'Low SES']
        result_subgroups = ['AllStd', 'NativeAmerican', 'Asian', 'AfricanAmerican', 'HispanicLatino', 'PacificIslander', 'White',
            'TwoorMoreRaces', 'ELFEP1thru4', 'SPED', 'IE12']
        subgroup_mapping = dict(zip(sql_subgroups, result_subgroups))

        csi_G_grad_rates = gradrates[(gradrates["EntityID"].isin(schools_included)) & (gradrates["CohortYear"]==self.fiscal_year-1) & (gradrates["GradRateType"]==5) & (gradrates["Type"].isin(sql_subgroups))].rename(
            columns={"Type":"Subgroup"})[["EntityID", "Subgroup", "NumCohort", "NumGraduates", "GradRate"]].replace({"Subgroup":subgroup_mapping})
        
        csi_G_grad_rates = pd.pivot(csi_G_grad_rates, index='EntityID', columns=['Subgroup'], values=['NumCohort', 'NumGraduates', 'GradRate'] ).reset_index(col_level=1)

        column_names =[]
        for i in csi_G_grad_rates.columns:
            if 'NumGraduates' in i[0]:
                column_names.append((i[1]+'NG'))
            elif 'GradRate' in i[0]:
                column_names.append((i[1]+f'Cohort{self.fiscal_year-1}'))
            elif 'NumCohort' in i[0]:
                column_names.append((i[1]+'NC'))
            else:
                column_names.append(i[1])
        csi_G_grad_rates = csi_G_grad_rates.droplevel(0, axis=1)
        csi_G_grad_rates.columns = column_names
        csi_G_grad_rates = csi_G_grad_rates.set_index("EntityID")


        # load in Oct 1 enrollment report
        if oct1_filepath is None: oct1_filepath = r"\\Asasprdvm01\acct\ACCOUNTABILITY\2022\Public files\1 - preliminary\Oct1Enrollment2023.xlsx"
        if oct1_sheetname is None: oct1_sheetname = "School by Ethnicity"
        oct1_enroll = pd.read_excel(oct1_filepath, sheet_name=oct1_sheetname).rename(columns={"School Entity ID":"EntityID", "Total UR":"TotalEnrollmentOct1"})[["EntityID", "TotalEnrollmentOct1"]]
        # "Asian UR", "Black/African American UR":"African", "Hispanic/Latino UR", "American Indian/Alaskan Native UR", "Native Hawaiian/Pacific Islander UR", "Multiple Races UR", "White UR", "Total UR"
        oct1_enroll = oct1_enroll[(oct1_enroll["EntityID"].isin(schools_included))].set_index("EntityID")


        # merge 5-year grad rates, historical csi-G results, and oct 1 enrollment total  on EntityID
        if new_identification:
            csi_G_data = pd.concat([csi_G_history, csi_G_grad_rates, oct1_enroll], axis=1).reset_index() # how should we handle missing values?
        else:
            csi_G_data = pd.merge(csi_G_history, csi_G_grad_rates, left_index=True, right_index=True, how="inner") # only include schools that were previously being tracked
            csi_G_data = pd.merge(csi_G_data, oct1_enroll, left_index=True, right_index=True, how="left").reset_index()
        # edit columns to match expected sql output
        csi_G_data["FiscalYear"] = self.fiscal_year
        csi_G_data[f"AllStudentsCohort{self.fiscal_year-1}"] = csi_G_data[f"AllStdCohort{self.fiscal_year-1}"]

        # identification. How? Using Cohort 2022?
        csi_G_data["IdentifiedforCSILowGradRate"] = np.nan

        return csi_G_data

        
            
        
# self = GRADUATION(2022)
# #%%
# fy= 2022
# run = 'Prelim'
# db = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive')
# gradrates = db.read_table(table_name ='GradRate')
# schooltypes = db.read_table(table_name ='SchoolType')
# #%% calculate component
# fy= 2022
# self = GRADUATION(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(gradrates, schooltypes)

# # Upload data

# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_gr_drilldown_columns()
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


# data = {'GraduationRate': [csi_drilldown, '', 'ssi']
#         ,'CSIGraduationRate':[csi_summary, 'Prelim', 'Results']
#         ,'ATSIGraduationRate': [atsi, 'Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][1]
#             ,schema = data[i][2]
#             ,database = 'AccountabilityArchive').upload_table_to_db(df=data[i][0], table_name=i)

# #%%upload 2023 table for Mayank
# fy= 2023
# self = GRADUATION(fy)
# csi_summary, csi_drilldown, atsi = self.calculate_component(gradrates, schooltypes)
# ## get list of column names for CSI drilldown and use it to arrange cols
# cols = TABLES(fy).get_csi_gr_drilldown_columns()
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
#         ,database = 'AccountabilityArchive').upload_table_to_db(df=csi_drilldown, table_name='GraduationRate')

# #%%drop tables
# fy= 2022
# data = {'CSIGraduationRate':['Prelim', 'Results']
#         ,'GraduationRate': ['', 'ssi']
#         ,'ATSIGraduationRate': ['Prelim', 'Results']}

# for i in data.keys():
#     DATABASE(fiscal_year = fy
#             ,run = data[i][0]
#             ,schema = data[i][1]
#             ,database = 'AccountabilityArchive').drop_tables_in_run(i, table_prefix=data[i][0])