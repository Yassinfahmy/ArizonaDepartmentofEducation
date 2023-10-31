# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 12:24:49 2022

By: Yassin Fahmy
"""
import pandas as pd
pd.options.display.max_columns=200
from CONNECTION import CONNECTION as con

import numpy as np
from SOURCES import SOURCES
from termcolor import colored

class EDORG(SOURCES):
    '''
    self.grade_map (dict): holds the str characters in grades as keys and their numeric transformation according to the codebook as values
    self.fiscal_year (int): holds current year
    '''
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        
    def get_first_year_schools(self):
        server_name = 'AACTASTPDDBVM01'
        sql_statment = F'''SELECT 
                                SchoolId as SchoolCode
                                ,1 as Year1School
                            FROM Accountability.dbo.FiscalYearEnrollment       
                            GROUP BY SchoolID
                            having min (fiscalYear) = {self.fiscal_year}'''
        cnxn = con().__call__(server_name)
        year1_schools = pd.read_sql(sql_statment, cnxn)
        cnxn.close()
        return year1_schools
    
    def get_model_corrections(self):
        server_name = 'AACTASTPDDBVM02'
        sql_statment = F'''SELECT [FiscalYear]
                                  ,[SchoolCode]
                                  ,[CorrectedModelType]
                                  ,[AccountabilityModel]
                              FROM [AccountabilityArchive].[dbo].[AccountabilitySchoolModels]
                              Where FiscalYear={self.fiscal_year}'''
        cnxn = con().__call__(server_name)
        corrections = pd.read_sql(sql_statment, cnxn)
        cnxn.close()
        return corrections
    
    def get_schools_from_database(self):
        print('\nGetting EdOrg data from database')
        #define table to pull from depending on connected server
        if self.server_name == 'AACTASTPDDBVM02':
            sql_statment = F'''SELECT *
                            FROM [AccountabilityArchive].[Static].[{self.run}EdOrg{self.fiscal_year}]
                            WHERE FiscalYear = {self.fiscal_year}'''
                            
        elif self.server_name == 'AACTASTPDDBVM01':
            # sql_statment = f'''SELECT sch.*, lea.DistrictName, lea.CTDS as DistrictCTDS
            #                     FROM [Accountability].[EdOrg].[School] sch
            #                     INNER JOIN (SELECT FiscalYear, DistrictKey, DistrictName, CTDS from [Accountability].[EdOrg].[LEA]) lea
            #                           ON lea.DistrictKey = sch.DistrictKey AND lea.FiscalYear = sch.FiscalYear
            #                     WHERE sch.FiscalYear = {self.fiscal_year}'''
            sql_statment = F'''SELECT sch.*, lea.DistrictName, lea.CTDS as DistrictCTDS, lea.CountyTypeDesc AS DistrictCounty
                                FROM [Accountability].[EdOrg].[School] sch
                                INNER JOIN (SELECT a.FiscalYear, DistrictKey, DistrictName, CTDS , CountyTypeDesc
                                			FROM [Accountability].[EdOrg].[LEA] a
                                			inner join (select distinct FiscalYear, CountyTypeKey, CountyTypeDesc From [Accountability].[EdOrg].[School]) b
                                			 ON a.FiscalYear = b.FiscalYear AND a.CountyTypeKey=b.CountyTypeKey) lea
                                      ON lea.DistrictKey = sch.DistrictKey AND lea.FiscalYear = sch.FiscalYear
                                WHERE sch.FiscalYear = {self.fiscal_year}'''
        else:
            raise ValueError(F'Server {self.server_name} is not a valid Database server. Set "server_name" in class constructor with proper server name')
        
        cnxn = con().__call__(server_name=self.server_name)
        schools = pd.read_sql(sql_statment, cnxn)
        cnxn.close()
        
        if self.raw_folder is not None:
            file_name = f'{self.run}Schools'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(schools, self.raw_folder, file_name)
        return schools     
         
    def format_school_type(self, enrollment, raw_data_schools=None, remove_jteds=False, remove_private_schools=False):
        if raw_data_schools is None:
            
            schools = self.get_schools_from_database()
        else:   
            schools = raw_data_schools
        print(colored('Starting SchoolType data formating', 'green'))
        #---------------------------------------------------------------------- filter data
        # we will only consider schools that are valid and public schools
        if remove_private_schools:
            schools = schools [(schools.IsValidSchool == True) & (schools.IsPublicSchool == True)]
        else:
            schools = schools [(schools.IsValidSchool == True) & ((schools.IsPublicSchool == True) | (schools.IsPrivateSchool == True))]
        schools['Private'] = schools.IsPrivateSchool.apply(lambda x: 1 if x==1 else 0)
        
        # exclude jted schools
        if remove_jteds:
            schools = schools[~schools.JTEDTypeKey.astype(float).between(1,4)]
        
        schools['JTED'] = schools.JTEDTypeKey.astype(float).apply(lambda x: 1 if x in [1,2,3,4] else 0)
        
        #exclude IsExceptionalEducationFacility schools (why? It was done historically), we are intentionally including AZ schools for deaf and blind
        schools = schools[(schools.IsExceptionalEducationFacility == False) | (schools.SchoolName.str.contains('ASDB'))]
        
        #---------------------------------------------------------------------- rename columns
        col_ren = {'IsCharterSchool':'Charter'
                   ,'IsAOI':'AOI'
                   ,'CTDS':'SchoolCTDS'
                   ,'DistrictKey':'DistrictCode'
                   ,'SchoolKey':'SchoolCode'
                   ,'DistrictCounty':'County'
                   ,'IsAlternativeSchoolAccountability': 'Alternative'}
        schools.rename(col_ren, inplace=True, axis=1)
        
        #if county is ADE revert to school county
        mask = schools.County=='Arizona Department Of Education'
        schools.loc[mask, 'County'] = schools.loc[mask, 'CountyTypeDesc']
        
        #only keep needed columns
        schools = schools[['FiscalYear', 'SchoolCode', 'SchoolName', 'SchoolCTDS', 'DistrictCode','DistrictName','DistrictCTDS', 'Charter', 'County', 'AOI', 'Alternative', 'JTED', 'Private']]
        
        #---------------------------------------------------------------------- merge studlist_agg (from enrollment aggregation) and schools
        studlist_agg = self.aggregate_enrollment_for_schooltype(enrollment)
        try:
            schooltype = pd.merge(left=schools, right=studlist_agg, on='SchoolCode', how='inner')
            for i in ['EnrolledCount9-12', 'EnrolledCountk-8']:
                schooltype[i] = schooltype[i].fillna(0)
        except Exception as ex:
            print('Error in Merging school level data from EdOrg to studlist_agg aggregate data from Fiscal Year Enrollment table on SchoolCode')
            print(ex)
        
        ##--------------------------------------------------------------------- add utility cols for Accountability components
        # define  function to apply the type logic x[0]=MaxGrade and x[1]=MinGrade
        def format_type(row):
            if row[0] < 9 and row[0] >= 3:
                return 2
            elif row[0] >= 9 and row[1] > 8:
                return 3
            elif row[0] >= 9 and row[1] <= 8:
                return 4
            elif row[0] <= 2 :
                if row[0]==0:
                    return 0
                else:
                    return 1
        schooltype['Type'] = schooltype[['MaxGrade', 'MinGrade']].apply(format_type, axis=1)
        
        # define function to apply schooltype logic x[0]=Alternative and x[1]=type
        def format_schooltype( row):
            if row[0] == True:
                schooltype = 3
            elif row[1] == 1:
                schooltype = 4
            else:
                schooltype = 1
            return schooltype
        schooltype['SchoolTypeS'] = schooltype[['Alternative', 'Type']].apply(format_schooltype, axis=1)
        
        def format_schooltypef(row):
            SchoolTypeF = np.nan
            if row[0] == 1:
                SchoolTypeF = 1
            elif row[0] == 2:
                SchoolTypeF = 2
            elif row[0] == 3:
                SchoolTypeF = 3
            elif row[0] == 4 and row[1] < 12 :
                SchoolTypeF = 4
            elif row[0] == 4 and row[1] == 12 :
                SchoolTypeF = 5
            return SchoolTypeF
        schooltype['SchoolTypeF'] = schooltype[[ 'Type', 'MaxGrade']].apply(format_schooltypef, axis=1)
        schooltype['FederalModel'] = schooltype['SchoolTypeF'].map(self.federal_school_type_map)
        
        # mark first year schools as such
        year1_schools = self.get_first_year_schools()
        schooltype = pd.merge(schooltype, year1_schools, on='SchoolCode', how='left')
        
        #formate the statemodel
        schooltype.loc[schooltype['EnrolledCountk-8'] >= self.state_n_count, 'StateModel'] = 'K8'
        schooltype.loc[schooltype['EnrolledCount9-12'] >= self.state_n_count, 'StateModel'] = '912'
        mask = (schooltype['EnrolledCountk-8'] >= self.state_n_count) & (schooltype['EnrolledCount9-12'] >= self.state_n_count)
        schooltype.loc[mask, 'StateModel'] = 'Non-Typical'
        schooltype.loc[schooltype.Alternative==True, 'StateModel'] = 'Alternative'
        schooltype.loc[schooltype.StateModel.isnull(), 'StateModel'] = 'InEligible'
        schooltype.loc[schooltype.Year1School==1, 'StateModel'] = 'InEligible'
        schooltype.loc[schooltype.Private==1, 'StateModel'] = 'InEligible'
        schooltype.loc[schooltype.JTED==1, 'StateModel'] = 'InEligible'
        
        #check for any model corrections
        corrections = self.get_model_corrections()
        schooltype = pd.merge(schooltype, corrections, on=['SchoolCode', 'FiscalYear'], how='left')
        #correct data
        for acct_model, col in zip(['State', 'Federal'], ['StateModel', 'FederalModel']):
            mask = (schooltype.AccountabilityModel==acct_model) & (schooltype.CorrectedModelType.notnull())
            schooltype.loc[mask, col] = schooltype.loc[mask, 'CorrectedModelType']
        schooltype.loc[schooltype.StateModel=='Alternative', 'Alternative'] = True
        
        # create grades served column
        schooltype['GradesServed'] = schooltype['MinGrade'].astype(int).astype(str) +'-' + schooltype['MaxGrade'].astype(int).astype(str)
        # add active column
        schooltype['Active'] = 1
        ##fill counts col with zeros
        cols_to_fill = ['EnrolledCount9-12', 'EnrolledCountk-8', 'Year1School']
        for i in cols_to_fill:
            schooltype[i] = schooltype[i].fillna(0)
        ## drop unused cols
        drop_cols = ['CorrectedModelType', 'AccountabilityModel']
        schooltype.drop(drop_cols, inplace=True, axis=1)
        
        if self.static_folder is not None:
            file_name = 'SchoolType'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(schooltype, self.static_folder, file_name)   

        return schooltype
        
    def format_jteds(self, schools_raw=None):
        if schools_raw is None:
            jted = self.get_schools_from_database()
        else:   
            jted = schools_raw
        jted.JTEDTypeKey = pd.to_numeric(jted.JTEDTypeKey, errors='coerce')
        
        # only keep jteds
        jted = jted[jted.JTEDTypeKey.astype(float).between(1,4)]
        
        # rename district code and school code to match the static file
        col_ren = {'DistrictKey':'DistrictCode'
                   ,'SchoolKey':'SchoolCode'}
        jted.rename(col_ren, inplace=True, axis=1)
        
        if self.static_folder is not None:
            file_name = 'jteds'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(jted, self.static_folder, file_name)
        return jted
    
    
    def aggregate_enrollment_for_schooltype(self, studlist_data):
        studlist = studlist_data.copy()
        #exclude StudentGrade values of 55,66,77 in studlist, rename StudentGrade value of 88(KG) to -1 for arithmatic calculations
        # The point is to be able to aggregate and find min and max grades per school
        studlist.loc[(studlist['StudentGrade'] == 88), 'StudentGrade'] = -1
        #only keep grades -1 to 12 inclusive
        inclusion_mask = (studlist.StudentGrade.between(-1,12))
        studlist = studlist [inclusion_mask].copy()
        
        #identify whether student is 9-12 or k8
        studlist.loc[studlist.StudentGrade.between(-1,8), 'ModelType'] = 'k-8'
        studlist.loc[studlist.StudentGrade.between(9,12), 'ModelType'] = '9-12'
        
        ##count number of students enrolled on test window only taking highest grade into account
        studlist.sort_values(['SchoolCode', 'SAISID', 'ELAMathWindow', 'StudentGrade'], ascending =False, inplace=True)
        studlist = studlist[~studlist.duplicated(['SchoolCode', 'SAISID'])].copy()


        ## how to identify schooltype right
        if self.count_fay_in_schooltype:
            mask = studlist.FAY>0
        else:
            mask = studlist.FAY>=0
        aggregate = (studlist[mask].groupby(['SchoolCode'])['ModelType'].value_counts()).reset_index(name='EnrolledCount')
        
        #pivot to put school data on one line
        aggregate = pd.pivot(aggregate, index='SchoolCode', values=['EnrolledCount'], columns='ModelType').reset_index()
        #resolve multiindex
        aggregate.columns = [i[0]+i[1] for i in aggregate.columns]
        
        #make a MaxGrade and MinGrade columns
        min_max_grades = (studlist.groupby(['SchoolCode'])['StudentGrade'].value_counts(normalize=True)*100).reset_index(name='PCTEnrolled')
        #remove enrolled grades of less than 2% of total enrollment
        min_max_grades = min_max_grades[min_max_grades.PCTEnrolled >= 2].copy()
        ##aggregate to get min and max grades
        min_max_grades = min_max_grades.groupby('SchoolCode').agg(MinGrade=('StudentGrade', 'min'), MaxGrade=('StudentGrade', 'max')).reset_index()
        
        #merge back to aggregate to make school_grade_levels
        studlist_agg = pd.merge(aggregate, min_max_grades, on='SchoolCode', how='outer')
        
# =============================================================================
#         #if k-8 enrollment is less than federal n-count then adjust MinGrade to be 9
#         #if 9-12 enrollment is less than federal n-count then adjust MaxGrade to be 8
#         studlist_agg.loc[studlist_agg['EnrolledCountk-8'] < self.federal_n_count, 'MinGrade'] = 9
#         studlist_agg.loc[studlist_agg['EnrolledCount9-12'] < self.federal_n_count, 'MaxGrade'] = 8
#         #fix any weirdness this logic caused
#         studlist_agg.loc[(studlist_agg.MinGrade > studlist_agg.MaxGrade), 'MinGrade'] = 8
#         studlist_agg.loc[(studlist_agg.MinGrade > studlist_agg.MaxGrade), 'MaxGrade'] = 9
# =============================================================================
        
        return studlist_agg 
