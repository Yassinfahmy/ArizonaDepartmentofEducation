# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 15:07:51 2023

@author: yfahmy
"""
from datetime import date
import pandas as pd


class COMPONENTS:
    def __init__(self
                 ,fiscal_year=None
                 ,run='Prelim'
                 ,n_count=10
                 ,el_grade_map = None
                 ,growth_grade_map=None
                 ,growth_band_weights=None
                 ,models_component_weights=None
                 ,growth_subjects=['Math', 'ELA']
                 ,subgroup_improvement_subjects =['Math', 'ELA']
                 ,subgroups=None
                 ,proficiency_weights=None
                 ,ar_grade_map=None
                 ,proficiency_grade_map=None
                 ,act_grade = 11
                 ,act_aspire_grade = 9
                 ,science_proficiency_grade_map=None
                 ,sped_enrollment_grade_map=None
                 ,test_type_map = {685:"MSAA", 680:"ASAA"}
                 ,percent_tested_expected = 95
                 ,include_late_submissions=True):
        
        # define fiscalYear
        if fiscal_year is None:
           self.fiscal_year = date.today().year
        else:
            self.fiscal_year = fiscal_year
        # print('Fiscal year defined as {}'.format(self.fiscal_year))        
        self.previous_fiscal_year = self.fiscal_year-1
        
        #define cohors and act grades
        self.cy_act_cohort = self.fiscal_year+1
        self.act_grade = act_grade
        self.act_aspire_grade = act_aspire_grade
        self.cy_grad_cohort = self.fiscal_year-1
        self.include_late_submissions = include_late_submissions
        
        #set run type
        self.run = run.capitalize()
        # print(F'Run Type set to----> {self.run}')
        
        ##define server name
        self.server_name = 'AACTASTPDDBVM02'
        
        # define n_count
        self.n_count = n_count

        # define string representations of State grade models
        self.str_k8 = "k-8"
        self.str_912 = "9-12"
        self.str_alt_912 = "Alt 9-12"
        
        #grade map for
        if el_grade_map is None:
            self.el_grade_map= {'k-8': [-1, 1,2,3,4,5,6,7,8]
                                ,'9-12': [9,10,11,12]}
        else:
            self.el_grade_map = el_grade_map
            
        # define growth grades to apply component to
        if growth_grade_map is None:
            self.growth_grade_map = {'k-8': [4,5,6,7,8]
                                     ,'9-12':[11]}
        else:
            self.growth_grade_map = growth_grade_map
            
        #define growth weights
        if growth_band_weights is None:
            self.growth_band_weights = {'PYPCYHighGrowth':1.2
                                         ,'PYPPCYHighGrowth':1.8
                                         ,'PYMPCYHighGrowth':2
                                         ,'PYMPCYLowGrowth':0
                                         ,'PYPPCYLowGrowth':0
                                         ,'PYPCYLowGrowth':0
                                         ,'PYHPCYLowGrowth':0
                                         ,'PYMPCYAverageGrowth':1
                                         ,'PYPPCYAverageGrowth':1
                                         ,'PYPCYAverageGrowth':1
                                         ,'PYHPCYAverageGrowth':1
                                         ,'PYHPCYHighGrowth':1}
        else:
            self.growth_band_weights = growth_band_weights
            
        #Model weights
        if models_component_weights is None:
            self.models_component_weights = {'k-8':{'Growth':50
                                                     ,'ELProficiencyandGrowth':10
                                                     ,'AccelerationReadiness':10
                                                     ,'Proficiency':30}  
                                              
                                              ,'9-12':{'ELProficiencyandGrowth':10
                                                       ,'GraduationRate':10
                                                       ,'GradRateImprovement':10
                                                       ,'CollegeandCareerReady_SRSS':20
                                                       ,'Proficiency':30
                                                       ,'SGPI':10
                                                       ,'SGGRI':5
                                                       ,'SGDRI':5}
                                              
                                              ,'Alt 9-12':{'Proficiency':15
                                                           ,'ELProficiencyandGrowth':10
                                                           ,'CollegeandCareerReady_SRSS':35
                                                           ,'GraduationRate':10
                                                           ,'AcademicPersistence':10
                                                           ,'CreditsEarned':10
                                                           ,'OnTracktoGraduate':10}
                                              }
        else:
            self.models_component_weights = models_component_weights

        ##define grad rates weights for trad model gradratetype:weight
        self.trad_gradrate_weights = {4:5
                                      ,5:4
                                      ,6:2.5
                                      ,7:0.5}
        #define growth subjects and subgroup improvment subjects
        self.growth_subjects = growth_subjects
        self.subgroup_improvement_subjects = subgroup_improvement_subjects
        
        # Define AR subgroup improvement subgroups
        if subgroups is None:
            self.subgroups = ['Ethnicity', 'SPED', 'Foster', 'IncomeEligibility1and2', 'Military', 'ELFEP', 'Homeless']
        else:
            self.subgroups = subgroups
        
        #define proficiency wieghts
        if proficiency_weights is None:
            self.proficiency_weights = {1:0
                                        ,2:0.6
                                       ,3:1
                                       ,4:1.3}
        else:
            self.proficiency_weights = proficiency_weights
            
        #define Acceleration Readdinness grade bounds
        if ar_grade_map is None:
            self.ar_grade_map = {'G8 Math':[8]
                                 ,'G3 ELA':[3]
                                 ,'CA':[1,8]
                                 ,'SG Improvement':[3,8]
                                 ,'SPED Inclusion':[-1,8]}
        else:
            self.ar_grade_map = ar_grade_map
            
        #define grades used in proficiency
        if proficiency_grade_map is None:
            self.proficiency_grade_map = {"Trad":{self.str_k8: [3,4,5,6,7,8]
                                                 ,self.str_912:[11]}
                                         ,"Alt":{self.str_alt_912:[11]}}
        else:
            self.proficiency_grade_map = proficiency_grade_map

        #define grades used in science proficiency (bonus points)
        if science_proficiency_grade_map is None:
            self.science_proficiency_grade_map = {self.str_k8: [5,8]
                                                 ,self.str_912:[11]}
        else:
            self.science_proficiency_grade_map = science_proficiency_grade_map

        #define grades included in sped enrollment bonus points
        if sped_enrollment_grade_map is None:
            self.sped_enrollment_grade_map = {'k-8': [-1,1,2,3,4,5,6,7,8]
                                             ,'9-12':[9,10,11,12]}
        else:
            self.sped_enrollment_grade_map = sped_enrollment_grade_map

        self.test_type_map = test_type_map
        self.percent_tested_expected = percent_tested_expected
        
    def calculate_component(self):
        pass
    
    def columns_to_numeric(self, df, columns):
        ## confirm numeric cols are numeric
        t = ['float', 'int']
        for i in columns:
            if t[0] in str(df.dtypes[i]):
                pass
            elif t[1] in str(df.dtypes[i]):
                pass
            else:
                df[i] = df[i].astype(str).str.replace('%','')
                df[i] = pd.to_numeric(df[i], errors='coerce')

        return df
    
    def round_numeric_cols(self, *df, sigfigs=2):
        '''
        

        Parameters
        ----------
        *df : this variable can be a single DF or a list of DFs that you unpack in the call
            Example Code:
                data = [df1, df2, df3]
                df1, df2, df3 = self.round_numeric_cols(*data)
        
        Returns
        -------
        A list containg rounded input dfs or each individual df depending on # of variables the output is assigned to

        '''
        ##round all numeric cols
        for i in df:
            i[i.select_dtypes(include='number').columns] = i.select_dtypes(include='number').round(sigfigs).copy()
        return df
    

    """
    This function adjusts the StudentGrade column of the StaticFile. It re-assigns grades 9-12 based on Cohort. 
        It does not change any StudentGrade values that are less than 9 as these should not have a Cohort.
    """
    def correct_high_school_grades(self, static_file:pd.DataFrame):
        sf = static_file.copy() # don't overwrite the dataframe provided
        for i, yr in enumerate(range(self.fiscal_year+3, self.fiscal_year-1,-1)):
            sf.loc[sf["StudentGrade"]==i+9, "StudentGrade"] = 111
            sf.loc[(sf["Cohort"]==yr) & (sf["StudentGrade"]>=9), "StudentGrade"] = i+9
        return sf
        