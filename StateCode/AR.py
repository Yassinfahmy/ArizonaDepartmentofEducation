# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 11:46:29 2023

@author: yfahmy
* for sped inclusion, only FAY students are considered (consistant with prior buissness rules)
* Sped inclusion doesn;t currently have N-count restriction since changes are pending what we apply the n-count to.
* (G8 math performance, G3 ELA Mp and Chronic Absenteeism) N-count is applied to CY only
"""

from DATABASE import DATABASE
from COMPONENTS import COMPONENTS
import pandas as pd
import numpy as np

class AR(COMPONENTS):
    def __init__(self,  fiscal_year=None, run='Prelim', **kwargs ):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.subject_map = {'675':'ELA'
                            ,'677':'Math'
                            ,'678':'Sci'}
        ## convert aggregate names for ease of use
        self.subgroup_name_change = {'SPED':{1:'SPED'}
                                    ,'Foster':{1:'FosterCare'}
                                    ,'IncomeEligibility1and2':{1:'IncomeEligibility'}
                                    ,'Military':{1:'Military'}
                                    ,'ELFEP':{1:'ELFEP1-4'}
                                    ,'Homeless':{1:'Homeless'}
                                    ,'Ethnicity':{'H':'Hispanic'
                                                 ,'W':'White'
                                                 ,'B':'AfricanAmerican'
                                                 ,'I':'NativeAmerican'
                                                 ,'A':'Asian'
                                                 ,'P':'PacificIslander'
                                                 ,'R':'TwoorMoreRaces'}}
        self.numeric_cols = ['SchoolCode', 'StudentGrade', 'Performance', 'FAY'                     
                           ,'ChronicAbsent','SPED', 'Foster', 'IncomeEligibility1and2'
                           ,'Military', 'ELFEP', 'Homeless', 'RAEL', 'SPEDInclusion']
        
    def calculate_component(self, static_file, py_staticfile):
        ###make sure cols from old staticfile are correct
        #====================== Some Data Wrangling ===========================
        ##Correct name  if wrong in Staticfile
        try:
            py_staticfile.rename({'EL_Fep':'ELFEP', 'RA_EL':'RAEL'}, inplace=True, axis=1)
            #code Subject col consistently
            if '677' in py_staticfile.Subject.unique():
                py_staticfile.Subject = py_staticfile.Subject.map(self.subject_map)
        except:
            pass
        #convert col data to numeric
        static_file = self.columns_to_numeric(static_file, self.numeric_cols)
        py_staticfile = self.columns_to_numeric(py_staticfile, self.numeric_cols)
        #======================================================================
        #exclude all those who don't pass integrity
        inclusion_mask = (static_file.ADMIntegrity==1) & (static_file.FiscalYear==self.fiscal_year)
        staticfile = static_file[inclusion_mask].copy()
        
        g8_math = self.calculate_g8_math_points(staticfile, py_staticfile)
        g3_ela = self.calculate_g3_ela_points(staticfile, py_staticfile)
        chronic = self.calculate_chronic_absenteeisim(staticfile, py_staticfile)
        sg_imp = self.calculate_subgroup_improvement(staticfile, py_staticfile)
        sped_inc = self.calculate_sped_inclusion(staticfile)
        
        # merge all datasets
        datasets = [g3_ela, chronic, sped_inc, sg_imp]
        ar = g8_math
        for i in datasets:
            ar = pd.merge(ar, i, on='SchoolCode', how='outer')
         
        #change schoolcode to EntityId 
        ar.rename({'SchoolCode':'EntityID'}, axis=1, inplace=True)
        ##add a model col
        ar['Model'] = 'k-8'
        ar['FiscalYear'] = self.fiscal_year
        #mark counts columns as Extra and mark summary and drilldown cols as such
        names_list=[]
        common_cols= ['EntityID', 'ChronicAbsentPointsEarned', 'G3ELAMPPointsEarned', 'SpecialEdInclusionPointsEarned', 'FiscalYear', 'Model']
        extra_cols = ['Count']
        for name in ar.columns:
            if name in extra_cols:
                names_list.append( name + '_Extra')
            elif name in common_cols:
                names_list.append(name + '_All')
            else:
                names_list.append( name+'_DrillDown' )
        ar.columns = names_list
        #add summary data
        ar = self.add_summary_cols(ar)
        
        return ar
    
    
    def add_summary_cols(self, ar):
        # sum all AR points without a cap
        points_col = [ name for name in ar.columns if 'points' in name.lower()]
        ar['ARPointsSum_DrillDown'] = ar[points_col].sum(axis=1, min_count=1)
        
        ##---------------------------------------------------Build summary cols
        #====================sum g8 math points
        g8_math_points_cols = ['G8MathHPPointsEarned_DrillDown', 'G8MathMPPointsEarned_DrillDown']
        ar['G8MathPointsEarned_Summary'] = ar[g8_math_points_cols].sum(axis=1, min_count=1)
        # cap G8 math points at 5
        ar.loc[ar.G8MathPointsEarned_Summary > 5,  'G8MathPointsEarned_Summary'] = 5
        
        #===================sum subGroup imp points and cap at 6
        subg_imp_cols = ['HispanicPointsELA_DrillDown',
                         'HispanicPointsMath_DrillDown',
                         'NativeAmericanPointsELA_DrillDown',
                         'NativeAmericanPointsMath_DrillDown',
                         'SPEDPointsMath_DrillDown',
                         'SPEDPointsELA_DrillDown',
                         'WhitePointsMath_DrillDown',
                         'WhitePointsELA_DrillDown',
                         'FosterCarePointsMath_DrillDown',
                         'FosterCarePointsELA_DrillDown',
                         'AfricanAmericanPointsMath_DrillDown',
                         'AfricanAmericanPointsELA_DrillDown',
                         'AsianPointsMath_DrillDown',
                         'AsianPointsELA_DrillDown',
                         'ELFEP1-4PointsMath_DrillDown',
                         'ELFEP1-4PointsELA_DrillDown',
                         'PacificIslanderPointsELA_DrillDown',
                         'PacificIslanderPointsMath_DrillDown',
                         'TwoorMoreRacesPointsMath_DrillDown',
                         'TwoorMoreRacesPointsELA_DrillDown',
                         'IncomeEligibilityPointsELA_DrillDown',
                         'IncomeEligibilityPointsMath_DrillDown',
                         'MilitaryPointsELA_DrillDown',
                         'MilitaryPointsMath_DrillDown',
                         'HomelessPointsELA_DrillDown',
                         'HomelessPointsMath_DrillDown']
        ar['SubGroupImprovePointsEarned_Summary'] = ar[subg_imp_cols].sum(axis=1, min_count=1)
        # cap points at 6
        ar.loc[ar.SubGroupImprovePointsEarned_Summary > 6, 'SubGroupImprovePointsEarned_Summary'] = 6
        
        #=============sum Capped AR points for final points
        ar_points_col = ['G8MathPointsEarned_Summary'
                         ,'G3ELAMPPointsEarned_All'
                         ,'ChronicAbsentPointsEarned_All'
                         ,'SpecialEdInclusionPointsEarned_All'
                         ,'SubGroupImprovePointsEarned_Summary']
        ar['TotalARPoints_All'] = ar[ar_points_col].sum(axis=1, min_count=1)
        #cap ar points at 10
        ar.loc[ar.TotalARPoints_All>10, 'TotalARPoints_All'] = 10
        
        #make final total col
        ar['AccelerationReadiness_Summary'] = ar['TotalARPoints_All'].copy()
        
        return ar
        
    def calculate_sped_inclusion(self, staticfile):
        ## select relevant grades only
        sf = staticfile[staticfile.StudentGrade.between(self.ar_grade_map['SPED Inclusion'][0], self.ar_grade_map['SPED Inclusion'][1])].copy()
        ## deduplicate static file
        sf.sort_values(['SAISID', 'SchoolCode', 'SPEDInclusion'], inplace=True, ascending=False)
        sf = sf[~sf.duplicated(['SAISID', 'SchoolCode'])]
        #only keep FAY students
        sf = sf[sf.FAY > 0].copy()
        #fill any nan value in SPEDInclusion col with zero (non-should exist but just in case)
        sf.SPEDInclusion = sf.SPEDInclusion.fillna(0)
        
        ##group by schoolCode and count # of SPEDInclusion and count of FAY students
        sped_incl = sf.groupby('SchoolCode').agg(SPEDInclusionCount = ('SPEDInclusion','sum')
                                                 ,AllStudentsCount = ('SAISID', lambda x: x.nunique())).reset_index()
        #calc PCt of sped incl
        sped_incl['SpecialEdInclusionCYPct'] = (sped_incl.SPEDInclusionCount / sped_incl.AllStudentsCount)*100
        
        ##calculate statewide mean
        ###sped_incl['SPEDInclusionMean'] = (sf.SPEDInclusion.sum() / sf.SAISID.nunique())*100
        mask = sped_incl.AllStudentsCount >= self.n_count
        sped_incl['SPEDInclusionMean'] = round(sped_incl.loc[mask, 'SpecialEdInclusionCYPct'].mean(), 2)
        
        #======================================================== Assign Points
        sped_incl.loc [sped_incl.SpecialEdInclusionCYPct.round(2) <=  sped_incl.SPEDInclusionMean, 'SpecialEdInclusionPointsEarned'] = 0
        sped_incl.loc [sped_incl.SpecialEdInclusionCYPct.round(2) >  sped_incl.SPEDInclusionMean, 'SpecialEdInclusionPointsEarned'] = 2
        
        ##apply n-count
        col_to_blanc= ['SpecialEdInclusionPointsEarned', 'SpecialEdInclusionCYPct']
        sped_incl.loc[sped_incl.AllStudentsCount < self.n_count, col_to_blanc] = np.nan
        
        return sped_incl
        
    def calculate_subgroup_improvement(self, staticfile, py_staticfile):
        #copy staticfiles to avoid altering original data
        sf = staticfile
        py_sf = py_staticfile
        
        ##select students applicable to SubgroupImp
        static_files = {}
        for file, name in zip([sf, py_sf], ['CY', 'PY']):
            #select applicable grades and subjects
            grades_mask = file.StudentGrade.between(self.ar_grade_map['SG Improvement'][0], self.ar_grade_map['SG Improvement'][1])
            file = file[grades_mask].copy()
            #select applicable subjects
            subjects_mask = file.Subject.isin(self.subgroup_improvement_subjects)
            file = file[subjects_mask].copy()
            # filter to fay tested kids only 
            fay_tested_mask = (file.FAY.astype(int) > 0) & (file.Performance.notnull())
            file = file [fay_tested_mask].copy()
            #filter out RAEL 1 and 2 from ELA records
            rael_mask = (file.RAEL.isin([1,2])) & (file.Subject=='ELA')
            file = file [~rael_mask].copy()
            ## convert performance column to categorical
            file['Performance'] = file['Performance'].astype('category')
            #change values in certain columns for SQL naming conventions
            file.replace(self.subgroup_name_change, inplace=True)
            #insert processed staticfile into dict
            static_files[name] = file
        
        ##aggregate staticfile to build calculation structure
        counts = {}
        data = {}
        for key, df in static_files.items():
            counts[key] = pd.DataFrame()
            data[key] = pd.DataFrame()
            for subg in self.subgroups:
                #===================groupby 'SchoolCode', 'subgroup' to calc total counts for n-count
                grouped = df.groupby(['SchoolCode', 'Subject', subg])
                total_counts = grouped.agg(FAYTestedCounts = ('SAISID', 'nunique')).reset_index()
                ##only keep values that are of the intended aggregate
                total_counts = total_counts [total_counts[subg]!=0].copy()
                ##rename aggregation col and concat to df
                total_counts.rename({subg:'SubGroup'}, inplace=True, axis=1)
                ##add data to dict
                counts[key] = pd.concat([counts[key], total_counts])
                
                #===================groupby 'SchoolCode', 'Subject', 'subgroup' to calculate counts per prof level
                
                ## get number of student that are PP, P, HP
                perf = grouped['Performance'].value_counts().reset_index(name='PerfCnt')
                ##only keep values that are of the intended aggregate
                perf = perf[perf[subg]!=0].copy()
                ##rename aggregation col
                perf.rename({subg:'SubGroup', 'level_3':'Performance'}, inplace=True, axis=1)
                ##add data to dict
                data[key] = pd.concat([data[key], perf])
                
        ##loop throught data to calculate prof pct for each subject 
        results = {}
        for yr, df in data.items():
            #apply proficiency weights
            df['Weights'] =  df.Performance.map(self.proficiency_weights)
            df['weighedCounts'] = df['Weights'].astype(float) * df['PerfCnt']
            # groupby 'SchoolCode', 'Subject', 'SubGroup' and calculate prof PCT
            grouped = df.groupby(['SchoolCode', 'Subject', 'SubGroup'])
            perf_pct = grouped.agg(Numerator = ('weighedCounts', 'sum')
                                   ,Denominator = ('PerfCnt', 'sum')).reset_index()
            ## calculate the prof pct per subgroup
            name = yr + 'Pct'
            perf_pct[name] = (perf_pct.Numerator / perf_pct.Denominator)*100
            #drop num and denom cols
            perf_pct.drop(['Numerator', 'Denominator'], axis=1, inplace=True)
            ## add data to dict
            results[yr] = perf_pct
            
        #merge data of cy and py
        all_results = pd.merge(results['CY'], results['PY'], on=['SchoolCode', 'Subject', 'SubGroup'], how='left')
        
        #=========================================================assign points
        all_results.loc[(all_results.CYPct.round(2) > all_results.PYPct.round(2)) ,'Points'] = 2
        all_results.loc[(all_results.CYPct.round(2) <= all_results.PYPct.round(2)) ,'Points'] = 0
        
        #bring in total counts per subgroup and apply n-count rule to CY and PY counts
        n_counts = pd.merge(counts['CY'], counts['PY'], on = ['SchoolCode', 'Subject', 'SubGroup'], how='left', suffixes=('CY','PY'))
        n_counts.fillna(0, inplace=True)
        all_results = pd.merge(all_results, n_counts, on=['SchoolCode', 'Subject', 'SubGroup'], how='outer')
        
        ##---------------- apply n_count rule
        ineligible_mask = (all_results.FAYTestedCountsCY < self.n_count) | (all_results.FAYTestedCountsPY < self.n_count)
        all_results.loc[ineligible_mask, 'Points'] = np.nan
        ##Remove unknown ethinicity subgroup
        all_results = all_results[all_results.SubGroup!='U'].copy()
        
        #===================================================pivot for wide data
        values=['FAYTestedCountsCY', 'FAYTestedCountsPY', 'CYPct', 'PYPct', 'Points']
        sg_imp = pd.pivot(all_results, index='SchoolCode', columns=['SubGroup', 'Subject'], values=values).reset_index()
        #resolve multiindex
        sg_imp.columns = [i[1]+i[0]+i[2] for i in sg_imp.columns]
        
        return sg_imp
        
    def calculate_chronic_absenteeisim (self, staticfile, py_staticfile):
        ## Student must be enrolled during the testing window
        
        #copy staticfiles to avoid altering original data
        sf = staticfile
        py_sf = py_staticfile
        
        ##select students applicable to cA
        static_files = {}
        for file, name in zip([sf, py_sf], ['CY', 'PY']):
            #apply chronic absenteeisim grades
            grades_mask = file.StudentGrade.between(self.ar_grade_map['CA'][0],self.ar_grade_map['CA'][1])
            file = file[grades_mask].copy()
            #deduplicate to keep one record per kid
            file = file[~file.duplicated(['FiscalYear','SAISID', 'SchoolCode'])].copy()
            #make the chronic absent col categorical to allow for zero counts
            file['ChronicAbsent'] = file['ChronicAbsent'].astype('category')
            #insert processed staticfile into dict
            static_files[name] = file
            
        ##aggregate staticfile to count number of chronic kids
        chronic = {}
        counts = {}
        for key, df in static_files.items():
            grouped = df.groupby('SchoolCode')
            total_counts = grouped.agg(ChronicAbsentCount = ('SAISID', 'nunique')).reset_index()
            chronic_pct = (grouped['ChronicAbsent'].value_counts(normalize=True)*100).reset_index(name=key+'Pct')
            chronic_pct.rename({'level_1':'ChronicAbsent'}, axis=1, inplace=True)
            ##only show chronicAbsent pct
            chronic_pct = chronic_pct[chronic_pct.ChronicAbsent==1].copy()
            #rename values to show chronic absent
            chronic_pct.replace({'ChronicAbsent':{1:'ChronicAbsent'}}, inplace=True)
            ##build dict of py and cy results plus counts for each year
            chronic[key] = chronic_pct
            counts[key] = total_counts
            
        ## merge prior year aggregation to current year agg
        chronic_absent = pd.merge(chronic['CY'], chronic['PY'], on=['SchoolCode', 'ChronicAbsent'], how='left')
        # create wide data
        chronic_absent = pd.pivot(chronic_absent, index='SchoolCode', columns='ChronicAbsent', values=['CYPct', 'PYPct'])
        #Resolve multiindex
        chronic_absent.columns = [i[1]+i[0] for i in chronic_absent.columns]
        
        #if chronic abseentisim Pct decreases or is below 4% asign 2 points
        chronic_absent.loc[chronic_absent.ChronicAbsentCYPct.round(2) > chronic_absent.ChronicAbsentPYPct.round(2), 'ChronicAbsentPointsEarned'] = 0
        chronic_absent.loc[chronic_absent.ChronicAbsentCYPct.round(2) <= 4, 'ChronicAbsentPointsEarned'] = 2
        chronic_absent.loc[chronic_absent.ChronicAbsentCYPct.round(2) <= chronic_absent.ChronicAbsentPYPct.round(2), 'ChronicAbsentPointsEarned'] = 2
        
        #apply n-count rule
        chronic_absent = pd.merge(counts['CY'], chronic_absent, on='SchoolCode', how='outer')
        mask= chronic_absent.ChronicAbsentCount < self.n_count
        cols_to_blanc = ['ChronicAbsentPointsEarned', 'ChronicAbsentCYPct', 'ChronicAbsentPYPct']
        chronic_absent.loc[mask, cols_to_blanc] = np.nan
        return chronic_absent
        
    def calculate_g3_ela_points(self, staticfile, py_staticfile):
        '''The intent of this metric is to reduce the percentage of grade 3 students who are minimally proficient on AASA ELA from prior year to current year. To be eligible for these points, a school must meet the minimum N-Size of 10 FAY students. Schools can earn five points two different ways:
1. Decreasing the school’s prior year percent minimally proficient
2. Have a current year percent minimally proficient less than 12%'''
        #copy staticfiles to avoid altering original data
        sf = staticfile
        py_sf = py_staticfile
        
        ##select students who took  8th grade math only from staticfiles
        ##filter to fay only
        static_files = {}
        for file, name in zip([sf, py_sf], ['CY', 'PY']):
            ela_mask = (file.Subject==675) | (file.Subject=='ELA')
            grade_3_mask = (file.StudentGrade.isin(self.ar_grade_map['G3 ELA'])) 
            final_mask = ela_mask & grade_3_mask
            file = file[final_mask].copy()
            #filter out RAEL 1 and 2 from ELA records
            rael_mask = (file.RAEL.isin([1,2])) & (file.Subject=='ELA')
            file = file [~rael_mask].copy()
            # filter to fay kids only with a valid score
            fay_tested_mask = (file.FAY.astype(int) > 0) & (file.Performance.notnull())
            file = file [fay_tested_mask].copy()
            #insert processed staticfile into dict
            static_files[name] = file
            
        ##aggregate staticfile to count number of MP kids, also total number of grade 3 ela fay tested kids
        performance_map = {1:'MinimallyProficient'}
        g3 = {}
        counts = {}
        for key, df in static_files.items():
            ##make a category col to get zero counts
            df['PerformanceLevel'] = df.Performance.astype('category')
            grouped = df.groupby('SchoolCode')
            total_counts = grouped.agg(ELAFAYTestedCount = ('SAISID', 'nunique')).reset_index()
            perf_pct = (grouped['PerformanceLevel'].value_counts(normalize=True)*100).reset_index(name=key+'Pct')
            ##only show results for MP ()
            perf_pct = perf_pct[perf_pct.level_1==1].copy()
            ##change performance level names to match sql server
            perf_pct['Performance'] = perf_pct.level_1.map(performance_map)
            g3[key] = perf_pct
            counts[key] = total_counts
            
        ## merge prior year aggregation to current year agg
        g3_ela = pd.merge(g3['CY'], g3['PY'], on=['SchoolCode','Performance'], how='left', suffixes=('CY','PY'))
        # create wide data
        g3_ela = pd.pivot(g3_ela, index='SchoolCode', columns='Performance', values=['CYPct', 'PYPct'])
        #Resolve multiindex
        g3_ela.columns = [i[0]+i[1] for i in g3_ela.columns]

        #=========================================assign points based on Buissness rules criteria
        #if Minimally proficient Pct decreases or is below 12% asign 5 points
        g3_ela.loc[g3_ela.CYPctMinimallyProficient.round(2) > g3_ela.PYPctMinimallyProficient.round(2), 'ELAMPPointsEarned'] = 0
        g3_ela.loc[g3_ela.CYPctMinimallyProficient.round(2) <= 12, 'ELAMPPointsEarned'] = 5
        g3_ela.loc[g3_ela.CYPctMinimallyProficient.round(2) <= g3_ela.PYPctMinimallyProficient.round(2), 'ELAMPPointsEarned'] = 5
        
        #apply n-count rule
        g3_ela = pd.merge(counts['CY'], g3_ela, on='SchoolCode', how='outer')
        mask= g3_ela.ELAFAYTestedCount < self.n_count
        col_to_blanc = ['ELAMPPointsEarned', 'CYPctMinimallyProficient', 'PYPctMinimallyProficient']
        g3_ela.loc[mask, col_to_blanc] = np.nan
        
        #add Prefix G3 to match sql server
        g3_ela.columns = ['G3'+i if i!='SchoolCode' else i for i in g3_ela.columns]
        return g3_ela

    def calculate_g8_math_points(self, staticfile, py_staticfile):
        '''Grade 8 Mathematics Performance Points (0, 2.5, or 5 points)
            • A school’s current year PCT of highly proficient is greater than prior year percentage = 2.5 points
            • A school’s current year PCT of highly proficient is greater than or equal to 60% = 2.5 points
            
            • A school’s current year PCT minimally proficient is less than prior year percentage = 2.5 points
            • A school’s current year PCT minimally proficient is less than or equal to 10% = 2.5 points'''
            
        ##================================================ calc Grade 8 Mathematics Performance
        #copy staticfiles to avoid altering original data
        sf = staticfile
        py_sf = py_staticfile
        
        ##select students who took  8th grade math only from staticfiles
        ##filter to fay only
        static_files = {}
        for file, name in zip([sf, py_sf], ['CY', 'PY']):
            math_mask = (file.Subject==677) | (file.Subject=='Math')
            grade_8_mask = (file.StudentGrade.isin(self.ar_grade_map['G8 Math']))
            final_mask = math_mask & grade_8_mask
            file = file[final_mask].copy()

            # filter to fay kids only with a valid score
            fay_tested_mask = (file.FAY.astype(int) > 0) & (file.Performance.notnull())
            file = file [fay_tested_mask].copy()
            #insert processed staticfile into dict
            static_files[name] = file
        
        ##aggregate staticfile to count number of MP and HP kids, also total number of grade 8 math fay tested kids
        performance_map = {1:'MinimallyProficient'
                           ,4:'HighlyProficient'}
        g8 = {}
        counts = {}
        for key, df in static_files.items():
            ##make a category col to get zero counts
            df['PerformanceLevel'] = df.Performance.astype('category')
            grouped = df.groupby('SchoolCode')
            total_counts = grouped.agg(MathFAYTestedCount = ('SAISID', 'nunique')).reset_index()
            perf_pct = (grouped['PerformanceLevel'].value_counts(normalize=True)*100).reset_index(name=key+'Pct')
            ##only show results for MP and HP 
            perf_pct = perf_pct[perf_pct.level_1.isin(performance_map.keys())].copy()
            ##change performance level names to match sql server
            perf_pct['Performance'] = perf_pct.level_1.map(performance_map)
            g8[key] = perf_pct
            counts[key] = total_counts
            
        ## merge prior year aggregation to current year agg
        g8_math = pd.merge(g8['CY'], g8['PY'], on=['SchoolCode','Performance'], how='left', suffixes=('CY','PY'))
        # create wide data
        g8_math = pd.pivot(g8_math, index='SchoolCode', columns='Performance', values=['CYPct', 'PYPct'])
        #Resolve multiindex
        g8_math.columns = [i[0]+i[1] for i in g8_math.columns]
        
        #=========================================assign points based on Buissness rules criteria
        #if highly proficient Pct increase or is above or equal to 60%
        g8_math.loc[g8_math.CYPctHighlyProficient.round(2)>=60, 'HPPointsTarget'] = 2.5
        mask = (g8_math.CYPctHighlyProficient.round(2) >= g8_math.PYPctHighlyProficient.round(2)) & (g8_math.CYPctHighlyProficient.round(2)!=0)
        g8_math.loc[mask, 'HPPointsImp'] = 2.5
        
        #if Minimally proficient Pct decreases or is below 10% asign 2.5 points
        g8_math.loc[g8_math.CYPctMinimallyProficient.round(2) <= 10, 'MPPointsTarget'] = 2.5
        g8_math.loc[g8_math.CYPctMinimallyProficient.round(2) <= g8_math.PYPctMinimallyProficient.round(2), 'MPPointsImp'] = 2.5
        
        #sum Mp and HP points
        g8_math['MathHPPointsEarned'] = g8_math[['HPPointsTarget','HPPointsImp']].sum(axis=1, min_count=1)
        
        g8_math['MathMPPointsEarned'] = g8_math[['MPPointsTarget','MPPointsImp']].sum(axis=1, min_count=1)
        
        #apply n-count rule
        g8_math = pd.merge(counts['CY'], g8_math, on='SchoolCode', how='outer')
        mask= g8_math.MathFAYTestedCount < self.n_count
        cols=['MathHPPointsEarned', 'MathMPPointsEarned', 'CYPctHighlyProficient', 'CYPctMinimallyProficient', 'PYPctHighlyProficient', 'PYPctMinimallyProficient']
        g8_math.loc[mask, cols] = np.nan
        
        ##drop unneeded cols
        cols_to_drop = ['MPPointsImp', 'MPPointsTarget', 'HPPointsImp', 'HPPointsTarget']
        g8_math.drop(cols_to_drop, axis=1, inplace=True)
        
        #add Prefix G8 to match sql server
        g8_math.columns = ['G8'+i if i!='SchoolCode' else i for i in g8_math.columns]
        return g8_math

#%% bring in the static file
# fy = 2023
# run = 'Prelim'
# static_file = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')

# # bring in last year's staticfile
# fy=2022
# prefix=''
# py_staticfile = DATABASE(fiscal_year = fy
#                       ,run = prefix
#                       ,schema = 'dbo'
#                       ,database = 'REDATA').read_table(table_name ='StaticFileData')

#%% Make instance of class and calc AR
# self = AR(2023)
# ar_data = self.calculate_component(staticfile, py_staticfile)

#%% upload table
# fy= 2023
# table_name='StateAR'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').upload_table_to_db(df=ar_data, table_name=table_name)

#%% drop table
# fy= 2023
# table_name='StateAR'
# run = 'Prelim'
# DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Results'
#                       ,database = 'AccountabilityArchive').drop_tables_in_run(table_name, table_prefix=run)