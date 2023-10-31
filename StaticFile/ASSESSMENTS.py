# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 09:53:30 2022

By: Yassin Fahmy assessments = azm2.copy()
"""

import pandas as pd
pd.options.display.max_columns=200
from CONNECTION import CONNECTION as con
import os
from termcolor import colored
from SOURCES import SOURCES

class ASSESSMENTS(SOURCES):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def __call__(self):
        return self.format_assessments()
    
    def get_assessments_from_database(self):
        print('\nGetting assessments data from database')
        if self.server_name == 'AACTASTPDDBVM02':
            table_name = F'AccountabilityArchive.Static.{self.run}Assessments{self.fiscal_year}'
        elif self.server_name == 'AACTASTPDDBVM01':
            table_name = '[Accountability].[assessment].[StudentAssessment]'
        else:
            raise ValueError('Invalid server name')
            
        sql_statment = F'''SELECT Distinct
                              [StateStudentID]
                              ,[FiscalYear]
                              ,[AssessmentPeriodDescription]
                              ,[SchoolId]
                              ,[DistrictId]
                              ,[AssessmentFamily]
                              ,[WhenAssessedGrade]
                              ,[EnrolledGrade]
                              ,[AssessmentTitle]
                              ,[AcademicSubject]
                              ,[AssessmentTestStatus]
                              ,[AssessmentTestMode]
                              ,[ScaleScoreResult]
                              ,[RawScoreResult]
                              ,[PerformanceLevelDescription]
                              ,[MoveOnWhenReadingIndicator]
                              ,[MoveOnWhenReadingScore]
                              ,[Accommodation]
                          FROM {table_name}
                          WHERE FiscalYear = {self.fiscal_year}'''
        
        # setup connection to db and read in data
        try:
            cnxn = con().__call__(server_name= self.server_name)
            assessments = pd.read_sql(sql_statment, cnxn)
            # print(colored('\033[1mRetrieved data from database successfully\033[0m', 'green'))
            cnxn.close()
        except Exception as ex:
            print(colored('\033[1mFailed to retrieve data, error in "get_assessments_from_database()"\033[0m', 'red'))
            print(ex)
        
        if self.raw_folder is not None:
            file_name = f'{self.run}Assessments'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(assessments, self.raw_folder, file_name)
        
        return assessments
    
    def format_assessments(self, raw_data_assessments=None):
        if raw_data_assessments is None:
            
            assessments = self.get_assessments_from_database()
        else:   
            assessments = raw_data_assessments
        
        #define some local variables for ease of use
        assessement_types = self.reg_assessement_types + self.alt_assessement_types
        academic_subjects = self.academic_subjects
        performance_level_map = self.performance_level_map
        fiscal_year = self.fiscal_year

        print(colored('\nStarting Assessments formatting\n', 'green'))
        
        ####----------------------------------------------------- rename cols for consistency
        # rename columns for consistency
        col_ren = {'SchoolId':'SchoolCode'
                   ,'StateStudentID':'SAISID'
                   ,'EnrolledGrade':'StudentGrade'
                   ,'WhenAssessedGrade':'AssessmentGrade'
                   ,'ScaleScoreResult': 'ScaleScore'}
        assessments.rename(col_ren, inplace=True, axis=1)
        
        ## add in fix for aspire
        aspire=self.fix_aspire_participation()
        assessments = pd.concat([assessments, aspire], axis=0)
        
        #convert col types to numeric
        assessments['ScaleScore'] = pd.to_numeric(assessments.ScaleScore, errors='coerce')
        assessments['Accommodation'] = assessments['Accommodation'].apply(lambda x: 1 if x==True else 0)
        
        ##------------------------------------------------------------- format studentGrade
        # Map the str grade values to numeric
        assessments['AssessmentGrade'] = pd.to_numeric(assessments['AssessmentGrade'], errors='coerce')
        ##overide ACT assess grade (it is showing as 10 in some instances)
        assessments.loc[assessments.AssessmentFamily=='AZACT', 'AssessmentGrade'] = self.test_type_grade_map ['AZACT'][0]
        assessments.loc[assessments.AssessmentFamily=='ACTASPIRE', 'AssessmentGrade'] = self.test_type_grade_map['ACTASPIRE'][0]
        #grade_values dictionary will hold the values that need to be mapped
        ## if AssessmentGrade==10 change it to 11, there seems to be an error in database
        to_replace ={'StudentGrade':{'UE':'66'
                                    ,'PS':'77'
                                    ,'KG':'88'}}
        assessments.replace(to_replace, inplace=True)
        assessments['StudentGrade'] = pd.to_numeric(assessments['StudentGrade'], errors='coerce')
        ##------------------------------------------------------------- format test types
        
        ### make a list that holds the type of assessments we are going to keep for the current fiscal year
        ### select assessments from raw data that are within these types only
        size_0 = assessments.shape[0]
        assessement_types = assessement_types
        assessments = assessments [assessments.AssessmentFamily.isin(assessement_types)]
        ### print a status update on total records left
        print(F'''Only {assessement_types} student records where kept
              Excluded records = {size_0 - assessments.shape[0]}
              Total records left = {assessments.shape[0]}\n''')
        ### start a subject column that assign a numeric value to each subject
        assessments['Subject'] = assessments.AcademicSubject.map(academic_subjects, na_action='ignore')
        
        # ------------------------------------------------------------- format performance
        ### make the performancelevel column
        ## performancelevel_map holds the numeric values for PerformanceLevelDescription
        ## this function will return np.nan for any value not present in the dictionary
        size_0 = assessments.shape[0]
        assessments['Performance'] = assessments.PerformanceLevelDescription.map(performance_level_map, na_action='ignore')
        ### Only include entries with Performance  or if AssessmentFamily is ACTASPIRE since (cut scores have not been set in 2022)
        mask = (assessments['Performance'].notnull()) | (assessments.AssessmentFamily == 'ACTASPIRE')
        assessments = assessments[mask]
        ## print a status update on excluded records
        print(F'''Only student records with PerformanceLevelDescription or ACTASPIRE as testype were kept (since cut scores for Aspire were not set in 2022)
              Excluded records = {size_0 - assessments.shape[0]}
              Total records left = {assessments.shape[0]}\n''')

        ##------------------------------------------------------- deduplicate records
        ###### if student took same assessment more than once we only keep the entry with his highest score
        #sort by 'SAISID', 'SchoolCode', 'AssessmentGrade', 'Subject' in descending order (de-duplicating)
        size_0 =  assessments.shape[0]
        assessments.sort_values(['SAISID','Subject', 'AssessmentGrade', 'ScaleScore'], inplace=True, ascending=False)
        # keep the first duplicate to keep highest scale score
        assessments = assessments [~assessments[['SAISID', 'Subject', 'AssessmentGrade']].duplicated(keep='first')]
        print(F'''Student entries where de-duplicated to keep entries with highest score in the same school for each student for each subject/grade
              Excluded entries = {size_0 - assessments.shape[0]}
              Total records left = {assessments.shape[0]}\n''')
        
        ##---------------------------------------------------------- produce wide data
        # #make AssessmentTypeGrade col
        # assessments['AssessmentTypeGrade'] = assessments.AssessmentFamily + ' Grade ' + assessments.AssessmentGrade.astype(str)
        # ##pivot assessments data
        # index = ['SAISID', 'AssessmentGrade']
        # cols = ['Subject']
        # values= ['ScaleScore', 'Performance', 'AssessmentTypeGrade']
        # assessments = pd.pivot(data=assessments, index=index, columns=cols, values=values).reset_index()
        # # resolve multilevel index and reformat col names
        # assessments.columns = assessments.columns.reorder_levels([1,0]).map(''.join).str.replace(' ','',regex=True)
        
        ##----------------------------------------------------------- Add utility cols
        ### these cols are discarded in wide staticfile format ut kept in code for logic refernce
        ### add G8Math column
        assessments['G8Math']=0
        mask = (assessments.StudentGrade==8) & (assessments.Subject=='Math') & (assessments.Performance.isin([1,4]))
        assessments.loc[mask,'G8Math'] = 1
        #### add G3ELA column
        mask = (assessments.StudentGrade == 3) & (assessments.Performance==1) & (assessments.Subject=='ELA')
        assessments['G3ELA'] = 0
        assessments.loc[mask,'G3ELA'] = 1
        
        ##add testtype col
        #make dict to hold values for test types
        test_types= {assess: 680 for assess in self.reg_assessement_types}
        for assess in self.alt_assessement_types:
            test_types[assess] = 685
        #map to assessment family col
        assessments['TestType'] = assessments.AssessmentFamily.map(test_types, na_action='ignore')
        
        # ###------------------------------------------------------- keep the usefull columns only
        assessments = assessments [['FiscalYear','SAISID', 'Subject', 'ScaleScore', 'G8Math'
                                    ,'Performance', 'Accommodation', 'AssessmentGrade', 'G3ELA', 'TestType'
                                    ,'AssessmentTestStatus', 'AssessmentFamily']]
        
        if self.static_folder is not None:
            file_name = 'Assessments'+ str(fiscal_year)[-2:] + '.csv'
            self.save_data(assessments, self.static_folder, file_name)
            
        return assessments
        
    def get_aspire_participation(self):
        print('\nGetting Raw aspire data from database')
        sql_statment = F'''SELECT  DISTINCT 
            sa.statestudentid as SAISID,
            sa.fiscalyear as FiscalYear,
            sa.TestSchoolKey AS SchoolCode,

            sch.districtkey                   AS DistrictId,
            COALESCE(gl.gradelevelcodevalue,'Not Provided')            AS AssessmentGrade,  
            fye.grade                         AS StudentGrade,
            asmt.assessmenttitle as AssessmentTitle,
soa.ObjectiveAssessmentKey,
            ast.academicsubjectdescription    AS AcademicSubject,
            --sa.administrationdate             AS AssessmentDate,

            sasr.result                       AS ScaleScoreResult

FROM [AccountabilityAssessmentDataMart].[ACTASPIRE ].[StudentAssessment] sa (nolock)
JOIN [AccountabilityAssessmentDataMart].[StudentDemo].[Student] s (nolock)
  ON sa.StateStudentID = s.StateStudentID
JOIN [AccountabilityAssessmentDataMart].[Assessment].[assessment] asmt  (nolock)
  ON sa.assessmentkey = asmt.assessmentkey
JOIN [AccountabilityAssessmentDataMart].[Assessment].[assessmentfamily] f  (nolock)
  ON asmt.assessmentfamilykey = f.assessmentfamilykey
JOIN [AccountabilityAssessmentDataMart].[Assessment].[assessmentperiodtype] p   (nolock)
  ON sa.assessmentperiodkey = p.assessmentperiodtypekey
JOIN [AccountabilityAssessmentDataMart].[ACTASPIRE ].StudentObjectiveAssessment soa
  ON sa.StudentAssessmentKey =soa.StudentAssessmentKey
JOIN [AccountabilityAssessmentDataMart].[Assessment].[ObjectiveAssessment] oa
  ON soa.ObjectiveAssessmentKey =oa.ObjectiveAssessmentKey
JOIN [AccountabilityAssessmentDataMart].[Assessment].AcademicSubjectType ast
  on oa.AcademicSubjectTypeID = ast.AcademicSubjectTypeKey
JOIN [AccountabilityAssessmentDataMart].[ACTASPIRE ].StudentObjectiveAssessmentScoreResult sasr
  ON soa.StudentObjectiveAssessmentKey = sasr.StudentObjectiveAssessmentKey
JOIN [AccountabilityAssessmentDataMart].[Assessment].[assessmentreportingmethodtype] arm   (nolock)
  ON sasr.assessmentreportingmethodtypekey = arm.assessmentreportingmethodtypekey  
--JOIN [Accountability].[assessment].[AssessmentCutScorePerformanceLevelMapping] pl
--ON soa.ObjectiveAssessmentKey = pl.[ObjectiveAssessmentId]
Left JOIN [AccountabilityAssessmentDataMart].[Assessment].[assessmentteststatus] ats   (nolock)
  ON sa.assessmentteststatuskey = ats.assessmentteststatuskey
Left JOIN [AccountabilityAssessmentDataMart].[Assessment].[assessmenttestmode] atm   (nolock)
  ON sa.assessmenttestmodekey = atm.assessmenttestmodekey
LEFT JOIN [AccountabilityAssessmentDataMart].[Common].[gradeleveltype] gl   (nolock)
  ON sa.whenassessedgradeleveltypekey = gl.gradeleveltypekey
LEFT OUTER JOIN (SELECT f.FiscalYear,f.SAISID,f.Grade
                    from Accountability.dbo.FiscalYearEnrollment f   (nolock)
   join(SELECT y.FiscalYear,y.SAISID,max(y.EntryDate) AS maxentrydate
   FROM Accountability.dbo.FiscalYearEnrollment y    (nolock)
   WHERE y.FiscalYear = {self.fiscal_year}
   GROUP BY y.FiscalYear,y.SAISID) me
   ON f.FiscalYear = me.FiscalYear AND f.SAISID = me.SAISID AND f.EntryDate = me.maxentrydate) fye
 ON sa.StateStudentID = fye.SAISID AND sa.FiscalYear = fye.FiscalYear
LEFT OUTER JOIN [Accountability].[EdOrg].[School] sch   (nolock)
                ON sa.TestSchoolKey = sch.schoolkey
                   AND sa.fiscalyear = sch.fiscalyear
LEFT OUTER JOIN [Accountability].[EdOrg].[LEA] sd   (nolock)
                ON sch.districtkey = sd.districtkey
                   AND sch.fiscalyear = sd.fiscalyear
WHERE  sa.fiscalyear = {self.fiscal_year}
and arm.assessmentreportingmethoddescription in ('Scale score')
AND soa.ObjectiveAssessmentKey in (7163,7165, 7167)'''
        
        # setup connection to db and read in data
        try:
            cnxn = con().__call__(server_name= 'AACTASTPDDBVM01')
            aspire = pd.read_sql(sql_statment, cnxn)
            # print(colored('\033[1mRetrieved data from database successfully\033[0m', 'green'))
            cnxn.close()
        except Exception as ex:
            print(colored('\033[1mFailed to retrieve data, error in "correct_aspire_participation()"\033[0m', 'red'))
            print(ex)
        
        return aspire
    def fix_aspire_participation(self):
        #get math reading and english for aspire
        aspire = self.get_aspire_participation()
        #identify those that have recieved a non-scorable response on writing and take them and any other record for the same kid at the same school.
        non_scorable_values = ['BL','NE','IL','OT']
        non_scorable = aspire.loc[aspire.ScaleScoreResult.isin(non_scorable_values),['SAISID']].copy()
        aspire = pd.merge(aspire, non_scorable, on=['SAISID'], how='inner')
        #make sure each kid has an english, writing and reading record
        #replace non-scorable responses with lowest scale score possible (400)
        aspire.loc[aspire.ScaleScoreResult.isin(non_scorable_values), 'ScaleScoreResult'] = 400
        #convert scaleScoreresults to numeric
        aspire['ScaleScoreResult'] = pd.to_numeric(aspire['ScaleScoreResult'], errors='coerce')
        cols=['SAISID', 'ObjectiveAssessmentKey', 'ScaleScoreResult']
        aspire.sort_values(cols, ascending=False, inplace=True)
        #deduplicate to keep one of English, reading and writing per kid
        aspire = aspire[~aspire.duplicated(cols)].copy()
        # agg count of records and only keep the records were kid took all 3 tests
        cols_to_keep = ['FiscalYear', 'SAISID', 'SchoolCode', 'AssessmentGrade', 'StudentGrade']
        aspire_objective = aspire.groupby(cols_to_keep).agg(NumberOfTests=('ObjectiveAssessmentKey', 'nunique')
                                                        ,ScaleScore=('ScaleScoreResult', 'mean')).reset_index()
        aspire_objective = aspire_objective[aspire_objective.NumberOfTests==3].copy()
        aspire_objective['ScaleScore'] = aspire_objective['ScaleScore'].round(0)
        ## add in assessmentfamily and academic subject
        aspire_objective['AssessmentFamily'] = 'ACTASPIRE'
        aspire_objective['AcademicSubject'] = 'English Language Arts'

        return aspire_objective
        

        
        
        