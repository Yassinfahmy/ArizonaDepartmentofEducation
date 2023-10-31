# -*- coding: utf-8 -*-
"""
Created on Mon Oct  3 15:58:19 2022

@author: yfahmy
"""
import pandas as pd

from termcolor import colored


from CONNECTION import CONNECTION as con

from SOURCES import SOURCES

class GROWTH(SOURCES):
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
            
    def get_assessments_from_database(self):
        
        if self.server_name == 'AACTASTPDDBVM02':
            assess_table_name = F'AccountabilityArchive.Static.{self.run}Assessments{self.fiscal_year}'
            fye_table_name = F'AccountabilityArchive.Static.{self.run}FiscalYearEnrollment{self.fiscal_year}'
        elif self.server_name == 'AACTASTPDDBVM01':
            assess_table_name = '[Accountability].[assessment].[StudentAssessment]'
            fye_table_name = '[Accountability].[dbo].[FiscalYearEnrollment]'
        else:
            raise ValueError('Invalid server name allowed server names are: ')

        sql_assess = F'''SELECT Distinct
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
                          FROM {assess_table_name}
                          WHERE FiscalYear = {self.fiscal_year}'''
                          
        sql_enroll = F'''SELECT Distinct [FiscalYear]
                                  ,[SAISID]
                                  ,[FirstName]
                                  ,[MiddleName]
                                  ,[LastName]
                                  ,[BirthDate]
                                  ,[Gender] 
                                  ,[SchoolId]
                                  ,[DistrictId]
                                  ,[Grade]                    
                                  ,[EntryDate] 
                                  ,[ExitDate]
                                  ,[EthnicGroupID]          
                                  ,[CohortYear]                
                                  ,[SPED]
                                  ,[EconomicDisadvantage]     
                                  ,[Homeless]
                                  ,[FTE]
                                  ,[ELLNeed]                 
                                ,[FEPYears]                    
                                  ,[FosterCare]                 
                                  ,[ScienceFAY]
                                ,[AZELLAFAY]
                                  ,[SchoolFAYStability]         
                                  ,[DistrictFAYStability]
                                  ,[Military]
                                  ,[DRP]
                            FROM {fye_table_name}
                            where FiscalYear = {self.fiscal_year}
                            AND TuitionPayerCode!=2
                            AND SPEDCodeJ!=1'''
        
        # setup connection to db and read in data
        try:
            cnxn = con().__call__(server_name= self.server_name)
            assessments = pd.read_sql(sql_assess, cnxn)
            enrollment = pd.read_sql(sql_enroll, cnxn)
            
            # print(colored('\033[1mRetrieved data from database successfully\033[0m', 'green'))
            cnxn.close()
        except Exception as ex:
            print(colored('\033[1mFailed to retrieve data, error in "get_assessments_from_database()"\033[0m', 'red'))
            print(ex)
        
        if self.raw_folder is not None:
            file_name = f'{self.run}Growth'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(assessments, self.raw_folder, file_name)
        
        return assessments
          
    def format_sgp(self, raw_sgp):
        data = raw_sgp
        # ### select columns of interest
        # data = raw_sgp[[ "ACHIEVEMENT_LEVEL", "ID", "GRADE_ENROLLED", "YEAR", 
        #  "GRADE", "VALID_CASE", "SGP", "SCALE_SCORE_PRIOR",
        # "SGP_NORM_GROUP", "SGP_NORM_GROUP_SCALE_SCORES", "SGP_BASELINE", "SCALE_SCORE_PRIOR_BASELINE", "SGP_NORM_GROUP_BASELINE",
        # "SGP_NORM_GROUP_BASELINE_SCALE_SCORES", "GENDER", "ETHNICITY", "ELL_STATUS", "FREE_REDUCED_LUNCH_STATUS", "SPED_STATUS",
        # 'CONTENT_AREA', 'ACHIEVEMENT_LEVEL_PRIOR', 'SCALE_SCORE']].copy()
        
        ### rename some columns
        columns_to_rename = {'ID' : 'SAISID',
                             'GRADE_ENROLLED' : 'StudentGrade',
                             'YEAR' : 'FiscalYear',
                             'SGP' : 'SGP_CCR',
                             'SGP_BASELINE' : 'SGP_PPCRG',
                             'SCALE_SCORE_PRIOR' : 'PYSS',
                             'CONTENT_AREA' : 'Subject',
                             'ACHIEVEMENT_LEVEL' : 'Performance',
                             'ACHIEVEMENT_LEVEL_PRIOR' : 'PYPerformance',
                             'SCALE_SCORE' : 'ScaleScore'}
        data.rename(columns_to_rename, axis=1, inplace=True)
        
        ### Only include valid cases
        data = data [data.VALID_CASE == 'VALID_CASE']
        
        ### convert numeric columns to numeric type
        data.SAISID            = pd.to_numeric(data.SAISID, errors='coerce')
        data.StudentGrade      = pd.to_numeric(data.StudentGrade, errors='coerce')
        data.FiscalYear        = pd.to_numeric(data.FiscalYear, errors='coerce')
        data.SGP_CCR           = pd.to_numeric(data.SGP_CCR, errors='coerce')
        data.PYSS             = pd.to_numeric(data.PYSS, errors='coerce')
        data.ScaleScore       = pd.to_numeric(data.ScaleScore, errors='coerce')
        data.SGP_PPCRG         = pd.to_numeric(data.SGP_PPCRG, errors='coerce')

        ### Map the Achievment levels to nums
        achievment_level_map = {'Minimally Proficient':1,
                              'Partially Proficient':2,
                              'Proficient':3,
                              'Highly Proficient':4}
        data.Performance    = data.Performance.map(achievment_level_map)
        data.PYPerformance = data.PYPerformance.map(achievment_level_map)
        
        ### map Subject to a number code: 677 for math and 675 for ELA
        data.replace({'Subject':{"MATHEMATICS":'Math'}}, inplace=True)
        
        ### map SGP_CCR and SGP_PPCRG to ordinal scale
        # define a sgp_cutscores dictionary to hold the cutscore and later become an attribute of the object type also define a
        # function sgp_categories for quick mapping
        sgp_cutscores = {'category_0':[0,34],
                        'category_1':[34,67],
                        'category_2':[67,100]}
        def sgp_categories(x):
            if x > sgp_cutscores['category_0'][0] and x < sgp_cutscores['category_0'][1]:
                return 0
            elif x >= sgp_cutscores['category_1'][0] and x < sgp_cutscores['category_1'][1]:
                return 1
            elif x >= sgp_cutscores['category_2'][0] and x <= sgp_cutscores['category_2'][1]:
                return 2
        data['SGP_CCR_Category']   = data.SGP_CCR.apply(sgp_categories)
        data['SGP_PPCRG_Category'] = data.SGP_PPCRG.apply(sgp_categories)
        
        ### only keep columns of interest
        data = data[['SAISID', 'FiscalYear','StudentGrade','Subject', 'SGP_CCR'
                             ,'SGP_CCR_Category'
                             ,'ScaleScore','Performance','PYSS', 'PYPerformance']]#, 'SGP_PPCRG', 'SGP_PPCRG_Category'
        
        if self.static_folder is not None:
            file_name = 'sgp'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(data, self.static_folder, file_name)
        return data
    
    def prepare_growth(self, assessments_raw, enrollments_raw):
        
        fiscal_year = self.fiscal_year
        aspire_cohort = self.aspire_cohort
        act_cohort = self.act_cohort
        
        # only include ('AASA', 'ACTASPIRE', 'AZACT') assessments
        assessments = assessments_raw [assessments_raw['AssessmentFamily'].isin(('AASA', 'ACTASPIRE', 'AZACT'))].copy()
        ## remove any duplicates
        assessments = assessments[~assessments.duplicated()].copy()
        
        # convert  Enrolled grade and WhenAssessedGrade columns to numeric (any non-number value will be NULL)
        assessments['EnrolledGrade'] = pd.to_numeric(assessments['EnrolledGrade'], errors='coerce')
        assessments['WhenAssessedGrade'] = pd.to_numeric(assessments['WhenAssessedGrade'], errors='coerce')
        assessments['ScaleScoreResult'] = pd.to_numeric(assessments['ScaleScoreResult'], errors='coerce')
        
        ## keep grades 3-12 only
        size_0 = assessments.shape[0]
        assessments = assessments [assessments['EnrolledGrade'].between(3,12)]
        print('''\nEnrolled Grades 3-12 only were kept
        Excluded entries = {}
        total Entries left = {}'''.format(size_0-assessments.shape[0], assessments.shape[0] ))
        
        ###### if student took same assessment in same school more than once we only keep the entry with his highest score
        ### Validation checks were done to make sure that the record i grab from the fiscal year enrollment table in case multiple records are present
        #### matches the recods IT uses to populate the enrolled grade column in the assessment table (pulled from the fiscal year enrollment table BTW)
        ### essentailly enrolled grade from fiscal year enrollment should match enrolled grade from assessments table
        #sort by 'StateStudentID','AcademicSubject','WhenAssessedGrade', 'EnrolledGrade', 'ScaleScoreResult' in descending order (de-duplicating)
        size_0 =  assessments.shape[0]
        assessments.sort_values(['StateStudentID','AcademicSubject','WhenAssessedGrade', 'EnrolledGrade', 'ScaleScoreResult'], inplace=True, ascending=False)
        # keep the first duplicate to keep highest scale score
        assessments = assessments [~assessments[['StateStudentID','AcademicSubject', 'WhenAssessedGrade']].duplicated(keep='first')]
        print('''\nStudents' entries where de-duplicated to keep entries with highest score for each subject/grade
              Excluded entries = {}
              Total entries left = {}\n'''.format(size_0 - assessments.shape[0], assessments.shape[0]))
        
        # keep only columns of interest
        columns_to_keep = ['StateStudentID', 'FirstName', 'LastName', 'SchoolId', 'SchoolName', 'DistrictId', 'DistrictName',
                           'WhenAssessedGrade', 'EnrolledGrade', 'AssessmentTitle', 'AcademicSubject', 'AssessmentTestMode',
                           'ScaleScoreResult', 'PerformanceLevelDescription', 'AssessmentFamily']
        assessments_cln = assessments[columns_to_keep].copy()
        
        #Rename columns
        rename_dict = {'StateStudentID':'ID'
                       , 'FirstName':'FIRST_NAME'
                       , 'LastName':'LAST_NAME'
                       , 'SchoolId':'SCHOOL_NUMBER'
                       , 'SchoolName':'SCHOOL_NAME'
                       ,'DistrictId':'DISTRICT_NUMBER'
                       , 'DistrictName':'DISTRICT_NAME'
                       , 'AcademicSubject':'CONTENT_AREA'
                       , 'PerformanceLevelDescription':'ACHIEVEMENT_LEVEL'
                       ,'EnrolledGrade':'GRADE'
                       , 'AssessmentTestMode':'TEST_MODE'
                       , 'ScaleScoreResult':'SCALE_SCORE'}
        assessments_cln.rename(rename_dict, axis=1, inplace=True)
        
        #------------------------------------------------------- Enrollment data pre-processing below
        #Change columns types (all non-numeric values will be NULL)
        enrollments_raw['Grade'] = pd.to_numeric(enrollments_raw.Grade, errors='coerce')
        enrollments_raw['SchoolFAYStability'] =  pd.to_numeric(enrollments_raw.SchoolFAYStability, errors='coerce')
        enrollments_raw['DistrictFAYStability'] = pd.to_numeric(enrollments_raw.DistrictFAYStability, errors='coerce')
        enrollments_raw['CohortYear'] = pd.to_numeric(enrollments_raw.CohortYear, errors='coerce')
        enrollments_raw['TuitionPayerCode'] = pd.to_numeric(enrollments_raw.TuitionPayerCode, errors='coerce')
        enrollments_raw['SAISID'] = pd.to_numeric(enrollments_raw.SAISID, errors='coerce')
        enrollments_raw['EntryDate'] = pd.to_datetime(enrollments_raw.EntryDate, errors='coerce')
        enrollments_raw['ExitDate'] = pd.to_datetime(enrollments_raw.ExitDate, errors='coerce')
        
        #make a copy of raw data to work on
        enrollment_refined = enrollments_raw.copy()
        
        # only keep Grades 3-12
        size_0 = enrollment_refined.shape[0]
        enrollment_refined = enrollment_refined [enrollment_refined.Grade.between(3,12)]
        print('''\nEnrolled Grade 3-12 only were kept
        Excluded entries = {}
        Total records left = {}'''.format(size_0-enrollment_refined.shape[0], enrollment_refined.shape[0] ))
        
        # Change values in Gender to Male and Female
        enrollment_refined['GENDER'] = enrollment_refined.Gender.str.replace('M','Male').str.replace('F', 'Female')
        
        # code for school fay and district fay
        # function to code school and district fay status
        def code_fay(x):
            if x>=1:
                x =1
            return x
        enrollment_refined['SCHOOL_ENROLLMENT_STATUS']= enrollment_refined.SchoolFAYStability.apply(code_fay)
        enrollment_refined['DISTRICT_ENROLLMENT_STATUS']= enrollment_refined.DistrictFAYStability.apply(code_fay)
        
        ### Validation checks were done to make sure that the record i grab from the fiscal year enrollment table in case multiple records are present
        #### matches the recods IT uses to populate the enrolled grade column in the assessment table (pulled from the fiscal year enrollment table BTW)
        ### essentailly enrolled grade from fiscal year enrollment should match enrolled grade from assessments table
        # if student has multiple records take the laterst entry date, highest grade and latest exitdate (matches what the IT does in the database)
        size_0 = enrollment_refined.shape[0]
        '''DONOT CHANGE THIS SECTION'''
        enrollment_refined.sort_values(['SAISID', 'EntryDate', 'Grade', 'ExitDate', 'SchoolFAYStability'], inplace=True, ascending=False, na_position='first')
        enrollment_refined = enrollment_refined [~enrollment_refined['SAISID'].duplicated(keep='first')]
        print('''\nEnrollement records were de-duplicated to keep record with lastest Entry Date and latest ExitDATE (if entry dates are identical)
        Excluded entries = {}
        Total records left = {}'''.format(size_0-enrollment_refined.shape[0], enrollment_refined.shape[0] ))
        
        #select the columns of interest only 
        enrollment_refined = enrollment_refined[['SAISID','GENDER', 'SchoolId', 'DistrictId','Grade', 'SPED', 'ELLNeed'
                                                 ,'SCHOOL_ENROLLMENT_STATUS', 'DISTRICT_ENROLLMENT_STATUS', 'CohortYear'
                                                 , 'TuitionPayerCode', 'EconomicDisadvantage', 'EthnicGroupID']]
        
        #rename columns
        columns_to_rename = {'SAISID':'ID'
                             , 'SchoolId':'SCHOOL_NUMBER'
                             , 'DistrictId':'DISTRICT_NUMBER'
                             ,'Grade':'GRADE'
                             , 'ELLNeed':'ELL_STATUS'
                             ,'SPED':'SPED_STATUS'
                             , 'EconomicDisadvantage':'FREE_REDUCED_LUNCH_STATUS'
                            ,'EthnicGroupID':'ETHNICITY'}
        enrollment_refined.rename(columns_to_rename, axis=1, inplace =True)
        
        #---------------------Merging enrollment and assessment data to create growth data
        # Merge assessments with enrollment
        growth_data = pd.merge(left=assessments_cln, right=enrollment_refined, on=['ID'], how='left', suffixes=('_x',''))
        print('Count of records:\nmerged {:,}'.format(growth_data.shape[0])
              ,'\nAssessments {:,}'.format(assessments_cln.shape[0])
              ,'\nEnrollment {:,}'.format(enrollment_refined.shape[0]))
        
        ################################################# this chunk is to make sure we select the right grades and records
        ## if students' cohort year is act_cohort and assessment family is AZACT we overide his when assessed grade to 11
        ## if students' cohort year is act_cohort we override their enrolled grade to 11
        #same for Aspire
        growth_data.loc[(growth_data.CohortYear==act_cohort) & (growth_data.AssessmentFamily=='AZACT') ,'WhenAssessedGrade']=11
        growth_data.loc[growth_data.CohortYear==act_cohort,'GRADE']=11
        growth_data.loc[(growth_data.CohortYear==aspire_cohort)  & (growth_data.AssessmentFamily=='ACTASPIRE'),'WhenAssessedGrade']=9
        growth_data.loc[growth_data.CohortYear==aspire_cohort,'GRADE']=9
        ### Now if student took ACT and they were the right cohort we only keep entries where Grade and when assessed grade are equal
        size_0 = growth_data.shape[0]
        growth_data = growth_data [growth_data.GRADE == growth_data.WhenAssessedGrade]
        ## only keep grades (3-9) and 11
        growth_data = growth_data[(growth_data.GRADE.between(3,9)) | (growth_data.GRADE==11)]
        print('''\nOnly entries where the student took the correct assessment for his current grade where kept Or the right cohort year for ACT and Aspire
        Excluded records = {}
        Total records left = {}'''.format(size_0-growth_data.shape[0], growth_data.shape[0]))
        #################################################
        
        ### remove entries where the same student took the same test more than once (if any are present)
        size_0 =  growth_data.shape[0]
        growth_data.sort_values(['ID','CONTENT_AREA','WhenAssessedGrade', 'GRADE', 'SCALE_SCORE'], inplace=True, ascending=False)
        # keep the first duplicate to keep highest scale score
        growth_data = growth_data [~growth_data[['ID','CONTENT_AREA','WhenAssessedGrade']].duplicated(keep='first')]
        print('''\nStudents' entries where de-duplicated to keep entries with highest score for each subject/grade
              Excluded entries = {}
              Total entries left = {}\n'''.format(size_0 - growth_data.shape[0], growth_data.shape[0]))
        
        #################################################This chunk is to look to see if students with null tution payer code generated a tution payer code at any point this year in any school and update their records
        size_0 = growth_data.shape[0]
        #Identify Students with NaN tuttion payer code
        null_tuition_payer_code_id = growth_data [growth_data['TuitionPayerCode'].isnull()].ID
        # specify a table to merge with growth data with updated Tuttion payer code based on SAISID and Grade (if student generated TPC =1 in one school then transfered to another school within same year, TPC is null in Enrollment table)
        enrollment_tpc = enrollments_raw [(enrollments_raw.SAISID.isin(null_tuition_payer_code_id)) & (enrollments_raw.TuitionPayerCode == 1)][['SAISID', 'TuitionPayerCode']]
        # # rename columns to merge correctly
        enrollment_tpc.rename({'SAISID':'ID', 'TuitionPayerCode':'tpc_updated'}, axis=1, inplace=True)
        #de-duplicate entries
        enrollment_tpc = enrollment_tpc [~enrollment_tpc.duplicated()]
        # # merge the made up enrollment table with the growth data to supplement where the tuition payer code is null
        growth_data_cln = pd.merge(left=growth_data, right=enrollment_tpc, on='ID', how='left')
        #update the TuitionPayerCode column for the new entries
        indices_to_update = growth_data_cln [ growth_data_cln.tpc_updated ==1].index
        growth_data_cln.loc[indices_to_update, 'TuitionPayerCode']= 1
        #only include entries where tuition payer code =1 (or null) as per federal buissness rules
        growth_data_cln = growth_data_cln [(growth_data_cln.TuitionPayerCode == 1) ].copy()
        print('''\nStudents' entries where Tution payer code was 1 only where kept
              Excluded entries = {}
              Total entries left = {}\n'''.format(size_0 - growth_data_cln.shape[0], growth_data_cln.shape[0]))
        #################################################
        
        ## make a new column called AssessmentSubtestTitle that contains subject and grade
        growth_data_cln ['AssessmentSubtestTitle'] = growth_data_cln.CONTENT_AREA + ' Grade ' + growth_data_cln.GRADE.astype(int).astype(str)
        growth_data_cln ['AssessmentSubtestTitle'] = growth_data_cln ['AssessmentSubtestTitle'].str.replace('English Language Arts', 'ELA')
        
        ##make a new column for year
        growth_data_cln['YEAR'] = 2022
        
        ## re-format the ELL_status column
        growth_data_cln.ELL_STATUS = growth_data_cln.ELL_STATUS.apply(lambda x: 1 if x>=1 else 0).copy()
        
        ##re-format TEST_MODE
        growth_data_cln.TEST_MODE = growth_data_cln.TEST_MODE.str.replace('O', 'CBT').str.replace('P', 'PBT').copy()
        
        
        ##rearrange the columns to match historic data format
        growth_data_cln = growth_data_cln [['ID', 'YEAR', 'SCHOOL_NUMBER', 'SCHOOL_NAME', 'DISTRICT_NUMBER',
               'DISTRICT_NAME', 'CONTENT_AREA', 'ACHIEVEMENT_LEVEL', 'GRADE','SCALE_SCORE', 'AssessmentSubtestTitle', 
                'FIRST_NAME', 'LAST_NAME', 'SCHOOL_ENROLLMENT_STATUS', 'DISTRICT_ENROLLMENT_STATUS', 'GENDER',
               'ETHNICITY', 'FREE_REDUCED_LUNCH_STATUS', 'TEST_MODE', 'SPED_STATUS', 'ELL_STATUS']].copy()
        
        if self.raw_folder is not None:
            file_name = 'sgpdata_all'+ str(fiscal_year)[-2:] + '.csv'
            self.save_data(growth_data_cln, self.raw_folder, file_name)
        return growth_data_cln
    
    
    