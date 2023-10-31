# coding: utf-8

"""
Import and Create Studlist

By: Yassin Fahmy
"""
import pandas as pd
from CONNECTION import CONNECTION as con
import os
from datetime import date
from termcolor import colored
from SOURCES import SOURCES

class FYE(SOURCES):
        
    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        '''
        

        Parameters
        ----------
        print_status : False
            set to True to print the variables in kwargs.

        Returns
        -------
        None.

        '''
        super().__init__(fiscal_year=fiscal_year, run=run,**kwargs)
        
        self.oct_1 = pd.Timestamp(self.previous_fiscal_year, 10, 1)
        
        self.py_oct_1 = pd.Timestamp(self.previous_fiscal_year-1, 10, 1)
        
        self.first_school_day = pd.Timestamp(self.previous_fiscal_year, 8, 1)
        
    def get_enrollment_from_database (self, year=None):
        '''Args:
            sql_statment (str): sql syntax to retrieve data from [Accountability].[dbo].[FiscalYearEnrollment] table for the current fiscal year
        Returns:
            pandas dataframe containing fiscal year enrollment table
        '''
        print('\nGetting FYE data from database')
        #define table to pull from depending on connected server
        if self.server_name == 'AACTASTPDDBVM02':
            table_name = F'AccountabilityArchive.Static.{self.run}FiscalYearEnrollment{self.fiscal_year}'
            snapshot_date = ',UploadDate'
        elif self.server_name == 'AACTASTPDDBVM01':
            table_name = '[Accountability].[dbo].[FiscalYearEnrollment]'
            snapshot_date = ',GETDATE() as UploadDate'
        else:
            raise ValueError(F'Server {self.server_name} is not a valid Database server. Set "server_name" in class constructor with proper server name')
            
        ## update fiscal year variable
        if year is None:
            year = self.fiscal_year
        
        sql_statment = F'''SELECT Distinct [FiscalYear]
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
                                  ,[YearEndExitCode]
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
                                  ,[Migrant]
                                  ,[DRP]
                                  ,[RALEP]
                                  ,[ADMIntegrity]
                                  , TuitionPayerCode
                                  ,SPEDCodeJ
                                  ,[Day1]
                                  {snapshot_date}
                            FROM {table_name}
                            LEFT JOIN (SELECT cd.Day1, educationorganizationid AS SchoolCode
                                        FROM [Accountability].[FYE].[SchoolCalendar] sc
                                        INNER JOIN [Accountability].[dbo].[NDayCalendarDates] cd
                                        	ON sc.schoolcalendaryearkey = cd.SchoolCalendarYearKey
                                        WHERE FiscalYear = {year}) SchoolCalender
                                ON SchoolCalender.SchoolCode = SchoolId
                            WHERE FiscalYear = {year}
                                '''
        if self.passed_integrity:
            sql_statment = sql_statment +' AND ADMIntegrity=1'
        if self.exclude_tuittion_payer_code_2:
            sql_statment = sql_statment +''' AND TuitionPayerCode!=2
                                            AND SPEDCodeJ!=1'''     
        
        try:
            # setup connection to db and read in data
            cnxn = con().__call__(server_name = self.server_name)
            stlist = pd.read_sql(sql_statment, cnxn)
            cnxn.close()
        except Exception as ex:
            print(colored('\033[1mFailed to retrieve data, error in "get_enrollment_from_database()"\033[0m', 'red'))
            print(ex)
        
        
        # save data to path if provided
        if self.raw_folder is not None:
            file_name = f'{self.run}Enrollment'+ str(self.fiscal_year)[-2:]+ '.csv'
            self.save_data(stlist, self.raw_folder, file_name)
       
        return stlist
                
    def remove_jted_districts(self, stlist, jted_file):
        '''
        Function to remove JTED district entries

        Parameters
        ----------
        stlist : dataframe
            A file containing all the districts that are classified as jteds

        Returns
        -------
        stlist file after removing entries for students enrolled in JTED districts

        '''
        stlist = stlist [~stlist.DistrictCode.isin(jted_file.DistrictCode.astype(float))]
        return stlist
            
        
    def format_enrollment(self, jted_file=None, raw_data_stlist=None, for_k2=False):
        ''' Method to format data of stlist as specified by historical code
        
        Parameters
        ----------
        from_file (Boolean): describing whether to load data from file or get from server
        file_path (raw string): file path to raw data
        jted_file (dataframe or CSV file): contains JTED DistrictId
        remove_jted (Boolean): removes JTED districts from STlist if True
        
        Returns
        -------
        Processed student list file
        
        '''
        
        if raw_data_stlist is None:
            if for_k2:
                stlist = self.get_enrollment_from_database(year = self.previous_fiscal_year)
            else:
                stlist = self.get_enrollment_from_database()
        else:   
            stlist = raw_data_stlist
            
        print(colored('\nStarting Enrollment data formatting:', 'green'))
        #---------------------------------------------------- rename columns and change data types
        col_ren = {'Grade':'StudentGrade'
                  ,'EthnicGroupID':'Ethnicity'
                  ,'CohortYear':'Cohort'
                   ,'EconomicDisadvantage':'IncomeEligibility1and2'
                   ,'FosterCare':'Foster'
                   ,'SchoolFAYStability': 'FAY'
                  ,'DistrictId':'DistrictCode'
                  ,'SchoolId':'SchoolCode'
                   ,'DistrictFAYStability':'DistrictFAY'
                   ,'UploadDate':'SnapShotDate'}
        stlist.rename(col_ren, inplace=True, axis=1)
        
        #set entry data nad exitdate to datetime type
        stlist['EntryDate'] = pd.to_datetime(stlist['EntryDate'], errors='coerce')
        stlist['ExitDate'] = pd.to_datetime(stlist['ExitDate'], errors='coerce')
        stlist['Day1'] = pd.to_datetime(stlist['Day1'], errors='coerce')
        # format ELL
        stlist['EL'] = pd.to_numeric(stlist['ELLNeed'], errors='coerce')
        stlist['FEPYears'] = pd.to_numeric(stlist['FEPYears'], errors='coerce')
        # format fte col
        stlist['FTE'] = pd.to_numeric(stlist['FTE'], errors='coerce')
        stlist['ScienceFAY'] = pd.to_numeric(stlist['ScienceFAY'], errors='coerce')
        stlist['RALEP'] = pd.to_numeric(stlist['RALEP'], errors='coerce')
        stlist['Migrant'] = pd.to_numeric(stlist['Migrant'], errors='coerce')
        stlist['TuitionPayerCode'] = pd.to_numeric(stlist['TuitionPayerCode'], errors='coerce')
        stlist['SPEDCodeJ'] = pd.to_numeric(stlist['SPEDCodeJ'], errors='coerce')
        
        ##-------------------------------------- format studentGrade
        # Map the str grade values to numeric
        #to_replace dictionary will hold the values that need to be mapped
        to_replace ={'StudentGrade':{'UE':'66'
                                    ,'PS':'77'
                                    ,'KG':'-1'}}
        stlist.replace(to_replace, inplace=True)
        
        #only keep grades1-12 and 66,77,-1
        stlist['StudentGrade'] = pd.to_numeric(stlist['StudentGrade'], errors='coerce')
        #change grades based on cohort year for act and aspire
        stlist.loc[stlist.Cohort==self.act_cohort, 'StudentGrade'] = self.test_type_grade_map ['AZACT'][0]
        stlist.loc[stlist.Cohort==self.aspire_cohort, 'StudentGrade'] = self.test_type_grade_map ['ACTASPIRE'][0]
        
        size_0 = stlist.shape[0]
        grades_mask = (stlist.StudentGrade.between(1,12)) | (stlist.StudentGrade.isin([66,77,-1]))
        stlist = stlist[grades_mask]
        ##remove 'CC' Exit codes
        cc_mask = (stlist.YearEndExitCode!='CC')
        stlist = stlist[cc_mask].copy()
        ## if this is called to generate list for_k2 only, keep 2nd grade only
        if for_k2:
            grades_mask = stlist.StudentGrade==2
            stlist = stlist[grades_mask]
        ### print an update of total records
        print('''\nUngraded Secondary student records where excluded
              Excluded records count = {}
              Total records left = {}'''.format(size_0 - stlist.shape[0], stlist.shape[0]))
        
        ##------------------------------------------------------- format ell cols
        # if Azella Fay is missing then set it to zzero
        stlist.AZELLAFAY.fillna(0, inplace=True)
        ## construct el, ELGroup and el_fep
        stlist['ELGroup'] =  stlist['EL'].copy()
        stlist['EL'] = stlist['EL'].apply(lambda x: 1 if x in [1,2,3,4,5] else 0)
         
        #construct the EL_fep (they must be el)
        stlist['ELFEP'] = stlist[['FEPYears', 'EL']].apply(lambda x: 1 if x[0] in [1,2,3,4,5] or x[1] == 1 else 0, axis=1)
        
        
        #------------------------------------------------------ format Ethinicity cols
        ethinicity_dict = {'Asian':'A'
            			    ,'Black/African American':'B'
                			,'Hispanic or Latino':'H'
                			,'American Indian or Alaska Native':'I'
                			,'White':'W'
                			,'Multiple Races':'R'
                			,'Native Hawaiian or Pacific Islander':'P'
                			,'Unknown':'U'}
        stlist['Ethnicity'] = stlist['Ethnicity'].map(ethinicity_dict, na_action='ignore')
        
        ##------------------------ restrict records to schoolyear
        if for_k2 is False:
            ## if the Day1 of school info is missing (mostly AOI) then impute it as 08/1
            stlist.loc[stlist.Day1.isnull(), 'Day1'] = self.first_school_day
            # Only pickup records where kid's entry date was within the school year (not in summer)
            mask = (stlist.EntryDate < self.last_school_day)  & ((stlist.ExitDate > stlist.Day1) | (stlist.ExitDate.isnull()))
            stlist = stlist[mask].copy()
    
        #------------------------------------------------------ remove jted records before de-duplication
        size_0 = stlist.shape[0]
        if jted_file is not None:
            try:
                stlist = self.remove_jted_districts(stlist, jted_file)
                ### print an update of total records
                print('''\nJTED student records where excluded
                      Excluded JTED records = {}
                      Total records left = {}'''.format(size_0 - stlist.shape[0], stlist.shape[0]))
            except Exception as ex:
                print(colored('''\033[1mWARNING: Error in removing JTEDS, processing incomplete: \
                              \nYou must provide a JTED dataframe to "jted_file" with a \
                              "DistrictCode" column.\033[0m''','red'))
                print(ex)
        elif for_k2:
            pass
        else:
            print(colored('''\033[1mWARNING:  JTED records were not removed\
                          \nPlease provide a JTED dataframe to "jted_file" with a \
                          "DistrictCode" column.\033[0m''','red'))
        
        ###------------------------------------ add october1 flag 
        ##------------------------------------- format records to show assessment window indicators
        stlist['ELAMathWindow'] = 0
        stlist['Oct1Enroll'] = 0
        if for_k2:
            k2_enrolled_mask = (stlist.EntryDate <= self.py_aasa_window) & (
                (stlist.ExitDate >= self.py_aasa_window) | (stlist.ExitDate.isnull()))
            stlist.loc[k2_enrolled_mask, 'ELAMathWindow'] = 1
            
            oct1_mask = (stlist.EntryDate <= self.py_oct_1) & (
                (stlist.ExitDate >= self.py_oct_1) | (stlist.ExitDate.isnull()))
            stlist.loc[oct1_mask, 'Oct1Enroll'] = 1
            
        else:
            ## we want to flag the records where entry is before assessement window and  
            ## exit date is after assessment window or missing
            aasa_enrolled_mask = (stlist.EntryDate <= self.aasa_window) & (
                (stlist.ExitDate >= self.aasa_window) | (stlist.ExitDate.isnull())) & (stlist.StudentGrade.isin(self.test_type_grade_map['AASA']))
            stlist.loc[aasa_enrolled_mask, 'ELAMathWindow'] = 1
            
            act_enrolled_mask = (stlist.EntryDate <= self.act_window) & (
                (stlist.ExitDate >= self.act_window) | (stlist.ExitDate.isnull())) & (stlist.StudentGrade.isin(self.test_type_grade_map['AZACT']))
            stlist.loc[act_enrolled_mask, 'ELAMathWindow'] = 1
            
            aspire_enrolled_mask = (stlist.EntryDate <= self.aspire_window) & (
                (stlist.ExitDate >= self.aspire_window) | (stlist.ExitDate.isnull())) & (stlist.StudentGrade.isin(self.test_type_grade_map['ACTASPIRE']))
            stlist.loc[aspire_enrolled_mask, 'ELAMathWindow'] = 1
        
            ###-------------- add oct1 flag
            oct1_mask = (stlist.EntryDate <= self.oct_1) & (
                (stlist.ExitDate >= self.oct_1) | (stlist.ExitDate.isnull()))
            stlist.loc[oct1_mask, 'Oct1Enroll'] = 1
        
        ##same thing for science
        sci_window_enrolled_mask = (stlist.EntryDate <= self.science_window) & (
            (stlist.ExitDate >= self.science_window) | (stlist.ExitDate.isnull())) & (
                stlist.StudentGrade.isin(self.subject_grade_map['Science']))
        stlist['SciWindow'] = 0
        stlist.loc[sci_window_enrolled_mask, 'SciWindow'] = 1
        
        #------------------------------------------ combine duplicate records in same schoo and grade
        ################ this section of the code is written based on FAY not being record specific in DB but rather school specific, Meaning if student has 2 records in DB for same school in same grade then if one is FAY other is also FAY
        ## we identify the enrollment during testing window flag to make sure if student was enrolled during testing window for any of their records then it will show up as enrolled during testing window for all their records at that school (even if they switch grades)
        # identify the dupliacte records
        duplicates = stlist[stlist[['SAISID', 'SchoolCode']].duplicated(keep=False)].copy()
        # we group those duplicates by 'SAISID', 'SchoolCode', 'StudentGrade' and take min and max ENtry and exit dates respectivly
        duplicates = duplicates.groupby(['SAISID', 'SchoolCode']).agg(
            {'ELAMathWindow':'max', 'Oct1Enroll':'max'}).reset_index()
        # then we merge back to stlist to change Entryand ExitDates on original records
        stlist = pd.merge(left=stlist, right=duplicates, on=['SAISID', 'SchoolCode'], how='left', suffixes=('', '_dup'))
        ## assign the window flags to the all instances of that student record at the same school regardless of grade
        ela_math_mask = stlist.ELAMathWindow_dup.notnull()
        oct1_mask = stlist.Oct1Enroll_dup.notnull()
        stlist.loc[ela_math_mask, 'ELAMathWindow'] = stlist.loc[ela_math_mask, 'ELAMathWindow_dup']
        stlist.loc[oct1_mask, 'Oct1Enroll'] = stlist.loc[oct1_mask, 'Oct1Enroll_dup']
        ##remove instances of testing window being 1 for non-testing grades (resulted from merging above on 'SAISID', 'SchoolCode' without StudentGrade)
        ##this works because we adjusted studentgrades above to make act_cohort-> grade 11 and aspire_cohort-> grade 9
        if for_k2 is False:
            mask = (stlist.ELAMathWindow==1) & (stlist.StudentGrade.isin(self.not_eligible_testing_grades))
            stlist.loc[mask, 'ELAMathWindow'] = 0
            
        ### for science window and enrollment dates we do the same but it is grade specific
        duplicates = stlist[stlist[['SAISID', 'SchoolCode', 'StudentGrade']].duplicated(keep=False)].copy()
        # we group those duplicates by 'SAISID', 'SchoolCode', 'StudentGrade' and take min and max ENtry and exit dates respectivly
        duplicates = duplicates.groupby(['SAISID', 'SchoolCode', 'StudentGrade']).agg(
            {'EntryDate':'min', 'ExitDate':'max', 'SciWindow':'max'}).reset_index()
        # then we merge back to stlist to change Entryand ExitDates on original records
        stlist = pd.merge(left=stlist, right=duplicates, on=['SAISID', 'SchoolCode', 'StudentGrade'], how='left', suffixes=('', '_dup'))
        ## assign the window flags to the all instances of that student record at the same school regardless of grade
        entry_mask = stlist.EntryDate_dup.notnull()
        sci_mask = stlist.SciWindow_dup.notnull()
        exit_mask = stlist.ExitDate_dup.notnull()
        #assign EntryDate_dup and ExitDates_dup to original where they are not null
        stlist.loc[sci_mask, 'SciWindow'] = stlist.loc[sci_mask, 'SciWindow_dup']
        stlist.loc[entry_mask, 'EntryDate'] = stlist.loc[entry_mask, 'EntryDate_dup']
        stlist.loc[exit_mask, 'ExitDate'] = stlist.loc[exit_mask, 'ExitDate_dup']
        ###############
            
        #----------------------------------------------------------------------remove duplicate records
        # then we need to make sure we have one record per student per school per grade.
        size_0 = stlist.shape[0]
        stlist.sort_values(['SAISID','SchoolCode', 'StudentGrade', 'FAY', 'FTE'], ascending=False, inplace=True)
        stlist = stlist[~stlist[['SAISID', 'SchoolCode', 'StudentGrade']].duplicated(keep='first')]
        ## print an update of total records left
        print('''\nStudent records where de-duplicated to keep one grade enrollment per student. 
              Excluded records = {}
              Total records left = {}'''.format(size_0 - stlist.shape[0], stlist.shape[0]))
        
        ##-------------------------------------------------------------- add utility cols
        ### make cols
        stlist['AOIFTE'] = stlist['FTE'].apply(lambda x: 1 if x==1 else 0)
        stlist['ScienceFAY'] = stlist['ScienceFAY'].apply(lambda x: 1 if x>0 else 0)
        ## if FAY is zero then Science FAY is zero
        stlist.loc[stlist.FAY==0, 'ScienceFAY'] = 0
        ##if not OCT1 then cannot be SPED
        mask = (stlist.Oct1Enroll != 1) & (stlist.SPED==1)
        stlist.loc[mask, 'SPED'] = 0
        
        ###creat RAEL from RALEP
        stlist['RAEL'] = 0
        stlist.loc[stlist.RALEP.isin([1,2,3]), 'RAEL'] = stlist.loc[stlist.RALEP.isin([1,2,3]), 'RALEP']
        #Make ELFAY col
        stlist['ELFAY'] = stlist.AZELLAFAY.apply(lambda x: 1 if x > 0 else 0)
        
        if for_k2:
            year=self.previous_fiscal_year
        else:
            year= self.fiscal_year
            
        #implement temp el fep fix
        #get list of students who reclassified in the past 4 years
        fep_list = self.el_fep_fix(year=year )
        #merge to stlist on student ID and school ID
        stlist = pd.merge(stlist, fep_list, on=['SAISID', 'SchoolCode'], how='left', suffixes=('', '_y'))
        #if ELFEP is not 1 set it to 1
        stlist.loc[(stlist.ELFEP != 1) & ((stlist.ELFEP_y == 1)), 'ELFEP'] = 1
        #if student has SPEDCodeJ make their tuittion payer code=2
        stlist.loc[stlist.SPEDCodeJ==1, 'TuitionPayerCode']=2

        #drop unneeded cols
        drop_cols = ['ELLNeed', 'FTE', 'FEPYears', 'RALEP', 'AZELLAFAY', 'EntryDate_dup', 'ExitDate_dup', 'ELAMathWindow_dup', 'SciWindow_dup', 'Oct1Enroll_dup', 'Day1', 'ScienceFAY', 'YearEndExitCode', 'ELFEP_y', 'SPEDCodeJ']
        stlist.drop(drop_cols, axis=1, inplace=True)
        
        if self.static_folder is not None:
            file_name = 'Enrollment'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(stlist, self.static_folder, file_name)
        
        return stlist     
    
    def el_fep_fix(self, year):
        ## retrieve the data
        sql_statment = f'''SELECT Distinct FiscalYear, SAISID, SchoolId as SchoolCode
                            FROM [Accountability].[dbo].[FiscalYearEnrollment]
                            WHERE FiscalYear between {year-4} AND {year}
                                AND ELLNeed = 5'''
        try:
            # setup connection to db and read in data
            cnxn = con().__call__(server_name = 'AACTASTPDDBVM01')
            fep_list  = pd.read_sql(sql_statment, cnxn)
            cnxn.close()
        except Exception as ex:
            print(colored('\033[1mFailed to retrieve data\033[0m', 'red'))
            print(ex)
            
        ## clean the data
        fep_list['ELFEP'] = 1
        fep_list.sort_values(['SAISID', 'SchoolCode', 'FiscalYear'], ascending=False, inplace=True)
        fep_list = fep_list[~fep_list.duplicated(['SAISID', 'SchoolCode'], keep='first')]
        fep_list.drop('FiscalYear', axis=1, inplace=True)
        return fep_list
            
            
            
            
    