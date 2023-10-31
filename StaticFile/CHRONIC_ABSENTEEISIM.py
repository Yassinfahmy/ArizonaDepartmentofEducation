# -*- coding: utf-8 -*-
"""
Created on Fri Jan 27 13:00:51 2023

@author: yfahmy
"""
import pandas as pd
from CONNECTION import CONNECTION as con
import os
from datetime import date
from SOURCES import SOURCES
from termcolor import colored

class CA(SOURCES):

    def __init__(self, fiscal_year=None, run='Prelim', **kwargs):
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
    
    def get_ca_from_database(self):
        ## include pct absensce
        print('Getting Chronic Absenteeisim data from database')
        #define sql statments to run
        aoi_exec_sql_statment = self.get_aoi_exec_ca_sql()
        aoi_sql_statment = self.get_aoi_ca_sql()
        
        bm_sql_statment = self.get_bm_ca_sql()
        
        ##establish connection
        cnxn = con().__call__(server_name=self.server_name)
        cursor = cnxn.cursor()
        ##get data
        bm_ca = pd.read_sql(bm_sql_statment, cnxn)
        #get online data in 2 steps
        ##first send execution statment to build temp tables
        ##then query the temp tables
        #then delete the temp tables
        cursor.execute(aoi_exec_sql_statment)
        cnxn.commit()
        aoi_ca = pd.read_sql(aoi_sql_statment, cnxn)
        cnxn.close()
        if aoi_ca.shape[0]==0:
            raise('Error in retrieving AOI Chronic Absenteeisim data')
        
        for df,name in zip([aoi_ca, bm_ca], ['AOICA', 'BrickMorterCA']):
            if self.raw_folder is not None:
                file_name = name + str(self.fiscal_year)[-2:] + '.csv'
                self.save_data(df, self.raw_folder, file_name)
                
        return aoi_ca, bm_ca 
         
    def format_raw_ca(self):
        #get from db
        aoi_ca, bm_ca = self.get_ca_from_database()
        
        print(colored('Starting Chronic Absenteeisim data formating', 'green'))
        ##----------------------------------------------------- Rename cols
        aoi_col_ren = {'schoolid':'SchoolCode'
                   ,'saisid':'SAISID'
                   ,'Grade':'StudentGrade'
                   ,'fte':'FTE'
                   ,'AOIChronicAbsentteism':'AOICA'}
        aoi_ca.rename(aoi_col_ren, inplace=True, axis=1)
        
        bm_col_ren = {'SchoolID':'SchoolCode'
                      ,'grade':'StudentGrade'}
        bm_ca.rename(bm_col_ren, inplace=True, axis=1)
        
        ##---------------------------------------------------- data wrangling
        bm_ca['ChronicAbsent'] = 1
        bm_ca = bm_ca[['FiscalYear', 'SchoolCode', 'SAISID', 'ChronicAbsent']]
        
        mask = (aoi_ca.AOICA.astype(float)==1) & (aoi_ca.FTE.astype(float)==1)
        aoi_ca = aoi_ca [mask].copy()
        aoi_ca['ChronicAbsent'] = 1
        aoi_ca = aoi_ca[['FiscalYear', 'SchoolCode', 'SAISID', 'ChronicAbsent']]
        
        ca = pd.concat([bm_ca, aoi_ca])
        ca = ca [~ca.duplicated()]
        
        for col in ca.columns:
            ca[col] = pd.to_numeric(ca[col], errors='coerce')
            
        if self.static_folder is not None:
            file_name = 'ChronicAbsenteeisim'+ str(self.fiscal_year)[-2:] + '.csv'
            self.save_data(ca, self.static_folder, file_name)
        
        return ca
        
    def get_aoi_exec_ca_sql(self):
        sql = F'''
                DECLARE @FiscalYear int= {self.fiscal_year}
               IF Object_id('tempdb..#GetOnlineStudentTotalMints') IS NOT NULL 
                  DROP TABLE #GetOnlineStudentTotalMints 
            
                CREATE TABLE #GetOnlineStudentTotalMints 
                  ( 
                     SchoolID         INT NULL, 
                     SAISID           INT NULL, 
                     AttendanceAmtSum INT NULL, 
                    fte  NVARCHAR (40) NULL
                  ) 
                ON [PRIMARY] 

                INSERT INTO #getonlinestudenttotalmints 
                            (SchoolID, 
                             SAISID, 
                             attendanceamtsum,
            				 fte
                             )    
                SELECT sa.SchoolID, 
                       sa.StateStudentID, 
                       Sum(sa.InstructionalMinutes) AS AttendanceAmtSum,
            		   fa.FTE
                FROM   accountability.dbo.fiscalyearenrollment fa with (nolock)
                       INNER JOIN [AccountabilityAssessmentDataMart].[Attendance].[StudentAttendance] (NOLOCK) sa 
                               ON  SA.StateStudentID = fa.SAISID 
            				   and SA.SchoolID = fa.SchoolID 
            				   and sa.FiscalYear = fa.FiscalYear 
            				   and sa.StudentEnrollmentKey = fa.SchoolMembershipBK 
                       INNER JOIN [Accountability].[EdOrg].[School] (NOLOCK) e 
                               ON e.SchoolKey = sa.SchoolID 
                                  AND e.FiscalYear = sa.FiscalYear 
                                  AND e.IsAOI = 1 and e.IsAlternativeSchoolAccountability <> 1
                WHERE  fa.FiscalYear = @FiscalYear AND Datepart(yyyy, SA.EventDate) >= @FiscalYear -1 AND fa.FTE = '1.00'
                GROUP  BY sa.SchoolID, 
                          sa.StateStudentID,
            			  fa.FTE

            SELECT f.saisid,
                   f.schoolid,
                   Datediff(day, f.entrydate, f.exitdate) -- Total Days
                   - ( Datediff(day, 0, f.exitdate) / 7 - Datediff(day, 0,f.entrydate) / 7 )-- Sundays
                   - ( Datediff(day, -1, f.exitdate) / 7 - Datediff(day, -1, f.entrydate) / 7 ) -- Saturdays
                   AS DaysAttended,
            	  f.Grade,
            	  f.FiscalYear
            INTO   #t1
            FROM   accountability.dbo.fiscalyearenrollment f WITH (nolock)
            WHERE  fiscalyear = @FiscalYear AND f.FTE = '1.00'
            SET NOCOUNT ON
            SELECT saisid,
                   schoolid,
                   Sum(daysattended) AS TotalDays,
                   CASE
                     WHEN Sum(daysattended) >= 180  THEN 180 --CAP Max days to 180
                     ELSE Sum(daysattended)
                   END               AS DAYSEnrolled	
            INTO #DaysEnrolled
            FROM   #t1
            GROUP  BY saisid,
                      schoolid 
            
            SELECT t.saisid,
                   t.schoolid,
            	  d.DAYSEnrolled,
                   CASE
                     WHEN( grade = '1' OR grade = '2' OR grade = '3' ) THEN DAYSEnrolled * 237.33
            	    WHEN( grade = '4' OR grade = '5' OR grade = '6' ) THEN DAYSEnrolled * 296.67  
            	    WHEN( grade = '7' OR grade = '8' ) THEN DAYSEnrolled * 356     
                   END               AS MinuesEnrolled,
            	  t.Grade,
            	  t.FiscalYear
            INTO #minutesenrolled
            FROM   #t1 t
            Join  #DaysEnrolled d
            ON t.SAISID = d.SAISID AND t.SchoolId = d.SchoolId
            WHERe t.Grade in ('1','2','3','4','5','6','7','8')
            order by SAISID


        '''
        return sql
    
    def get_aoi_ca_sql(self):
        sql ='''SELECT m.FiscalYear,
                           m.schoolid,
                           m.saisid,
                           m.Grade,
                           m.daysenrolled,
                           m.minuesenrolled,
                           o.attendanceamtsum,
                    	     o.fte,
                    	    CASE
                             WHEN (m.minuesenrolled * 0.9) <= (o.attendanceamtsum) THEN 0 
                             ELSE 1
                           END AS AOIChronicAbsentteism
                    	    
                    FROM   #minutesenrolled m
                           JOIN #getonlinestudenttotalmints o
                             ON m.saisid = o.saisid
                                AND m.schoolid = o.schoolid 
                    			
                    Drop table #getonlinestudenttotalmints
                   DROP table #minutesenrolled
                   DROP table #t1
                   DROP table #DaysEnrolled'''
        return sql
    
    def get_bm_ca_sql(self):
        sql = F'''DECLARE @FiscalYear int= {self.fiscal_year}
                SELECT a.*
                ,IsNull(c.SchoolWeekTypeKey,d.SchoolWeekTypeKey) as SchoolWeekTypeKey
                FROM   (SELECT  sm.FiscalYear, sm.SchoolId AS SchoolID, sm.DistrictId AS DistrictID,
                				t.TrackNumberDescription, sm.SAISID, sm.FirstName, sm.LastName, sm.MiddleName,
                                sm.ELLNeed, sm.SPED, sm.EthnicGroupID, sm.Gender, sm.EconomicDisadvantage,
                                sm.grade, Sum(CONVERT(float, ( am.RefAbsentAmtID - 2497 )) * 0.25) AS Absences
                        FROM  AccountabilityAssessmentDataMart.Absence.StudentAbsence sa
                        INNER JOIN Accountability.DBO.FiscalYearEnrollment sm
                           ON sm.SAISID = sa.StateStudentID AND sm.SchoolId = sa.SchoolID AND sm.FiscalYear = sa.FiscalYear
                        INNER JOIN (Select CASE WHEN Cast(AbsenceAmountTypeCodeValue AS FLOAT) = 0.25 THEN 2498
                                                WHEN Cast(AbsenceAmountTypeCodeValue AS FLOAT) = 0.5 THEN 2499
                                                WHEN Cast(AbsenceAmountTypeCodeValue AS FLOAT) =  0.75 THEN  2500
                                                WHEN Cast(AbsenceAmountTypeCodeValue AS FLOAT) = 1 THEN  2501  else 0
                                            END AS RefAbsentAmtID,
                                            AbsenceAmountTypeKey
                                    FROM  AccountabilityAssessmentDataMart.Absence.StudentAbsenceAmountType) am
                           ON sa.AbsenceAmountTypeKey = am.AbsenceAmountTypeKey
                        JOIN [AccountabilityAssessmentDataMart].[Common].[TrackNumber] t (nolock)
                           ON sm.TrackNumber = t.TrackNumber
                        WHERE  sa.EventDate between sm.EntryDate AND IsNull(sm.ExitDate, '2099-12-31') 
                            AND sm.grade in('1','2','3','4','5','6','7','8') AND SM.EconomicDisadvantage IN('1','0')
                            AND sm.FiscalYear = @FiscalYear
                        GROUP BY sm.FiscalYear,sm.SchoolId,sm.DistrictId,t.TrackNumberDescription,sm.SAISID,sm.FirstName,
                                 sm.LastName,sm.MiddleName,sm.ELLNeed,sm.SPED,sm.EthnicGroupID,sm.Gender,
                                 sm.EconomicDisadvantage , sm.grade)as a
                LEFT JOIN (SELECT sc.educationorganizationid, sc.fiscalyear,sc.status, sc.SchoolWeekTypeKey, 
                                  sc.tracknumberkey, t.tracknumberdescription
                            FROM accountabilityassessmentdatamart.calendar.schoolcalendaryear sc
                            JOIN [AccountabilityAssessmentDataMart].[Common].[TrackNumber] t
                           ON sc.tracknumberkey = t.tracknumberkey ) c
                   ON   a.SchoolID = c.EducationOrganizationID AND a.FiscalYear = c.FiscalYear AND c.Status = 1
                        AND a.tracknumberdescription = c.tracknumberdescription
                LEFT JOIN   (SELECT sc.educationorganizationid, sc.TrackLocalEducationAgencyID, sc.fiscalyear, sc.status,
                			        sc.SchoolWeekTypeKey, sc.tracknumberkey, t.tracknumberdescription
                            FROM   accountabilityassessmentdatamart.calendar.schoolcalendaryear sc
                            JOIN   [AccountabilityAssessmentDataMart].[Common].[TrackNumber] t
                                ON sc.tracknumberkey = t.tracknumberkey ) d
                    ON a.SchoolId = d.EducationOrganizationID AND a.DistrictId = d.TrackLocalEducationAgencyID AND d.Status = 1
                       AND a.tracknumberdescription = d.tracknumberdescription
                Left JOIN AccountabilityAssessmentDataMart.[StudentProgram].[StudentNeed] n
                    ON a.SAISID = n.StateStudentID AND n.FiscalYear = @FiscalYear AND n.NeedKey = 50
                WHERE  a.Absences >= CASE IsNull(c.SchoolWeekTypeKey,d.SchoolWeekTypeKey)
                                       WHEN 2 THEN 14.4
                                       else 18.0
                                     END
                AND   n.StudentNeedKey IS NULL
        '''
        return sql