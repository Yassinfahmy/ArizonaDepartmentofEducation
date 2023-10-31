"""
Authors: ADE Accountability & Research

Last Updated: 05/25/2023

Description: This Python module contains a Growth_to_Grad class capable of producing the results from the Growth to Graduation component of the Arizona State A-F Letter Grades 
(https://www.azed.gov/accountability-research/state-accountability). In order to calculate points, one will need to provide a version of the Accountability Static File ().
"""

import pandas as pd
import numpy as np
from COMPONENTS import COMPONENTS
from DATABASE import DATABASE

"""
This class contains methods for creating Pandas DataFrames that contain the Summary Growth to Graduation information present on ADEConnect for alternative schools. To find this information, navigate to 
    adeconnect.azed.gov/ -> View Applications -> Accountability -> Accountability: State and Federal Profile.
"""
class GTG(COMPONENTS):
    """
    Constructor for Growth to Graduation. This class inherits from COMPONENTS, which contains variables and functionality that is shared across the different A-F Letter Grades Components.
    """
    def __init__(self,  fiscal_year:int=None, run='Prelim', **kwargs):
        """
        Parameters:
        ----------
        fiscal_year (int): The current AZ D.O.E. fiscal year
        """
        super().__init__(fiscal_year=fiscal_year, run=run, **kwargs)
        self.necessary_columns = ["FiscalYear", "SchoolCode", "Alternative"]
        self.max_persistence_points = 10
        self.max_credits_earned_points = 10
        self.max_OT2G_points = 10
        self.db = DATABASE(self.fiscal_year)
    
    """
    This method takes a version of the Static File, obtains a list of unique alternative schools from it, and calls methods to access 
    the database to obtain the necessary Persistence Rate, Credits Earned, & On Track To Graduate related results for the Growth To 
    Graduation component. 
    """
    def calculate_component(self, static_file:pd.DataFrame):
        """
        Parameters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (FY 2023 or beyond). It must contain the following columns: FiscalYear, SchoolCode, Alternative

        Returns:
        ----------
        pd.DataFrame: A dataframe containing all of the Proficiency information that is shown in the ADEConnect State Letter Grades summary and drilldown tables. Columns contain
        suffixes that denote the page on ADEConnect in which they appear
        """
        if not set(self.necessary_columns).issubset(static_file.columns): 
            missing_columns = [x for x in self.necessary_columns if x not in static_file.columns]
            raise ValueError(f"The DataFrame argument \"static_file\" is missing the following columns: {missing_columns}.")
        
        # get a list of alternative schools to filter for when retrieving results from the database
        alternative_schools = list(static_file[static_file["Alternative"]==1]["SchoolCode"].unique())

        # gather results
        persistence_results = self.persistence_points(alternative_schools).set_index(["EntityID"])
        credits_earned_results = self.credits_earned_points(alternative_schools).set_index(["EntityID"])
        on_track_to_graduate_results = self.on_track_to_graduate_points(alternative_schools).set_index(["EntityID"])

        # combine results
        all_results = pd.concat([persistence_results, credits_earned_results, on_track_to_graduate_results], axis=1).reset_index()

        # add additional point columns
        new_cols = ["AcademicPersistence", "CreditsEarned", "OnTracktoGraduate"]
        existing_cols = ["GTGAcademicPersistenceTP", "GTGCreditsEarnedTotalpoints", "GTGOnTrackToGradTotalpoints"]
        for col1, col2 in zip(new_cols, existing_cols):
            all_results[col1] = all_results[col2].copy()

        # add Model column
        columns = all_results.columns
        all_results["Model"] = "Alt 9-12"
        all_results = all_results[["Model"] + list(columns)] # reorder to put Model first. This is not necessary but is nice for validating
        all_results['FiscalYear'] = self.fiscal_year

        return all_results
    

    """
    Connects to database and obtains already-calculated persistence counts and rates. Points are determined from the persistence rate.
    """
    def persistence_points(self, alternative_schools:list):
        sql = f"""
            SELECT 
                [EntityID]
                ,[PYEligibleCount] AS GTGAcademicPersistenceDen
                ,[CYEnrolledCount] AS GTGAcademicPersistenceNum
                ,[PersistenceRate]*100 AS GTGAcadPersistencePctEarned 
            FROM [AccountabilityArchive].[Static].[{self.run}PersistRate{self.fiscal_year}]
            WHERE FiscalYear = {self.fiscal_year} 
                AND EntityID IN {tuple(alternative_schools)}"""
        persistence_rates = self.db.read_sql_query(sql=sql)

        # assign points. All alt schools are eligible for points
        persistence_rates["GTGAcademicPersistencePctAvlbl"] = 100

        # limit points
        persistence_rates["GTGAcademicPersistenceTP"] = np.minimum(self.max_persistence_points, persistence_rates["GTGAcadPersistencePctEarned"]/10)
        return persistence_rates

    """
    Connects to database to obtain already-calculated Credits-Earned counts and points
    """
    def credits_earned_points(self, alternative_schools:list):
        
        sql = f"""
            SELECT 
                [SchoolID] AS EntityID
                , [TotalStudents] AS GTGCreditsEarnedDen
                , [TotalGraduates] AS GTGCreditsEarnedNum
                , [Points] AS GTGCreditsEarnedTotalpoints
                , [IsEligible]
            FROM [AccountabilityArchive].[Static].[{self.run}CE{self.fiscal_year}]
            WHERE FiscalYear = {self.fiscal_year} 
                AND SchoolID IN {tuple(alternative_schools)}"""
        credits_earned = self.db.read_sql_query(sql=sql)
        if self.include_late_submissions:
            credits_earned = self.add_late_ce_submissions(credits_earned, alternative_schools)
        # limit points
        credits_earned["GTGCreditsEarnedTotalpoints"] = np.minimum(self.max_credits_earned_points, credits_earned["GTGCreditsEarnedTotalpoints"])

        # assign points to eligible schools
        eligible_schools = (credits_earned["IsEligible"]==1)
        credits_earned.loc[eligible_schools, "GTGCreditsEarnedPctEarned"] = credits_earned.loc[eligible_schools, "GTGCreditsEarnedNum"]/credits_earned.loc[eligible_schools,"GTGCreditsEarnedDen"]*100
        credits_earned.loc[eligible_schools, "GTGCreditsEarnedPctAvlbl"] = 100

        # assign points to ineligible schools
        ineligible_schools = (credits_earned["IsEligible"]==0)
        credits_earned.loc[ineligible_schools, ["GTGCreditsEarnedPctEarned", "GTGCreditsEarnedPctAvlbl"]] = [np.nan, np.nan]

        # assign points to schools that did not submit
        not_reported_schools = credits_earned["IsEligible"].isna()
        credits_earned.loc[not_reported_schools, ["GTGCreditsEarnedTotalpoints", "GTGCreditsEarnedPctAvlbl"]] = [0, 100]
        credits_earned = credits_earned.drop(columns=["IsEligible"])

        return credits_earned


    """
    Connects to database to obtain already-calculated On-Track-To-Graduate counts and points
    """
    def on_track_to_graduate_points(self, alternative_schools:list):
        sql = f"""
            SELECT 
                [SchoolID] AS EntityID
                , [TotalStudents] AS GTGOnTrackToGradDen
                , [TotalGraduates] AS GTGOnTrackToGradNum
                , [Points] AS GTGOnTrackToGradTotalpoints
                , [IsEligible]
            FROM [AccountabilityArchive].[Static].[{self.run}OTG{self.fiscal_year}]
            WHERE FiscalYear = {self.fiscal_year} 
                AND SchoolID IN {tuple(alternative_schools)}
        """
        on_track_to_graduate = self.db.read_sql_query(sql=sql)
        if self.include_late_submissions:
            on_track_to_graduate = self.add_late_otg_submissions(on_track_to_graduate, alternative_schools)
        # limit points
        on_track_to_graduate["GTGOnTrackToGradTotalpoints"] = np.minimum(self.max_OT2G_points, on_track_to_graduate["GTGOnTrackToGradTotalpoints"])

        # assign points to eligible schools
        eligible_schools = (on_track_to_graduate["IsEligible"]==1)
        on_track_to_graduate.loc[eligible_schools, "GTGOnTrackToGradPctEarned"] = on_track_to_graduate.loc[eligible_schools, "GTGOnTrackToGradNum"]/on_track_to_graduate.loc[eligible_schools, "GTGOnTrackToGradDen"]*100
        on_track_to_graduate.loc[eligible_schools, "GTGOnTrackToGradPctAvlbl"] = 100

        # assign points to ineligible schools
        ineligible_schools = (on_track_to_graduate["IsEligible"]==0)
        on_track_to_graduate.loc[ineligible_schools, ["GTGOnTrackToGradPctEarned", "GTGOnTrackToGradPctAvlbl"]] = [np.nan, np.nan]

        # assign points to schools that did not submit
        not_reported_schools = on_track_to_graduate["IsEligible"].isna()
        on_track_to_graduate.loc[not_reported_schools, ["GTGOnTrackToGradTotalpoints", "GTGOnTrackToGradPctAvlbl"]] = [0, 100]

        on_track_to_graduate = on_track_to_graduate.drop(columns=["IsEligible"]) # drop eligibility column since we do not report it

        return on_track_to_graduate
    
    def fetch_late_otg_submissions(self, alternative_schools):
        db = DATABASE(fiscal_year=self.fiscal_year, database='AccountabilityArchive', schema='dbo', run='', server_name='AACTASTPDDBVM02')
        sql = f"""select 
                [SchoolID] AS EntityID
                , n_stu_elig AS GTGOnTrackToGradDen
                , n_stu_met AS GTGOnTrackToGradNum
                , [Points] AS GTGOnTrackToGradTotalpoints
                , [IsEligible]
                  FROM [AccountabilityArchive].[dbo].[LateLateSelfReportedData]
                 Where FiscalYear={self.fiscal_year} and type='otg'
                     AND SchoolID IN {tuple(alternative_schools)}"""
        late_otg = db.read_sql_query(sql=sql)
        return late_otg
    
    def add_late_otg_submissions(self, on_track_to_graduate, alternative_schools):
        
        late_otg = self.fetch_late_otg_submissions(alternative_schools)
        late_otg['IsEligible'] = late_otg['IsEligible'].apply(lambda x: True if x==1 else False)
        
        #delete the late submission value from original table and add new data
        on_track_to_graduate = on_track_to_graduate[~on_track_to_graduate.EntityID.isin(late_otg.EntityID)].copy()
        on_track_to_graduate = pd.concat([on_track_to_graduate, late_otg], axis=0)
        
        return on_track_to_graduate
    
    def fetch_late_ce_submissions(self, alternative_schools):
        db = DATABASE(fiscal_year=self.fiscal_year, database='AccountabilityArchive', schema='dbo', run='', server_name='AACTASTPDDBVM02')
        sql = f"""select 
                [SchoolID] AS EntityID
                , n_stu_elig AS GTGCreditsEarnedDen
                , n_stu_met AS GTGCreditsEarnedNum
                , [Points] AS GTGCreditsEarnedTotalpoints
                , [IsEligible]
                  FROM [AccountabilityArchive].[dbo].[LateLateSelfReportedData]
                 Where FiscalYear={self.fiscal_year} and type='CE'
                     AND SchoolID IN {tuple(alternative_schools)}"""
        late_ce = db.read_sql_query(sql=sql)
        return late_ce
    
    def add_late_ce_submissions(self, credits_earned, alternative_schools):
        
        late_ce = self.fetch_late_ce_submissions(alternative_schools)
        late_ce['IsEligible'] = late_ce['IsEligible'].apply(lambda x: True if x==1 else False)
        
        #delete the late submission value from original table and add new data
        credits_earned = credits_earned[~credits_earned.EntityID.isin(late_ce.EntityID)].copy()
        credits_earned = pd.concat([credits_earned, late_ce], axis=0)
        
        return credits_earned
    


# fy= 2023
# run = 'PrelimV6'
# self = GTG(fy, run)
# staticfile = DATABASE(fiscal_year = fy
#                       ,run = run
#                       ,schema = 'Static'
#                       ,database = 'AccountabilityArchive').read_table(table_name ='StaticFile')
# static_file = staticfile.copy()
