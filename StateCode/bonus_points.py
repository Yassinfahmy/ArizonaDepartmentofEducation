"""
Created by: ADE Accountability & Research

Last Updated: 06/12/2023

Description: This Python module contains a Bonus_Points class capable of producing bonus points from the AZ State A-F Lettergrades business rules 
(https://www.azed.gov/accountability-research/resources) for each school. In order to utilize the methods of the Bonus Point class, one will need to 
provide a version of the Accountability Static File () and be able to connect to the AccountabilityArchive SQL database.
"""

import pandas as pd
import numpy as np
from COMPONENTS import COMPONENTS
from DATABASE import DATABASE

"""
This class contains methods for creating a Pandas DataFrame that contains the bonus point results displayed on ADEConnect. To see these results displayed, navigate to 
    adeconnect.azed.gov/ -> View Applications -> Accountability -> Accountability: State and Federal Profile.
"""
class Bonus_Points(COMPONENTS):
    """
    Constructor for Bonus_Points class. This class inherits from COMPONENTS, which contains variables and functionality that is shared across the different State A-F indicator components.
    """
    def __init__(self,  fiscal_year:int=None, **kwargs):
        """
        Parameters:
        ----------
        fiscal_year (int): The current AZ D.O.E. fiscal year
        """
        super().__init__(fiscal_year=fiscal_year, **kwargs)
        self.act_aspire_subjects = ["Math", "ELA"]
        self.act_aspire_assessment_families = ["ACTASPIRE", "NotTested"]
    

    """
    This method takes a version of the Static File, filters it for the columns and rows necessary to calculate the bonus point results for all grade models, 
        and returns a single DataFrame that contains all of the bonus points results (SPED enrollment, Science Proficiency, ACT Aspire Percent Tested, 
        Subgroup 5-year Grad Rate) for each school included in the Static File provided, depending on the school's grade model.
    """
    def calculate_component(self, static_file:pd.DataFrame):
        """
        Parameters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (2022-2023 or beyond). It must contain the following columns: FiscalYear, 
            SchoolCode, SAISID, StudentGrade, FAY, Subject, Performance, ELAMathWindow, SciWindow, SPED, ScaleScore, AssessmentFamily,
            Alternative, Oct1Enroll, ADMIntegrity, StateModel.
 
        Returns:
        ----------
        pd.DataFrame: a dataframe containing all of the bonus points results for every grade model. When a type of bonus point is not considered for 
            a particular grade model, records corresponding to those grade models with have missing values.
        """
        filtered_static_file = static_file[static_file["FiscalYear"]==self.fiscal_year] # filter for the current fiscal year

        # calculate all bonus points
        sped_enrollment_bp = self.sped_enrollment_bonus_points(static_file=filtered_static_file).set_index(["SchoolCode", "Model"])
        science_prof_bp = self.science_prof_bonus_points(static_file=filtered_static_file).set_index(["SchoolCode", "Model"])
        act_aspire_bp = self.act_aspire_bonus_points(static_file=filtered_static_file).set_index(["SchoolCode", "Model"])
        subgroup_5yr_gr_bp = self.subgroup_5yr_gr_bonus_points(static_file=filtered_static_file).set_index(["SchoolCode", "Model"])

        # combine results
        all_bonus_points = pd.concat([sped_enrollment_bp, science_prof_bp, act_aspire_bp, subgroup_5yr_gr_bp], axis=1).reset_index(
            ).rename(columns={"SchoolCode":"EntityID"})
        
        ### Remove results for school-model pairs that don't exist in the static file. These unwanted results are introduced by the outer merges used throughout the module 
        # between schools present in the static file and statewide averages. We want every school to report statewide averages, even if they do not meet the eligibility requirements 
        # for any bonus points, so we outer merge schools in the static file with the statewide averages. If we did not do this, then a school may not have any records left after 
        # filtering for the eligibility requirements and therefore have no row in the results and not report the statewide averages.

        # get lists of K-8, 9-12, and alt 9-12 schools according to static file
        schools_k_8 = static_file[static_file["StateModel"]=="K8"]["SchoolCode"].unique()
        schools_9_12 = static_file[static_file["StateModel"]=="912"]["SchoolCode"].unique()
        schools_alt_9_12 = static_file[static_file["StateModel"]=="Alternative"]["SchoolCode"].unique()

        # drop bonus points results for those schools above where the state model does not match the static file state model
        results_to_drop = (all_bonus_points["EntityID"].isin(schools_k_8) & (all_bonus_points["Model"] != self.str_k8)) | (all_bonus_points["EntityID"].isin(schools_9_12) & 
            (all_bonus_points["Model"] != self.str_912)) | (all_bonus_points["EntityID"].isin(schools_alt_9_12) & (all_bonus_points["Model"] != self.str_alt_912))
        all_bonus_points = all_bonus_points[~results_to_drop]
        
        ### calculate total bonus points for each school-model ###
        # get schools that were not eligible for any bonus points
        all_bp_missing_mask = all_bonus_points[["SPBP", "SPBPPoints", "SCBP", "SCBPPoints", 
            "ACTAspireBPPoints", "GRFosterCarePoints", "GRMckineyVentoPoints", "GRSPEDPoints"]].isna().all(1)
        
        # put their total as NAN
        all_bonus_points.loc[all_bp_missing_mask, "TotalBonusPoints"] = np.nan

        # for school-models that were eligible for at least one bonus point category, sum across categories, ignoring NAN values
        all_bonus_points.loc[~all_bp_missing_mask, "TotalBonusPoints"] = all_bonus_points[["SPBP", "SPBPPoints", 
            "SCBP", "SCBPPoints", "ACTAspireBPPoints", "GRFosterCarePoints", "GRMckineyVentoPoints", 
            "GRSPEDPoints"]].fillna(0).sum(1)
        
        # Notes: - next year can remove SPBPPoints and SCBPPoints from above once SQL column names are made consistent across models
        #        - This total does not include the CCRI bonus point. That will be added on later.

        # make a fiscalyear col
        all_bonus_points['FiscalYear'] = self.fiscal_year
        return all_bonus_points
 
    """
    Follows the 2022-2023 School Year Business Rules. Calculates the number of SPED students enrolled and the number of FAY students enrolled at every school. 
        Schools that have less than 10 SPED students enrolled are not eligible for bonus points. With the school-models that are eligible, the 
        "SPED Enrollment Percentage" is calculated by dividing the number SPED enrolled by the number FAY enrolled. A statewide average is calculated from 
        these percentages for each grade model (K-8, 9-12). Bonus points are then assigned based on how a school-model's SPED enrollment percentage compares 
        to the statewide average. In the 2023-2024 School Year, we intend to use enrollment on Oct 1 instead of FAY enrollment in the calculation.
    """
    def sped_enrollment_bonus_points(self, static_file:pd.DataFrame, use_fay:bool = True):
        """
        Parameters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (2022-2023 or beyond). It must contain the following columns: FiscalYear, 
            SchoolCode, SAISID, StudentGrade, FAY, ELAMathWindow, SPED, Alternative, Oct1Enroll, ADMIntegrity.

        use_fay (bool): determines whether to use FAY enrollment or enrollment on Oct 1 in calculation.

        Returns:
        ----------
        pd.DataFrame: a dataframe that contains SchoolCode, Model, SPED Enrollment Percent, some statewide average info, and SPED Enrollment bonus points earned

        """
        # filter for the relevant columns and traditional school records, no sped-enrollment BP for alternative schools
        filtered_static_file = self._select_columns(static_file, ["FiscalYear", "SchoolCode", "SAISID", "FAY", "SPED", 
            "StudentGrade", "Cohort", "Alternative", "Oct1Enroll", "ADMIntegrity"])
        
        # filter for integrity and traditional records
        filtered_static_file = filtered_static_file[(filtered_static_file["Alternative"]==0) & (filtered_static_file["ADMIntegrity"]==1)]

        # keep track of schools that should have something reported, even if they are ineligible
        schools_to_include = filtered_static_file[["SchoolCode"]].drop_duplicates()

        # reassign high school grades based on cohort
        filtered_static_file = self.correct_high_school_grades(filtered_static_file)

        for grade_model in self.sped_enrollment_grade_map.keys(): # add model type to dataframe for future groupby operations
            filtered_static_file.loc[filtered_static_file["StudentGrade"].isin(self.sped_enrollment_grade_map[grade_model]), "Model"] = grade_model

        # remove grades not included in SPED enrollment bonus points calculation
        filtered_static_file = filtered_static_file[filtered_static_file["Model"].notnull()]

        if use_fay: # using this method for 2022-2023 School Year
            # count number of eligible sped students enrolled on october 1 at each school
            filtered_for_sped = filtered_static_file[filtered_static_file["SPED"]==1].groupby(
                by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_SPED"})

            # count number of FAY students enrolled at each school
            filtered_for_fay = filtered_static_file[filtered_static_file["FAY"]>0].groupby(
                by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_FAY"})

            # combine number eligible sped and number fay and calculate percentage sped enrolled at each school
            merged_sped_fay = pd.merge(filtered_for_sped, filtered_for_fay, on=["SchoolCode", "Model"], how="outer").fillna(0)
            merged_sped_fay["SPED"] = np.minimum(100,merged_sped_fay["Number_SPED"]/merged_sped_fay["Number_FAY"]*100)
            merged_sped_fay.loc[merged_sped_fay["Number_FAY"]==0, "SPED"] = np.nan

            # calculate statewide average sped enrollment
            sped_statewide_results = merged_sped_fay[merged_sped_fay["Number_SPED"]>=self.n_count].groupby(by=["Model"]).agg(
                **{"Statewide_Average":pd.NamedAgg(column="SPED", aggfunc="mean")}).reset_index()
            sped_statewide_results.loc[:,["SpclEduBP60PctSA", "SpclEduBP70PctSA", "SpclEduBP80PctSA"]] = pd.concat(
                [0.6*sped_statewide_results["Statewide_Average"], 0.7*sped_statewide_results["Statewide_Average"], 
                0.8*sped_statewide_results["Statewide_Average"]], axis=1).values

            # full outer join schools and statewide averages so that every school reports statewide average, regardless of eligibility
            sped_statewide_results["TEMP"] = 0
            schools_to_include["TEMP"]=0
            sped_statewide_results = pd.merge(schools_to_include, sped_statewide_results, on=["TEMP"]).drop(columns=["TEMP"])

            # combine school-level and statewide sped enrollment
            sped_results = pd.merge(merged_sped_fay, sped_statewide_results, on=["SchoolCode", "Model"], how="outer")

            # calculate sped bonus points earned
            conditions = [sped_results["SPED"] >= 0.8*sped_results["Statewide_Average"], 
                (sped_results["SPED"] >= 0.7*sped_results["Statewide_Average"]) & (sped_results["SPED"] < 0.8*sped_results["Statewide_Average"]), 
                (sped_results["SPED"] >= 0.6*sped_results["Statewide_Average"]) & (sped_results["SPED"] < 0.7*sped_results["Statewide_Average"]), 
                sped_results["SPED"] < 0.6*sped_results["Statewide_Average"]
            ]
            sped_results["SPBP"] = np.select(condlist=conditions, choicelist=[2, 1.5, 1, 0], default=np.nan)
            sped_results.loc[(sped_results["Number_SPED"]<=self.n_count) | (sped_results["Number_FAY"]==0), "SPBP"] = np.nan # not eligible

            # rename columns to match desired output
            sped_results = sped_results.rename(columns={"Statewide_Average":"SpclEduBPSA", "SPED":"SpclEduBonusPct"}).drop(columns=["Number_SPED", "Number_FAY"])

        # Other way is same as above but replace FAY with enrolled on Oct 1. -- this will be used for the 2023-2024 school year
        else:
            # count number of eligible sped students enrolled on october 1 at each school
            filtered_for_sped = filtered_static_file[filtered_static_file["SPED"]==1].groupby(
                by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_SPED"})

            # count number of students enrolled on Oct 1 at each school
            filtered_for_fay = filtered_static_file[filtered_static_file["Oct1Enroll"]==1].groupby(
                by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_Enrolled"})

            # combine number eligible sped and number enrolled on Oct 1 and calculate percentage sped enrolled at each school
            merged_sped_fay = pd.merge(filtered_for_sped, filtered_for_fay, on=["SchoolCode", "Model"], how="outer").fillna(0)
            merged_sped_fay["SPED"] = np.minimum(100,merged_sped_fay["Number_SPED"]/merged_sped_fay["Number_Enrolled"]*100)
            merged_sped_fay.loc[merged_sped_fay["Number_Enrolled"]==0, "SPED"] = np.nan

            # calculate statewide average sped enrollment, ignoring nan values
            sped_statewide_results = merged_sped_fay.groupby(by=["Model"]).agg({"Number_SPED":"sum", "Number_Enrolled":"sum"}).reset_index()
            sped_statewide_results["Statewide_Average"] = sped_statewide_results["Number_SPED"]/sped_statewide_results["Number_Enrolled"]*100
            sped_statewide_results.loc[:,["SpclEduBP60PctSA", "SpclEduBP70PctSA", "SpclEduBP80PctSA"]] = pd.concat(
                [0.6*sped_statewide_results["Statewide_Average"], 0.7*sped_statewide_results["Statewide_Average"], 
                0.8*sped_statewide_results["Statewide_Average"]], axis=1).values

            # full outer join schools and statewide averages so that every school is included reports statewide average, regardless of eligibility
            sped_statewide_results["TEMP"] = 0
            schools_to_include["TEMP"]=0
            sped_statewide_results = pd.merge(schools_to_include, sped_statewide_results, on=["TEMP"]).drop(columns=["TEMP"])

            # combine school-level and statewide sped enrollment
            sped_results = pd.merge(merged_sped_fay, sped_statewide_results.drop(columns=["Number_SPED", "Number_Enrolled"]), 
                on=["SchoolCode", "Model"], how="outer")

            # calculate sped inclusion bonus points earned
            conditions = [sped_results["SPED"] >= 0.8*sped_results["Statewide_Average"], 
                (sped_results["SPED"] >= 0.7*sped_results["Statewide_Average"]) & (sped_results["SPED"] < 0.8*sped_results["Statewide_Average"]), 
                (sped_results["SPED"] >= 0.6*sped_results["Statewide_Average"]) & (sped_results["SPED"] < 0.7*sped_results["Statewide_Average"]), 
                sped_results["SPED"] < 0.6*sped_results["Statewide_Average"]
            ]
            sped_results["SPBP"] = np.select(condlist=conditions, choicelist=[2, 1.5, 1, 0])
            sped_results.loc[(sped_results["Number_SPED"]<=self.n_count) | (sped_results["Number_Enrolled"]==0), "SPBP"] = np.nan # not eligible

            # rename columns to match desired output
            sped_results = sped_results.rename(columns={"Statewide_Average":"SpclEduBPSA", "SPED":"SpclEduBonusPct"}).drop(columns=["Number_SPED", "Number_Enrolled"])

        # temporary fix to match column names between K-8 and 9-12 results
        sped_results.loc[sped_results["Model"]==self.str_912, ["SpclEduBPPct", "SPBPPoints"]] = sped_results[
            sped_results["Model"]==self.str_912][["SpclEduBonusPct", "SPBP"]].values
        sped_results.loc[sped_results["Model"]==self.str_912, ["SpclEduBonusPct", "SPBP"]] = [np.nan, np.nan]

        return sped_results


    """
    Following the 2022-2023 School Year Business Rules, this method calculates "Percent Proficient" and "Percent Tested" values on the Science 
    assessments for every school-model. The "Percent Proficient" is a weighted average of the proficiency levels (on Science) and the "Percent Tested" 
    is determined by the percentage of students enrolled during the testing window (for Science) that were actually tested. A school-model must have
    meet or exceed 95% tested in order to be eligible for these bonus points (we round to the nearest integer to help schools that are extremely close to 95%). 
    A statewide average "Percent Proficient" is calculated for each model by averaging all of the eligible schools' "Percent Proficient" values. 
    Points are then awarded to eligible school-models based on how their "Percent Proficient" value compares to the statewide average for that model.
    """
    def science_prof_bonus_points(self, static_file:pd.DataFrame):
        """
        Paramters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (2022-2023 or beyond). It must contain the following columns: 
            FiscalYear, SchoolCode, SAISID, StudentGrade, Cohort, SciWindow, Subject, Performance, FAY, Alternative, and ADMIntegrity.

        Returns:
        ----------
        pd.DataFrame: a dataframe containing SchoolCode, Model, the Percent Proficient on Science, the Percent Tested on Science, 
            the statewide average Percent Proficient on Science, and the Science Proficiency bonus points awared
        """
        # filter for relevant columns and traditional school records. Alternative schools do not qualify for these bonus points
        filtered_static_file = self._select_columns(static_file, ["FiscalYear", "SchoolCode", "SAISID", "SciWindow", "FAY",
            "Performance", "StudentGrade", "Cohort", "Subject", "Alternative", "ADMIntegrity"])
        
        # filter for integrity and traditional records
        filtered_static_file = filtered_static_file[(filtered_static_file["Alternative"]==0) & (filtered_static_file["ADMIntegrity"]==1)]

        # keep track of schools that should report the statewide average in the final gui table, even if they are ineligible
        schools_to_include = filtered_static_file[["SchoolCode"]].drop_duplicates()

        # reassign high school grades based on Cohort
        filtered_static_file = self.correct_high_school_grades(static_file=filtered_static_file)

        # add model type to dataframe for future groupby operations
        for grade_model in self.science_proficiency_grade_map.keys(): 
            filtered_static_file.loc[filtered_static_file["StudentGrade"].isin(self.science_proficiency_grade_map[grade_model]), "Model"] = grade_model

        # remove grades not included in science proficiency bonus points calculation
        filtered_static_file = filtered_static_file[filtered_static_file["Model"].notnull()]

        # count number of students enrolled during science testing window at each school-model
        num_enrolled = filtered_static_file[filtered_static_file["SciWindow"]==1].groupby(
            by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_Enrolled"})

        # count number of students tested on science at each school-model
        num_tested = filtered_static_file[(filtered_static_file["Subject"]=="Sci") & (~filtered_static_file["Performance"].isna())].groupby(
            by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_Tested"})

        # combine number enrolled and number tested, then calculate percent tested at each school-model
        percent_tested_info = pd.merge(num_enrolled, num_tested, on=["SchoolCode", "Model"], how="outer")
        percent_tested_info["Percent_Tested"] = percent_tested_info["Number_Tested"]/percent_tested_info["Number_Enrolled"]*100

        # get counts of each proficiency level on Sci assessment at each school-model. Records must be FAY to be included.
        fay_tested = filtered_static_file[(filtered_static_file["Subject"]=="Sci") & (filtered_static_file["FAY"]>0) & 
            (~filtered_static_file["Performance"].isna())] 
        
        # filter for students with valid science test records
        proficiency_results = fay_tested.groupby(by=["SchoolCode", "Model"])["Performance"].value_counts().unstack().reset_index().rename(
            columns={key:f"P{int(key)}" for key in fay_tested["Performance"].unique()}).fillna(0)

        # calculate percent proficient at each school-model
        proficiency_results["Percent_Proficient"] = (proficiency_results["P3"]+proficiency_results["P4"])/(
            proficiency_results["P1"]+proficiency_results["P2"]+proficiency_results["P3"]+proficiency_results["P4"])*100
        proficiency_results = pd.merge(proficiency_results, percent_tested_info, on=["SchoolCode", "Model"], how="outer")

        # calculate statewide average percent proficient, as the mean of eligible (>= 95% tested) school-models' percent proficient
        sci_prof_statewide_results = proficiency_results[proficiency_results["Percent_Tested"].round(0)>=95].groupby(by="Model").agg(
            **{"Statewide_Average":pd.NamedAgg(column="Percent_Proficient", aggfunc="mean"), 
                "Statewide_Std":pd.NamedAgg(column="Percent_Proficient", aggfunc="std")}).reset_index()

        # full outer join traditioal schools from the static file and statewide averages so that every traditional school reports the statewide averages
        sci_prof_statewide_results["TEMP"] = 0
        schools_to_include["TEMP"]=0
        sci_prof_statewide_results = pd.merge(schools_to_include, sci_prof_statewide_results, on=["TEMP"]).drop(columns=["TEMP"])

        # merge each school-model proficiency info with the statewide results
        proficiency_results = pd.merge(proficiency_results, sci_prof_statewide_results, on=["SchoolCode", "Model"], how="outer")

        # assign points according to the business rules
        conditions = [
            proficiency_results["Percent_Proficient"] >= proficiency_results["Statewide_Average"] + proficiency_results["Statewide_Std"],
            (proficiency_results["Percent_Proficient"] > proficiency_results["Statewide_Average"]) & (
                proficiency_results["Percent_Proficient"] < proficiency_results["Statewide_Average"] + proficiency_results["Statewide_Std"]),
            proficiency_results["Percent_Proficient"] < proficiency_results["Statewide_Average"]
        ]
        proficiency_results["SCBP"] = np.select(condlist=conditions, choicelist=[3,1.5,0])

        # replace non-eligible school-model points with nan
        proficiency_results.loc[proficiency_results["Percent_Tested"].fillna(0).round(0)<self.percent_tested_expected,"SCBP"] = np.nan # not eligible

        # rename columns to match destination SQL table
        proficiency_results = proficiency_results[["SchoolCode", "Model", "Percent_Proficient", "Percent_Tested", "Statewide_Average", "SCBP"]].rename(
            columns={"Percent_Proficient":"SciAssmtBonusPct", "Percent_Tested":"SciPctTested", "Statewide_Average":"SciAssmtBPSA"})
        
        # temporary fix to match column names between K-8 and 9-12 results
        proficiency_results.loc[proficiency_results["Model"]==self.str_912, ["SciAssmtBPPct", "SCBPPoints"]] = proficiency_results[
            proficiency_results["Model"]==self.str_912][["SciAssmtBonusPct", "SCBP"]].values
        proficiency_results.loc[proficiency_results["Model"]==self.str_912, ["SciAssmtBonusPct", "SCBP"]] = [np.nan, np.nan]
        
        # add some columns that are expected but contain all missing values
        # proficiency_results[['SciAssmtBP60PctSA', 'SciAssmtBP70PctSA', 'SciAssmtBP80PctSA']] = [np.nan, np.nan, np.nan]
        return proficiency_results

    """
    Following the 2022-2023 School Year Business Rules, this method calculates ACTAspire bonus points for traditional and alternative 9-12 schools 
    based on their participation on the ACTAspire assessment. For each school-model, a "Percent Tested" value is calculated from the number 
    of 9th graders (determined by Cohort year) that took both the Math and ELA porion of the ACTAspire assessment, divided by the number of 
    students that were enrolled during the testing window. Bonus points are awarded based on whether a school-model met or exceeded 90% tested 
    and 95% tested on the ACTAspire assessment.
        
    Changes to make in 2023-2024: 
    - make sure column names match between traditional 9-12 and alt 9-12 results
    """
    def act_aspire_bonus_points(self, static_file:pd.DataFrame):
        """
        Parameters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (2022-2023 or beyond). It must contain the following columns: 
            FiscalYear, SchoolCode, SAISID, StudentGrade, Cohort, ELAMathWindow, Subject, ScaleScore, Alternative, and ADMIntegrity.

        Returns:
        ----------
        pd.DataFrame: a dataframe containing SchoolCode, Model, the Percent Tested on ACTAspire, and the ACTAspire Participation bonus 
            points awarded
        """
        # select the columns that will be used to calculate points
        filtered_static_file = self._select_columns(static_file, ["FiscalYear", "SchoolCode", "SAISID", "ELAMathWindow", 
            "ScaleScore", "StudentGrade", "Cohort", "Subject", "AssessmentFamily", "Alternative", "ADMIntegrity"])
        
        # filter for integrity and traditional records
        filtered_static_file = filtered_static_file[(filtered_static_file["ADMIntegrity"]==1)]

        # reassign high school grades based on Cohort
        filtered_static_file = self.correct_high_school_grades(static_file=filtered_static_file)

        # assign model type
        alternative_mask = filtered_static_file["Alternative"]==1
        filtered_static_file = filtered_static_file[filtered_static_file["StudentGrade"]==self.act_aspire_grade]
        filtered_static_file.loc[~alternative_mask, "Model"] = self.str_912
        filtered_static_file.loc[alternative_mask, "Model"] = self.str_alt_912

        # Filter for valid ACT Aspire assessment family (not including MASS according to business rules)
        filtered_static_file = filtered_static_file[(filtered_static_file["AssessmentFamily"].isin(self.act_aspire_assessment_families))]

        # keep track of schools that should have something reported, even if they are ineligible
        schools_to_include = filtered_static_file[["SchoolCode", "Model"]].drop_duplicates()

        # count number of students enrolled during the Math/ELA assessment window
        num_enrolled = filtered_static_file[filtered_static_file["ELAMathWindow"]==1].groupby(
            by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_Enrolled"})

        # Filter for valid test records and pivot to wide so that each student has a Math and ELA assessment result.
        num_tested = filtered_static_file[(filtered_static_file["Subject"].isin(self.act_aspire_subjects)) & 
            (~filtered_static_file["ScaleScore"].isna()) & (filtered_static_file["ELAMathWindow"]==1)].pivot(
            columns=["Subject"], index=["SchoolCode", "Model", "SAISID"], values=["ScaleScore"]) # using ScaleScore because Performance is missing for act aspire

        # keep student records that have both valid math and ela test records
        num_tested = num_tested[num_tested.loc[:,("ScaleScore", slice(None))].notnull().all(axis=1)].reset_index()

        # count number of students tested at each school
        num_tested = num_tested.groupby(by=["SchoolCode", "Model"])["SAISID"].nunique().reset_index().rename(columns={"SAISID":"Number_Tested"})

        # combine number enrolled and number tested to calculate percent tested at each school
        act_aspire_participation = pd.merge(num_enrolled, num_tested, on=["SchoolCode", "Model"], how="outer").fillna(0)
        act_aspire_participation["Percent_Tested"] = act_aspire_participation["Number_Tested"]/act_aspire_participation["Number_Enrolled"]*100

        # assign points based on the percent tested at each school
        conditions = [
            act_aspire_participation["Percent_Tested"] >= 95, 
            (act_aspire_participation["Percent_Tested"] < 95) & (act_aspire_participation["Percent_Tested"] >= 90)
        ]
        act_aspire_participation["ACTAspireBPPoints"] = np.select(condlist=conditions, choicelist=[3,1.5], default=0)

        # School-models that have zero students enrolled during testing window are not eligible for points. This avoids dividing by zero.
        act_aspire_participation.loc[act_aspire_participation["Number_Enrolled"]<1, "ACTAspireBPPoints"] = np.nan

        # add back in schools that were dropped when filtering for enrolled on testing window
        act_aspire_participation = pd.merge(act_aspire_participation, schools_to_include, on=["SchoolCode", "Model"], how="outer")
        
        act_aspire_participation = act_aspire_participation[["SchoolCode", "Model", "Percent_Tested",
                "ACTAspireBPPoints"]].rename(columns={"Percent_Tested":"ACTAspireBPPct"})

        # IT wants to have the following columns included to help build the GUI tables, but they all appear blank so just fill with NAN
        # act_aspire_participation[['ACTAspireBP60PctSA', 'ACTAspireBP70PctSA', 'ACTAspireBP80PctSA', 'ACTAspireBPSA']] = [np.nan, np.nan, np.nan, np.nan]

        return act_aspire_participation
        
        

    """
    Following the 2022-2023 Business Rules, this method calculates the Five-Year Subgrouop Graduation Rate bonus points for alternative schools. 
    Graduation rates are based on Cohort and are obtained from Accountability SQL database. A school-model-subgroup must have at least 10 students 
    in its 5-year cohort to be eligible for these bonus points. For each subgroup, a statewide average 5-year graduation rate is calculated as the 
    average of the eligible 5-year grad rates for that subgroup. Bonus points are then awarded based on how a school-model-subgroup's 5-year grad 
    rate compares to the statewide average.
    """
    def subgroup_5yr_gr_bonus_points(self, static_file:pd.DataFrame):
        """
        Parameters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (2022-2023 or beyond). It must contain the following columns: 
            SchoolCode, Alternative. This is not used to calculate 5-year grad rates. It is used to identify schools to report.

        Returns:
        ----------
        pd.DataFrame: a dataframe containing SchoolCode, Model, the 5-year grad rate for Homeless, Foster Care, and SPED Cohorts, 
            the statewide average 5-year grade rate for each subgroup, and the bonus points awarded for each subgroup.
        """
        alternative_schools = static_file[static_file["Alternative"]==1]["SchoolCode"].drop_duplicates()

        db = DATABASE(self.fiscal_year)
        sql = f"""
            SELECT EntityID AS SchoolCode, Type, GradRate 
            FROM [AccountabilityArchive].[Static].[{self.run}GradRate{self.fiscal_year}]
            WHERE CohortYear = {self.fiscal_year-2} AND IsFiveYear = 1 AND NumCohort >= 10 
            AND Type IN ('Homeless Cohort', 'Foster Care Cohort', 'SPED Cohort') AND EntityID IN {tuple(alternative_schools)}
        """
        grad_rates = db.read_sql_query(sql=sql)

        # calculate statewide average grad rate for each subgroup
        grad_rates["SA"] = grad_rates.groupby(by=["Type"])["GradRate"].transform('mean').round(2)
        grad_rates["80PctSA"] = (grad_rates["SA"]*0.8).round(2)
        grad_rates["Points"] = np.select(condlist=[grad_rates["GradRate"]>=grad_rates["80PctSA"]], choicelist=[2], default=0)

        # pivot to wide so that there is one record per school
        grad_rates = grad_rates.pivot(columns=["Type"], index=["SchoolCode"])

        # change column names to match desired output
        columns = grad_rates.columns.to_frame()
        columns = columns.replace({"GradRate":"SchoolPct", "Homeless":"MckineyVento", "Cohort":"", " ":""}, regex=True)
        columns = "GR" + columns.iloc[:,1] + columns.iloc[:,0]
        grad_rates.columns = columns
        grad_rates = grad_rates.reset_index()

        # add back in ineligible schools that were dropped
        grad_rates = pd.merge(grad_rates, alternative_schools.to_frame(), on=["SchoolCode"], how="outer")

        # fill missing state average values 
        state_average_values = grad_rates.loc[:, ["GRFosterCareSA", "GRMckineyVentoSA", "GRSPEDSA", "GRFosterCare80PctSA", 
            "GRMckineyVento80PctSA", "GRSPED80PctSA"]].mean(0)
        grad_rates.fillna(value = state_average_values, inplace=True)
        grad_rates["Model"] = self.str_alt_912

        return grad_rates
    

    """
    Checks that the static file provided contains the columns provided. This is used at the beginning of methods 
        throughout this module to ensure that the static file is properly formatted.
    """
    def _select_columns(self, df:pd.DataFrame, columns:list):
        # check that static file contains the necessary columns:
        if not set(columns).issubset(df.columns):
            raise KeyError("The following columns are missing from the static file provided: {}".format(set(columns).difference(df.columns)))
        
        # select the desired columns and rename some for convenience
        return df[columns]
