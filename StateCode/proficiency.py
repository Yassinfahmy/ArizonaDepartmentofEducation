"""
Authors: ADE Accountability & Research

Last Updated: 05/01/2023

Description: This Python module contains a Proficiency class capable of producing the results from the Proficiency component of the Arizona State A-F Letter Grades 
(https://www.azed.gov/accountability-research/state-accountability). In order to utilize the method of the Proficiency class, one will need to provide a version of 
the Accountability Static File ().
"""

import pandas as pd
import numpy as np
from COMPONENTS import COMPONENTS

"""
This class contains methods for creating Pandas DataFrames that contain the Summary and Drilldown information present on ADEConnect. To find this information, navigate to 
    adeconnect.azed.gov/ -> View Applications -> Accountability -> Accountability: State and Federal Profile.
"""
class PROFICIENCY(COMPONENTS):
    """
    Constructor for Proficiency. This class inherits from COMPONENTS, which contains variables and functionality that is shared across the different A-F Letter Grades Components.
    """
    def __init__(self,  fiscal_year:int=None, **kwargs):
        """
        Parameters:
        ----------
        fiscal_year (int): The current AZ D.O.E. fiscal year
        """
        super().__init__(fiscal_year=fiscal_year, **kwargs)
        self.str_k8 = "k-8"
        self.str_912 = "9-12"
        self.str_alt_912 = "Alt 9-12"
        self.necessary_columns = ["FiscalYear", "SchoolCode", "SAISID", "StudentGrade", "FAY", "Subject", "Performance", "TestType", "ELAMathWindow", "RAEL", "Alternative", "ADMIntegrity"]
        self.static_file_column_changes = {"StudentGrade":"Grade"}
        self.subjects_used = ["Math", "ELA"]
        self.rael_values_to_ignore = [1,2]
        self.fay_stability_weight_map = {1:{1:30}, 2:{1:12,2:18}, 3:{1:5, 2:10, 3:15}} # has format {num_fay_groups:{yrs_fay:weight, }, }
        
    

    """
    This method takes a version of the Static File, filters it for the columns and rows necessary to calculate the results of the Proficiency component, 
        and produces a single dataframe that contains all of the Proficiency results (both summary and drilldown) for each school included in the Static File provided
    """
    def calculate_component(self, static_file:pd.DataFrame, separate_tables:bool = False):
        """
        Parameters:
        ----------
        static_file (pd.DataFrame): A version of the Static File (FY 2023 or beyond). It must contain the following columns: FiscalYear, SchoolCode, SAISID, StudentGrade, FAY, Subject, 
            Performance, TestType, ELAMathWindow, RAEL, and Cohort. 

        Returns:
        ----------
        pd.DataFrame: A dataframe containing all of the Proficiency information that is shown in the ADEConnect State Letter Grades summary and drilldown tables. Columns contain
        suffixes that denote the page on ADEConnect in which they appear
        """
        # check that the static file provided has the necessary columns for Proficiency calculations
        if not set(self.necessary_columns).issubset(static_file.columns): 
            missing_columns = [x for x in self.necessary_columns if x not in static_file.columns]
            raise ValueError(f"The DataFrame argument \"static_file\" is missing the following columns: {missing_columns}.")

        # reassign high school grades based on Cohort
        filtered_static_file = self.correct_high_school_grades(static_file=static_file)

        # filter for correct year and records that pass integrity
        filtered_static_file = filtered_static_file[self.necessary_columns].rename(columns=self.static_file_column_changes) #.query(f"`FiscalYear` == {self.fiscal_year} and `ADMIntegrity` == 1")

        filtered_static_file = filtered_static_file[(filtered_static_file["FiscalYear"]==self.fiscal_year) & (filtered_static_file["ADMIntegrity"]==1)]
        
        # assign model based on alternative status and grade in the static file
        alt_mask = filtered_static_file["Alternative"]==1
        for grade_model, grades in self.proficiency_grade_map["Trad"].items(): # add model type to dataframe for future groupby operations
            filtered_static_file.loc[(~alt_mask) & (filtered_static_file["Grade"].isin(grades)), "Model"] = grade_model
        for grade_model, grades in self.proficiency_grade_map["Alt"].items(): # add model type to dataframe for future groupby operations
            filtered_static_file.loc[(alt_mask) & (filtered_static_file["Grade"].isin(grades)), "Model"] = grade_model

        # remove records with grades that are not included in the Proficiency component
        filtered_static_file = filtered_static_file[filtered_static_file["Model"].notnull()]

        # calculate percent tested, 95% tested multiplier, and number FAY for every school
        percent_tested_info = self._calculate_percent_tested(filtered_static_file) # produce counts of students enrolled in each grade at each school
        
        # filter for fay and tested in math or ela. Also drop ELA assessments for RAEL students
        # fay_tested_static = filtered_static_file.query(f"`FAY` > 0 and `Subject` in {tuple(self.subjects_used)} and `Performance`.notna() and not (`Subject` == 'ELA' and RAEL in (1,2))")

        fay_tested_static = filtered_static_file[(filtered_static_file["FAY"]>0) & (filtered_static_file["Subject"].isin(self.subjects_used)) & 
            (~filtered_static_file["Performance"].isna()) & ~((filtered_static_file["Subject"]=="ELA") & (filtered_static_file["RAEL"].isin([1,2])))]

        # produce counts of each proficiency level for each grade-testtype-subject at every school
        proficiencies_by_grade = self._agg_by_grade(fay_tested_static) 

        # produce counts of each proficiency level for each FAY-year group at every school (only uses K-8)
        proficiencies_by_fay = self._agg_by_fay(fay_tested_static) 
        
        # produce proficiency data for each grade and each fay-level at each school
        proficiency_data = self._agg_by_grade_and_fay(proficiencies_by_grade, proficiencies_by_fay).reset_index(drop=True)
        proficiency_data.columns.name = None

        # HERE NEED TO change num to Number 
        # produce drilldown gui tables
        drilldown_tables = self.create_drilldown_tables(proficiency_data)

        # produce summary gui tables
        summary_tables = self.create_summary_tables(proficiency_data, percent_tested_info)

        # merge drilldown and summary data
        proficiency_gui = pd.merge(drilldown_tables, summary_tables, on=["EntityID_All", "Model_All"])

        proficiency_gui["FiscalYear_All"] = self.fiscal_year

        if not separate_tables: return proficiency_gui

        drilldown_k_8 = self.pull_out_k_8_drilldown( proficiency_gui)
        summary_k_8 = self.pull_out_k_8_summary( proficiency_gui)
        drilldown_9_12 = self.pull_out_9_12_drilldown(proficiency_gui)
        summary_9_12 = self.pull_out_9_12_summary(proficiency_gui)
        alt_summary_9_12 = self.pull_out_alt_9_12_summary(proficiency_gui)
        return drilldown_k_8, summary_k_8, drilldown_9_12, summary_9_12, alt_summary_9_12


    """
    This is a protected method that takes a reduced version of the static file and calculates the number of students who were FAY and who were assessed at each school.
    """
    def _calculate_percent_tested(self, filtered_static_file:pd.DataFrame):
        """
        Parameters:
        ----------
        filtered_static_file (pd.DataFrame): A reduced version of the Static File

        Returns:
        ----------
        pd.DataFrame: a dataframe containing the number of FAY students and the percentage of enrolled students that were assessed at each school
        """
        # filter for students that were enrolled during the testing window
        # static_enrolled = filtered_static_file.query("ELAMathWindow == 1") 
        static_enrolled = filtered_static_file[filtered_static_file["ELAMathWindow"]==1]

        # get counts of students enrolled at each school-grade model. This is used in the denominator of the 95% tested multiplier
        enrollment_counts = static_enrolled.groupby(by=["SchoolCode", "Model"]).agg({"SAISID":"nunique"}).reset_index().rename(columns={"SAISID":"NumberEnrolled"})

        # get counts of students tested at each school-grade model. This is used in the numerator of the 95% tested multiplier
        tested_counts = static_enrolled[~(static_enrolled["Performance"].isna()) & (static_enrolled["Subject"].isin(self.subjects_used))].groupby(by=["SchoolCode", "Model"]).agg(
            {"SAISID":"count"}).reset_index().rename(columns={"SAISID":"NumberTested"})
        tested_counts["NumberTested"] = tested_counts["NumberTested"] / 2 # divide number of assessments by 2 to account for two expected assessments per student

        # get number of FAY students at each school-grade model. This is used to determine which schools are eligible for Proficiency points (N-count)
        fay_counts = filtered_static_file[(filtered_static_file["FAY"]>0) & ~((filtered_static_file["Subject"]=="ELA") & (filtered_static_file["RAEL"].isin(self.rael_values_to_ignore)))].groupby(
            by=["SchoolCode", "Model"]).agg({"SAISID":"nunique"}).reset_index().rename(columns={"SAISID":"NumberFAY"})

        # combine the information calculated above
        percent_tested_info = pd.merge(enrollment_counts, tested_counts, on=["SchoolCode", "Model"]).merge(fay_counts, on=["SchoolCode", "Model"]) 

        percent_tested_info["PercentTested"] = percent_tested_info["NumberTested"] / percent_tested_info["NumberEnrolled"] * 100 # calculate the percent tested for each school-grade model
        percent_tested_info["PercentMultiplier"] = percent_tested_info["NumberTested"] / (self.percent_tested_expected/100*percent_tested_info["NumberEnrolled"]) # calculate the percent multiplier for each school-grade model
        return percent_tested_info[["SchoolCode", "Model", "NumberFAY", "PercentTested", "PercentMultiplier"]]


    """
    A protected method to compute Proficiency level results for each grade-testtype-subject at every school
    """
    def _agg_by_grade(self, fay_tested_static:pd.DataFrame):
        """
        Parameters:
        ----------
        fay_tested_static (pd.DataFrame): A version of the Static File that has been filtered for students who were FAY and who took the Math or ELA assessments

        Returns:
        ----------
        pd.DataFrame: a dataframe containing proficiency level counts, number tested, and an average percent proficient score for each grade at each school
        """
        # group by school, assessment type, subject, and grade to obtain counts of each proficiency level
        proficiencies_by_grade = fay_tested_static.groupby(by=["SchoolCode", "Model", "TestType", "Subject", "Grade"])["Performance"].value_counts().unstack().reset_index().rename(
            columns={key:f"P{int(key)}" for key in fay_tested_static["Performance"].unique()}).fillna(0)
        
        # a reference to the new columns created above, containing counts of performance levels
        performance_columns = proficiencies_by_grade.columns[~proficiencies_by_grade.columns.isin(fay_tested_static.columns)] 

        # sum the counts to produce number of fay students tested at each school-grade level
        proficiencies_by_grade["NumberTested"] = proficiencies_by_grade[list(performance_columns)].sum(axis=1) 
        
        # aggregate over school, subject, and grade to obtain proficiency counts for all assessment types
        all_tests_p = proficiencies_by_grade.groupby(by=["SchoolCode", "Model", "Subject", "Grade"]).sum(numeric_only = True).reset_index()
        all_tests_p["TestType"] = "All"

        # combine aggregations calculated above
        proficiencies_by_grade = pd.concat([proficiencies_by_grade, all_tests_p], axis=0) 

        return proficiencies_by_grade
        

    """
    This protected method computes the Proficiency level results for each FAY-year group (across all grades included in the K-8 model) at each school
    """
    def _agg_by_fay(self, fay_tested_static):
        """
        Parameters:
        ----------
        fay_tested_static (pd.DataFrame): a dataframe containing proficiency level counts, number tested, and an average percent proficient score for each grade at each school

        Returns:
        ----------
        pd.DataFrame: a dataframe containing proficiency level counts, number tested, and an average percent proficient score for each FAY-year group at each school (only 
            includes students in the K-8 model)
        """
        # filter for grades included in K-8 component calculations
        fay_tested_static_k8 = fay_tested_static[(fay_tested_static["Model"]==self.str_k8)] 
        if fay_tested_static_k8.empty: return fay_tested_static_k8 # skip if there are no K-8 records

        # for all students with FAY > 0, combine subjects and change fay values so that each tested FAY group meets the n-count
        combined_subjects = fay_tested_static_k8.copy()
        combined_subjects["NumberFAYTested"] = combined_subjects.groupby(by=["SchoolCode", "FAY"])["SAISID"].transform("count")

        # combine 3 & 2 year FAY-level counts when the 3-year FAY is less than the n_count
        combined_subjects.loc[(~(combined_subjects["NumberFAYTested"] >= (self.n_count*2))) & (combined_subjects["FAY"]==3), "FAY"] = 2
        combined_subjects["NumberFAYTested"] = combined_subjects.groupby(by=["SchoolCode", "FAY"])["SAISID"].transform("count")

        # combine 2 & 1 year FAY-level counts when the resulting 2-year FAY is less than the n_count
        combined_subjects.loc[(~(combined_subjects["NumberFAYTested"] >= (self.n_count*2))) & (combined_subjects["FAY"]==2), "FAY"] = 1
        combined_subjects["NumberFAYTested"] = combined_subjects.groupby(by=["SchoolCode", "FAY"])["SAISID"].transform("count")

        # aggregate by school and model to get counts of groups that meet the n-count
        combined_subjects["FAYGroups"] = combined_subjects.groupby(by=["SchoolCode"])["FAY"].transform(lambda x: sum(x.value_counts()>=(self.n_count*2)))
        combined_subjects["FAY_MAX"] = combined_subjects.groupby(by=["SchoolCode"])["FAY"].transform("max")

        temp_df = combined_subjects.copy() # define temporary dataframe to avoid overwriting in the next step.

        # If 1 fay-level meets the n-count, combine all groups into 1
        combined_subjects.loc[(temp_df["FAYGroups"]==1), "FAY"] = 1

        # if 2 fay-levels meet the n-count, and 3-year fay is one of them, set 3 -> 2 and 2 -> 1
        combined_subjects.loc[(temp_df["FAYGroups"]==2) & (temp_df["FAY_MAX"]==3) & (temp_df["FAY"]==3), "FAY"] = 2
        combined_subjects.loc[(temp_df["FAYGroups"]==2) & (temp_df["FAY_MAX"]==3) & (temp_df["FAY"]==2), "FAY"] = 1
        # if 2 fay-levels meet the n-count, and 3-year fay is NOT one of them, then we are good to go

        # combine fay-levels and drop the temporary columns added in the process above
        combined_subjects = combined_subjects.drop(columns=["NumberFAYTested", "FAYGroups", "FAY_MAX"])

        combined_subjects["Subject"] = "All" # assign subject as all. These records are used to calculate the final stability points

        # concatenate original fay data with adjusted fay data
        fay_tested_static = pd.concat([fay_tested_static, combined_subjects], axis=0) # combine FAY for combined subjects (with FAY values changed based on n-count) and FAY for separate math and ela subjects

        # group by school, subject, and fay to obtain counts of each proficiency level
        proficiencies_by_fay = fay_tested_static.groupby(by=["SchoolCode", "Model", "Subject", "FAY"])["Performance"].value_counts().unstack().reset_index().rename(
            columns={key:f"P{int(key)}" for key in fay_tested_static["Performance"].unique()}).fillna(0)

        performance_columns = proficiencies_by_fay.columns[~proficiencies_by_fay.columns.isin(fay_tested_static.columns)] # reference new columns

        proficiencies_by_fay["NumberTested"] = proficiencies_by_fay[(list(performance_columns))].sum(axis=1)

        return proficiencies_by_fay

    """ 
    This protected method combines the Proficiency level results grouped by grade and by FAY-year group 
        at each school.
    """
    def _agg_by_grade_and_fay(self, proficiencies_by_grade:pd.DataFrame, proficiencies_by_fay:pd.DataFrame):
        """
        Parameters:
        ----------
        proficiencies_by_grade (pd.DataFrame): Produced by the _agg_by_grade() method
        proficiencies_by_fay (pd.DataFrame): Produced by the _agg_by_fay() method

        Returns:
        ----------
        pd.DataFrame: a dataframe containing proficiency level counts, number tested, and an average percent proficient score for each 
        grade and FAY-year group at each school.
        """
        # combine grade and FAY columns into a single group column
        proficiencies_by_grade["Grade"] = "Grade " + proficiencies_by_grade["Grade"].astype(int).astype(str) # rename grades to differentiate from fay values
        proficiencies_by_fay["TestType"] = "All" # fay groups include all assessment types
        proficiencies_by_fay["FAY"] = "FAY " + proficiencies_by_fay["FAY"].astype(int).astype(str) # rename fay values to differentiate from grades
        proficiency_data = pd.concat([proficiencies_by_grade.rename(columns={"Grade":"Group"}), proficiencies_by_fay.rename(columns={"FAY":"Group"})], axis=0).fillna(0)

        # convert numerical values to integers
        proficiency_data[proficiency_data.select_dtypes("number").columns] = proficiency_data.select_dtypes("number").astype(int)

        # calculate percent proficient for each grade and FAY group, using equation from the business rules (no percent tested multiplier because these are for the drilldown results)
        proficiency_data["PercentProficient"] = (((self.proficiency_weights[2] * proficiency_data["P2"]) + (self.proficiency_weights[3] * proficiency_data["P3"]) + 
        (self.proficiency_weights[4] * proficiency_data["P4"])) / (proficiency_data["NumberTested"]) * 100)

        # replace testtype id numbers with string names
        proficiency_data.replace({"TestType":self.test_type_map}, inplace=True)
        proficiency_data.loc[(proficiency_data["Group"]=="Grade 11") & (proficiency_data["TestType"]=="ASAA"), "TestType"] = "ACT"

        # rename columns to match desired output
        proficiency_data = proficiency_data.rename(columns={"P1":"NumberMP", "P2":"NumberPP", "P3":"NumberP", "P4":"NumberHP"})

        return proficiency_data
    
    """
    This method takes Proficiency level results grouped by grade/FAY-year, subject, and testtype, organizes it into a single table containing 
    all of the results for the drilldown GUI tables.
    
    For the 2022-2023 School Year, we report Proficiency drilldown tables for traditional K-8 models and 9-12 models. The K-8 drilldown table contains:
        For each combination of grade, subject, and testtype (plus aggregations over those):
            the numer of students tested, the number of students that scored Partially Proficient, the number of students that scored Proficient, 
            the number of student that scored Highly Proficient, and the Percent Proficient (using Business Rules formula).
    The 9-12 drilldown table contains:
        For each grade, subject, and testtype (no aggregations over those):
            the numer of students tested, the number of students that scored Partially Proficient, the number of students that scored Proficient, 
            the number of student that scored Highly Proficient, and the Percent Proficient (using Business Rules formula).
    """
    def create_drilldown_tables(self, proficiency_data:pd.DataFrame):
        """
        Parameters:
        ----------
        proficiency_data (pd.DataFrame): The first dataframe returned from the organize_proficiency() method (containing proficiency level 
            information for each grade and FAY-year group at each school)

        Returns:
        ----------
        pd.DataFrame: A dataframe containing all of the information provided in the Proficiency drilldown pages on ADEConnect
        """
        # check that the given dataframe contains the necessary columns 
        necessary_columns = {"SchoolCode", "Group", "Model", "TestType", "Subject", "NumberMP", "NumberPP", "NumberP", "NumberHP", "NumberTested", "PercentProficient"}
        if not necessary_columns.issubset(proficiency_data.columns):
            missing_columns = [x for x in necessary_columns if x not in proficiency_data.columns]
            raise ValueError(f"The following columns must be included in the \"proficiency_data\" argument provided: {missing_columns}")
        
        # select proficiency data for all k-8 grade-subject groups and for math/ela 9-12 Grade groups
        drilldown_df = proficiency_data[(~proficiency_data["Group"].str.contains(pat="FAY", regex=True)) & ~((proficiency_data["Model"]!=self.str_k8) & 
            (proficiency_data["TestType"]=="All"))].drop(columns=["NumberMP"])

        # combine Group, Subject, and Assessment name. Replace assessment names with "reg" or "alt" denoting regular and traditional assessments
        drilldown_df["TestGroup"] = drilldown_df["TestType"].replace(regex={"ASAA":"Reg", "ACT":"Reg", "MSAA":"Alt"}) + drilldown_df["Subject"] + drilldown_df["Group"].str.replace("Grade ", "")
        drilldown_df = drilldown_df.drop(columns=["Subject", "Group", "TestType"]) # drop old columns

        # convert table to wide format so that there is one record per school
        drilldown_df = drilldown_df.pivot(index=["SchoolCode", "Model"], columns=["TestGroup"], 
            values=["NumberPP", "NumberP", "NumberHP", "NumberTested", "PercentProficient"])
        
        # rename the columns of this wide table
        columns = drilldown_df.columns.to_frame().replace(regex={"Number":"Num", "PercentProficient":"PctProf"}).reset_index(drop=True)
        columns =  columns.iloc[:,1] + columns.iloc[:,0] + "_DrillDown"

        # Change Number to Num in 9-12 drilldown columns. This is a temporary name fix because I forgot to change Number to Num in the target 9-12 drilldown SQL table... can remove this in 2023-2024
        columns.loc[columns.str.contains("11Num")] = columns.loc[columns.str.contains("11Num")].replace({"Num":"Number"}, regex=True)

        drilldown_df.columns = columns

        # format table to match desired output into SQL gui table
        drilldown_df = drilldown_df.reset_index().rename(columns={"SchoolCode":"EntityID_All", "Model":"Model_All"})
        return drilldown_df

    
    """
    This method takes Proficiency level results that have been grouped by grade/FAY-year, subject, and testtype, and uses those results to calcuate Proficiency points earned 
        by each school. All of that information is then organized into a single table.

    For the 2022-2023 School Year, we report summary tables for all models. These tables contain the following Proficiency results:
        For the traditional K-8 model:
            The percent tested (out of students enrolled during the testing window), the percent proficient for each grade and subject, the percent proficient for each 
            FAY-year gorup and subject, the percent proficient for all students (applying the 95% tested multiplier), the number of Proficiency points earned using the 
            regular equation, and the number of Proficiency points earned using the stability-model equation (see business rules for details).
        For the traditional 9-12 and alt 9-12 model: 
            The percent tested (out of students enrolled during the testing window), percent proficient on ELA and Math for both regular and alternative assessments, 
            a percent proficient for all students (that applies the percent tested multiplier), and the points earned based on that perent proficient for all students
    """
    def create_summary_tables(self, proficiency_data:pd.DataFrame, percent_tested_info:pd.DataFrame):
        """
        Parameters:
        ----------
        proficiency_data (pd.DataFrame): The first dataframe returned from the organize_proficiency() method (containing proficiency level 
            information for each grade and FAY-year group at each school)

        percent_tested_info (pd.DataFrame): The second dataframe returned from the organize_proficiency() method (containing information 
            about the number of FAY students and the percentage of students enrolled that were tested for each school)

        Returns:
        ----------
        pd.DataFrame: A dataframe containing all of the information related to Proficiency that is provided in the summary pages on ADEConnect
        """
        # check that the necessary columns are included in the dataframe provided
        necessary_columns = {"SchoolCode", "Group", "Model", "TestType", "Subject", "NumberMP", "NumberPP", "NumberP", "NumberHP", "NumberTested", "PercentProficient"}
        if not necessary_columns.issubset(proficiency_data.columns):
            missing_columns = [x for x in necessary_columns if x not in proficiency_data.columns]
            raise ValueError(f"The following columns must be included in the \"proficiency_data\" argument provided: {missing_columns}")

        # select proficiency data for k-8 and 9-12 groups
        summary = proficiency_data[((proficiency_data["TestType"]=="All") & (proficiency_data["Model"]==self.str_k8)) | (~proficiency_data["TestType"].str.contains(pat="All")) & 
            (proficiency_data["Model"].isin([self.str_912, self.str_alt_912]))].copy()

        # combine assessment name, subject, and grade/fay together into one column
        summary["TestGroup"] = summary["TestType"].replace({"All":"", "ACT":"Reg", "MSAA":"Alt"}, regex=True) + summary["Subject"] + summary["Group"]
        summary = summary.replace({"TestGroup":{"Grade ":"Grade", "FAY ":"FAY"}}, regex=True).drop(columns=["Subject", "Group", "TestType"]).fillna(0)

        # convert to wide form to match sql gui tables
        summary_wide = summary.pivot(index=["SchoolCode", "Model"], columns=["TestGroup"], values="PercentProficient")
        column_names = summary_wide.columns.to_frame()
        summary_wide.columns = column_names.iloc[:,0] + "_Summary"
        summary_wide = summary_wide.reset_index()
        # select non-fay proficiency data and sum up all of the students across grades, subjects, and assessment types for each school
        reg_proficiency_scores = summary.drop(columns=["PercentProficient"])[~summary["TestGroup"].str.contains("FAY", regex=True)].groupby(
            by=["SchoolCode", "Model"]).sum(numeric_only=True).reset_index()
        reg_proficiency_scores = pd.merge(reg_proficiency_scores, percent_tested_info, on=["SchoolCode", "Model"]) # merge with percent tested, number fay, 95% tested info

        # calculate percent proficient for each school (combining all students)
        reg_proficiency_scores["PercentProficient"] = (((self.proficiency_weights[2] * reg_proficiency_scores["NumberPP"]) + (self.proficiency_weights[3] * reg_proficiency_scores["NumberP"]) + 
            (self.proficiency_weights[4] * reg_proficiency_scores["NumberHP"])) / reg_proficiency_scores["NumberTested"]) * 100 * reg_proficiency_scores["PercentMultiplier"]

        # use percent proficient to calculate regular proficiency points
        for grade_model, component_weights in self.models_component_weights.items():
            reg_proficiency_scores.loc[reg_proficiency_scores["Model"]==grade_model, "ProficiencyPoints"] = np.minimum(component_weights["Proficiency"], 
                (reg_proficiency_scores["PercentProficient"]/ 100 * component_weights["Proficiency"]))

        # assign NAN points to ineligible schools (not meeting the N-count)
        reg_proficiency_scores.loc[reg_proficiency_scores["NumberFAY"] < self.n_count, "ProficiencyPoints"] = np.nan 

        # select proficiency data for each FAY group at each school
        stability_prof_scores = summary[summary["TestGroup"].str.contains("AllFAY", regex=True)].reset_index(drop=True)

        if not stability_prof_scores.empty: # check that FAY groups are present (means that there are k-8 students included in static file)
            # calculate how many FAY groups there are for each school
            stability_prof_scores = stability_prof_scores.merge(stability_prof_scores["SchoolCode"].value_counts().rename("FAYLevels"), left_on="SchoolCode", right_index=True)
            
            # assign weights based on the number of FAY levels for which each school qualifies 
            conditions = [
                (stability_prof_scores["TestGroup"].str.contains("FAY1")) & (stability_prof_scores["FAYLevels"]==1), (stability_prof_scores["TestGroup"].str.contains("FAY1")) & (stability_prof_scores["FAYLevels"]==2), 
                (stability_prof_scores["TestGroup"].str.contains("FAY2")) & (stability_prof_scores["FAYLevels"]==2), (stability_prof_scores["TestGroup"].str.contains("FAY1")) & (stability_prof_scores["FAYLevels"]==3), 
                (stability_prof_scores["TestGroup"].str.contains("FAY2")) & (stability_prof_scores["FAYLevels"]==3), (stability_prof_scores["TestGroup"].str.contains("FAY3")) & (stability_prof_scores["FAYLevels"]==3)
            ]
            p = self.fay_stability_weight_map
            assignments = [p[1][1], p[2][1], p[2][2], p[3][1], p[3][2], p[3][3]]
            stability_prof_scores["Weight"] = np.select(condlist=conditions, choicelist=assignments)

            # convert table to wide format, such that there is one record per school
            stability_prof_scores = stability_prof_scores[["SchoolCode", "Model", "TestGroup", "PercentProficient", "Weight"]].pivot(index=["SchoolCode", "Model"], 
                columns="TestGroup", values=["Weight", "PercentProficient"]).reset_index().fillna(0)

            # rename columns to incorporate model type
            cols = stability_prof_scores.columns.to_frame().replace({"All":""}, regex=True)
            stability_prof_scores.columns = cols.iloc[:,0]+cols.iloc[:,1]

            # get the 95% tested percent proficient penalty calculated previously
            stability_prof_scores = pd.merge(stability_prof_scores, reg_proficiency_scores[["SchoolCode", "Model", "PercentMultiplier"]], on=["SchoolCode", "Model"])

            # calculate stability proficiency points (don't report percentage)
            stability_prof_scores["TotalStabilityPrfPoints"] = np.minimum(self.models_component_weights[self.str_k8]["Proficiency"], ((((stability_prof_scores["WeightFAY1"] * stability_prof_scores["PercentProficientFAY1"]) + 
                (stability_prof_scores["WeightFAY2"] * stability_prof_scores["PercentProficientFAY2"]) + (stability_prof_scores["WeightFAY3"] * stability_prof_scores["PercentProficientFAY3"])) / 
                self.models_component_weights[self.str_k8]["Proficiency"]) * stability_prof_scores["PercentMultiplier"] / 100 * self.models_component_weights[self.str_k8]["Proficiency"]))
            stability_prof_scores["TotalStabilityPrfPoints"] = np.maximum(0, stability_prof_scores["TotalStabilityPrfPoints"]) # negative points not allowed

            reg_proficiency_scores = reg_proficiency_scores[["SchoolCode", "Model", "PercentTested", "PercentProficient", "ProficiencyPoints"]]
            stability_prof_scores = stability_prof_scores[["SchoolCode", "Model", "WeightFAY1", "WeightFAY2", "WeightFAY3", "TotalStabilityPrfPoints"]]

            # combine regular and fay proficiencies. Left merge because 9-12 model schools will not have stability model results
            summary = pd.merge(reg_proficiency_scores, stability_prof_scores, on=["SchoolCode", "Model"], how="left")
            
            # track which schools don't meet the n-count
            summary.loc[(summary["Model"]==self.str_k8) & (summary["ProficiencyPoints"].isna()), "TotalStabilityPrfPoints"] = np.nan

            # report best score between regular and fay-stability proficiency points. 
            summary["Proficiency"] = np.maximum(summary["ProficiencyPoints"], summary["TotalStabilityPrfPoints"].fillna(0)) # Prioritizes NAN over 0

        else: # select only the columns that will be reported in the gui tables
            summary = reg_proficiency_scores[["SchoolCode", "Model", "PercentTested", "PercentProficient", "ProficiencyPoints"]]
            summary["Proficiency"] = summary["ProficiencyPoints"]

        # convert columns to wide, such that there is one column for each model-result
        summary = summary.set_index(["SchoolCode", "Model"])
        column_names = summary.columns.to_frame().replace({" ":""}, regex=True)
        summary.columns = column_names.iloc[:,0] + "_Summary" 
        summary = summary.dropna(axis=1, how="all").reset_index() # drop columns that contain all NAN values (such as TotalStabilityPrfPoints_Summary912)

        summary = pd.merge(summary, summary_wide, on=["SchoolCode", "Model"]).rename(columns={"SchoolCode":"EntityID_All", "Model":"Model_All"})
        
        # temporary fix for changing column name that is different between k-8 and 9-12 tables (forgot to change TotalProficiencyPoints to ProficiencyPoints in the K-8 gui table)
        summary.loc[summary["Model_All"]==self.str_k8, "TotalProficiencyPoints_Summary"] = summary.loc[summary["Model_All"]==self.str_k8, "ProficiencyPoints_Summary"]
        summary.loc[summary["Model_All"]==self.str_k8, "ProficiencyPoints_Summary"] = np.nan

        # summary.columns = summary.columns.str.replace("SchoolCode", "EntityID", regex=True)
        return summary

    

    """
    This method takes a gui_table dataframe (produced by the calculate_component() method) and returns only the columns included in the K-8 summary gui table
    """
    def pull_out_k_8_drilldown(self, gui_table:pd.DataFrame):
        # check that gui_table has proper formatting to work with this method
        df = self._pull_out_gui_table(gui_table, model=self.str_k8, column_suffix="_DrillDown|_All")
        df = df[df.columns[df.notna().any(0)]]
        return df

    """
    This method takes a gui_table dataframe (produced by the calculate_component() method) and returns only the columns included in the 9-12 drilldown gui table
    """
    def pull_out_9_12_drilldown(self, gui_table:pd.DataFrame):
        # check that gui_table has proper formatting to work with this method
        df = self._pull_out_gui_table(gui_table, model=self.str_912, column_suffix="_DrillDown|_All")
        df = df[df.columns[df.notna().any(0)]]
        return df

    """
    This method takes a gui_table dataframe (produced by the calculate_component() method) and returns only the columns included in the Alternative 9-12 drilldown gui table
    """
    def pull_out_alt_9_12_drilldown(self, gui_table:pd.DataFrame):
        # check that gui_table has proper formatting to work with this method
        df = self._pull_out_gui_table(gui_table, model=self.str_alt_912, column_suffix="_DrillDown|_All")
        df = df[df.columns[df.notna().any(0)]]
        return df
    
    """
    This method takes a gui_table dataframe (produced by the calculate_component() method) and returns only the columns included in the K-8 summary gui table
    """
    def pull_out_k_8_summary(self, gui_table:pd.DataFrame):
        # check that gui_table has proper formatting to work with this method
        df = self._pull_out_gui_table(gui_table, model=self.str_k8, column_suffix="_Summary|_All")
        df = df[df.columns[df.notna().any(0)]]
        return df

    """
    This method takes a gui_table dataframe (produced by the calculate_component() method) and returns only the columns included in the 9-12 summary gui table
    """
    def pull_out_9_12_summary(self, gui_table:pd.DataFrame):
        # check that gui_table has proper formatting to work with this method
        df = self._pull_out_gui_table(gui_table, model=self.str_912, column_suffix="_Summary|_All")
        df = df[df.columns[df.notna().any(0)]]
        return df
    
    """
    This method takes a gui_table dataframe (produced by the calculate_component() method) and returns only the columns included in the Alternative 9-12 summary gui table
    """
    def pull_out_alt_9_12_summary(self, gui_table:pd.DataFrame):
        # check that gui_table has proper formatting to work with this method
        df = self._pull_out_gui_table(gui_table, model=self.str_alt_912, column_suffix="_Summary|_All")
        df = df[df.columns[df.notna().any(0)]]
        return df

    """
    This private method takes a dataframe and a string suffix, and returns a copy of the dataframe with only the columns that contained the suffix. The suffix is removed from all columns.
    """
    def _pull_out_gui_table(self, gui_table:pd.DataFrame, model:str, column_suffix:str):
        columns = gui_table.columns
        columns = columns[columns.str.contains(column_suffix, regex=True)] # select columns that contain the suffix provided
        gui_table = gui_table[(gui_table["Model_All"]==model)][columns]
        gui_table.columns = gui_table.columns.str.replace(column_suffix, "", regex=True) # remove the suffix from the columns
        return gui_table