import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    CEDS_MAPPING: Dict[str, str] = {
        "INFANT/TODDLER": "IT", "PRESCHOOL": "PR", "PRE-K": "PK",
        "PREKINDERGARTEN": "PK", "TK": "TK", "TRANSITIONAL KINDERGARTEN": "TK",
        "KINDERGARTEN": "KG", "K": "KG", "01": "01", "1": "01", "02": "02", "2": "02",
        "03": "03", "3": "03", "04": "04", "4": "04", "05": "05", "5": "05", "06": "06", "6": "06",
        "07": "07", "7": "07", "08": "08", "8": "08", "09": "09", "9": "09", "10": "10", "11": "11",
        "12": "12", "13": "13", "POSTSECONDARY": "PS", "UGRADED": "UG", "UNGRADED": "UG",
        "UG": "UG", "OTHER": "Other", "EL": "KG", "KF": "KG",
    }

    def __init__(self):
        self.school_year: int = 0
        self.academic_start: str = ""
        self.academic_end: str = ""
        self.homeroom_classes_df = pd.DataFrame()

    @staticmethod
    def grade_to_ceds(grade_value: Any) -> str:
        original = str(grade_value).strip().upper() if pd.notna(grade_value) else ""
        return DataTransformer.CEDS_MAPPING.get(original, "UG")

    @staticmethod
    def map_role(teaching_flag: Any) -> str:
        val = str(teaching_flag).strip().lower()
        return "teacher" if val == "y" else "administrator"

    def determine_school_year(self, all_data: Dict[str, pd.DataFrame], source_config: Any) -> int:
        """
        Determine school year from source files based on configuration
        """
        normalized_sources = self.normalize_source_config(source_config)
        
        for role, filename in normalized_sources.items():
            df = all_data.get(filename)
            if df is not None and "school year" in df.columns:
                years = df["school year"].dropna().astype(str).str[:4].unique()
                if len(years) > 0:
                    try:
                        return int(years[0])
                    except ValueError:
                        pass

        now = datetime.now()
        if now.month >= 8:
            return now.year
        return now.year - 1

    def set_school_year(self, year: int) -> None:
        self.school_year = year
        self.academic_start = f"{year}-08-25"
        self.academic_end = f"{year + 1}-07-25"

    def generate_class_id(self, row: pd.Series, mt_id_col: str, append_year: bool = False) -> str:
        mt_id = row.get(mt_id_col, "")
        if mt_id and append_year:
            return f"{mt_id}_{self.school_year}"
        return mt_id

    def generate_class_name(self, row: pd.Series, teacher_flag_col: str, teacher_last_col: str, course_title_col: str, section_letter_col: str) -> str:
        course_title = str(row.get(course_title_col, row.get("title", "Unknown Course"))).strip()
        teacher_last = ""

        if teacher_flag_col and teacher_flag_col in row:
            if str(row.get(teacher_flag_col, "")).strip().lower() == "y":
                teacher_last = str(row.get(teacher_last_col, "")).strip()
        else:
            teacher_last = str(row.get(teacher_last_col, "")).strip()
            
        if pd.isna(teacher_last) or teacher_last.lower() == 'nan':
            teacher_last = ""

        section = str(row.get(section_letter_col, "")).strip()
        year = self.school_year

        parts = []
        if teacher_last:
            parts.append(teacher_last)
        parts.append(course_title)
        if section:
            if parts:
                parts[-1] = f"{parts[-1]} ({section})"
            else:
                parts.append(f"({section})")
        parts.append(str(year))

        return " ".join(parts).strip()

    def generate_user_role(self, row: pd.Series, staff_id_col: str, student_id_col: str) -> str:
        staff_val = row.get(staff_id_col, "")
        if pd.notna(staff_val) and str(staff_val).strip() != "":
            return "teacher"

        student_val = row.get(student_id_col, "")
        if pd.notna(student_val) and str(student_val).strip() != "":
            return "student"

        return "unknown"

    def generate_user_id(self, row: pd.Series, staff_id_col: str, student_id_col: str) -> str:
        staff_val = row.get(staff_id_col, "")
        if pd.notna(staff_val) and str(staff_val).strip() != "":
            return str(staff_val)

        student_val = row.get(student_id_col, "")
        if pd.notna(student_val) and str(student_val).strip() != "":
            return str(student_val)

        return "UNKNOWN_ID"
        
    def generate_student_email(self, row: pd.Series, format_str: str) -> str:
        """
        Generates a student email address based on a format string.
        The format string can contain column names in curly braces, e.g., "{student number}@sd74.bc.ca"
        """
        try:
            # Create a dictionary with lowercase keys for case-insensitive formatting
            row_lower = {k.lower(): v for k, v in row.to_dict().items()}
            return format_str.format(**row_lower)
        except KeyError as e:
            logger.warning(f"Could not generate email. Missing key: {e}")
            return ""

    def get_source_file(self, raw_data: Dict[str, pd.DataFrame], source_config: Any, role: str) -> pd.DataFrame:
        """
        Get source file based on configuration structure.
        Supports both old list format and new structured format.
        """
        normalized_sources = self.normalize_source_config(source_config)
        
        filename = normalized_sources.get(role)
        if filename and filename in raw_data:
            return raw_data[filename].copy()
        
        logger.warning(f"Source file for role '{role}' not found in configuration")
        return pd.DataFrame()

    def normalize_source_config(self, source_config: Any) -> Dict[str, str]:
        """
        Normalize source configuration to a consistent dictionary format.
        """
        normalized = {}
        
        if isinstance(source_config, dict):
            # Already in the preferred format
            return source_config
        elif isinstance(source_config, list):
            if all(isinstance(item, dict) for item in source_config):
                # [{"file": "file.txt", "role": "schedule"}, ...]
                for item in source_config:
                    if "role" in item and "file" in item:
                        normalized[item["role"]] = item["file"]
            elif all(isinstance(item, str) for item in source_config):
                # Legacy list format: ["file1.txt", "file2.txt", ...]
                roles = ["student_schedule", "course_info", "staff_info", "student_demographic"]
                for i, filename in enumerate(source_config):
                    if i < len(roles):
                        normalized[roles[i]] = filename
        
        return normalized

    def transform(self, df: pd.DataFrame, mapping: Dict[str, Any], entity: str, raw_data: Dict[str, pd.DataFrame], global_config: Dict[str, Any]) -> pd.DataFrame:
        working = df.copy()
        result = pd.DataFrame()
        working.columns = [col.strip().lower() for col in working.columns]
        field_map = mapping.get("field_map", {})
        
        if entity == "Students":
            email_format_config = field_map.get("Email Address", {})
            if isinstance(email_format_config, dict):
                email_format = email_format_config.get("format")
                if email_format:
                    result["Email Address"] = working.apply(self.generate_student_email, format_str=email_format.lower(), axis=1)

            source_config = mapping.get("source_files", {})
            normalized_sources = self.normalize_source_config(source_config)
            student_source = normalized_sources.get("student_demographic", "")
            
            if "EnrollStatus" not in result.columns:
                # Check if we're using enhanced demographic file
                if "enrolment status" in working.columns:
                    # Priority 1: Use the explicit 'enrolment status' column if it exists
                    result["EnrollStatus"] = working["enrolment status"].apply(
                        lambda x: str(x).strip() if str(x).strip() in ["Active", "PreReg"] else "Inactive"
                    )
                elif "withdraw date" in working.columns:
                    # Priority 2: If no status column, infer from 'withdraw date'
                    result["EnrollStatus"] = working["withdraw date"].apply(
                        lambda x: "Active" if pd.isna(x) or str(x).strip() == "" else "Inactive"
                    )
                else:
                    # Fallback: If neither key column is found, log a warning and default to Active
                    logger.warning("[Students] Could not find 'enrolment status' or 'withdraw date' column. Defaulting to 'Active'.")
                    result["EnrollStatus"] = "Active"

        if entity == "Staff":
            source_config = mapping.get("source_files", {})
            normalized_sources = self.normalize_source_config(source_config)
            
            if len(normalized_sources) > 1:
                staff_filename = normalized_sources.get("staff_info", "")
                roster_filename = list(normalized_sources.values())[1] if len(normalized_sources) > 1 else ""
                
                staff_df = raw_data.get(staff_filename, pd.DataFrame())
                roster_df = raw_data.get(roster_filename, pd.DataFrame())

                if not staff_df.empty and not roster_df.empty and "teacher id" in staff_df.columns and "staff sourceid" in roster_df.columns:
                    working = staff_df.merge(
                        roster_df[["teacher id", "staff sourceid"]].drop_duplicates("teacher id"),
                        on="teacher id",
                        how="left",
                    )
                    working.columns = [col.strip().lower() for col in working.columns]

        if entity == "Classes":
            source_config = mapping.get("source_files", {})
            normalized_sources = self.normalize_source_config(source_config)
            
            # Get schedule data
            schedule_df = self.get_source_file(raw_data, normalized_sources, "student_schedule")
            if schedule_df.empty:
                logger.warning("[Classes] Student schedule data not available")
                return pd.DataFrame()
            
            schedule_df.columns = [col.strip().lower() for col in schedule_df.columns]
            homeroom_grades = global_config.get("homeroom_grades", [])
            final_classes = []

            # Process K-7 Homeroom Classes
            student_demo_df = self.get_source_file(raw_data, normalized_sources, "student_demographic")
            if not student_demo_df.empty:
                student_demo_df.columns = [col.strip().lower() for col in student_demo_df.columns]
                
                # Get grade column from Students mapping for consistency
                students_mapping = global_config.get("mappings", {}).get("Students", {})
                students_field_map = students_mapping.get("field_map", {})
                grade_config = students_field_map.get("Grade", {})
                grade_col = grade_config.get("column", "grade").lower() if isinstance(grade_config, dict) else "grade"
                
                student_demo_df[grade_col] = student_demo_df[grade_col].apply(self.grade_to_ceds)
                
                homeroom_mask = student_demo_df[grade_col].isin(homeroom_grades)
                homeroom_students = student_demo_df[homeroom_mask]
                
                if not homeroom_students.empty:
                    homeroom_col = students_field_map.get("Homeroom", "homeroom").lower()
                    
                    # Get unique homerooms with teacher info
                    unique_homerooms = homeroom_students.drop_duplicates(
                        subset=["school number", homeroom_col, "teacher id"]
                    )
                    
                    # Only create homeroom classes if we have valid data
                    if not unique_homerooms.empty and homeroom_col in unique_homerooms.columns:
                        homeroom_classes = unique_homerooms.copy()
                        homeroom_classes["Class ID"] = (
                            homeroom_classes["school number"].astype(str) + "_" + 
                            homeroom_classes[homeroom_col].fillna("UnassignedHomeroom").astype(str) + f"_{self.school_year}"
                        )
                        
                        # Build class name using available teacher info
                        teacher_name_col = "teacher name"  # From student demographic

                        def create_homeroom_name(row, homeroom_col, teacher_name_col, year):
                            homeroom = row[homeroom_col]
                            teacher = row[teacher_name_col]
                            
                            # Handle missing values
                            has_homeroom = pd.notna(homeroom) and str(homeroom).strip() != ""
                            has_teacher = pd.notna(teacher) and str(teacher).strip() != ""
                            
                            parts = []
                            
                            if has_homeroom:
                                parts.append(str(homeroom))
                            else:
                                parts.append("Unassigned Homeroom")
                            
                            if has_teacher:
                                parts.append(f"- {teacher}")
                            
                            parts.append(f"({year})")
                            
                            return " ".join(parts)

                        homeroom_classes["Name"] = homeroom_classes.apply(
                            lambda row: create_homeroom_name(row, homeroom_col, teacher_name_col, self.school_year), 
                            axis=1
                        )
                        homeroom_classes["Grade"] = homeroom_classes[grade_col]
                        homeroom_classes["School ID"] = homeroom_classes["school number"]
                        homeroom_classes["Start Date"] = self.academic_start
                        homeroom_classes["End Date"] = self.academic_end
                        
                        # Store for enrollment processing
                        self.homeroom_classes_df = homeroom_classes[["school number", homeroom_col, "Class ID", "teacher id"]].copy()
                        
                        # Add homeroom classes to final result - ensure we only include field_map columns
                        homeroom_output = pd.DataFrame()
                        for tgt_field in field_map.keys():
                            if tgt_field in homeroom_classes.columns:
                                homeroom_output[tgt_field] = homeroom_classes[tgt_field]
                            else:
                                logger.warning(f"[Classes] Column '{tgt_field}' not found in homeroom classes")
                                homeroom_output[tgt_field] = pd.NA
                        
                        final_classes.append(homeroom_output)
                        logger.info(f"[Classes] Created {len(homeroom_classes)} homeroom classes")

            # Process Grade 8+ Subject Classes
            non_homeroom_mask = ~schedule_df["grade"].isin(homeroom_grades)
            non_homeroom_df = schedule_df[non_homeroom_mask].copy()
            
            if not non_homeroom_df.empty:
                course_df = self.get_source_file(raw_data, normalized_sources, "course_info")
                staff_df = self.get_source_file(raw_data, normalized_sources, "staff_info")
                
                if not course_df.empty:
                    course_df.columns = [col.strip().lower() for col in course_df.columns]
                    if "district course code" in non_homeroom_df.columns:
                        non_homeroom_df.rename(columns={"district course code": "course code"}, inplace=True)
                    
                    merged_df = non_homeroom_df.merge(
                        course_df[["school number", "course code", "title"]], 
                        on=["school number", "course code"], 
                        how="left"
                    )
                else:
                    merged_df = non_homeroom_df
                
                if not staff_df.empty:
                    staff_df.columns = [col.strip().lower() for col in staff_df.columns]
                    merged_df = merged_df.merge(
                        staff_df[["teacher id", "last name"]], 
                        on="teacher id", 
                        how="left"
                    )
                
                # Apply field mapping transformations
                subject_output = pd.DataFrame()
                for tgt_field, src_info in field_map.items():
                    if tgt_field == "Class ID" and isinstance(src_info, dict) and src_info.get("append_year_to_id"):
                        col_name = src_info.get("column", "").lower()
                        if col_name in merged_df.columns:
                            subject_output[tgt_field] = merged_df.apply(
                                self.generate_class_id, 
                                mt_id_col=col_name, 
                                append_year=True, 
                                axis=1
                            )
                        else:
                            logger.warning(f"[Classes] Column '{col_name}' not found for Class ID generation")
                            subject_output[tgt_field] = pd.NA
                            
                    elif tgt_field == "Name" and isinstance(src_info, dict):
                        teacher_flag_col = src_info.get("primary_teacher_flag", "").lower()
                        teacher_last_col = src_info.get("teacher_last_name", "last name").lower()
                        course_title_col = src_info.get("course_title", "title").lower()
                        section_letter_col = src_info.get("section_letter", "section letter").lower()
                        
                        subject_output[tgt_field] = merged_df.apply(
                            self.generate_class_name,
                            teacher_flag_col=teacher_flag_col,
                            teacher_last_col=teacher_last_col,
                            course_title_col=course_title_col,
                            section_letter_col=section_letter_col,
                            axis=1
                        )
                        
                    elif isinstance(src_info, dict) and src_info.get("use_academic_year"):
                        subject_output[tgt_field] = self.academic_start if tgt_field == "Start Date" else self.academic_end
                        
                    elif isinstance(src_info, dict) and "column" in src_info:
                        src_col = src_info["column"].lower()
                        if src_col in merged_df.columns:
                            subject_output[tgt_field] = merged_df[src_col]
                        else:
                            logger.warning(f"[Classes] Column '{src_col}' not found for field '{tgt_field}'")
                            subject_output[tgt_field] = pd.NA
                            
                    elif isinstance(src_info, str):
                        src_col = src_info.lower()
                        if src_col in merged_df.columns:
                            subject_output[tgt_field] = merged_df[src_col]
                        else:
                            logger.warning(f"[Classes] Column '{src_col}' not found for field '{tgt_field}'")
                            subject_output[tgt_field] = pd.NA
                
                final_classes.append(subject_output)
                logger.info(f"[Classes] Created {len(subject_output)} subject classes")

            # Combine all classes
            if final_classes:
                result = pd.concat(final_classes, ignore_index=True).drop_duplicates()
                logger.info(f"[Classes] Total classes created: {len(result)} (homeroom: {len(homeroom_output) if 'homeroom_output' in locals() else 0}, subject: {len(subject_output) if 'subject_output' in locals() else 0})")
                return result
            
            return pd.DataFrame()

        if entity == "Enrollments":
            source_config = mapping.get("source_files", {})
            normalized_sources = self.normalize_source_config(source_config)
            
            # Get schedule data
            schedule_df = self.get_source_file(raw_data, normalized_sources, "student_schedule")
            if schedule_df.empty:
                return pd.DataFrame()
            
            schedule_df.columns = [col.strip().lower() for col in schedule_df.columns]
            homeroom_grades = global_config.get("homeroom_grades", [])
            final_enrollments = []
            
            # Get student demographic data for homeroom processing
            student_demo_df = self.get_source_file(raw_data, normalized_sources, "student_demographic")
            if not student_demo_df.empty:
                student_demo_df.columns = [col.strip().lower() for col in student_demo_df.columns]
            else:
                # Try to get it from the raw_data using the expected filename
                student_demo_filename = "StudentDemographicInformation.txt"
                if student_demo_filename in raw_data:
                    student_demo_df = raw_data[student_demo_filename].copy()
                    student_demo_df.columns = [col.strip().lower() for col in student_demo_df.columns]
                else:
                    logger.warning("[Enrollments] Student demographic data not available")
            
            # DEBUG: Check if we have homeroom classes
            logger.info(f"[Enrollments] homeroom_classes_df empty: {self.homeroom_classes_df.empty}")
            if not self.homeroom_classes_df.empty:
                logger.info(f"[Enrollments] homeroom_classes_df columns: {self.homeroom_classes_df.columns.tolist()}")
            
            # Process K-7 Homeroom Enrollments
            if not student_demo_df.empty and not self.homeroom_classes_df.empty:
                # Use standard column names from student demographic file
                grade_col = "grade"
                homeroom_col = "homeroom"
                student_id_col = "student number"

                student_demo_df[grade_col] = student_demo_df[grade_col].apply(self.grade_to_ceds)
                
                # Check if the homeroom column exists in student demographic data
                if homeroom_col not in student_demo_df.columns:
                    logger.warning(f"[Enrollments] Homeroom column '{homeroom_col}' not found in student demographic data")
                    # Try alternative column names
                    homeroom_col = "homeroom"  # Try different variations if needed
                    
                homeroom_mask = student_demo_df[grade_col].isin(homeroom_grades)
                homeroom_students = student_demo_df[homeroom_mask]
                
                logger.info(f"[Enrollments] Found {len(homeroom_students)} students in homeroom grades")
                
                if not homeroom_students.empty:
                    # Check if the homeroom column exists in both dataframes
                    if homeroom_col not in self.homeroom_classes_df.columns:
                        logger.warning(f"[Enrollments] Homeroom column '{homeroom_col}' not found in homeroom_classes_df")
                        # Try to find the correct column name
                        for col in self.homeroom_classes_df.columns:
                            if "homeroom" in col.lower():
                                homeroom_col = col
                                logger.info(f"[Enrollments] Using homeroom column: {homeroom_col}")
                                break
                    
                    # Merge students with their homeroom classes
                    try:
                        homeroom_enroll_df = homeroom_students.merge(
                            self.homeroom_classes_df, 
                            on=["school number", homeroom_col], 
                            how="left"
                        )
                        logger.info(f"[Enrollments] Merged {len(homeroom_enroll_df)} students with homeroom classes")
                        
                        # Check if the merge was successful
                        if "Class ID" not in homeroom_enroll_df.columns:
                            logger.warning("[Enrollments] Class ID column not found after merge")
                        else:
                            # Count how many students were successfully matched with homeroom classes
                            matched_count = homeroom_enroll_df["Class ID"].notna().sum()
                            logger.info(f"[Enrollments] {matched_count} students matched with homeroom classes")
                        
                        # Student enrollments in homeroom
                        if student_id_col in homeroom_enroll_df.columns and "Class ID" in homeroom_enroll_df.columns:
                            # Filter out rows where Class ID is null (no homeroom match)
                            valid_homeroom_enrollments = homeroom_enroll_df[homeroom_enroll_df["Class ID"].notna()]
                            
                            if not valid_homeroom_enrollments.empty:
                                student_enroll = valid_homeroom_enrollments[["Class ID", student_id_col, "school number"]].copy()
                                student_enroll.rename(columns={student_id_col: "User ID"}, inplace=True)
                                student_enroll["Role"] = "student"

                                # Add student enrollments to final list
                                final_enrollments.append(student_enroll)
                                logger.info(f"[Enrollments] Created {len(student_enroll)} student homeroom enrollments")
                                
                                # Teacher enrollments in homeroom
                                teacher_id_cols = [col for col in valid_homeroom_enrollments.columns if 'teacher' in col.lower() and 'id' in col.lower()]
                                if teacher_id_cols:
                                    teacher_id_col = teacher_id_cols[0]  # Use the first matching column
                                    teacher_enroll = valid_homeroom_enrollments[["Class ID", teacher_id_col, "school number"]].copy()
                                    teacher_enroll.rename(columns={teacher_id_col: "User ID"}, inplace=True)
                                    teacher_enroll["Role"] = "teacher"
                                    
                                    # Remove any rows where User ID is null/empty
                                    teacher_enroll = teacher_enroll[teacher_enroll["User ID"].notna()]
                                    teacher_enroll = teacher_enroll[teacher_enroll["User ID"].astype(str).str.strip() != ""]
                                    
                                    final_enrollments.append(teacher_enroll)
                                    logger.info(f"[Enrollments] Created {len(teacher_enroll)} teacher homeroom enrollments")
                                else:
                                    logger.warning("[Enrollments] Could not find teacher ID column in homeroom data")
                                    
                                    # Fallback: Get teacher IDs from the original homeroom_classes_df
                                    try:
                                        # Create teacher enrollments directly from homeroom classes
                                        teacher_enroll = self.homeroom_classes_df[["Class ID", "teacher id", "school number"]].copy()
                                        teacher_enroll.rename(columns={"teacher id": "User ID"}, inplace=True)
                                        teacher_enroll["Role"] = "teacher"
                                        
                                        # Remove any rows where User ID is null/empty
                                        teacher_enroll = teacher_enroll[teacher_enroll["User ID"].notna()]
                                        teacher_enroll = teacher_enroll[teacher_enroll["User ID"].astype(str).str.strip() != ""]
                                        
                                        final_enrollments.append(teacher_enroll)
                                        logger.info(f"[Enrollments] Created {len(teacher_enroll)} teacher homeroom enrollments (fallback)")
                                    except Exception as e:
                                        logger.error(f"[Enrollments] Failed to create teacher enrollments: {e}")
                            else:
                                logger.warning("[Enrollments] No valid homeroom enrollments found after filtering null Class IDs")
                    except Exception as e:
                        logger.error(f"[Enrollments] Error merging homeroom data: {e}")

            # Process Grade 8+ Subject Enrollments
            non_homeroom_mask = ~schedule_df["grade"].isin(homeroom_grades)
            non_homeroom_enroll_df = schedule_df[non_homeroom_mask].copy()
            
            if not non_homeroom_enroll_df.empty:
                # Apply field mapping for Class ID generation
                class_id_config = field_map.get("Class ID", {})
                if isinstance(class_id_config, dict) and class_id_config.get("append_year_to_id"):
                    mt_id_col = class_id_config.get("column", "").lower()
                    if mt_id_col and mt_id_col in non_homeroom_enroll_df.columns:
                        non_homeroom_enroll_df["Class ID"] = non_homeroom_enroll_df.apply(
                            self.generate_class_id, 
                            mt_id_col=mt_id_col, 
                            append_year=True, 
                            axis=1
                        )
                
                # Apply field mapping for User ID and Role
                user_id_config = field_map.get("User ID", {})
                role_config = field_map.get("Role", {})
                
                if isinstance(user_id_config, dict) and isinstance(role_config, dict):
                    student_id_col = user_id_config.get("student_id_col", "").lower()
                    staff_id_col = user_id_config.get("staff_id_col", "").lower()
                    
                    # Use standard column names if the configured ones don't exist
                    if not student_id_col or student_id_col not in non_homeroom_enroll_df.columns:
                        student_id_col = "student id"
                    if not staff_id_col or staff_id_col not in non_homeroom_enroll_df.columns:
                        staff_id_col = "teacher id"
                    
                    # Student enrollments
                    if student_id_col in non_homeroom_enroll_df.columns and "Class ID" in non_homeroom_enroll_df.columns:
                        student_enroll = non_homeroom_enroll_df[["Class ID", student_id_col, "school number"]].copy()
                        student_enroll.rename(columns={student_id_col: "User ID"}, inplace=True)
                        student_enroll["Role"] = "student"
                        final_enrollments.append(student_enroll)
                    
                    # Teacher enrollments
                    if staff_id_col in non_homeroom_enroll_df.columns and "Class ID" in non_homeroom_enroll_df.columns:
                        teacher_enroll = non_homeroom_enroll_df[["Class ID", staff_id_col, "school number"]].copy()
                        teacher_enroll.rename(columns={staff_id_col: "User ID"}, inplace=True)
                        teacher_enroll["Role"] = "teacher"
                        final_enrollments.append(teacher_enroll)
                        
                        logger.info(f"[Enrollments] Created {len(student_enroll) if 'student_enroll' in locals() else 0} student and {len(teacher_enroll) if 'teacher_enroll' in locals() else 0} teacher subject enrollments")
            
            if final_enrollments:
                result = pd.concat(final_enrollments, ignore_index=True).drop_duplicates()
                
                # Ensure School ID column exists - use standard column name
                if "school number" in result.columns:
                    result.rename(columns={"school number": "School ID"}, inplace=True)
                elif "school_number" in result.columns:
                    result.rename(columns={"school_number": "School ID"}, inplace=True)
                
                logger.info(f"[Enrollments] Created {len(result)} total enrollments")
                return result
        
            logger.warning("[Enrollments] No enrollments created")
            return pd.DataFrame()
        
        for tgt_field, src_info in field_map.items():
            try:
                if tgt_field in result.columns: continue

                if isinstance(src_info, dict) and "value" in src_info:
                    result[tgt_field] = src_info["value"]
                elif isinstance(src_info, dict) and src_info.get("use_academic_year"):
                    result[tgt_field] = self.academic_start if tgt_field == "Start Date" else self.academic_end
                elif isinstance(src_info, dict) and src_info.get("append_year_to_id"):
                    col_name = src_info.get("column", "").lower()
                    result[tgt_field] = working.apply(self.generate_class_id, mt_id_col=col_name, append_year=True, axis=1)
                elif entity == "Classes" and tgt_field == "Name":
                    teacher_flag_col = src_info.get("primary_teacher_flag", "").lower()
                    teacher_last_col = src_info.get("teacher_last_name", "last name").lower()
                    course_title_col = src_info.get("course_title", "title").lower()
                    section_letter_col = src_info.get("section_letter", "section letter").lower()
                    result[tgt_field] = working.apply(self.generate_class_name,
                                                       teacher_flag_col=teacher_flag_col,
                                                       teacher_last_col=teacher_last_col,
                                                       course_title_col=course_title_col,
                                                       section_letter_col=section_letter_col,
                                                       axis=1)
                elif isinstance(src_info, dict):
                    column_name = src_info.get("column", "").lower()
                    transform_name = src_info.get("transform", "")
                    if column_name in working.columns:
                        series = working[column_name]
                        if transform_name:
                            func = getattr(self, transform_name)
                            result[tgt_field] = series.apply(func)
                        else:
                            result[tgt_field] = series
                    else:
                        logger.warning(f"[{entity}] Missing column '{column_name}' for field '{tgt_field}' -> filling nulls")
                        result[tgt_field] = pd.NA
                else:
                    col = str(src_info).lower()
                    if col in working.columns:
                        result[tgt_field] = working[col]
                    else:
                        logger.warning(f"[{entity}] Missing column '{col}' for field '{tgt_field}' -> filling nulls")
                        result[tgt_field] = pd.NA

            except Exception as ex:
                logger.exception(f"Error transforming {entity}.{tgt_field}: {ex}")
                result[tgt_field] = pd.NA
        return result