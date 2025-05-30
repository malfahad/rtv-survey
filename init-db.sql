
-- Switch to default rtv_survey database
\c rtv_survey

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS metrics;

-- Create survey_summary table
CREATE TABLE IF NOT EXISTS metrics.survey_summary (
    district_ INTEGER,
    "Quartile_" TEXT,
    source_file_ TEXT,
    hhid_2_nunique INTEGER,
    hh_size_mean FLOAT,
    hh_size_median FLOAT,
    hhh_age_mean FLOAT,
    hhh_age_median FLOAT,
    "hhh_sex_<lambda>" TEXT,
    survey_duration_mean FLOAT,
    survey_duration_median FLOAT,
    "GPS-Accuracy_mean" FLOAT,
    "hhh_educ_level_<lambda>" TEXT,
    hhh_read_write_mean FLOAT,
    age_6_12_Attend_Sch_1_mean FLOAT,
    age_13_18_Attend_Sch_1_mean FLOAT,
    Number_Radios_mean FLOAT,
    Number_Radios_sum INTEGER,
    Number_Mobile_Phones_mean FLOAT,
    Number_Mobile_Phones_sum INTEGER,
    assets_reported_total_mean FLOAT,
    assets_reported_total_sum INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (district_, "Quartile_", source_file_)
);

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_survey_summary_district ON metrics.survey_summary(district_);
CREATE INDEX IF NOT EXISTS idx_survey_summary_quartile ON metrics.survey_summary("Quartile_");
CREATE INDEX IF NOT EXISTS idx_survey_summary_source_file ON metrics.survey_summary(source_file_);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA metrics TO rtv_test_user;
GRANT ALL PRIVILEGES ON TABLE metrics.survey_summary TO rtv_test_user;
