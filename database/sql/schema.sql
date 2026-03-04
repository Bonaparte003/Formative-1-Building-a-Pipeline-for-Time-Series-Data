-- LCA Time-Series Pipeline – MySQL Schema (Source of Truth)
-- Minimum 3 tables: periods, employers, cases
-- See docs/ERD.md for ERD and design rationale.

-- ---------------------------------------------------------------------------
-- Table: periods (time dimension)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS periods (
    period_id    INT AUTO_INCREMENT PRIMARY KEY,
    fiscal_year  INT NOT NULL,
    quarter      VARCHAR(2) NOT NULL,
    start_date   DATE NOT NULL,
    end_date     DATE NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_period (fiscal_year, quarter)
);

-- ---------------------------------------------------------------------------
-- Table: employers (dimension)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS employers (
    employer_id       INT AUTO_INCREMENT PRIMARY KEY,
    employer_name     VARCHAR(255) NOT NULL,
    employer_location VARCHAR(255),
    employer_country  VARCHAR(100),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_employer_name (employer_name(100))
);

-- ---------------------------------------------------------------------------
-- Table: cases (fact table – one row per LCA application)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cases (
    case_id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    period_id          INT NOT NULL,
    employer_id        INT NOT NULL,
    soc_title          VARCHAR(255),
    visa_class         VARCHAR(50),
    job_title          VARCHAR(255),
    full_time_position CHAR(1),
    worksite           VARCHAR(255),
    wage               DECIMAL(12, 2),
    unit_of_pay        VARCHAR(10),
    case_status        VARCHAR(50),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_case_period   FOREIGN KEY (period_id)   REFERENCES periods(period_id),
    CONSTRAINT fk_case_employer FOREIGN KEY (employer_id) REFERENCES employers(employer_id),
    KEY idx_period_id (period_id),
    KEY idx_created_at (created_at),
    KEY idx_case_status (case_status)
);
