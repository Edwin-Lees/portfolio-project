-- Data Cleaning 

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- raw data

SELECT *
FROM layoffs
LIMIT 100
;



-- create staging table to clean data in 
-- copy raw data to staging table
-- returns only column names

-- 1. find and remove duplicates
-- 2. standardize the data (data types)
-- 3. Nulls and Blanks
-- 4. Remove any columns

CREATE TABLE layoffs_staging
LIKE layoffs
;



SELECT *
FROM layoffs_staging
;


-- copy raw data & insert into stagin table

INSERT layoffs_staging
SELECT *
FROM layoffs
;


-- CTE assigns row a number based on the partitioning
-- unique rows return 1, while duplicates with matching values in rows return 2
-- filter duplicates

WITH duplicate_cte AS
(
SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1
;





-- 1. duplicate check

SELECT *
FROM layoffs_staging
WHERE company = 'Casper'
;



-- returns error since a CTE is not updateable, (row_num does not exist in main table)
WITH duplicate_cte AS
(
SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, 
        percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1
;




-- create second staging table to create extra row (row_num) to delete duplciates
-- schema layoffs_staging < copy clipboard < create statement

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- insert duplicate data from staging table 1 to 2, and delete filtered data 

INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, 
        percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
;

DELETE
FROM layoffs_staging2
WHERE row_num > 1
;

SELECT *
FROM layoffs_staging2
;





-- 2. Standardizing data 

-- TRIM extra space

SELECT company, TRIM(company)
FROM layoffs_staging2
;


-- Update staging table with TRIM compnay data

UPDATE layoffs_staging2
SET company = TRIM(company);


-- industry column

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;


-- replace all industry starting with crypto% to crypto
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;



-- Location column (is fine), Country

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
;


-- Date (date data type is STRING change to DATE)

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2
ORDER BY 1;

-- change data type from TEXT to DATE in schema < table < column < date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;





-- 3. NULLS and BLANKS (populate or delete)

-- check for NULLS
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;

-- check for NULLS and BLANKS
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
	OR industry = '';

-- check to see which industry can be populated based off related company
SELECT *
FROM layoffs_staging2
where company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- populate NULLS with BLANKS
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- self-join and populate NULLS with relevant values
UPDATE Layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;


-- check for additional NULLS
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- delete irrelevant NULLS
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- check 
SELECT *
FROM layoffs_staging2;

-- delete row_num column 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





























