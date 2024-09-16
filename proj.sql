-- Data Cleaning Project

SELECT *
FROM layoffs_dataset;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Remove Null or Empty Values
-- 4. Remove Columns if Necessary

CREATE TABLE layoffs_dataset_staging
LIKE layoffs_dataset
;

SELECT *
FROM layoffs_dataset_staging
;

INSERT INTO layoffs_dataset_staging
SELECT *
FROM layoffs_dataset
;

WITH duplicate_cte AS
( SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
							  `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_dataset_staging
) 
SELECT * 
FROM duplicate_cte
WHERE row_num > 1 ;

SELECT *
FROM layoffs_dataset_staging
WHERE company = 'Cazoo';

CREATE TABLE `layoffs_dataset_staging2` (
  `company` text DEFAULT NULL,
  `location` text DEFAULT NULL,
  `industry` text DEFAULT NULL,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text DEFAULT NULL,
  `date` text DEFAULT NULL,
  `stage` text DEFAULT NULL,
  `country` text DEFAULT NULL,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

SELECT *
FROM layoffs_dataset_staging2;

INSERT INTO layoffs_dataset_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
							  `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_dataset_staging ;

DELETE 
FROM layoffs_dataset_staging2
WHERE row_num > 1 ;

SELECT * 
FROM layoffs_dataset_staging2
WHERE row_num > 1 ;

SET SQL_SAFE_UPDATES = 0;

-- Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_dataset_staging2;

UPDATE layoffs_dataset_staging2
SET company = TRIM(company)
;

SELECT *
FROM layoffs_dataset_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_dataset_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_dataset_staging2
ORDER BY 1;

UPDATE layoffs_dataset_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%' ;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_dataset_staging2;

UPDATE layoffs_dataset_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

ALTER TABLE layoffs_dataset_staging2
MODIFY COLUMN `date` DATE;

-- Removing Null and Empty Values

SELECT *
FROM layoffs_dataset_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT DISTINCT *
FROM layoffs_dataset_staging2
WHERE industry IS NULL
OR industry = ''
;

SELECT DISTINCT *
FROM layoffs_dataset_staging2 AS t1
JOIN layoffs_dataset_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;

UPDATE layoffs_dataset_staging2
SET industry = NULL
WHERE industry = ''
; 

UPDATE layoffs_dataset_staging2 t1
JOIN layoffs_dataset_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL
;

SET SQL_SAFE_UPDATES = 0;

SELECT *
FROM layoffs_dataset_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE 
FROM layoffs_dataset_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

ALTER TABLE layoffs_dataset_staging2
DROP COLUMN row_num;