-- Exploratory Data Analysis

SELECT SUBSTRING(`date`, 1, 7 ) AS `Month` , SUM(total_laid_off) AS total_off
FROM layoffs_dataset_staging2
WHERE SUBSTRING(`date`, 1, 7 ) IS NOT NULL
GROUP BY `Month`
ORDER BY 2
;

WITH Rolling_Total AS
( SELECT SUBSTRING(`date`, 1, 7 ) AS `Month` , SUM(total_laid_off) AS total_off
FROM layoffs_dataset_staging2
WHERE SUBSTRING(`date`, 1, 7 ) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 )
SELECT `Month`, total_off,
SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_Total
;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_dataset_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
;

WITH Company_Year (Company, `Year`, Total_laid_off) AS
( SELECT company, YEAR(`date`), total_laid_off
  FROM layoffs_dataset_staging2
  GROUP BY company, YEAR(`date`)
  ORDER BY 3 DESC ) , Company_Year_Ranking AS
( SELECT *, 
DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY Total_laid_off DESC) AS `Rank`
FROM Company_Year
WHERE `Year` IS NOT NULL )
SELECT *
FROM Company_Year_Ranking
WHERE `Rank` <= 5
;

SELECT *
FROM layoffs_dataset_staging2;

