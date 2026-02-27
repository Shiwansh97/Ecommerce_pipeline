SELECT
    Department,
    COUNT(*) AS Total_Employees,
    SUM(Attrition_Flag) AS Employees_Left,
    ROUND(SUM(Attrition_Flag)*100.0/COUNT(*),2) AS Attrition_Rate
FROM {{ ref('clean_employee') }}
GROUP BY Department