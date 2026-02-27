SELECT
    Department,
    COUNT(*) AS total_employees,
    SUM(Attrition_Flag) AS employees_left,
    ROUND(SUM(Attrition_Flag)*100.0/COUNT(*),2) AS attrition_rate
FROM {{ ref('stg_employee') }}
GROUP BY Department