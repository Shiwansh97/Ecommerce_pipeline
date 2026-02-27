SELECT
    EmpID,
    Age,
    Department,
    JobRole,
    MonthlyIncome,
    Attrition,
    CASE 
        WHEN Attrition = 'Yes' THEN 1 
        ELSE 0 
    END AS Attrition_Flag
FROM ecommerce_db.RAW.EMPLOYEE_ATTRITION