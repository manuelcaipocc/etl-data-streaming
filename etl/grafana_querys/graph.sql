SELECT
    "timestamp" AS time,   
    "browsename",          
    "value"                
FROM
    sandbox.ctrlx_data
WHERE
    "timestamp" >= NOW() - INTERVAL '10 seconds'  
    AND "browsename" IN ('V') 
ORDER BY
    "timestamp" DESC
LIMIT 10;
