-- CTE to isolate the initial pickup and final delivery times
WITH scan_milestones AS (
    SELECT 
        tracking_number,
        MIN(CASE WHEN scan_type = 'PICKUP' THEN timestamp END) as pickup_time,
        MAX(CASE WHEN scan_type = 'DELIVERED' THEN timestamp END) as delivery_time
    FROM {{ ref('stg_logistics') }}
    GROUP BY tracking_number
),

-- CTE to calculate delivery duration and rolling averages using Window Functions
delivery_metrics AS (
    SELECT 
        tracking_number,
        pickup_time,
        delivery_time,
        EXTRACT(EPOCH FROM (delivery_time - pickup_time))/3600 AS transit_hours,
        AVG(EXTRACT(EPOCH FROM (delivery_time - pickup_time))/3600) OVER (
            ORDER BY delivery_time 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7_delivery_avg_hours
    FROM scan_milestones
    WHERE delivery_time IS NOT NULL
)

-- Final Output: Flag SLA breaches (assuming a 48-hour SLA)
SELECT 
    tracking_number,
    pickup_time,
    delivery_time,
    transit_hours,
    rolling_7_delivery_avg_hours,
    CASE 
        WHEN transit_hours > 48 THEN TRUE 
        ELSE FALSE 
    END as is_sla_breached
FROM delivery_metrics