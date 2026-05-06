-- Mocking the raw logistics events for the downstream SLA model
SELECT 
    'EVT-001' as event_id,
    'TRK-98765' as tracking_number,
    'PICKUP' as scan_type,
    CAST('2023-10-01 08:00:00' AS TIMESTAMP) as timestamp
UNION ALL
SELECT 
    'EVT-002' as event_id,
    'TRK-98765' as tracking_number,
    'DELIVERED' as scan_type,
    CAST('2023-10-03 14:00:00' AS TIMESTAMP) as timestamp