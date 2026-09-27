
-- View columns of tables

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'risk_rule';

SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'flagged_order';

-- Check FKs

SELECT
    tc.constraint_name,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_name = 'flagged_order';


SELECT conname, conrelid::regclass, confrelid::regclass
FROM pg_constraint
WHERE contype = 'f';

--