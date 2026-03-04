SELECT schemaname, tablename FROM pg_tables WHERE schemaname='fdw' ORDER BY tablename;
SELECT servername, srvtype FROM pg_foreign_server ORDER BY servername;
SELECT count(*) FROM risk.features WHERE feature_set_id=(SELECT id FROM risk.feature_sets WHERE code='buildings');
