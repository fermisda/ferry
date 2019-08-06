rm test_*;(source dbpassPRD; pg_dump -c --host cdpgsprd.fnal.gov -p 5436 -U ferry_admin -d ferry_prd -w > ferry-prd-db_NEW.sql)
