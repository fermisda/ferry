

ALTER TABLE "public".allocations ADD piname text    ;

ALTER TABLE "public".allocations ADD email text    ;

ALTER TABLE "public".adjustments ADD last_updated timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL  ;

ALTER TABLE "public".allocations ADD last_updated timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL  ;
