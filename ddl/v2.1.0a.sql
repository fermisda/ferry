
-- Squeezing this into the v2.1.0 release which is not yet in production.

ALTER TABLE "public".capability_sets ADD token_subject text   ;

ALTER TABLE "public".capability_sets ADD vault_storage_key text   ;
