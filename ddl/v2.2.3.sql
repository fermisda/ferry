
ALTER TABLE "public".users ADD is_banned boolean DEFAULT false NOT NULL  ;

COMMENT ON COLUMN "public".grid_access.is_banned IS 'Bans a user from using the associated FQAN.';

COMMENT ON COLUMN "public".users.is_banned IS 'Bans a user, completely, from FERRY while leaving the records intact.';
