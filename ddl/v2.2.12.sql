
-- Written years ago or so for Tanya.  She stated
-- the users when another direction and this  has
-- never been used.  I am removing the code and table.

drop table compute_resource_shared_account;

ALTER TABLE "public".users drop column is_sharedaccount;
