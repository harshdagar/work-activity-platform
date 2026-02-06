select
  current_timestamp() as ts,
  current_user() as user,
  current_role() as role,
  current_database() as db,
  current_schema() as schema
