-- Create mycel_vera_products login user
-- Runs as: mycel_app_db_admin
-- Phase 1, Step 2

-- Module login user
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mycel_vera_products_db_user') THEN
    CREATE ROLE mycel_vera_products_db_user LOGIN PASSWORD 'mycel_vera_products_dev_password';
  END IF;
END $$;

-- Grant owner role (full access to own schema)
GRANT mycel_vera_products_role TO mycel_vera_products_db_user;
