-- Add worker_id to pool_snapshots for multi-worker mode (one row per worker per key).
-- Run if 001 was applied before worker_id was added.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'pgpooler' AND table_name = 'pool_snapshots' AND column_name = 'worker_id'
  ) THEN
    ALTER TABLE pgpooler.pool_snapshots ADD COLUMN worker_id SMALLINT;
  END IF;
END $$;
