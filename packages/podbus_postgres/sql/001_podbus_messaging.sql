-- Reference schema. PostgresMessagingSchema.install() applies the same objects.
CREATE TABLE IF NOT EXISTS podbus_outbox (
  id text PRIMARY KEY,
  topic text NOT NULL,
  message_key text,
  payload bytea NOT NULL,
  content_type text NOT NULL,
  schema_version integer NOT NULL CHECK (schema_version > 0),
  message_type text,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'processing', 'failed', 'published', 'dead')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_by text,
  locked_until timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE TABLE IF NOT EXISTS podbus_inbox (
  message_id text PRIMARY KEY,
  state text NOT NULL CHECK (state IN ('processing', 'completed', 'failed')),
  attempts integer NOT NULL DEFAULT 1 CHECK (attempts > 0),
  locked_by text NOT NULL,
  locked_until timestamptz NOT NULL,
  last_error text,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS podbus_idempotency (
  idempotency_key text PRIMARY KEY,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
