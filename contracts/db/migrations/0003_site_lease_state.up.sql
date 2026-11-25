CREATE TABLE jammy.site_lease_state (
    site_id text PRIMARY KEY,
    holder boolean NOT NULL,
    epoch bigint NOT NULL CHECK (epoch >= 0),
    fenced boolean NOT NULL,
    renewed_at timestamptz NOT NULL
);

ALTER TABLE jammy.instructions
    ADD COLUMN lease_epoch bigint CHECK (lease_epoch >= 0);
