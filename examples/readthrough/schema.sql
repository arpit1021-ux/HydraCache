CREATE TABLE IF NOT EXISTS products (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    price_cents BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (name, price_cents) VALUES
    ('HydraCache Sticker Pack', 500),
    ('Distributed Systems Mug', 1500),
    ('Consistent Hashing Poster', 2000)
ON CONFLICT DO NOTHING;
