-- +goose Up
ALTER TABLE upkeep_registrations
    ADD COLUMN last_keeper_index integer DEFAULT NULL;

-- +goose Down
ALTER TABLE upkeep_registrations
    DROP COLUMN last_keeper_index;
