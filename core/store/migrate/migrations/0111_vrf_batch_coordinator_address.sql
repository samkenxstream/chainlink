-- +goose Up
ALTER TABLE vrf_specs ADD COLUMN batch_coordinator_address bytea, 
	ADD COLUMN batch_fulfillment_enabled bool NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE vrf_specs DROP COLUMN batch_coordinator_address, 
	DROP COLUMN batch_fulfillment_enabled;
