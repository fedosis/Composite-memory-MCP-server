-- Frozen original git HEAD 739705c9f4bfbc7c9251027fc36e8f06ab94d8c5
-- Original migrations upgrade 70e6afc8d15d; B2 additionally requires runtime claim_relations.
CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL, 
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
CREATE TABLE decisions (
	id VARCHAR NOT NULL, 
	context VARCHAR NOT NULL, 
	choice VARCHAR NOT NULL, 
	rejected_alternatives TEXT NOT NULL, 
	reason VARCHAR NOT NULL, 
	confidence FLOAT NOT NULL, 
	source VARCHAR, 
	creator VARCHAR NOT NULL, 
	created_at VARCHAR NOT NULL, 
	updated_at VARCHAR NOT NULL, 
	verification_status VARCHAR NOT NULL, 
	lifecycle_state VARCHAR NOT NULL, 
	version VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE TABLE entities (
	id VARCHAR NOT NULL, 
	type VARCHAR NOT NULL, 
	name VARCHAR NOT NULL, 
	attributes TEXT NOT NULL, 
	source VARCHAR, 
	creator VARCHAR NOT NULL, 
	created_at VARCHAR NOT NULL, 
	updated_at VARCHAR NOT NULL, 
	confidence FLOAT NOT NULL, 
	verification_status VARCHAR NOT NULL, 
	lifecycle_state VARCHAR NOT NULL, 
	version VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE TABLE facts (
	id VARCHAR NOT NULL, 
	subject VARCHAR NOT NULL, 
	predicate VARCHAR NOT NULL, 
	object VARCHAR NOT NULL, 
	confidence FLOAT NOT NULL, 
	source VARCHAR, 
	creator VARCHAR NOT NULL, 
	created_at VARCHAR NOT NULL, 
	updated_at VARCHAR NOT NULL, 
	verification_status VARCHAR NOT NULL, 
	lifecycle_state VARCHAR NOT NULL, 
	version VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE TABLE lifecycle_events (
	id VARCHAR NOT NULL, 
	memory_id VARCHAR NOT NULL, 
	memory_type VARCHAR NOT NULL, 
	from_state VARCHAR NOT NULL, 
	to_state VARCHAR NOT NULL, 
	reason VARCHAR NOT NULL, 
	triggered_by VARCHAR NOT NULL, 
	timestamp VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE TABLE lifecycle_states (
	id VARCHAR NOT NULL, 
	memory_id VARCHAR NOT NULL, 
	memory_type VARCHAR NOT NULL, 
	current_state VARCHAR NOT NULL, 
	previous_state VARCHAR, 
	confidence FLOAT NOT NULL, 
	updated_at VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE TABLE receipts (
	id VARCHAR NOT NULL, 
	memory_type VARCHAR NOT NULL, 
	source VARCHAR NOT NULL, 
	created_by VARCHAR NOT NULL, 
	timestamp VARCHAR NOT NULL, 
	confidence FLOAT NOT NULL, 
	verification_status VARCHAR NOT NULL, 
	history TEXT NOT NULL, 
	updated_at VARCHAR NOT NULL, 
	lifecycle_state VARCHAR NOT NULL, 
	version VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE TABLE skills (
	id VARCHAR NOT NULL, 
	name VARCHAR NOT NULL, 
	version VARCHAR NOT NULL, 
	purpose VARCHAR NOT NULL, 
	steps TEXT NOT NULL, 
	constraints TEXT NOT NULL, 
	validation TEXT NOT NULL, 
	success_rate FLOAT NOT NULL, 
	source VARCHAR, 
	creator VARCHAR NOT NULL, 
	created_at VARCHAR NOT NULL, 
	updated_at VARCHAR NOT NULL, 
	confidence FLOAT NOT NULL, 
	verification_status VARCHAR NOT NULL, 
	lifecycle_state VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE INDEX ix_facts_predicate ON facts (predicate);
CREATE INDEX ix_facts_subject ON facts (subject);
CREATE INDEX ix_lifecycle_events_memory_id ON lifecycle_events (memory_id);
CREATE INDEX ix_lifecycle_states_memory_id ON lifecycle_states (memory_id);
CREATE INDEX ix_receipts_memory_type ON receipts (memory_type);
CREATE INDEX ix_receipts_source ON receipts (source);
INSERT INTO alembic_version VALUES ('70e6afc8d15d');
