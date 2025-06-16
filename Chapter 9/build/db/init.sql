CREATE SCHEMA IF NOT EXISTS eda;

CREATE TABLE IF NOT EXISTS eda.processed_events
(
	event_id varchar NOT NULL,
	event_name varchar NOT NULL,
	"timestamp" timestamp NULL,
	order_id varchar NOT NULL
);

CREATE TABLE IF NOT EXISTS eda.delivery (
	event_id varchar NOT NULL,
	event_name varchar NOT NULL,
	order_id varchar NOT NULL,
	delivery_data varchar NOT NULL
);
CREATE TABLE IF NOT EXISTS eda.food (
	event_id varchar NOT NULL,
	event_name varchar NOT NULL,
	order_id varchar NOT NULL,
	food_data varchar NULL
);
CREATE TABLE IF NOT EXISTS eda.orders (
	event_id varchar NOT NULL,
	event_name varchar NOT NULL,
	order_id varchar NOT NULL,
	event_data varchar NOT NULL
);
commit();