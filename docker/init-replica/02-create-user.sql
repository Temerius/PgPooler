CREATE USER reporting_reader WITH PASSWORD 'rreader';
GRANT CONNECT ON DATABASE reporting TO reporting_reader;
GRANT CONNECT ON DATABASE postgres TO reporting_reader;
