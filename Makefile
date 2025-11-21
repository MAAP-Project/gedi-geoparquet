.DEFAULT_GOAL := schema

SCHEMA_DIR ?= src/gedi_geoparquet/schema/resources
GRANULE_ID ?= 2025050224403_O35067_01_T03238
OUTPUT_DIR ?= data

GEDI_L2A_PREFIX := https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/GEDI02_A.002
GEDI_L2B_PREFIX := https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/GEDI02_B.002
GEDI_L4A_PREFIX := https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4A_AGB_Density_V2_1/data
GEDI_L4C_PREFIX := https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4C_WSCI/data

GEDI_L2A_HDF5_URL := $(GEDI_L2A_PREFIX)/GEDI02_A_$(GRANULE_ID)_02_004_02_V002/GEDI02_A_$(GRANULE_ID)_02_004_02_V002.h5
GEDI_L2B_HDF5_URL := $(GEDI_L2B_PREFIX)/GEDI02_B_$(GRANULE_ID)_02_004_01_V002/GEDI02_B_$(GRANULE_ID)_02_004_01_V002.h5
GEDI_L4A_HDF5_URL := $(GEDI_L4A_PREFIX)/GEDI04_A_$(GRANULE_ID)_02_004_01_V002.h5
GEDI_L4C_HDF5_URL := $(GEDI_L4C_PREFIX)/GEDI04_C_$(GRANULE_ID)_02_001_01_V002.h5

GEDI_L2A_SCHEMA := $(SCHEMA_DIR)/gedi_l2a.arrows
GEDI_L2B_SCHEMA := $(SCHEMA_DIR)/gedi_l2b.arrows
GEDI_L4A_SCHEMA := $(SCHEMA_DIR)/gedi_l4a.arrows
GEDI_L4C_SCHEMA := $(SCHEMA_DIR)/gedi_l4c.arrows

SCHEMA_FILES := $(GEDI_L2A_SCHEMA) $(GEDI_L2B_SCHEMA) $(GEDI_L4A_SCHEMA) $(GEDI_L4C_SCHEMA)

GEDI_L2A_PARQUET := $(OUTPUT_DIR)/GEDI02_A_$(GRANULE_ID)_02_004_02_V002.parquet
GEDI_L2B_PARQUET := $(OUTPUT_DIR)/GEDI02_B_$(GRANULE_ID)_02_004_01_V002.parquet
GEDI_L4A_PARQUET := $(OUTPUT_DIR)/GEDI04_A_$(GRANULE_ID)_02_004_01_V002.parquet
GEDI_L4C_PARQUET := $(OUTPUT_DIR)/GEDI04_C_$(GRANULE_ID)_02_001_01_V002.parquet

PARQUET_FILES := $(GEDI_L2A_PARQUET) $(GEDI_L2B_PARQUET) $(GEDI_L4A_PARQUET) $(GEDI_L4C_PARQUET)

.PHONY: schema
schema: $(SCHEMA_FILES)
SCHEMA_SOURCES := scripts/generate_schema.py src/gedi_geoparquet/pyarrow.py

$(GEDI_L2A_SCHEMA): $(SCHEMA_SOURCES)
	uv run scripts/generate_schema.py $(GEDI_L2A_HDF5_URL) "$@"

$(GEDI_L2B_SCHEMA): $(SCHEMA_SOURCES)
	uv run scripts/generate_schema.py $(GEDI_L2B_HDF5_URL) "$@"

$(GEDI_L4A_SCHEMA): $(SCHEMA_SOURCES)
	uv run scripts/generate_schema.py $(GEDI_L4A_HDF5_URL) "$@"

$(GEDI_L4C_SCHEMA): $(SCHEMA_SOURCES)
	uv run scripts/generate_schema.py $(GEDI_L4C_HDF5_URL) "$@"

.PHONY: convert
convert: $(PARQUET_FILES)
CONVERT_SOURCES := scripts/convert.py src/gedi_geoparquet/schema/**/* src/gedi_geoparquet/hdf5.py

$(GEDI_L2A_PARQUET): $(GEDI_L2A_SCHEMA) $(CONVERT_SOURCES)
	uv run scripts/convert.py $(GEDI_L2A_HDF5_URL) $(OUTPUT_DIR)

$(GEDI_L2B_PARQUET): $(GEDI_L2B_SCHEMA) $(CONVERT_SOURCES)
	uv run scripts/convert.py $(GEDI_L2B_HDF5_URL) $(OUTPUT_DIR)

$(GEDI_L4A_PARQUET): $(GEDI_L4A_SCHEMA) $(CONVERT_SOURCES)
	uv run scripts/convert.py $(GEDI_L4A_HDF5_URL) $(OUTPUT_DIR)

$(GEDI_L4C_PARQUET): $(GEDI_L4C_SCHEMA) $(CONVERT_SOURCES)
	uv run scripts/convert.py $(GEDI_L4C_HDF5_URL) $(OUTPUT_DIR)

.PHONY: join
join: $(OUTPUT_DIR)/GEDI_$(GRANULE_ID).parquet

$(OUTPUT_DIR)/GEDI_$(GRANULE_ID).parquet: scripts/join.py $(PARQUET_FILES)
	uv run scripts/join.py $(OUTPUT_DIR)/GEDI0*$(GRANULE_ID)* "$@"

.PHONY: clean
clean:
	rm -f $(SCHEMA_DIR)/gedi_l*.arrows
	rm -f data/*$(GRANULE_ID)*.parquet

.PHONY: test
test:
	uv run pytest
