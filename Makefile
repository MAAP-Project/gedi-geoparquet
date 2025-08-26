.DEFAULT_GOAL := all

CACHE_DIR ?= .cache
COOKIE_JAR ?= $(HOME)/.edl-cookies

# Arbitrary selection of (nearly) oldest granule from each GEDI collection
GEDI_L2A_HDF5 := $(CACHE_DIR)/GEDI02_A_2019108002012_O01959_01_T03909_02_003_01_V002.h5
GEDI_L2B_HDF5 := $(CACHE_DIR)/GEDI02_B_2019108002012_O01959_01_T03909_02_003_01_V002.h5
GEDI_L4A_HDF5 := $(CACHE_DIR)/GEDI04_A_2019108002012_O01959_01_T03909_02_002_02_V002.h5
GEDI_L4C_HDF5 := $(CACHE_DIR)/GEDI04_C_2019108002012_O01959_01_T03909_02_001_01_V002.h5

CURL_OPTS := --netrc --location --fail --silent --cookie-jar $(COOKIE_JAR) --cookie $(COOKIE_JAR)
LPDAAC_BASE_URL := https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected
ORNLDAAC_BASE_URL := https://data.ornldaac.earthdata.nasa.gov/protected/gedi

datasets := $(wildcard datasets/*.txt)
schemas := $(datasets:datasets/%.txt=schemas/%)

.PHONY: all
all: $(schemas)

schemas/GEDI_L2A: $(GEDI_L2A_HDF5) datasets/GEDI_L2A.txt scripts/generate_schema.py
	uv run scripts/generate_schema.py --datasets $(@:schemas/%=datasets/%).txt --output ${@}.arrows $<

schemas/GEDI_L2B: $(GEDI_L2B_HDF5) datasets/GEDI_L2B.txt scripts/generate_schema.py
	uv run scripts/generate_schema.py --datasets $(@:schemas/%=datasets/%).txt --output ${@}.arrows $<

schemas/GEDI_L4A: $(GEDI_L4A_HDF5) datasets/GEDI_L4A.txt scripts/generate_schema.py
	uv run scripts/generate_schema.py --datasets $(@:schemas/%=datasets/%).txt --output ${@}.arrows $<

schemas/GEDI_L4C: $(GEDI_L4C_HDF5) datasets/GEDI_L4C.txt scripts/generate_schema.py
	uv run scripts/generate_schema.py --datasets $(@:schemas/%=datasets/%).txt --output ${@}.arrows $<

$(GEDI_L2A_HDF5):
	curl $(CURL_OPTS) --output $@ $(LPDAAC_BASE_URL)/GEDI02_A.002/$(shell basename $@ .h5)/$(shell basename $@)

$(GEDI_L2B_HDF5):
	curl $(CURL_OPTS) --output $@ $(LPDAAC_BASE_URL)/GEDI02_B.002/$(shell basename $@ .h5)/$(shell basename $@)

$(GEDI_L4A_HDF5):
	curl $(CURL_OPTS) --output $@ $(ORNLDAAC_BASE_URL)/GEDI_L4A_AGB_Density_V2_1/data/$(shell basename $@)

$(GEDI_L4C_HDF5):
	curl $(CURL_OPTS) --output $@ $(ORNLDAAC_BASE_URL)/GEDI_L4C_WSCI/data/$(shell basename $@)

.PHONY: clean
clean:
	rm $(CACHE_DIR)/*.h5
