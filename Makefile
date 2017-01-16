TOP_DIR = ../..
DEPLOY_RUNTIME ?= /disks/patric-common/runtime
TARGET ?= /tmp/deployment
include $(TOP_DIR)/tools/Makefile.common

SERVICE_SPEC = Minhash.spec
SERVICE_NAME = Minhash
SERVICE_PORT = 7138
SERVICE_DIR  = $(SERVICE_NAME)
SERVICE_URL = http://localhost:$(SERVICE_PORT)

PATH := $(DEPLOY_RUNTIME)/build-tools/bin:$(PATH)

SERVICE_PSGI = $(SERVICE_NAME).psgi
SERVICE_PSGI_FILE = $(SERVICE_NAME).psgi

TPAGE_ARGS = --define kb_runas_user=$(SERVICE_USER) \
	--define kb_top=$(TARGET) \
	--define kb_runtime=$(DEPLOY_RUNTIME) \
	--define kb_service_name=$(SERVICE_NAME) \
	--define kb_service_dir=$(SERVICE_DIR) \
	--define kb_service_port=$(SERVICE_PORT) \
	--define kb_psgi=$(SERVICE_PSGI) \
	--define kb_app_dir=$(SERVICE_APP_DIR) \
	--define kb_app_script=$(APP_SCRIPT) 

# to wrap scripts and deploy them to $(TARGET)/bin using tools in
# the dev_container. right now, these vars are defined in
# Makefile.common, so it's redundant here.
TOOLS_DIR = $(TOP_DIR)/tools
WRAP_PERL_TOOL = wrap_perl
WRAP_PERL_SCRIPT = bash $(TOOLS_DIR)/$(WRAP_PERL_TOOL).sh
SRC_PERL = $(wildcard scripts/*.pl)

default: bin compile-typespec 

bin: $(BIN_PERL)

dist: 

test: 

deploy: deploy-client deploy-service

deploy-all: deploy-client deploy-service

deploy-client: 

deploy-scripts:
	export KB_TOP=$(TARGET); \
	export KB_RUNTIME=$(DEPLOY_RUNTIME); \
	export KB_PERL_PATH=$(TARGET)/lib bash ; \
	for src in $(SRC_PERL) ; do \
		basefile=`basename $$src`; \
		base=`basename $$src .pl`; \
		echo install $$src $$base ; \
		cp $$src $(TARGET)/plbin ; \
		$(WRAP_PERL_SCRIPT) "$(TARGET)/plbin/$$basefile" $(TARGET)/bin/$$base ; \
	done

deploy-service: deploy-run-scripts deploy-libs

deploy-run-scripts:
	mkdir -p $(TARGET)/services/$(SERVICE_DIR)
	$(TPAGE) $(TPAGE_ARGS) service/start_service.tt > $(TARGET)/services/$(SERVICE_DIR)/start_service
	chmod +x $(TARGET)/services/$(SERVICE_DIR)/start_service
	$(TPAGE) $(TPAGE_ARGS) service/stop_service.tt > $(TARGET)/services/$(SERVICE_DIR)/stop_service
	chmod +x $(TARGET)/services/$(SERVICE_DIR)/stop_service
	if [ -f service/upstart.tt ] ; then \
		$(TPAGE) $(TPAGE_ARGS) service/upstart.tt > service/$(SERVICE_NAME).conf; \
	fi
	echo "done executing deploy-service target"

deploy-upstart: deploy-service
	-cp service/$(SERVICE_NAME).conf /etc/init/
	echo "done executing deploy-upstart target"

deploy-cfg:

deploy-docs:
	-mkdir -p $(TARGET)/services/$(SERVICE_DIR)/webroot/.
	cp docs/*.html $(TARGET)/services/$(SERVICE_DIR)/webroot/.


build-libs:

compile-typespec: Makefile
	mkdir -p lib/biop3/$(SERVICE_NAME_PY)
	touch lib/biop3/__init__.py #do not include code in biop3/__init__.py
	touch lib/biop3/$(SERVICE_NAME_PY)/__init__.py 
	mkdir -p lib/javascript/$(SERVICE_NAME)
	compile_typespec \
		--psgi $(SERVICE_PSGI) \
		--impl Bio::P3::$(SERVICE_NAME)::%sImpl \
		--service Bio::P3::$(SERVICE_NAME)::Service \
		--client Bio::P3::$(SERVICE_NAME)::Client \
		--py biop3/$(SERVICE_NAME_PY)/client \
		--js javascript/$(SERVICE_NAME)/Client \
		--url $(SERVICE_URL) \
		--enable-retries \
		$(SERVICE_SPEC) lib



include $(TOP_DIR)/tools/Makefile.common.rules
