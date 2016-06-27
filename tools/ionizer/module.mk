ionizer := $(brazil.pwd)

src := $(wildcard $(ionizer)/*.c)

CFLAGS += -I$(ionizer)
ionizer_app := $(call cc.app,$(src),ionizer)

# This module depends on libionc
$(ionizer_app): $(libionc)

$(ionizer_app): LIBS += $(if $(findstring $@,$(ionizer_app)),-lionc)
