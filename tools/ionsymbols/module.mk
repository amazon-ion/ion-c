ionsymbols := $(brazil.pwd)

src := $(wildcard $(ionsymbols)/*.c)

CFLAGS += -I$(ionsymbols)
ionsymbols_app := $(call cc.app,$(src),ionsymbols)

# This module depends on libionc
$(ionsymbols_app): $(libionc)

$(ionsymbols_app): LIBS += $(if $(findstring $@,$(ionsymbols_app)),-lionc)
