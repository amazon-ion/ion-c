tests := $(brazil.pwd)

test_src := $(wildcard $(tests)/*.c)

CFLAGS += -I$(tests)

# Use gcov flavor to prevent this from showing up in the normal bin directory.
tester_app := $(call cc.app,$(test_src),tester,gcov)

# This module depends on libionc
$(tester_app): $(libionc)

$(tester_app): LIBS += $(if $(findstring $@,$(tester_app)),-lionc)


#### Running tests

brazil.variables += {IonTests}pkg.src

test: $(tester_app)
	LD_LIBRARY_PATH=$(var.lib.lib) \
	$(tester_app) $(var.{IonTests}pkg.src)
