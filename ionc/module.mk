ionc := $(brazil.pwd)

######################################################################
# The IonC core library:

src := $(wildcard $(ionc)/*.c)

CFLAGS += -I$(ionc) -I$(ionc)/inc -fvisibility=hidden
libionc := $(call cc.lib,$(src),ionc)
