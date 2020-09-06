DI_OBJDIR=$(OBJDIR)/dispatcher
$(DI_OBJDIR): ; mkdir -p $@

DI_DEPDIR=$(DEPDIR)/dispatcher
$(DI_DEPDIR): ; mkdir -p $@

DISPATCHER = dispatcher.c
DISPATCHER := $(foreach file,$(DISPATCHER),dispatcher/$(file))
SRCS += $(DISPATCHER)

LIBDIR += $(DI_OBJDIR) $(DI_DEPDIR)