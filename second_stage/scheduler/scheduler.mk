SC_OBJDIR=$(OBJDIR)/scheduler
$(SC_OBJDIR): ; mkdir -p $@

SC_DEPDIR=$(DEPDIR)/scheduler
$(SC_DEPDIR): ; mkdir -p $@

SCHEDULER = scheduler.c
SCHEDULER := $(foreach file,$(SCHEDULER),scheduler/$(file))
SRCS += $(SCHEDULER)

LIBDIR += $(SC_OBJDIR) $(SC_DEPDIR)