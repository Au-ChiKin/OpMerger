MO_OBJDIR=$(OBJDIR)/monitor
$(MO_OBJDIR): ; mkdir -p $@

MO_DEPDIR=$(DEPDIR)/monitor
$(MO_DEPDIR): ; mkdir -p $@

MONITOR = monitor.c event_manager.c
MONITOR := $(foreach file,$(MONITOR),monitor/$(file))
SRCS += $(MONITOR)

LIBDIR += $(MO_OBJDIR) $(MO_DEPDIR)