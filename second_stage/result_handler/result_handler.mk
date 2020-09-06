RE_OBJDIR=$(OBJDIR)/result_handler
$(RE_OBJDIR): ; mkdir -p $@

RE_DEPDIR=$(DEPDIR)/result_handler
$(RE_DEPDIR): ; mkdir -p $@

HANDLER = result_handler.c
HANDLER := $(foreach file,$(HANDLER),result_handler/$(file))
SRCS += $(HANDLER)

LIBDIR += $(RE_OBJDIR) $(RE_DEPDIR)
