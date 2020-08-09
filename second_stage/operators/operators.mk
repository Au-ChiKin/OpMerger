OP_OBJDIR=$(OBJDIR)/operators
$(OP_OBJDIR): ; mkdir -p $@

OP_DEPDIR=$(DEPDIR)/operators
$(OP_DEPDIR): ; mkdir -p $@

OPERATOR = aggregation.c reduction.c selection.c
OPERATOR := $(foreach file,$(OPERATOR),operators/$(file))
SRCS += $(OPERATOR)

LIBDIR += $(OP_OBJDIR) $(OP_DEPDIR)