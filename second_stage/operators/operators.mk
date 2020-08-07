OUTDIR=$(OBJDIR)/operators
$(OUTDIR): ; mkdir -p $@

DEPOUTDIR=$(DEPDIR)/operators
$(DEPOUTDIR): ; mkdir -p $@

FORCE: $(OUTDIR) $(DEPOUTDIR)

OPERATOR = aggregation.c reduction.c selection.c
OPERATOR := $(foreach file,$(OPERATOR),operators/$(file))
SRCS += $(OPERATOR)

FORCE: $(echo $(OPERATOR))
