OUTDIR=$(OBJDIR)/libcirbuf
$(OUTDIR): ; mkdir -p $@

DEPOUTDIR=$(DEPDIR)/libcirbuf
$(DEPOUTDIR): ; mkdir -p $@

FORCE: $(OUTDIR) $(DEPOUTDIR)

SRCS += libcirbuf/circular_buffer.c
