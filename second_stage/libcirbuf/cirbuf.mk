CB_OBJDIR=$(OBJDIR)/libcirbuf
$(CB_OBJDIR): ; mkdir -p $@

CB_DEPDIR=$(DEPDIR)/libcirbuf
$(CB_DEPDIR): ; mkdir -p $@

SRCS += libcirbuf/circular_buffer.c

LIBDIR += $(CB_OBJDIR) $(CB_DEPDIR)