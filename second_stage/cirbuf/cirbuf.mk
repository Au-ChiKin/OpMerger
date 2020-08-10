CB_OBJDIR=$(OBJDIR)/cirbuf
$(CB_OBJDIR): ; mkdir -p $@

CB_DEPDIR=$(DEPDIR)/cirbuf
$(CB_DEPDIR): ; mkdir -p $@

SRCS += cirbuf/circular_buffer.c

LIBDIR += $(CB_OBJDIR) $(CB_DEPDIR)