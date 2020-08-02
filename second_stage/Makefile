# Compiler Configurations
CC        = gcc
CFLAGS    = -Wall -g
UNAME := $(shell uname -s)
LDLIBS    =
DEPFLAGS  = -MT $@ -MMD -MP -MF $(DEPDIR)/$*.d
COMPILE.c = $(CC) $(DEPFLAGS) $(CFLAGS) $(CPPFLAGS) $(TARGET_ARCH) $(LDLIBS) -c
LINK.o    = $(CC) $(CFLAGS)
ifeq ($(UNAME),Linux)
	CFLAGS += -I/usr/local/cuda/include
	LDLIBS += -lOpenCL
else
ifeq ($(UNAME),Darwin)
	LDLIBS += -framework OpenCL
endif
endif

# Environmrnt
OBJDIR  := obj
DEPDIR  := $(OBJDIR)/.deps
VPATH    = $(OBJDIR)

# Targets
PROGS   = test_gcd
GPU_LIB = gpu_agg.c gpu_config.c gpu_query.c gpu_input_buffer.c gpu_output_buffer.c
OPERATOR = aggregation.c reduction.c selection.c
SRCS = test_gcd.c openclerrorcode.c config.c helpers.c $(GPU_LIB) $(OPERATOR)

# Submodules
include ./libcirbuf/cirbuf.mk

OBJECTS = $(patsubst %.c,%.o,$(SRCS))

# Scripts
$(DEPDIR): ; mkdir -p $@
$(OBJDIR): ; mkdir -p $@

# Special case
obj/openclerrorcode.o : ../clib/openclerrorcode.c obj/.deps/openclerrorcode.d | $(DEPDIR)
		$(CC) -MT $@ -MMD -MP -MF obj/.deps/openclerrorcode.d $(CFLAGS) $(CPPFLAGS) $(TARGET_ARCH) -c \
		$(OUTPUT_OPTION) $<


# Compile objects with auto dependency
$(OBJDIR)/%.o : %.c $(DEPDIR)/%.d | $(DEPDIR)
		$(COMPILE.c) $(OUTPUT_OPTION) $<

DEPFILES := $(SRCS:%.c=$(DEPDIR)/%.d)
$(DEPFILES):
include $(wildcard $(DEPFILES))


# Compile the main program
OBJPROG = $(addprefix $(OBJDIR)/, $(PROGS))

all: $(OUTDIR) $(DEPOUTDIR) $(OBJPROG)

$(OBJPROG): $(addprefix $(OBJDIR)/, $(OBJECTS)) 
		$(LINK.o) $^ $(LDLIBS) -o $@


# clean
.PHONY: clean
clean:
	rm -f $(OBJDIR)/*.o $(DEPDIR)/*.d $(OBJPROG) *~
