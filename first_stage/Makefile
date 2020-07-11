# Compiler Configurations
CC        = gcc
CFLAGS    = -Wall -g
LDLIBS    = -framework OpenCL
DEPFLAGS  = -MT $@ -MMD -MP -MF $(DEPDIR)/$*.d
COMPILE.c = $(CC) $(DEPFLAGS) $(CFLAGS) $(CPPFLAGS) $(TARGET_ARCH) -c
LINK.o    = $(CC) $(CFLAGS)

# Environmrnt
OBJDIR  := obj
DEPDIR  := $(OBJDIR)/.deps
VPATH    = $(OBJDIR)

# Targets
PROGS   = test_merged_filters # test_separate_filters
SRCS = gpu.c gpu_config.c test_separate_filters.c openclerrorcode.c
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

all: $(OBJPROG)

$(OBJPROG): $(addprefix $(OBJDIR)/, $(OBJECTS))
		$(LINK.o) $^ $(LDLIBS) -o $@


# clean
.PHONY: clean
clean:
	rm -f $(OBJDIR)/*.o $(DEPDIR)/*.d $(TARGET) *~